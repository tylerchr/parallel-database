package main

import (
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/tylerchr/parallel-database/query"
)

type DatabaseRPC struct {
	DB        *Database
	Hosts     []string
	startedAt time.Time
}

func (db *DatabaseRPC) Uptime(_ bool, up_time *time.Duration) error {
	*up_time = time.Since(db.startedAt)
	return nil
}

func (db *DatabaseRPC) Execute(q query.Query, accs *[]string) error {

	numHosts := len(db.Hosts)
	if q.Hosts > 0 && q.Hosts < numHosts {
		numHosts = q.Hosts
	}

	includedHosts := db.Hosts[:numHosts]
	fmt.Printf("Running a query across %d hosts %s\n", numHosts, includedHosts)

	hostAcc := make(chan []Accumulator)

	for idx, host := range includedHosts {

		// carve out a subrange
		start := idx * (0xFF / numHosts)
		end := start + (0xFF / numHosts)

		if idx+1 < numHosts {
			end--
		} else {
			end = 0xFF
		}

		go func(host string, start, end byte) {

			if client, err := rpc.Dial("tcp", host); err != nil {
				panic(fmt.Errorf("Failed to connect to server: %s", host))
			} else {

				// run query range
				rangedQuery := query.RangedQuery{
					Query: q,
					Start: byte(start),
					End:   byte(end),
				}

				var boxedAccs []BoxedAccumulator
				if err := client.Call("DatabaseRPC.ExecuteRange", rangedQuery, &boxedAccs); err != nil {
					fmt.Printf("[error] %s\n", err)
					// return fmt.Errorf("[error] %s", err)
					hostAcc <- []Accumulator{}
				} else {
					hostAcc <- unboxAccumulators(boxedAccs)
				}
			}

		}(host, byte(start), byte(end))

	}

	// collect and reduce all the accumulators together
	accumulators := make([][]Accumulator, len(includedHosts))
	for i := 0; i < len(includedHosts); i++ {
		accumulators[i] = <-hostAcc
	}
	reducedAccs := ReduceAccumulators(accumulators)

	// dump the results to the output
	*accs = make([]string, len(reducedAccs))
	for idx, a := range reducedAccs {
		(*accs)[idx] = fmt.Sprintf("%#v", a)
	}

	return nil
}

type BoxedAccumulator struct {
	AccType string
	Average *AverageAccumulator
	Count   *CountAccumulator
	Min     *MinAccumulator
	Max     *MaxAccumulator
}

func boxAccumulators(accs []Accumulator) []BoxedAccumulator {
	ba := make([]BoxedAccumulator, len(accs))
	for idx, acc := range accs {
		switch typedAcc := acc.(type) {
		case *AverageAccumulator:
			ba[idx] = BoxedAccumulator{AccType: "average", Average: typedAcc}
		case *CountAccumulator:
			ba[idx] = BoxedAccumulator{AccType: "count", Count: typedAcc}
		case *MinAccumulator:
			ba[idx] = BoxedAccumulator{AccType: "min", Min: typedAcc}
		case *MaxAccumulator:
			ba[idx] = BoxedAccumulator{AccType: "max", Max: typedAcc}
		default:
			fmt.Println("unknown!")
		}
	}
	return ba
}

func unboxAccumulators(accs []BoxedAccumulator) []Accumulator {
	uba := make([]Accumulator, len(accs))
	for idx, acc := range accs {
		switch acc.AccType {
		case "average":
			uba[idx] = acc.Average
		case "count":
			uba[idx] = acc.Count
		case "min":
			uba[idx] = acc.Min
		case "max":
			uba[idx] = acc.Max
		}
	}
	return uba
}

func (db *DatabaseRPC) ExecuteRange(q query.RangedQuery, accs *[]BoxedAccumulator) error {

	fmt.Printf("Executing ranged query [ 0x%X - 0x%X ]\n", q.Start, q.End)
	fmt.Printf(" > Metrics: %#v\n", q.Query.Metrics)
	fmt.Printf(" > Filters: %#v\n", q.Query.Filter)

	if results, err := db.DB.ExecuteRange(q.Query, q.Start, q.End); err != nil {
		return err
	} else {
		*accs = boxAccumulators(results)
	}

	return nil
}

func (db *DatabaseRPC) Fields(_ bool, fields *map[string]string) error {
	fmt.Printf("Responding with fields\n")
	*fields = db.DB.Fields()
	return nil
}

func (db *DatabaseRPC) Stats(_ bool, stats *bolt.BucketStats) error {
	fmt.Printf("Responding with stats\n")
	return db.DB.BoltDatabase.View(func(tx *bolt.Tx) error {
		*stats = tx.Bucket([]byte("songs")).Stats()
		return nil
	})
}

func (db *DatabaseRPC) Hostlist(_ bool, hosts *map[string]time.Duration) error {
	fmt.Printf("Responding with hosts\n")
	var host_durations map[string]time.Duration = make(map[string]time.Duration)

	for _, host := range db.Hosts {

		if client, err := rpc.Dial("tcp", host); err != nil {
			// error
		} else {
			var uptime time.Duration
			if err := client.Call("DatabaseRPC.Uptime", true, &uptime); err != nil {
				// error
			} else {
				host_durations[host] = uptime
			}
		}

	}
	*hosts = host_durations
	return nil
}

func main() {

	port := flag.Int("port", 6771, "the port to start the RPC server on")
	hostPortList := flag.String("hosts", ":6771", "comma-separated list of cluster ports")
	flag.Parse()

	fmt.Printf("Starting server on port: %d\n", *port)

	// open database
	database, _ := NewDatabase("data.db")
	defer database.Close()

	// figure out which hosts to include
	hosts := strings.Split(*hostPortList, ",")
	fmt.Printf("Started with %d hosts: %s\n", len(hosts), hosts)

	die := make(chan struct{})

	// start the RPC server
	go func() {
		rpc.Register(&DatabaseRPC{DB: database, Hosts: hosts, startedAt: time.Now()})
		l, e := net.Listen("tcp", fmt.Sprintf(":%d", *port))
		if e != nil {
			panic(e)
		}
		for {
			// fmt.Println("Waiting for connection...")
			conn, err := l.Accept()
			if err != nil {
				continue
			}
			go rpc.ServeConn(conn)
		}
		die <- struct{}{}
	}()

	<-die

}
