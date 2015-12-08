package main

import (
	"flag"
	"fmt"
	"net"
	"net/rpc"

	"github.com/boltdb/bolt"
	"github.com/tylerchr/parallel-database/query"
)

type DatabaseRPC struct {
	DB *Database
}

func (db *DatabaseRPC) Execute(q query.Query, accs *[]string) error {

	fmt.Printf("Running a query: %#v\n", q)
	if results, err := db.DB.Execute(q); err != nil {
		return err
	} else {
		*accs = results
	}

	return nil
}

func (db *DatabaseRPC) Fields(_ bool, fields *map[string]string) error {
	fmt.Printf("Responding with fields\n")
	*fields = db.DB.Fields()
	return nil
}

func (db *DatabaseRPC) Stats(_ bool, stats *bolt.Stats) error {
	fmt.Printf("Responding with stats\n")
	*stats = db.DB.BoltDatabase.Stats()
	return nil
}

func main() {

	port := flag.Int("port", 6771, "the port to start the RPC server on")
	flag.Parse()
	fmt.Printf("Starting server on port: %d\n", *port)

	db, _ := NewDatabase("data.db")

	die := make(chan struct{})

	// start the RPC server
	go func() {
		rpc.Register(&DatabaseRPC{DB: db})
		l, e := net.Listen("tcp", fmt.Sprintf(":%d", *port))
		if e != nil {
			panic(e)
		}
		for {
			fmt.Println("Waiting for connection...")
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
