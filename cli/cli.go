package main

import (
	"flag"
	"fmt"
	"io"
	"net/rpc"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/jaredririe/pseudo-terminal-go/terminal"
	"github.com/tylerchr/parallel-database/query/parser"
)

func main() {

	hostname := flag.String("host", "127.0.0.1", "the server hostname")
	port := flag.Int("port", 6771, "the server port")
	flag.Parse()

	fmt.Printf("Starting server on port: %s:%d\n", *hostname, *port)

	term, err := terminal.NewWithStdInOut()
	if err != nil {
		panic(err)
	}

	defer term.ReleaseFromStdInOut() // defer this
	fmt.Println("[ctrl-d or 'quit' to exit]")
	term.SetPrompt("⚡  ")

	var line string

	for {

		line, err := term.ReadLine()

		if err == io.EOF {
			term.Write([]byte(line))
			fmt.Println()
			return
		}

		client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", *hostname, *port))
		if err != nil {
			panic(fmt.Errorf("Failed to connect to server: %s:%d", *hostname, *port))
		}

		if (err != nil && strings.Contains(err.Error(), "control-c break")) || len(line) == 0 {
			line, err = term.ReadLine()
		} else {
			switch line {

			case "help":
				fmt.Println("Commands you can try:")
				fmt.Println("\tfields\tshows fields in database")
				fmt.Println("\tstats\tdisplays database information")
				fmt.Println("\t<Query>\tsee below")
				fmt.Println("\thistory\tdisplays command history")
				fmt.Println("\tquit\texit\n")

				fmt.Println("Queries use an SQL-like syntax. Examples:")
				fmt.Println("\tSELECT avg(duration) WHERE title contains \"One\"")
				fmt.Println("\tSELECT max(artist_familiarity) WHERE title contains \"One\" AND artist_hotttnesss > 0.5\n")

				fmt.Println("Control the max hosts to distribute across with HOSTS. Examples:")
				fmt.Println("\tSELECT avg(duration) HOSTS 2")

			case "quit":
				fmt.Println("exit")
				return

			case "stats":

				var stats bolt.BucketStats
				if err := client.Call("DatabaseRPC.Stats", true, &stats); err != nil {
					fmt.Println("Failed to connect to database server")
				} else {
					fmt.Printf("%#v\n", stats)
				}

			case "history":
				for i, entry := range term.GetHistory() {
					fmt.Printf("% 3d %s\n", i, entry)
				}

			case "fields":
				var f map[string]string
				if err := client.Call("DatabaseRPC.Fields", true, &f); err != nil {
					fmt.Println("Failed to connect to database server")
				} else {
					var idx int32
					for field, fieldType := range f {
						fmt.Printf("% 3d %-24s => %s\n", idx, field, fieldType)
						idx++
					}
				}
			case "easter egg":
				fmt.Printf("🐇\n\r")

			default:

				if q, err := parser.ParseQuery(line); err != nil {
					fmt.Printf("[error] %v\n", err)
				} else {
					// fmt.Printf("[parsed] Metrics: %v Filters: %v\n", q.Metrics, q.Filter)
					t0 := time.Now()

					var logs []string
					if err := client.Call("DatabaseRPC.Execute", q, &logs); err != nil {
						fmt.Printf("  > ERR  %s\n", err.Error())
					} else {
						for idx, log := range logs {
							fmt.Printf("  > % 3d  %s\n", idx, log)
						}
					}

					fmt.Printf("         [took %v]\n", time.Now().Sub(t0))
				}
			}
		}
	}

	term.Write([]byte(line))
}
