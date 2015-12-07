package main

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jaredririe/pseudo-terminal-go/terminal"
	"github.com/tylerchr/parallel-database/query/parser"
)

func main() {
	db, _ := NewDatabase("data.db")

	// query := query.Query{
	// 	Metrics: []query.QueryMetric{
	// 		query.QueryMetric{Column: "song_hotttnesss", Metric: "average"},
	// 	},
	// 	Filter: []query.QueryFilter{
	// 		// query.QueryFilter{Column: "title", Operator: "contains", Operand: "One"},
	// 		// query.QueryFilter{Column: "artist_location", Operator: "equals", Operand: "Detroit, MI"},
	// 	},
	// }
	// executeQuery(db, query)

	term, err := terminal.NewWithStdInOut()
	if err != nil {
		panic(err)
	}

	defer term.ReleaseFromStdInOut() // defer this
	fmt.Println("[ctrl-d or 'quit' to exit]")
	term.SetPrompt("⚡  ")
	line, err := term.ReadLine()
	for {
		if err == io.EOF {
			term.Write([]byte(line))
			fmt.Println()
			return
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
				fmt.Println("\tSELECT max(artist_familiarity) WHERE title contains \"One\" AND artist_hotttnesss > 0.5")

			case "quit":
				fmt.Println("exit")
				db.Close()
				return

			case "stats":
				fmt.Printf("%#v\n", db.BoltDatabase.Stats())

			case "history":
				for i, entry := range term.GetHistory() {
					fmt.Printf("% 3d %s\n", i, entry)
				}

			case "fields":
				var idx int32
				for field, fieldType := range db.Fields() {
					fmt.Printf("% 3d %-24s => %s\n", idx, field, fieldType)
					idx++
				}
			case "easter egg":
				fmt.Printf("🐇\n\r")

			default:
				if q, err := parser.ParseQuery(line); err != nil {
					fmt.Printf("[error] %v\n", err)
				} else {
					fmt.Printf("[parsed] Metrics: %v Filters: %v\n", q.Metrics, q.Filter)
					t0 := time.Now()

					db.Execute(q)
					// db.ExecuteRange(q, byte(0x00), byte(0xFF))

					fmt.Printf("took %v\n", time.Now().Sub(t0))
				}
			}

			line, err = term.ReadLine()
		}
	}
	db.Close()
	term.Write([]byte(line))
}
