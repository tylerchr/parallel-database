package main

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/carmark/pseudo-terminal-go/terminal"
	"github.com/tylerchr/parallel-database/query/parser"
)

func main() {

	db, _ := NewDatabase("data.db")

	// start the timer
	// t0 := time.Now()

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
	fmt.Println("Ctrl-D to exit")
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
			//term.Write([]byte(line + "\r\n"))

			switch line {

			case "help":
				fmt.Println("sorry")

			case "quit":
				fmt.Println("Goodbye!")

			case "stats":
				fmt.Printf("%#v\n", db.BoltDatabase.Stats())

			case "fields":
				var idx int32
				for field, fieldType := range db.Fields() {
					fmt.Printf("% 3d %-24s => %s\n", idx, field, fieldType)
					idx++
				}

			default:
				if q, err := parser.ParseQuery(line); err != nil {
					fmt.Printf("[error] %v\n", err)
				} else {
					fmt.Printf("[parsed] %v\n", q)
					t0 := time.Now()
					db.Execute(q)
					fmt.Printf("took %v\n", time.Now().Sub(t0))
				}
			}

			line, err = term.ReadLine()
		}
	}
	db.Close()
	term.Write([]byte(line))
}
