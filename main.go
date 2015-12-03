package main

import (
	"fmt"
	"time"
	"bufio"
	"os"

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

	reader := bufio.NewReader(os.Stdin)

replLoop:
	for {

		fmt.Print("⚡  ")
		text, _ := reader.ReadString('\n')

		switch text {

		case "help\n":
			fmt.Println("sorry")

		case "quit\n":
			fmt.Println("Goodbye!")
			break replLoop

		case "stats\n":
			fmt.Printf("%#v\n", db.BoltDatabase.Stats())

		case "fields\n":
			var idx int32
			for field, fieldType := range db.Fields() {
				fmt.Printf("% 3d %-24s => %s\n", idx, field, fieldType)
				idx++
			}

		default:
			if q, err := parser.ParseQuery(text); err != nil {
				fmt.Printf("[error] %v\n", err)
			} else {
				fmt.Printf("[parsed] %v\n", q)
				t0 := time.Now()
				db.Execute(q)
				fmt.Printf("took %v\n", time.Now().Sub(t0))
			}

		}

	}

	db.Close()

}
