package main

import (
	"fmt"
	"bytes"
	"time"
	"reflect"

	"github.com/boltdb/bolt"
	"github.com/tylerchr/parallel-database/query"
)

func main() {
	doWork()
}

func doWork() {

	db, err := bolt.Open("data.db", 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// start the timer
	t0 := time.Now()

	query := query.Query{
		Metrics: []query.QueryMetric{
			query.QueryMetric{Column: "song_hotttnesss", Metric: "average"},
		},
		Filter: []query.QueryFilter{
			// query.QueryFilter{Column: "title", Operator: "contains", Operand: "One"},
			// query.QueryFilter{Column: "artist_location", Operator: "equals", Operand: "Detroit, MI"},
		},
	}

	// validate query
	// make sure query is semantically valid
	valid := true

	_ = valid

	db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("songs")).Cursor()

		count := 0

		songMap := make(map[string][]byte, 10)
		var currentRecord []byte

		accs := []Accumulator{
			&AverageAccumulator{Col: "song_hotttnesss"},
			&CountAccumulator{Col: "title"},
		}

		finishedLastRow := false
		for k, v := c.First(); k != nil || finishedLastRow == false; k, v = c.Next() {

			if k == nil || !bytes.HasPrefix(k, currentRecord) {

				count += 1
				if len(songMap) > 0 {

					if passesFilters, err := evaluateFilters(query.Filter, songMap); err != nil {
						panic(err)
					} else if passesFilters {

						for _, acc := range accs {
							if data, ok := songMap[acc.Column()]; ok {
								if err := acc.Add(data); err != nil {
									fmt.Printf("Problem adding data %s.%s to accumulator %s\n", songMap["title"], acc.Column(), reflect.TypeOf(acc))
									fmt.Printf("    %v\n", songMap["song_hotttnesss"])
									// panic(err)
								}
							}
						}

					}

				}

				// clear the map for the next record
				for k, _ := range songMap {
					delete(songMap, k)
				}

			}

			if k != nil {
				currentRecord = k[0:16]

				// split the key into useful parts
				_, fieldName := k[0:16], string(k[17:])

				// add data for this field to the current song map
				songMap[fieldName] = v
			} else {
				finishedLastRow = true
			}

		}

		fmt.Printf("Scanned %d rows\n", count)

		for _, acc := range accs {
			fmt.Printf("%#v\n", acc)
		}
		fmt.Printf("took %v\n", time.Now().Sub(t0))

		return nil

	})

}

func evaluateFilters(filters []query.QueryFilter, songMap map[string][]byte) (bool, error) {
	for _, filter := range filters {
		op := filter.Operator
		switch op {
		case "equals":
			if passed := bytes.Equal(songMap[filter.Column], []byte(filter.Operand)); !passed {
				return false, nil
			}
		case "contains":
			if passed := bytes.Contains(songMap[filter.Column], []byte(filter.Operand)); !passed {
				return false, nil
			}
		default:
			return false, fmt.Errorf("unsupported operator: %s\n", filter.Operator)
		}
	}

	return true, nil
}