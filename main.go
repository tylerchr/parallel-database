package main

import (
	// "crypto/md5"
	"encoding/binary"
	// "encoding/hex"
	"fmt"
	"bytes"
	"time"
	"math"

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
			query.QueryFilter{Column: "title", Operator: "contains", Operand: "One"},
			query.QueryFilter{Column: "artist_location", Operator: "equals", Operand: "Detroit, MI"},
		},
	}

	// validate query
	// make sure query is semantically valid
	valid := true

	_ = valid

	db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("songs")).Cursor()

		var sum float64 = 0.0
		count := 0

		var songMap map[string][]byte = make(map[string][]byte, 10)
		var currentRecord []byte

		// prefix := []byte("a")
		var number float64
		finishedLastRow := false
		for k, v := c.First(); k != nil && finishedLastRow == false; k, v = c.Next() {

			// split the key into useful parts
			recordKey, fieldName := k[0:16], string(k[17:])

			if currentRecord == nil || !bytes.HasPrefix(k, currentRecord) {

				if songMap != nil {

					if passesFilters, err := evaluateFilters(query.Filter, songMap); err != nil {
						panic(err)
					} else if passesFilters {

						fmt.Printf("%s by %s\n", songMap["title"], songMap["artist_name"])

						count += 1

						// evaluate filters and handle the metrics from that map

						binary.Read(bytes.NewReader(songMap["song_hotttnesss"]), binary.BigEndian, &number)
						if !math.IsNaN(number) {
							sum += number
						}

					}

				}


				// clear the map for the next record
				for k, _ := range songMap {
					delete(songMap, k)
				}

				currentRecord = recordKey

			}

			// add data for this field to the current song map
			songMap[fieldName] = v

			if k == nil {
				finishedLastRow = true
			}

		}

		fmt.Printf("sum=%f ct=%d avg=%.3f\n", sum, count, float32(sum) / float32(count))
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