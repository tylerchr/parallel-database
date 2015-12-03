package main

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/boltdb/bolt"
	"github.com/tylerchr/parallel-database/query"
)

type Database struct {
	BoltDatabase *bolt.DB
}

func NewDatabase(file string) (*Database, error) {

	db, err := bolt.Open("data.db", 0600, nil)
	if err != nil {
		return nil, err
	}

	return &Database{BoltDatabase: db}, nil

}

func (db *Database) Fields() map[string]string {

	fields := make(map[string]string, 0)

	db.BoltDatabase.View(func(tx *bolt.Tx) error {
		fmt.Println(tx)
		c := tx.Bucket([]byte("songsSchema")).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fields[string(k)] = string(v)
		}
		return nil
	})

	return fields

}

func (db *Database) ExecuteRange(q query.Query, start, end byte) error {

	// validate query
	// make sure query is semantically valid
	valid := true

	_ = valid

	accs := make([]Accumulator, len(q.Metrics))
	for i, metric := range q.Metrics {
		switch metric.Metric {
		case "avg":
			fallthrough
		case "average":
			accs[i] = &AverageAccumulator{Col: metric.Column}
		case "count":
			accs[i] = &CountAccumulator{Col: metric.Column}
		case "min":
			accs[i] = &MinAccumulator{Col: metric.Column}
		case "max":
			accs[i] = &MaxAccumulator{Col: metric.Column}
		}
	}

	return db.BoltDatabase.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("songs")).Cursor()

		count := 0

		songMap := make(map[string][]byte, 10)
		var currentRecord []byte

		idx := 0
		finishedLastRow := false
		for k, v := c.Seek([]byte{start}); (k != nil && k[0] <= end) || finishedLastRow == false; k, v = c.Next() {

			if k == nil || !bytes.HasPrefix(k, currentRecord) {

				count += 1
				if len(songMap) > 0 {

					if passesFilters, err := db.evaluateFilters(q.Filter, songMap); err != nil {
						return err
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

			if k != nil && k[0] <= end {
				currentRecord = k[0:16]

				// split the key into useful parts
				_, fieldName := k[0:16], string(k[17:])

				// add data for this field to the current song map
				songMap[fieldName] = v
			} else {
				finishedLastRow = true
			}

			idx += 1
		}

		fmt.Printf("Scanned %d rows\n", count)
		fmt.Printf("Scanned %d keys\n", idx)

		for _, acc := range accs {
			fmt.Printf("%#v\n", acc)
		}

		return nil

	})

}

func (db *Database) evaluateFilters(filters []query.QueryFilter, songMap map[string][]byte) (bool, error) {
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

func (db *Database) Close() {
	db.BoltDatabase.Close()
}
