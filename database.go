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
		c := tx.Bucket([]byte("songsSchema")).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fields[string(k)] = string(v)
		}
		return nil
	})

	return fields

}

func (db *Database) Execute(q query.Query) error {

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

	// make sure query is semantically valid
	err := db.validateQuerySemantics(q, accs)

	if err != nil {
		fmt.Println(err)
		return err
	} else {
		fmt.Printf("Query is semantically valid\n")
	}

	return db.BoltDatabase.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("songs")).Cursor()

		count := 0

		songMap := make(map[string][]byte, 10)
		var currentRecord []byte

		finishedLastRow := false
		for k, v := c.First(); k != nil || finishedLastRow == false; k, v = c.Next() {

			if k == nil || !bytes.HasPrefix(k, currentRecord) {

				count += 1
				if len(songMap) > 0 {

					if passesFilters, err := db.evaluateFilters(q.Filter, songMap); err != nil {
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

func (db *Database) validateQuerySemantics(q query.Query, accs []Accumulator) error {

	schema := db.Fields()

	for i, metric := range q.Metrics {

		if schema[metric.Column] == "" {
			return fmt.Errorf("Column '%s' does not exist", metric.Column)
		}

		if !accs[i].CanAccumulateType(schema[metric.Column]) {
			return fmt.Errorf("Invalid metric '%s' for field type '%s'", metric.Metric, schema[metric.Column])
		}
	}

	for _, filter := range q.Filter {

		if schema[filter.Column] == "" {
			return fmt.Errorf("Column '%s' does not exist", filter.Column)
		}

		switch schema[filter.Column] {
		case "int":
			switch filter.Operator {
			case "equals":
			case "between":
			case "<":
			case ">":
			case "contains":
				fallthrough
			default:
				return fmt.Errorf("Invalid operator '%s' for field type '%s'", filter.Operator, schema[filter.Column])

			}

		case "float":
			switch filter.Operator {
			case "equals":
			case "between":
			case "<":
			case ">":
			case "contains":
				fallthrough
			default:
				return fmt.Errorf("Invalid operator '%s' for field type '%s'", filter.Operator, schema[filter.Column])

			}
		case "string":
			switch filter.Operator {
			case "equals":
			case "contains":
			case "<":
				fallthrough
			case ">":
				fallthrough
			case "between":
				fallthrough
			default:
				return fmt.Errorf("Invalid operator '%s' for field type '%s'", filter.Operator, schema[filter.Column])
			}
		default:
			return fmt.Errorf("Fatal Error")
		}

	}

	return nil

}


func (db *Database) Close() {
	db.BoltDatabase.Close()
}
