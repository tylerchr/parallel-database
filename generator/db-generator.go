package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"os"
	"strconv"
	"strings"
	"sort"
	"math"
	"time"
)

const (
	DBFILE       = "../data.db"
	INPUTFILE    = "msd.txt"
	SCHEMABUCKET = "songsSchema"
	SONGSBUCKET  = "songs"
	KEYCOLUMN    = "track_id"
)

type Column struct {
	name     string
	dataType string
}

func main() {

	var file *os.File
	var schema []Column

	db, err := bolt.Open(DBFILE, 0600, nil)

	checkError(err)
	defer db.Close()

	file, err = os.Open(INPUTFILE)

	checkError(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	schema, err = getTableSchema(scanner)
	checkError(err)

	dataTypeMap := make(map[string]string)

	for i := 0; i < len(schema); i++ {
		dataTypeMap[schema[i].name] = schema[i].dataType
	}

	db.Update(func(tx *bolt.Tx) error {

		// Save the table meta deta
		err := saveMetaData(tx, schema)

		if err != nil {
			return err
		}

		// Create songs bucket
		var b *bolt.Bucket
		b, err = tx.CreateBucketIfNotExists([]byte(SONGSBUCKET))

		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		// Temporary map to sort the keys
		var columnMap = make(map[string]map[string]string)

		// Add songs to DB
		count := 1;
		startTime := time.Now()
		for scanner.Scan() {

			columns := parseLine(scanner.Text(), schema)
			hash := md5.Sum([]byte(columns[KEYCOLUMN]))
			columnMap[string(hash[:])] = columns

		}

		var keys []string
		for k := range columnMap {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		// Iterate over sorted keys
		for _, k := range keys {

			for columnName, value := range columnMap[k] {
				key := k + "_" + columnName

				err = putTypedValue(b, key, value, dataTypeMap[columnName])

				if err != nil {
					return err
				}
			}

			if count % 10000 == 0 {
				fmt.Printf("[%d] took %s\n", count, time.Since(startTime))
				startTime = time.Now()
			}

			count++

		}

		return err
	})

}

func getTableSchema(scanner *bufio.Scanner) ([]Column, error) {

	var columnNames []string
	var dataTypes []string

	//  Get the column names
	didScan := scanner.Scan()

	if didScan {
		columnNames = strings.Split(scanner.Text(), "\t")
	} else {
		return nil, errors.New("Invalid input file")
	}

	// Get the data types
	didScan = scanner.Scan()

	if didScan {
		dataTypes = strings.Split(scanner.Text(), "\t")
	} else {
		return nil, errors.New("Invalid input file")
	}

	// Sanity check
	if len(columnNames) != len(dataTypes) {
		return nil, errors.New("Invalid input file")
	}

	schema := make([]Column, len(columnNames))

	for i := 0; i < len(columnNames); i++ {
		schema[i] = Column{name: columnNames[i], dataType: dataTypes[i]}
	}

	return schema, nil

}

func saveMetaData(tx *bolt.Tx, schema []Column) error {
	b, err := tx.CreateBucketIfNotExists([]byte(SCHEMABUCKET))

	if err != nil {
		return err
	}

	for i := 0; i < len(schema); i++ {
		b.Put([]byte(schema[i].name), []byte(schema[i].dataType))
	}

	return nil

}

func parseLine(line string, schema []Column) map[string]string {
	columns := make(map[string]string)
	data := strings.Split(line, "\t")

	for i := 0; i < len(data); i++ {
		columns[schema[i].name] = data[i]
	}

	return columns
}

func putTypedValue(bucket *bolt.Bucket, key string, value string, dataType string) error {

	buf := new(bytes.Buffer)
	shouldWrite := true;

	switch dataType {
	case "int":
		i64, err := strconv.ParseInt(value, 10, 64)
		checkError(err)
		binary.Write(buf, binary.BigEndian, i64)

	case "float":
		f64, err := strconv.ParseFloat(value, 64)
		checkError(err)
		binary.Write(buf, binary.BigEndian, f64)

		if math.IsNaN(f64) {
			shouldWrite = false
		}

	default:
		err := bucket.Put([]byte(key), []byte(value))
		return err
	}

	if !shouldWrite {
		return nil
	}

	err := bucket.Put([]byte(key), buf.Bytes())

	return err
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
