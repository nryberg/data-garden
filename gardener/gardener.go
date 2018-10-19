package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// drop a piece of the array/slice
func remove(s []int, i int) []int {
	s[len(s)-1], s[i] = s[i], s[len(s)-1]
	return s[:len(s)-1]
}

// Attribute holds metadata of table
type Attribute struct {
	rows    int
	cols    int
	headers []string
}

// Data Holds the rows
type Data struct {
	rows []Row
}

// Row is the columns of data with an id field
type Row struct {
	id   int
	cols []string
}

func loadFiles(path string) (map[string]Attribute, map[string]Data) {
	log.Println("In Load Files")
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}
	var attributes map[string]Attribute
	attributes = make(map[string]Attribute)

	var tables map[string]Data
	tables = make(map[string]Data)

	var rows []Row
	var data Data
	var row Row
	var attribute Attribute

	for _, file := range files {
		fileName := file.Name()
		extension := strings.Split(fileName, ".")[1]
		attributeName := strings.Split(fileName, ".")[0]
		rowCount := 0
		if extension == "csv" {
			csvFile, _ := os.Open(path + "/" + file.Name())
			reader := csv.NewReader(bufio.NewReader(csvFile))
			for {
				line, error := reader.Read()
				if error == io.EOF {
					break
				} else if error != nil {
					log.Fatal(error)
				}
				rowCount++
				attribute.rows = rowCount
				if rowCount == 1 {
					attribute.cols = len(line)
					attribute.headers = line
				} else {
					row.cols = line
					row.id = rowCount
					rows = append(rows, row)
				}
			}
			attributes[attributeName] = attribute
			data.rows = rows
			tables[attributeName] = data

		}
	}
	log.Println("Done with load files")
	return attributes, tables
}

func printAttributes(attributes map[string]Attribute) {
	keys := make([]string, 0, len(attributes))
	for k := range attributes {
		keys = append(keys, k)
	}

	fmt.Println(keys)
}

func buildSQLiteDB(filename string, attributes map[string]Attribute) (db *sql.DB, err error) {
	keys := make([]string, 0, len(attributes))
	for k := range attributes {
		keys = append(keys, k)
	}

	tableName := keys[0]
	cols := attributes[tableName].headers

	log.Println("Opening DB")
	database, err := sql.Open("sqlite3", filename)

	if err != nil {
		log.Fatal(err)
	}

	log.Println("Dropping table just in case")
	sql := "DROP TABLE IF EXISTS " + tableName
	log.Println("Preparing SQL and executing drop table")
	statement, err := database.Prepare(sql)
	if err != nil {
		log.Fatal(err)
	}
	statement.Exec()

	sql = "CREATE TABLE " + tableName + "(id INTEGER PRIMARY KEY, "

	sql += strings.Join(cols, ",")

	sql += ")"

	log.Println("Preparing SQL and executing")
	statement, err = database.Prepare(sql)

	if err != nil {
		log.Fatal(err)
	}

	log.Println("Executing statement")
	statement.Exec()
	return database, err
}

func addDataSQLiteDB(tables map[string]Data, attributes map[string]Attribute, db *sql.DB) (err error) {
	var sb strings.Builder

	keys := make([]string, 0, len(attributes))
	for k := range attributes {
		keys = append(keys, k)
	}

	tableName := keys[0]

	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(attributes[tableName].headers, ","))

	var data Data

	data = tables[tableName]

	sb.WriteString(") VALUES (")

	prefixSQL := sb.String()

	for _, row := range data.rows {
		sb.Reset()
		sb.WriteString(prefixSQL)
		//TODO Figure out how to quote the strings so it inserts appropriately
		sb.WriteString(strings.Join(row.cols, ","))
		sb.WriteString(")")

		log.Println(sb.String())

		statement, errSQL := db.Prepare(sb.String())

		if errSQL != nil {
			log.Fatal(errSQL)
		}

		statement.Exec()

	}

	return err
}

func main() {

	path := "../templates/fruit"

	attributes, data := loadFiles(path)

	log.Println("Starting build db")
	db, err := buildSQLiteDB("test.db", attributes)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Going to add data ")
	err = addDataSQLiteDB(data, attributes, db)
	if err != nil {
		log.Fatal(err)
	}
	db.Close()
}
