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
	log.Println("Preparing SQL and executing")
	statement, err := database.Prepare(sql)
	if err != nil {
		log.Fatal(err)
	}
	statement.Exec()

	sql = "CREATE TABLE " + tableName + "(id INTEGER PRIMARY KEY "

	for _, colName := range cols {
		sql += ", "
		sql += colName
		//// TODO:
	}

	sql += ")"

	fmt.Println(sql)
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
	err = nil
	fmt.Println("In add data ")

	keys := make([]string, 0, len(attributes))
	for k := range attributes {
		keys = append(keys, k)
	}

	tableName := keys[0]

	sql := "INSERT INTO TABLE " + tableName + " ("

	cols := strings.Join(attributes[tableName].headers, ",")
	fmt.Println(cols)
	//sql += cols

	//var data Data

	//data = tables[tableName]
	//	fmt.Println(len(data.rows))
	sql += ") VALUES ("

	fmt.Println(sql)

	return err
}

func main() {

	path := "../templates/fruit"

	attributes, data := loadFiles(path)
	fmt.Println(len(attributes))
	fmt.Println(len(data))
	log.Println("Starting build db")
	db, err := buildSQLiteDB("test.db", attributes)
	if err != nil {
		log.Fatal(err)
	}

	//err = addDataSQLiteDB(data, attributes, db)
	if err != nil {
		log.Fatal(err)
	}
	db.Close()
}
