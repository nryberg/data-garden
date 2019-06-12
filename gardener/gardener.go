package main

import (
	"bufio"
	"database/sql"
	"fmt"
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

func loadFiles(path string) {
	log.Println("In Load Files")
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		fileName := file.Name()
		extension := strings.Split(fileName, ".")[2]
		purpose := strings.Split(fileName, ".")[1]
		log.Println(purpose)

		rowCount := 0
		if extension == "txt" {

			txtFile, _ := os.Open(path + "/" + file.Name())
			scanner := bufio.NewScanner(txtFile)
			for scanner.Scan() {
				line := scanner.Text()
				if err := scanner.Err(); err != nil {
					fmt.Fprintln(os.Stderr, "reading standard input:", err)
				}

				switch purpose {
				case "dimension":
					fmt.Println(line)
				case "date":
				case "measure":

				}
				rowCount++

			}

		}
	}
	log.Println("Done with load files")

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

	path := "../schema/Cell_Phone_Data_Use"

	//attributes, data := loadFiles(path)
	loadFiles(path)
	log.Println("Starting build db")
	//db, err := buildSQLiteDB("test.db", attributes)
	//if err != nil {
	//	log.Fatal(err)
	//}

	log.Println("Going to add data ")
	//err = addDataSQLiteDB(data, attributes, db)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//db.Close()
}
