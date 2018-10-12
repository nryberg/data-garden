package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

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

// Row is the columns of data with an id field
type Row struct {
	id   int
	cols []string
}

func loadFiles(path string) (map[string]Attribute, map[string][]Row) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}
	var attributes map[string]Attribute
	attributes = make(map[string]Attribute)

	var data map[string][]Row
	data = make(map[string][]Row)

	for _, file := range files {
		fileName := file.Name()
		extension := strings.Split(fileName, ".")[1]
		attributeName := strings.Split(fileName, ".")[0]
		var attribute Attribute
		var row Row
		var rows []Row
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
			data[attributeName] = rows
		}
	}
	return attributes, data
}

func printAttributes(attributes map[string]Attribute) {
	keys := make([]string, 0, len(attributes))
	for k := range attributes {
		keys = append(keys, k)
	}

	fmt.Println(keys)
}

func main() {

	path := "../templates/fruit"

	attributes, data := loadFiles(path)

	fmt.Println(len(attributes))
	fmt.Println(len(data))

	printAttributes(attributes)

}
