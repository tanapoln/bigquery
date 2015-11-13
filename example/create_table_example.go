package main

import (
	"fmt"

	"github.com/dailyburn/bigquery/client"
)

const PROJECTID = "your-projectid"
const DATASET = "your-dataset-name"
const JSON_PEM_PATH = "path to your local json pem file"

func main() {
	bqClient := client.New(JSON_PEM_PATH)

	fields := map[string]string{
		"name":       "STRING",
		"timestamp":  "INTEGER",
		"desc":       "STRING",
		"created_at": "TIMESTAMP",
	}

	tableName := "some_table_name"

	err := bqClient.InsertNewTable(PROJECTID, DATASETID, tableName, fields)
	if err != nil {
		fmt.Println("Table insert failed:", err)
	}
}
