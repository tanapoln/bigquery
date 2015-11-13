package main

import (
	"fmt"

	"github.com/dailyburn/bigquery/client"
)

const JSON_PEM_PATH = "path to your local json pem file"
const PROJECTID = "your-project-id"

func main() {
	bqClient := client.New(JSON_PEM_PATH)

	// run a sync query
	query := "select * from publicdata:samples.shakespeare limit 100;"

	rows, headers, err := bqClient.Query("shakespeare", PROJECTID, query)
	if err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println("Got rows: ", len(rows))
		fmt.Println("Headers: ", headers)
		fmt.Println("Rows: ", rows)
	}
}
