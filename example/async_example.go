package main

import (
	"fmt"

	"github.com/Dailyburn/bigquery/client"
)

const PEM_PATH = "path to your local pem file"
const PROJECTID = "your-projectid"
const DATASET = "your-dataset-name"

func main() {
	bqClient := client.New(JSON_PEM_PATH)
	query := "select * from publicdata:samples.shakespeare limit 500;"

	dataChan := make(chan client.Data)
	go bqClient.AsyncQuery(100, DATASET, PROJECTID, query, dataChan)

L:
	for {
		select {
		case d, ok := <-dataChan:
			if d.Err != nil {
				fmt.Println("Error with data: ", d.Err)
				break L
			}

			if d.Rows != nil && d.Headers != nil {
				fmt.Println("Got rows: ", len(d.Rows))
				fmt.Println("Headers: ", d.Headers)
			}

			if !ok {
				fmt.Println("Data channel closed")
				break L
			}
		}
	}
}
