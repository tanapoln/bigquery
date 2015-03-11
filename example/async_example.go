package main

import (
	"fmt"

	"github.com/Dailyburn/bigquery/client"
)

const PEM_PATH = "path to your local pem file"
const SERVICE_ACCOUNT_EMAIL = "your account email"
const SERVICE_ACCOUNT_CLIENT_ID = "your service account client id"
const SECRET = "your-secret"
const PROJECTID = "your-projectid"
const DATASET = "your-dataset-name"

func main() {
	bqClient := client.New(PEM_PATH, SERVICE_ACCOUNT_EMAIL, SERVICE_ACCOUNT_CLIENT_ID, SECRET)
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
