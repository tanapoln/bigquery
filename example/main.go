package main

import (
	"fmt"
	"github.com/dailyburn/bigquery/client"
)

const PEM_PATH = "path to your local pem file"
const SERVICE_ACCOUNT_EMAIL = "your account email"
const SERVICE_ACCOUNT_CLIENT_ID = "your service account client id"
const SECRET = "your-secret"
const DATASET = "your-dataset-name"

func main() {
	bqClient := client.New(PEM_PATH, SERVICE_ACCOUNT_EMAIL, SERVICE_ACCOUNT_CLIENT_ID, SECRET)

	// run a sync query
	query := "select * from publicdata:samples.shakespeare limit 100;"

	rows, headers, err := bqClient.Query("shakespeare", DATASET, query)
	if err != nil {
		fmt.Println("Error: ", err)
	} else {
		fmt.Println("Got rows: ", len(rows))
		fmt.Println("Headers: ", headers)
		fmt.Println("Rows: ", rows)
	}
}
