package client

import (
	"code.google.com/p/goauth2/oauth"
	"code.google.com/p/goauth2/oauth/jwt"
	bigquery "code.google.com/p/google-api-go-client/bigquery/v2"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
)

const AuthUrl = "https://accounts.google.com/o/oauth2/auth"
const TokenUrl = "https://accounts.google.com/o/oauth2/token"

type Client struct {
	Service   *bigquery.Service
	DatasetId string
	ProjectId string
}

// Instantiate a new client with the given params and return a reference to it
func New(pemPath, serviceAccountEmailAddress, serviceUserAccountClientId, clientSecret, datasetId, projectId string) *Client {
	// generate auth token and create service object
	authScope := bigquery.BigqueryScope
	pemKeyBytes, err := ioutil.ReadFile(pemPath)
	if err != nil {
		panic(err)
	}

	t := jwt.NewToken(serviceAccountEmailAddress, bigquery.BigqueryScope, pemKeyBytes)

	c := &http.Client{}
	token, err := t.Assert(c)
	if err != nil {
		panic(err)
	}

	config := &oauth.Config{
		ClientId:     serviceUserAccountClientId,
		ClientSecret: clientSecret,
		Scope:        authScope,
		AuthURL:      "https://accounts.google.com/o/oauth2/auth",
		TokenURL:     "https://accounts.google.com/o/oauth2/token",
	}

	transport := &oauth.Transport{
		Token:  token,
		Config: config,
	}

	client := transport.Client()

	service, err := bigquery.New(client)
	if err != nil {
		panic(err)
	}

	return &Client{Service: service, DatasetId: datasetId, ProjectId: projectId}
}

func (c *Client) InsertRow(projectId, datasetId, tableId string, rowData map[string]interface{}) error {
	rows := []*bigquery.TableDataInsertAllRequestRows{
		{
			Json: rowData,
		},
	}

	insertRequest := &bigquery.TableDataInsertAllRequest{Rows: rows}

	result, err := c.Service.Tabledata.InsertAll(projectId, datasetId, tableId, insertRequest).Do()
	if err != nil {
		fmt.Println("Error inserting row: ", err)
		return err
	}

	return nil
}

// SyncQuery executes an arbitrary query string and returns the result synchronously (unless the response takes longer than the provided timeout)
func (c *Client) SyncQuery(queryStr string, maxResults int64) ([][]interface{}, error) {
	datasetRef := &bigquery.DatasetReference{
		DatasetId: c.DatasetId,
		ProjectId: c.ProjectId,
	}

	query := &bigquery.QueryRequest{
		DefaultDataset: datasetRef,
		MaxResults:     maxResults,
		Kind:           "json",
		Query:          queryStr,
	}

	results, err := c.Service.Jobs.Query(c.ProjectId, query).Do()
	if err != nil {
		fmt.Println("Query Error: ", err)
		return nil, err
	}

	// credit to https://github.com/getlantern/statshub for the row building approach
	numRows := int(results.TotalRows)
	if numRows > int(maxResults) {
		numRows = int(maxResults)
	}

	rows := make([][]interface{}, numRows)
	for r := 0; r < int(numRows); r++ {
		numColumns := len(results.Schema.Fields)
		dataRow := results.Rows[r]
		row := make([]interface{}, numColumns)
		for c := 0; c < numColumns; c++ {
			row[c] = dataRow.F[c].V
		}
		rows[r] = row
	}

	return rows, nil
}

// Count loads the row count for the provided dataset.tablename
func (c *Client) Count(datasetTable string) int64 {
	qstr := fmt.Sprintf("select count(*) from [%s]", datasetTable)
	res, err := c.SyncQuery(qstr, 1)
	if err == nil {
		if len(res) > 0 {
			val, _ := strconv.ParseInt(res[0][0].(string), 10, 64)
			return val
		}
	}
	return 0
}
