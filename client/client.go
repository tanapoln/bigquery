package client

import (
	"code.google.com/p/goauth2/oauth"
	"code.google.com/p/goauth2/oauth/jwt"
	"errors"
	"fmt"
	bigquery "github.com/Dailyburn/google-api-go-client-bigquery/bigquery/v2"
	"io/ioutil"
	"net/http"
	"strconv"
)

const AuthUrl = "https://accounts.google.com/o/oauth2/auth"
const TokenUrl = "https://accounts.google.com/o/oauth2/token"

const DefaultPageSize = 5000

type Client struct {
	accountEmailAddress string
	userAccountClientId string
	clientSecret        string
	pemPath             string
	token               *oauth.Token
	service             *bigquery.Service
}

type ClientData struct {
	Headers []string
	Rows    [][]interface{}
	Err     error
}

// Instantiate a new client with the given params and return a reference to it
func New(pemPath, serviceAccountEmailAddress, serviceUserAccountClientId, clientSecret string) *Client {
	return &Client{pemPath: pemPath, clientSecret: clientSecret, accountEmailAddress: serviceAccountEmailAddress, userAccountClientId: serviceUserAccountClientId}
}

func (c *Client) connect() (*bigquery.Service, error) {
	if c.token != nil {
		fmt.Println("token expired", c.token.Expired())
		fmt.Println("token expiry", c.token.Expiry)

		if !c.token.Expired() && c.service != nil {
			fmt.Println("REUSE SERVICE")
			return c.service, nil
		}
	}

	// generate auth token and create service object
	authScope := bigquery.BigqueryScope
	pemKeyBytes, err := ioutil.ReadFile(c.pemPath)
	if err != nil {
		panic(err)
	}

	t := jwt.NewToken(c.accountEmailAddress, bigquery.BigqueryScope, pemKeyBytes)

	httpClient := &http.Client{}
	token, err := t.Assert(httpClient)
	if err != nil {
		return nil, err
	}

	c.token = token

	config := &oauth.Config{
		ClientId:     c.userAccountClientId,
		ClientSecret: c.clientSecret,
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
		return nil, err
	}

	c.service = service
	return service, nil
}

func (c *Client) InsertRow(projectId, datasetId, tableId string, rowData map[string]interface{}) error {
	service, err := c.connect()
	if err != nil {
		return err
	}

	rows := []*bigquery.TableDataInsertAllRequestRows{
		{
			Json: rowData,
		},
	}

	insertRequest := &bigquery.TableDataInsertAllRequest{Rows: rows}

	result, err := service.Tabledata.InsertAll(projectId, datasetId, tableId, insertRequest).Do()
	if err != nil {
		fmt.Println("Error inserting row: ", err)
		return err
	}

	// build an insert error string
	if len(result.InsertErrors) > 0 {
		outputStr := "Insert Errors: "
		for _, e := range result.InsertErrors {
			for _, m := range e.Errors {
				outputStr += "error: " + m.Message + " | "
			}
		}
		return errors.New(outputStr)
	}

	return nil
}

// AsyncQuery loads the data by paging through the query results and sends back payloads over the dataChan - dataChan sends a payload containing ClientData objects made up of the headers, rows and an error attribute
func (c *Client) AsyncQuery(pageSize int, dataset, project, queryStr string, dataChan chan ClientData) {
	c.pagedQuery(pageSize, dataset, project, queryStr, dataChan)
}

// Query load the data for the query paging if necessary and return the data rows, headers and error
func (c *Client) Query(dataset, project, queryStr string) ([][]interface{}, []string, error) {
	return c.pagedQuery(DefaultPageSize, dataset, project, queryStr, nil)
}

// pagedQuery executes the query using bq's paging mechanism to load all results and sends them back via dataChan if available, otherwise it returns the full result set, headers and error as return values
func (c *Client) pagedQuery(pageSize int, dataset, project, queryStr string, dataChan chan ClientData) ([][]interface{}, []string, error) {
	// connect to service
	service, err := c.connect()
	if err != nil {
		if dataChan != nil {
			dataChan <- ClientData{Err: err}
		}
		return nil, nil, err
	}

	datasetRef := &bigquery.DatasetReference{
		DatasetId: dataset,
		ProjectId: project,
	}

	query := &bigquery.QueryRequest{
		DefaultDataset: datasetRef,
		MaxResults:     int64(pageSize),
		Kind:           "json",
		Query:          queryStr,
	}

	// start query
	qr, err := service.Jobs.Query(project, query).Do()

	if err != nil {
		fmt.Println("Error loading query: ", err)
		if dataChan != nil {
			dataChan <- ClientData{Err: err}
		}

		return nil, nil, err
	}

	var headers []string
	rows := [][]interface{}{}

	// if query is completed process, otherwise begin checking for results
	if qr.JobComplete {
		headers = c.headersForResults(qr)
		rows = c.formatResults(qr, len(qr.Rows))
		if dataChan != nil {
			dataChan <- ClientData{Headers: headers, Rows: rows}
		}
	}

	if qr.TotalRows > uint64(pageSize) || !qr.JobComplete {
		resultChan := make(chan processedData)

		go c.pageOverJob(len(rows), qr.JobReference, qr.PageToken, resultChan)

	L:
		for {
			select {
			case data, ok := <-resultChan:
				if !ok {
					break L
				}
				if dataChan != nil {
					if len(data.headers) > 0 {
						headers = data.headers
					}
					dataChan <- ClientData{Headers: headers, Rows: data.rows}
				} else {
					headers = data.headers
					rows = append(rows, data.rows...)
				}
			}
		}
	}

	if dataChan != nil {
		close(dataChan)
	}

	return rows, headers, nil
}

type processedData struct {
	rows    [][]interface{}
	headers []string
}

// pageOverJob loads results for the given job reference and if the total results has not been hit continues to load recursively
func (c *Client) pageOverJob(rowCount int, jobRef *bigquery.JobReference, pageToken string, resultChan chan processedData) error {
	service, err := c.connect()
	if err != nil {
		return err
	}

	qrc := service.Jobs.GetQueryResults(jobRef.ProjectId, jobRef.JobId)
	if len(pageToken) > 0 {
		qrc.PageToken(pageToken)
	}

	qr, err := qrc.Do()
	if err != nil {
		fmt.Println("Error loading additional data: ", err)
		close(resultChan)
		return err
	}

	if qr.JobComplete {
		// send back the rows we got
		headers := c.headersForJobResults(qr)
		rows := c.formatResultsFromJob(qr, len(qr.Rows))
		resultChan <- processedData{rows, headers}
		rowCount = rowCount + len(rows)
	}

	if qr.TotalRows > uint64(rowCount) || !qr.JobComplete {
		if qr.JobReference == nil {
			c.pageOverJob(rowCount, jobRef, pageToken, resultChan)
		} else {
			c.pageOverJob(rowCount, qr.JobReference, qr.PageToken, resultChan)
		}
	} else {
		close(resultChan)
		return nil
	}

	return nil
}

// SyncQuery executes an arbitrary query string and returns the result synchronously (unless the response takes longer than the provided timeout)
func (c *Client) SyncQuery(dataset, project, queryStr string, maxResults int64) ([][]interface{}, error) {
	service, err := c.connect()
	if err != nil {
		return nil, err
	}

	datasetRef := &bigquery.DatasetReference{
		DatasetId: dataset,
		ProjectId: project,
	}

	query := &bigquery.QueryRequest{
		DefaultDataset: datasetRef,
		MaxResults:     maxResults,
		Kind:           "json",
		Query:          queryStr,
	}

	results, err := service.Jobs.Query(project, query).Do()
	if err != nil {
		fmt.Println("Query Error: ", err)
		return nil, err
	}

	// credit to https://github.com/getlantern/statshub for the row building approach
	numRows := int(results.TotalRows)
	if numRows > int(maxResults) {
		numRows = int(maxResults)
	}

	rows := c.formatResults(results, numRows)
	return rows, nil
}

// headersForResults extracts the header slice from a QueryResponse
func (c *Client) headersForResults(results *bigquery.QueryResponse) []string {
	headers := []string{}
	numColumns := len(results.Schema.Fields)
	for c := 0; c < numColumns; c++ {
		headers = append(headers, results.Schema.Fields[c].Name)
	}
	return headers
}

// formatResults extracts the result rows from a QueryResponse
func (c *Client) formatResults(results *bigquery.QueryResponse, numRows int) [][]interface{} {
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
	return rows
}

// formatResultsFromJob extracts the result rows from a GetQueryResultsResponse
func (c *Client) formatResultsFromJob(results *bigquery.GetQueryResultsResponse, numRows int) [][]interface{} {
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
	return rows
}

// headersForJobResults extracts the header slice from a GetQueryResultsResponse
func (c *Client) headersForJobResults(results *bigquery.GetQueryResultsResponse) []string {
	headers := []string{}
	numColumns := len(results.Schema.Fields)
	for c := 0; c < numColumns; c++ {
		headers = append(headers, results.Schema.Fields[c].Name)
	}
	return headers
}

// Count loads the row count for the provided dataset.tablename
func (c *Client) Count(dataset, project, datasetTable string) int64 {
	qstr := fmt.Sprintf("select count(*) from [%s]", datasetTable)
	res, err := c.SyncQuery(dataset, project, qstr, 1)
	if err == nil {
		if len(res) > 0 {
			val, _ := strconv.ParseInt(res[0][0].(string), 10, 64)
			return val
		}
	}
	return 0
}
