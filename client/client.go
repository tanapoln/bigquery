package client

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"code.google.com/p/goauth2/oauth"
	"code.google.com/p/goauth2/oauth/jwt"
	bigquery "github.com/Dailyburn/google-api-go-client-bigquery/bigquery/v2"
)

const authURL = "https://accounts.google.com/o/oauth2/auth"
const tokenURL = "https://accounts.google.com/o/oauth2/token"

const defaultPageSize = 5000

// Client a big query client instance
type Client struct {
	accountEmailAddress string
	userAccountClientID string
	clientSecret        string
	pemPath             string
	token               *oauth.Token
	service             *bigquery.Service
	allowLargeResults   bool
	tempTableName       string
}

// Data is a containing type used for Async data response handling including Headers, Rows and an Error that will be populated in the event of an Error querying
type Data struct {
	Headers []string
	Rows    [][]interface{}
	Err     error
}

// New instantiates a new client with the given params and return a reference to it
func New(pemPath, serviceAccountEmailAddress, serviceUserAccountClientID, clientSecret string, options ...func(*Client) error) *Client {
	c := Client{pemPath: pemPath, clientSecret: clientSecret, accountEmailAddress: serviceAccountEmailAddress, userAccountClientID: serviceUserAccountClientID}

	for _, option := range options {
		err := option(&c)
		if err != nil {
			return nil
		}
	}

	return &c
}

// AllowLargeResults is a configuration function that can be used to enable the AllowLargeResults setting of a bigquery request, as well as a temp table name to use to build the result data
func AllowLargeResults(shouldAllow bool, tempTableName string) func(*Client) error {
	return func(c *Client) error {
		return c.setAllowLargeResults(shouldAllow, tempTableName)
	}
}

//setAllowLargeResults - private function to set the AllowLargeResults and tempTableName values
func (c *Client) setAllowLargeResults(shouldAllow bool, tempTableName string) error {
	c.allowLargeResults = shouldAllow
	c.tempTableName = tempTableName
	return nil
}

// connect - opens a new connection to bigquery, reusing the token if possible or regenerating a new auth token if required
func (c *Client) connect() (*bigquery.Service, error) {
	if c.token != nil {
		if !c.token.Expired() && c.service != nil {
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
		ClientId:     c.userAccountClientID,
		ClientSecret: c.clientSecret,
		Scope:        authScope,
		AuthURL:      authURL,
		TokenURL:     tokenURL,
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

// InsertRow inserts a new row into the desired project, dataset and table or returns an error
func (c *Client) InsertRow(projectID, datasetID, tableID string, rowData map[string]interface{}) error {
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

	result, err := service.Tabledata.InsertAll(projectID, datasetID, tableID, insertRequest).Do()
	if err != nil {
		fmt.Println("Error inserting row: ", err)
		return err
	}

	if len(result.InsertErrors) > 0 {
		return errors.New("Error inserting row")
	}

	return nil
}

// AsyncQuery loads the data by paging through the query results and sends back payloads over the dataChan - dataChan sends a payload containing Data objects made up of the headers, rows and an error attribute
func (c *Client) AsyncQuery(pageSize int, dataset, project, queryStr string, dataChan chan Data) {
	c.pagedQuery(pageSize, dataset, project, queryStr, dataChan)
}

// Query loads the data for the query paging if necessary and return the data rows, headers and error
func (c *Client) Query(dataset, project, queryStr string) ([][]interface{}, []string, error) {
	return c.pagedQuery(defaultPageSize, dataset, project, queryStr, nil)
}

// stdPagedQuery executes a query using default job parameters and paging over the results, returning them over the data chan provided
func (c *Client) stdPagedQuery(service *bigquery.Service, pageSize int, dataset, project, queryStr string, dataChan chan Data) ([][]interface{}, []string, error) {
	fmt.Println("std paged query")
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

	qr, err := service.Jobs.Query(project, query).Do()

	if err != nil {
		fmt.Println("Error loading query: ", err)
		if dataChan != nil {
			dataChan <- Data{Err: err}
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
			dataChan <- Data{Headers: headers, Rows: rows}
		}
	}

	if qr.TotalRows > uint64(pageSize) || !qr.JobComplete {
		resultChan := make(chan [][]interface{})
		headersChan := make(chan []string)

		go c.pageOverJob(len(rows), qr.JobReference, qr.PageToken, resultChan, headersChan)

	L:
		for {
			select {
			case h, ok := <-headersChan:
				if ok {
					headers = h
				}
			case newRows, ok := <-resultChan:
				if !ok {
					break L
				}
				if dataChan != nil {
					dataChan <- Data{Headers: headers, Rows: newRows}
				} else {
					rows = append(rows, newRows...)
				}
			}
		}
	}

	if dataChan != nil {
		close(dataChan)
	}

	return rows, headers, nil
}

// largeDataPagedQuery builds a job and inserts it into the job queue allowing the flexibility to set the custom AllowLargeResults flag for the job
func (c *Client) largeDataPagedQuery(service *bigquery.Service, pageSize int, dataset, project, queryStr string, dataChan chan Data) ([][]interface{}, []string, error) {
	fmt.Println("largeDataPagedQuery")
	// start query
	tableRef := bigquery.TableReference{DatasetId: dataset, ProjectId: project, TableId: c.tempTableName}
	jobConfigQuery := bigquery.JobConfigurationQuery{}

	jobConfigQuery.AllowLargeResults = true
	jobConfigQuery.Query = queryStr
	jobConfigQuery.DestinationTable = &tableRef
	jobConfigQuery.WriteDisposition = "WRITE_TRUNCATE"
	jobConfigQuery.CreateDisposition = "CREATE_IF_NEEDED"

	jobConfig := bigquery.JobConfiguration{}

	jobConfig.Query = &jobConfigQuery

	job := bigquery.Job{}
	job.Configuration = &jobConfig

	jobInsert := service.Jobs.Insert(project, &job)
	runningJob, jerr := jobInsert.Do()

	if jerr != nil {
		fmt.Println("Error inserting job!", jerr)
	}

	qr, err := service.Jobs.GetQueryResults(project, runningJob.JobReference.JobId).Do()

	if err != nil {
		fmt.Println("Error loading query: ", err)
		if dataChan != nil {
			dataChan <- Data{Err: err}
		}

		return nil, nil, err
	}

	var headers []string
	rows := [][]interface{}{}

	// if query is completed process, otherwise begin checking for results
	if qr.JobComplete {
		headers = c.headersForJobResults(qr)
		rows = c.formatResultsFromJob(qr, len(qr.Rows))
		if dataChan != nil {
			dataChan <- Data{Headers: headers, Rows: rows}
		}
	}

	if qr.TotalRows > uint64(pageSize) || !qr.JobComplete {
		resultChan := make(chan [][]interface{})
		headersChan := make(chan []string)

		go c.pageOverJob(len(rows), runningJob.JobReference, qr.PageToken, resultChan, headersChan)

	L:
		for {
			select {
			case h, ok := <-headersChan:
				if ok {
					headers = h
				}
			case newRows, ok := <-resultChan:
				if !ok {
					break L
				}
				if dataChan != nil {
					dataChan <- Data{Headers: headers, Rows: newRows}
				} else {
					rows = append(rows, newRows...)
				}
			}
		}
	}

	if dataChan != nil {
		close(dataChan)
	}

	return rows, headers, nil
}

// pagedQuery executes the query using bq's paging mechanism to load all results and sends them back via dataChan if available, otherwise it returns the full result set, headers and error as return values
func (c *Client) pagedQuery(pageSize int, dataset, project, queryStr string, dataChan chan Data) ([][]interface{}, []string, error) {
	// connect to service
	service, err := c.connect()
	if err != nil {
		if dataChan != nil {
			dataChan <- Data{Err: err}
		}
		return nil, nil, err
	}

	if c.allowLargeResults && len(c.tempTableName) > 0 {
		return c.largeDataPagedQuery(service, pageSize, dataset, project, queryStr, dataChan)
	}

	return c.stdPagedQuery(service, pageSize, dataset, project, queryStr, dataChan)
}

// pageOverJob loads results for the given job reference and if the total results has not been hit continues to load recursively
func (c *Client) pageOverJob(rowCount int, jobRef *bigquery.JobReference, pageToken string, resultChan chan [][]interface{}, headersChan chan []string) error {
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
		if headersChan != nil {
			headersChan <- c.headersForJobResults(qr)
			close(headersChan)
		}

		// send back the rows we got
		rows := c.formatResultsFromJob(qr, len(qr.Rows))
		resultChan <- rows
		rowCount = rowCount + len(rows)
	}

	if qr.TotalRows > uint64(rowCount) || !qr.JobComplete {
		if qr.JobReference == nil {
			c.pageOverJob(rowCount, jobRef, pageToken, resultChan, headersChan)
		} else {
			c.pageOverJob(rowCount, qr.JobReference, qr.PageToken, resultChan, nil)
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
