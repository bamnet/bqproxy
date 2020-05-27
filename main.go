package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"gopkg.in/yaml.v2"
)

// SQLQuery represents a configured SQL query.
type SQLQuery struct {
	// The Name of the query, part of the URL used to call it.
	Name string `yaml:"name"`
	// The SQL function to run.
	SQL string `yaml:"query"`
	// Named-parameters the SQL function expects, with their type information.
	Parameters map[string]bigquery.FieldType `yaml:"parameters"`
}

var bqClient *bigquery.Client
var sqlQueries = map[string]SQLQuery{}

func main() {
	ctx := context.Background()

	projectName := os.Getenv("GCP_PROJECT")
	if projectName == "" {
		log.Fatalf("Missing project name, set GCP_PROJECT env variable.")
	}

	var err error
	if bqClient, err = bigquery.NewClient(ctx, projectName); err != nil {
		log.Fatalf("Error connecting to Bigquery: %v", err)
	}

	if sqlQueries, err = loadQueries("example.yaml"); err != nil {
		log.Fatalf("Error loading queries: %v", err)
	}

	// TODO(bamnet): Move this "/query/"" to a config or flag.
	http.HandleFunc("/query/", queryHandler)

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func loadQueries(path string) (map[string]SQLQuery, error) {
	dat, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	queries := []SQLQuery{}
	if err := yaml.Unmarshal(dat, &queries); err != nil {
		return nil, err
	}

	result := map[string]SQLQuery{}
	for _, q := range queries {
		result[q.Name] = q
	}

	return result, nil
}

func queryHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	queryName := strings.TrimPrefix(r.URL.Path, "/query/")
	query, ok := sqlQueries[queryName]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	q := bqClient.Query(query.SQL)

	// Add query paramters.
	var err error
	q.Parameters, err = buildQueryParams(query.Parameters, r.URL.Query())
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Printf("Error parsing params: %v", err)
		return
	}

	// Run the query.
	it, err := q.Read(ctx)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Printf("BigQuery error: %v", err)
		return
	}

	rows := []map[string]interface{}{}
	for {
		rawRow := map[string]bigquery.Value{}
		err := it.Next(&rawRow)
		if err == iterator.Done {
			break
		}
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Printf("BigQuery read error: %v", err)
		}
		row := make(map[string]interface{})

		for _, field := range it.Schema {
			row[field.Name] = castField(field.Type, rawRow[field.Name])
		}
		rows = append(rows, row)
	}

	jsonStr, _ := json.Marshal(rows)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonStr)
}

func castField(fieldType bigquery.FieldType, v bigquery.Value) interface{} {
	if v == nil {
		return nil
	}
	switch fieldType {
	case bigquery.IntegerFieldType:
		return v.(int64)
	case bigquery.StringFieldType:
		return v.(string)
	case bigquery.BooleanFieldType:
		return v.(bool)
	case bigquery.FloatFieldType:
		return v.(float64)
	}
	return v
}

func buildQueryParams(config map[string]bigquery.FieldType, values url.Values) ([]bigquery.QueryParameter, error) {
	params := []bigquery.QueryParameter{}

	for key, fieldType := range config {
		var v interface{}
		var err error

		// Convert the form input (string) into the native type before being passed to BiqQuery.
		switch fieldType {
		case bigquery.IntegerFieldType:
			v, err = strconv.Atoi(values.Get(key))
		case bigquery.BooleanFieldType:
			v = (values.Get(key) == "true")
		case bigquery.FloatFieldType:
			v, err = strconv.ParseFloat(values.Get(key), 64)
		default:
			v = values.Get(key)
		}

		if err != nil {
			return nil, err
		}

		params = append(params, bigquery.QueryParameter{
			Name:  key,
			Value: v,
		})
	}

	return params, nil
}
