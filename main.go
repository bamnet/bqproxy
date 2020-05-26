package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// SQLQuery represents a configured SQL query.
type SQLQuery struct {
	Name       string
	SQL        string
	Parameters []string
}

// Hardcode a sample query to test.
var sqlQueries = map[string]SQLQuery{
	"hello-world": {
		Name:       "hello-world",
		SQL:        "SELECT * FROM UNNEST([(1, -1, 'a', null), (2, 0, 'bravo', 1)]);",
		Parameters: []string{},
	},
}

var bqClient *bigquery.Client

func main() {
	ctx := context.Background()

	projectName := os.Getenv("GCP_PROJECT")
	if projectName == "" {
		log.Fatalf("Missing project name, set GCP_PROJECT env variable.")
	}

	var err error
	bqClient, err = bigquery.NewClient(ctx, projectName)
	if err != nil {
		log.Fatalf("Error connecting to Bigquery: %v", err)
	}

	http.HandleFunc("/query/", queryHandler)

	log.Fatal(http.ListenAndServe(":8080", nil))
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

		}
		row := make(map[string]interface{})

		for _, field := range it.Schema {
			if rawRow[field.Name] == nil {
				row[field.Name] = nil
				continue
			}
			switch field.Type {
			case bigquery.IntegerFieldType:
				row[field.Name] = rawRow[field.Name].(int64)
			case bigquery.StringFieldType:
				row[field.Name] = rawRow[field.Name].(string)
			case bigquery.BooleanFieldType:
				row[field.Name] = rawRow[field.Name].(bool)
			case bigquery.FloatFieldType:
				row[field.Name] = rawRow[field.Name].(float64)
			default:
				row[field.Name] = rawRow[field.Name]
			}
		}
		rows = append(rows, row)
	}

	jsonStr, _ := json.Marshal(rows)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonStr)
}
