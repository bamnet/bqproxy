package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// SQLQuery represents a configured SQL query.
type SQLQuery struct {
	// The Name of the query, part of the URL used to call it.
	Name string
	// The SQL function to run.
	SQL string
	// Named-parameters the SQL function expects, with their type information.
	Parameters map[string]bigquery.FieldType
}

// Hardcode a sample query to test.
var sqlQueries = map[string]SQLQuery{
	"hello-world": {
		Name: "hello-world",
		SQL:  "SELECT * FROM UNNEST([(1, -1, 'a', null), (2, 0, 'bravo', 1)]);",
	},
	"param": {
		Name: "param",
		SQL:  "SELECT * FROM UNNEST([(@name, @id)]);",
		Parameters: map[string]bigquery.FieldType{
			"name": bigquery.StringFieldType,
			"id":   bigquery.FloatFieldType,
		},
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

	// TODO(bamnet): Move this "/query/"" to a config or flag.
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

	// Add query paramters, if any are configured.
	values := r.URL.Query()
	for key, fieldType := range query.Parameters {
		var v interface{}

		// Types are used to convert the string input into the native type
		// before being passed to BiqQuery.
		// TODO(bamnet): Add error handling.
		switch fieldType {
		case bigquery.IntegerFieldType:
			v, _ = strconv.Atoi(values.Get(key))
		case bigquery.BooleanFieldType:
			v = (values.Get(key) == "true")
		case bigquery.FloatFieldType:
			v, _ = strconv.ParseFloat(values.Get(key), 64)
		default:
			v = values.Get(key)
		}

		q.Parameters = append(q.Parameters, bigquery.QueryParameter{
			Name:  key,
			Value: v,
		})
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
