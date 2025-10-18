package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"text/tabwriter"
	"time"

	_ "github.com/trinodb/trino-go-client/trino"
)

const (
	catalog = "hive"
	schema  = "default"
	table   = "fhir_raw_patient"
)

var interestingFields = []string{
	"resourcetype",
	"id",
	"name",
	"birthdate",
}

func main() {
	dsn := "http://user@localhost:8091?catalog=" + catalog + "&schema=" + schema
	db, err := sql.Open("trino", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	fmt.Printf("=== Tables in %s.%s ===\n", catalog, schema)
	tables, err := getTables(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	for _, t := range tables {
		fmt.Println("-", t)
	}

	// Check if target table exists
	found := false
	for _, t := range tables {
		if t == table {
			found = true
			break
		}
	}
	if !found {
		log.Printf("[ERR] Table %q not found in schema %s.%s\n", table, catalog, schema)
		return
	}

	fmt.Println("\n=== FHIR Patient Preview ===")
	query := fmt.Sprintf("SELECT %s FROM %s LIMIT 20", joinColumns(interestingFields), table)
	if err := runQuery(ctx, db, query); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\n=== IDs of Patients born on 1971-09-30 ===")
	filterQuery := fmt.Sprintf(`SELECT "id" FROM %s WHERE "birthdate" = '1971-09-30'`, table)
	if err := runQuery(ctx, db, filterQuery); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nDone.")
}

func getTables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

func runQuery(ctx context.Context, db *sql.DB, query string) error {
	start := time.Now()
	defer func() { fmt.Printf("Duration of query: %q, Duration: %s\n", query, time.Since(start).String()) }()
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, col := range cols {
		fmt.Fprintf(w, "%s\t", col)
	}
	fmt.Fprintln(w)

	for range cols {
		fmt.Fprintf(w, "--------\t")
	}
	fmt.Fprintln(w)

	for rows.Next() {
		values := make([]any, len(cols))
		valPtrs := make([]any, len(cols))
		for i := range values {
			valPtrs[i] = &values[i]
		}

		if err := rows.Scan(valPtrs...); err != nil {
			return err
		}

		for _, v := range values {
			if v == nil {
				fmt.Fprintf(w, "NULL\t")
			} else {
				fmt.Fprintf(w, "%v\t", v)
			}
		}
		fmt.Fprintln(w)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	w.Flush()
	return nil
}

func joinColumns(cols []string) string {
	s := `"` + cols[0] + `"`
	for _, c := range cols[1:] {
		s += `, "` + c + `"`
	}
	return s
}
