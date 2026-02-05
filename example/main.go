package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "geodedb.com/geode"
)

func main() {
	// Connect to Geode server
	// DSN format: host:port?options or quic://host:port?options
	//
	// PRODUCTION (recommended): Use proper TLS certificates
	//   db, err := sql.Open("geode", "localhost:3141?ca=/path/to/ca.crt")
	//
	// For mTLS (mutual TLS):
	//   db, err := sql.Open("geode", "localhost:3141?ca=/path/to/ca.crt&cert=/path/to/client.crt&key=/path/to/client.key")
	//
	// ============================================================================
	// WARNING: The line below uses insecure=true which DISABLES TLS verification.
	// This makes connections vulnerable to man-in-the-middle attacks.
	// NEVER use insecure=true in production environments!
	// This is ONLY acceptable for local development with self-signed certificates.
	// ============================================================================
	db, err := sql.Open("geode", "localhost:3141?insecure=true")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Set connection pool settings
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Ping to verify connection
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	fmt.Println("Connected to Geode!")

	// Simple query
	fmt.Println("\n--- Simple Query ---")
	rows, err := db.QueryContext(ctx, "RETURN 1 AS x, 'hello' AS y")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var x int64
		var y string
		if err := rows.Scan(&x, &y); err != nil {
			log.Fatalf("Scan failed: %v", err)
		}
		fmt.Printf("x=%d, y=%s\n", x, y)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Rows error: %v", err)
	}

	// Query with parameters
	fmt.Println("\n--- Query with Parameters ---")
	rows, err = db.QueryContext(ctx, "RETURN ? AS value", 42)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var value int64
		if err := rows.Scan(&value); err != nil {
			log.Fatalf("Scan failed: %v", err)
		}
		fmt.Printf("value=%d\n", value)
	}

	// Transaction example
	fmt.Println("\n--- Transaction ---")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatalf("Begin transaction failed: %v", err)
	}

	// Execute queries within transaction
	_, err = tx.ExecContext(ctx, "RETURN 1")
	if err != nil {
		tx.Rollback()
		log.Fatalf("Exec in transaction failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Commit failed: %v", err)
	}
	fmt.Println("Transaction committed successfully!")

	// Prepared statement example
	fmt.Println("\n--- Prepared Statement ---")
	stmt, err := db.PrepareContext(ctx, "RETURN ? AS n")
	if err != nil {
		log.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Close()

	for i := 1; i <= 3; i++ {
		rows, err := stmt.QueryContext(ctx, i)
		if err != nil {
			log.Fatalf("Query failed: %v", err)
		}

		for rows.Next() {
			var n int64
			if err := rows.Scan(&n); err != nil {
				rows.Close()
				log.Fatalf("Scan failed: %v", err)
			}
			fmt.Printf("n=%d\n", n)
		}
		rows.Close()
	}

	fmt.Println("\nDone!")
}
