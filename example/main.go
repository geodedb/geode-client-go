package main

import (
	"database/sql"
	_ "geode/clients/go/geode"
	"fmt"
)

func main() {
	db, err := sql.Open("geode", "process:./zig-out/bin/geoded?page_size=5")
	if err != nil { panic(err) }
	defer db.Close()
	rows, err := db.Query("RETURN 1 AS x ORDER BY x LIMIT 1")
	if err != nil { panic(err) }
	defer rows.Close()
	for rows.Next() {
		var x int64
		if err := rows.Scan(&x); err != nil { panic(err) }
		fmt.Println(x)
	}
}
