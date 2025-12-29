Geode Go Client (database/sql driver)

Usage

- Install in your Go module by adding a replace to this path or copying into your project.
- Connect using database/sql with driver name "geode".
- The driver spawns the geoded process and communicates over its JSON line protocol (stdin/stdout).

Example

package main

import (
	"database/sql"
	_ "geode/clients/go/geode"
	"fmt"
)

func main() {
	db, err := sql.Open("geode", "process:./zig-out/bin/geoded?page_size=10")
	if err != nil { panic(err) }
	defer db.Close()
	rows, err := db.Query("RETURN 1 AS x ORDER BY x LIMIT 1")
	if err != nil { panic(err) }
	defer rows.Close()
	for rows.Next() {
		var x int64
		if err := rows.Scan(&x); err != nil { panic(err) }
		fmt.Println("x=", x)
	}
}

DSN

- process:<path>?page_size=N&format=(json|txt)&hello_name=&hello_ver=&conformance=
- If DSN is empty, GEODE_SERVER_PATH env var may provide the server path; otherwise defaults to ./zig-out/bin/geoded

Transactions

- Use db.Begin / tx.Commit / tx.Rollback. The server supports single active transaction per connection.

Notes

- The server produces SCHEMA then BINDINGS pages; the driver handles paging by issuing PULL requests. It stops on final=true or NoResult.
- PROFILE queries return SCHEMA then PROFILE; the driver exposes an empty result set for such statements.
