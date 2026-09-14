// doc_graphrag_test.go holds the runnable documentation for the GraphRAG
// surface added in v0.2.0 (SONNY-806 / GEO-GO-UP-001, Lane D Task 15):
// typed parameter and vector binding, capability discovery, session pinning
// and retry classification.
//
// These are Example functions, so `go vet` compiles them and `go test` runs
// them — documentation that cannot drift out of the API without failing the
// build. They are written to run WITHOUT a server: there is nothing listening
// in a unit-test environment, so each connecting example logs the connection
// error and prints its fixed output rather than fatally exiting. What they
// assert is the SHAPE of the call, which is what a reader copies.
//
// The connect timeouts are deliberately short for the same reason: an example
// that cannot connect should cost the unit lane milliseconds, not seconds. A
// real caller picks a timeout that suits its workload.
package geode_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"geodedb.com/geode"
)

// exampleTimeout bounds the doomed connection attempt in each example.
const exampleTimeout = 500 * time.Millisecond

// ExampleVector shows binding an embedding as a parameter. Values never
// appear in query text: the `?` placeholders become typed wire parameters
// ($_gq0, $_gq1) and the vector travels in ExecuteRequest.params.
func ExampleVector() {
	db, err := sql.Open("geode",
		"quic://localhost:3141?tenant=acme&graph=context&read_only=true")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), exampleTimeout)
	defer cancel()

	var ids any
	err = db.QueryRowContext(ctx,
		"RETURN knn('embedding', ?, ?, 'cosine')",
		geode.Vector{0.1, 0.2, 0.3}, 10,
	).Scan(&ids)
	if err != nil {
		log.Print(err)
	}
	fmt.Println("ok")
	// Output: ok
}

// ExampleConnector_ServerInfo shows fail-closed capability gating at startup:
// a feature the engine does not advertise is unsupported, and so is a probe
// that could not be completed.
func ExampleConnector_ServerInfo() {
	cfg, err := geode.ParseDSN("quic://localhost:3141")
	if err != nil {
		log.Fatal(err)
	}
	connector, err := geode.NewConnector(cfg)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), exampleTimeout)
	defer cancel()

	info, err := connector.ServerInfo(ctx)
	if err != nil {
		log.Print(err)
		fmt.Println("ok")
		return
	}
	if !info.Capabilities.Has(geode.CapVectorKNN) {
		log.Printf("engine %s lacks vector_knn", info.Version)
	}
	fmt.Println("ok")
	// Output: ok
}

// ExamplePool_Session shows pinning one connection for a multi-statement
// ingest, so the unit of work cannot straddle connections.
func ExamplePool_Session() {
	// NewPool dials eagerly, so with no server listening this is where the
	// example stops. A real caller treats a pool it could not build as fatal.
	pool, err := geode.NewPool("quic://localhost:3141?tenant=acme&graph=context")
	if err != nil {
		log.Print(err)
		fmt.Println("ok")
		return
	}
	defer func() { _ = pool.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), exampleTimeout)
	defer cancel()

	s, err := pool.Session(ctx)
	if err != nil {
		log.Print(err)
		fmt.Println("ok")
		return
	}
	defer func() { _ = s.Close() }()

	err = s.TX(ctx, nil, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "MERGE (n:Chunk {chunk_id: ?})", "c-1")
		return err
	})
	if err != nil && geode.Classify(err) != geode.RetryNever {
		log.Printf("retryable (%s): %v", geode.Classify(err), err)
	}
	fmt.Println("ok")
	// Output: ok
}

// ExampleClassify shows the decision a bulk loader actually makes. It needs no
// server: classification is a pure function of the error.
//
// Note the two ends of the scale. A read-only refusal is deterministic, so
// retrying it is a hot loop against an engine that is already saying no; a
// connection that went bad is worth another attempt, but only on a DIFFERENT
// connection.
func ExampleClassify() {
	fmt.Println(geode.Classify(nil))
	fmt.Println(geode.Classify(geode.ErrReadOnly))
	fmt.Println(geode.Classify(context.Canceled))
	fmt.Println(geode.Classify(errors.New("something the driver has never seen")))
	// Output:
	// never
	// never
	// never
	// never
}
