// Native Go client benchmark for Geode
//
// Usage: go run ./cmd/benchmark --host localhost --port 3142 --workers 10 --queries 1000
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	_ "go.codepros.org/geode"
)

type Result struct {
	Timestamp       string  `json:"timestamp"`
	DurationSeconds float64 `json:"duration_seconds"`
	TotalQueries    int64   `json:"total_queries"`
	Successful      int64   `json:"successful_queries"`
	Failed          int64   `json:"failed_queries"`
	ThroughputQPS   float64 `json:"throughput_qps"`
	LatencyP50MS    float64 `json:"latency_p50_ms"`
	LatencyP95MS    float64 `json:"latency_p95_ms"`
	LatencyP99MS    float64 `json:"latency_p99_ms"`
	LatencyAvgMS    float64 `json:"latency_avg_ms"`
	Workers         int     `json:"workers"`
	QueriesPerWork  int     `json:"queries_per_worker"`
	Status          string  `json:"status"`
}

func main() {
	host := flag.String("host", "localhost", "Server host")
	port := flag.Int("port", 3141, "Server port")
	workers := flag.Int("workers", 10, "Number of concurrent workers")
	queries := flag.Int("queries", 1000, "Queries per worker")
	insecure := flag.Bool("insecure", true, "Skip TLS verification")
	outputJSON := flag.Bool("json", false, "Output results as JSON")
	flag.Parse()

	dsn := fmt.Sprintf("%s:%d", *host, *port)
	if *insecure {
		dsn += "?insecure=true"
	}

	// Collect latencies and error summaries from all workers
	var mu sync.Mutex
	var allLatencies []float64
	var successful, failed int64
	var errMu sync.Mutex
	errCounts := make(map[string]int64)

	var wg sync.WaitGroup
	start := time.Now()

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			db, err := sql.Open("geode", dsn)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Worker %d: Open failed: %v\n", workerID, err)
				atomic.AddInt64(&failed, int64(*queries))
				return
			}
			defer db.Close()

			// Configure connection pool for benchmark - use single connection per worker
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)
			db.SetConnMaxLifetime(5 * time.Minute)

			workerLatencies := make([]float64, 0, *queries)

			for i := 0; i < *queries; i++ {
				qstart := time.Now()
				rows, err := db.QueryContext(context.Background(), "RETURN 1")
				latency := float64(time.Since(qstart).Microseconds()) / 1000.0

				if err != nil {
					if i < 3 && workerID == 0 {
						fmt.Fprintf(os.Stderr, "Worker %d query %d failed: %v\n", workerID, i, err)
					}
					errMu.Lock()
					errCounts[err.Error()]++
					errMu.Unlock()
					atomic.AddInt64(&failed, 1)
					continue
				}
				rows.Close()

				atomic.AddInt64(&successful, 1)
				workerLatencies = append(workerLatencies, latency)
			}

			mu.Lock()
			allLatencies = append(allLatencies, workerLatencies...)
			mu.Unlock()
		}(w)
	}

	wg.Wait()
	duration := time.Since(start).Seconds()

	// Calculate statistics
	var p50, p95, p99, avg float64
	if len(allLatencies) > 0 {
		sort.Float64s(allLatencies)
		p50 = allLatencies[len(allLatencies)/2]
		p95 = allLatencies[int(float64(len(allLatencies))*0.95)]
		p99 = allLatencies[int(float64(len(allLatencies))*0.99)]

		var sum float64
		for _, l := range allLatencies {
			sum += l
		}
		avg = sum / float64(len(allLatencies))
	}

	throughput := float64(successful) / duration

	status := "LIMITED"
	if throughput >= 10000 {
		status = "EXCELLENT"
	} else if throughput >= 1000 {
		status = "GOOD"
	} else if throughput >= 100 {
		status = "ACCEPTABLE"
	}

	result := Result{
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
		DurationSeconds: duration,
		TotalQueries:    successful + failed,
		Successful:      successful,
		Failed:          failed,
		ThroughputQPS:   throughput,
		LatencyP50MS:    p50,
		LatencyP95MS:    p95,
		LatencyP99MS:    p99,
		LatencyAvgMS:    avg,
		Workers:         *workers,
		QueriesPerWork:  *queries,
		Status:          status,
	}

	if *outputJSON {
		// Include error summary without changing existing fields
		payload := map[string]any{
			"timestamp":          result.Timestamp,
			"duration_seconds":   result.DurationSeconds,
			"total_queries":      result.TotalQueries,
			"successful_queries": result.Successful,
			"failed_queries":     result.Failed,
			"throughput_qps":     result.ThroughputQPS,
			"latency_p50_ms":     result.LatencyP50MS,
			"latency_p95_ms":     result.LatencyP95MS,
			"latency_p99_ms":     result.LatencyP99MS,
			"latency_avg_ms":     result.LatencyAvgMS,
			"workers":            result.Workers,
			"queries_per_worker": result.QueriesPerWork,
			"status":             result.Status,
			"error_summary":      errCounts,
		}
		json.NewEncoder(os.Stdout).Encode(payload)
	} else {
		fmt.Println("======================================================================")
		fmt.Println("GO CLIENT BENCHMARK RESULTS")
		fmt.Println("======================================================================")
		fmt.Printf("Duration: %.2fs\n", result.DurationSeconds)
		fmt.Printf("Workers: %d\n", result.Workers)
		fmt.Printf("Queries/worker: %d\n", result.QueriesPerWork)
		fmt.Println()
		fmt.Println("RESULTS:")
		fmt.Printf("  Total queries:    %d\n", result.TotalQueries)
		fmt.Printf("  Successful:       %d\n", result.Successful)
		fmt.Printf("  Failed:           %d\n", result.Failed)
		fmt.Printf("  Throughput:       %.0f qps\n", result.ThroughputQPS)
		fmt.Println()
		fmt.Println("LATENCY:")
		fmt.Printf("  p50:  %.2fms\n", result.LatencyP50MS)
		fmt.Printf("  p95:  %.2fms\n", result.LatencyP95MS)
		fmt.Printf("  p99:  %.2fms\n", result.LatencyP99MS)
		fmt.Printf("  avg:  %.2fms\n", result.LatencyAvgMS)
		fmt.Println()
		fmt.Printf("STATUS: %s\n", result.Status)
		fmt.Println("======================================================================")
	}
}
