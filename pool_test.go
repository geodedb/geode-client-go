package geode

import (
	"testing"
	"time"
)

func TestParseDSN_Graph(t *testing.T) {
	tests := []struct {
		dsn   string
		graph string
	}{
		{"quic://localhost:3141?graph=mydb", "mydb"},
		{"grpc://localhost:50051?graph=test_graph", "test_graph"},
		{"localhost:3141?graph=abs_eagle3&user=admin", "abs_eagle3"},
		{"localhost:3141", ""},
		{"quic://localhost:3141?graph=", ""},
	}

	for _, tt := range tests {
		t.Run(tt.dsn, func(t *testing.T) {
			cfg, err := ParseDSN(tt.dsn)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.Graph != tt.graph {
				t.Errorf("expected graph %q, got %q", tt.graph, cfg.Graph)
			}
		})
	}
}

func TestDefaultPoolConfig(t *testing.T) {
	pc := defaultPoolConfig()
	if pc.maxOpenConns != DefaultMaxOpenConns {
		t.Errorf("expected maxOpenConns %d, got %d", DefaultMaxOpenConns, pc.maxOpenConns)
	}
	if pc.maxIdleConns != DefaultMaxIdleConns {
		t.Errorf("expected maxIdleConns %d, got %d", DefaultMaxIdleConns, pc.maxIdleConns)
	}
	if pc.connMaxLifetime != DefaultConnMaxLifetime {
		t.Errorf("expected connMaxLifetime %v, got %v", DefaultConnMaxLifetime, pc.connMaxLifetime)
	}
	if pc.connMaxIdleTime != DefaultConnMaxIdleTime {
		t.Errorf("expected connMaxIdleTime %v, got %v", DefaultConnMaxIdleTime, pc.connMaxIdleTime)
	}
}

func TestPoolOptions(t *testing.T) {
	pc := defaultPoolConfig()

	WithMaxOpenConns(32)(&pc)
	if pc.maxOpenConns != 32 {
		t.Errorf("expected maxOpenConns 32, got %d", pc.maxOpenConns)
	}

	WithMaxIdleConns(8)(&pc)
	if pc.maxIdleConns != 8 {
		t.Errorf("expected maxIdleConns 8, got %d", pc.maxIdleConns)
	}

	WithConnMaxLifetime(10 * time.Minute)(&pc)
	if pc.connMaxLifetime != 10*time.Minute {
		t.Errorf("expected connMaxLifetime 10m, got %v", pc.connMaxLifetime)
	}

	WithConnMaxIdleTime(15 * time.Second)(&pc)
	if pc.connMaxIdleTime != 15*time.Second {
		t.Errorf("expected connMaxIdleTime 15s, got %v", pc.connMaxIdleTime)
	}
}

func TestNewPool_InvalidDSN(t *testing.T) {
	_, err := NewPool("http://invalid:1234")
	if err == nil {
		t.Fatal("expected error for invalid DSN scheme")
	}
}

func TestNewPool_NoServer(t *testing.T) {
	// NewPool should fail on ping when no server is available.
	// Use a non-routable address to trigger a fast timeout.
	_, err := NewPool("quic://192.0.2.1:3141?connect_timeout=1")
	if err == nil {
		t.Fatal("expected error when server is unreachable")
	}
}

func TestConfigClone_PreservesGraph(t *testing.T) {
	cfg := &Config{
		Host:  "localhost",
		Port:  "3141",
		Graph: "testgraph",
	}
	clone := cfg.Clone()
	if clone.Graph != "testgraph" {
		t.Errorf("expected graph %q in clone, got %q", "testgraph", clone.Graph)
	}
}
