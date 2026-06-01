package geode_test

import (
	"testing"

	geode "geodedb.com/geode"
)

func TestNewConnector_ValidConfig(t *testing.T) {
	cfg := &geode.Config{
		Transport: geode.TransportQUIC,
		Host:      "localhost",
		Port:      geode.DefaultPort,
	}
	c, err := geode.NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector valid config: %v", err)
	}
	if c == nil {
		t.Fatal("NewConnector returned nil connector")
	}
	got := c.Config()
	if got.Host != "localhost" || got.Port != geode.DefaultPort {
		t.Errorf("Connector.Config() = {Host:%q Port:%q}, want {localhost:%q}", got.Host, got.Port, geode.DefaultPort)
	}
}

func TestNewConnector_InvalidConfig(t *testing.T) {
	// Empty host must fail validation.
	cfg := &geode.Config{
		Transport: geode.TransportQUIC,
		Port:      geode.DefaultPort,
		// Host intentionally empty
	}
	if _, err := geode.NewConnector(cfg); err == nil {
		t.Fatal("NewConnector with empty host: expected error, got nil")
	}
}

func TestNewConnector_NilConfig(t *testing.T) {
	if _, err := geode.NewConnector(nil); err == nil {
		t.Fatal("NewConnector(nil): expected error, got nil")
	}
}
