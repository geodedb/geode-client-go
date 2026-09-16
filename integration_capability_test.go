//go:build integration

package geode

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// TestIntegration_ServerInfo records the real capability surface of the pinned
// engine and pins it to testdata/capabilities_0.8.18.golden, which the unit
// lane uses to keep the Cap* constants honest without Docker.
//
// Set GEODE_UPDATE_GOLDEN=1 to rewrite the golden from the live engine after a
// deliberate engine bump:
//
//	make test-integration-docker GOFLAGS="-run=TestIntegration_ServerInfo"
//	GEODE_UPDATE_GOLDEN=1 make test-integration-docker GOFLAGS="-run=TestIntegration_ServerInfo"
func TestIntegration_ServerInfo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg, err := ParseDSN(sharedDSN)
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}

	info, err := connector.ServerInfo(ctx)
	if err != nil {
		t.Fatalf("ServerInfo: %v", err)
	}

	// SHOW VERSION is kept verbatim, leading "v" included.
	if info.Version != pinnedEngineVersion {
		t.Fatalf("Version = %q, want %q", info.Version, pinnedEngineVersion)
	}

	if os.Getenv("GEODE_UPDATE_GOLDEN") == "1" {
		writeCapabilityGolden(t, info.Rows)
		t.Logf("rewrote %s from the live engine (%d rows)", capabilityGoldenPath, len(info.Rows))
	}

	// The raw rows, in order, are the artifact a capability-profile validator
	// consumes; compare them against the committed snapshot row for row.
	golden := readCapabilityGolden(t)
	if len(info.Rows) != len(golden) {
		t.Fatalf("engine emitted %d capability rows, golden has %d (run with GEODE_UPDATE_GOLDEN=1 after a deliberate engine bump)",
			len(info.Rows), len(golden))
	}
	for i := range golden {
		if info.Rows[i] != golden[i] {
			t.Errorf("row %d = %+v, golden has %+v", i+1, info.Rows[i], golden[i])
		}
	}

	// 58 boolean rows plus exactly one version-valued metadata row, which must
	// never have been coerced into the boolean map.
	if len(info.Capabilities) != 58 {
		t.Errorf("boolean capabilities = %d, want 58", len(info.Capabilities))
	}
	if info.Metadata[CapSonnyProfileVersion] != "1" {
		t.Errorf("Metadata[%s] = %q, want \"1\"", CapSonnyProfileVersion, info.Metadata[CapSonnyProfileVersion])
	}
	if len(info.Metadata) != 1 {
		t.Errorf("Metadata = %v, want exactly one entry", info.Metadata)
	}
	if _, ok := info.Capabilities[CapSonnyProfileVersion]; ok {
		t.Errorf("%s was coerced into the boolean capability map", CapSonnyProfileVersion)
	}

	// The nine capabilities the Sonny profile requires of an existing engine.
	for _, feat := range []string{
		CapVectorKNN, CapKNNMetricValidation,
		CapFTSSearch, CapFTSRebuild, CapFTSRowVisibility,
		CapReadOnlyExecute, CapExplain, CapProfile, CapExplainPolicy,
	} {
		if !info.Capabilities.Has(feat) {
			t.Errorf("capability %q is not advertised by engine %s", feat, info.Version)
		}
	}

	for _, row := range info.Rows {
		t.Logf("capability %-34s = %s", row.Feature, row.Supported)
	}
	t.Logf("HELLO capabilities: %v", info.HelloCapabilities)
	t.Logf("HELLO graph=%q session=%q", info.Graph, info.SessionID)
}

// TestIntegration_ServerInfoReadOnly proves capability discovery works on a
// read-only connector. Both statements are reads, so a consumer gating
// features on a read-only DSN must not have to drop the flag to discover them.
func TestIntegration_ServerInfoReadOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg, err := ParseDSN(sharedDSN + "&read_only=true")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if !cfg.ReadOnly {
		t.Fatal("ParseDSN did not set ReadOnly; the test would prove nothing")
	}
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}

	info, err := connector.ServerInfo(ctx)
	if err != nil {
		t.Fatalf("ServerInfo on a read-only connector: %v", err)
	}
	if info.Version != pinnedEngineVersion {
		t.Errorf("Version = %q, want %q", info.Version, pinnedEngineVersion)
	}
	if !info.Capabilities.Has(CapReadOnlyExecute) {
		t.Errorf("capability %q is not advertised", CapReadOnlyExecute)
	}
}

// TestIntegration_HelloCapabilitiesInheritedByPooledStreams proves a stream
// that skips the HELLO handshake still reports what the server said.
//
// pooledHello runs the handshake once per underlying QUIC connection, so every
// logical connection after the first sees no HelloResponse at all. Both
// connections are held open at once deliberately: the second must be a stream
// on the same already-authenticated QUIC connection for the inheritance to be
// under test.
func TestIntegration_HelloCapabilitiesInheritedByPooledStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg, err := ParseDSN(sharedDSN)
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}

	first, err := connector.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect (first): %v", err)
	}
	defer func() { _ = first.Close() }()

	second, err := connector.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect (second): %v", err)
	}
	defer func() { _ = second.Close() }()

	handshaker, ok := first.(*Conn)
	if !ok {
		t.Fatalf("first conn is %T, want *Conn", first)
	}
	inheritor, ok := second.(*Conn)
	if !ok {
		t.Fatalf("second conn is %T, want *Conn", second)
	}

	if len(handshaker.HelloCapabilities()) == 0 {
		t.Fatal("the handshaking connection reported no HELLO capabilities; the test would prove nothing")
	}
	if got, want := strings.Join(inheritor.HelloCapabilities(), ","), strings.Join(handshaker.HelloCapabilities(), ","); got != want {
		t.Errorf("inherited HELLO capabilities = %q, want %q", got, want)
	}
	if inheritor.SessionID() != handshaker.SessionID() {
		t.Errorf("inherited session id = %q, want %q", inheritor.SessionID(), handshaker.SessionID())
	}
	if inheritor.BoundGraph() != handshaker.BoundGraph() {
		t.Errorf("inherited bound graph = %q, want %q", inheritor.BoundGraph(), handshaker.BoundGraph())
	}
	t.Logf("HELLO capabilities: %v (session %q)", handshaker.HelloCapabilities(), handshaker.SessionID())
}

// writeCapabilityGolden rewrites the committed snapshot from live engine rows.
func writeCapabilityGolden(t *testing.T, rows []CapabilityRow) {
	t.Helper()

	var b strings.Builder
	for _, row := range rows {
		b.WriteString(row.Feature)
		b.WriteByte('\t')
		b.WriteString(row.Supported)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(capabilityGoldenPath, []byte(b.String()), 0o600); err != nil {
		t.Fatalf("write %s: %v", capabilityGoldenPath, err)
	}
}
