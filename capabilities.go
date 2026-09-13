package geode

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
)

// Capability feature names emitted by `SHOW CAPABILITIES`, verified against
// geode 0.8.15 (src/execution/admin_ddl.zig:167-263). The engine documents
// these row names as stable: "every legacy row name below MUST keep its name
// and only become more truthful" (admin_ddl.zig:171-174).
//
// The pinned engine emits exactly 59 rows — 58 boolean-valued rows plus the
// version-valued sonny_profile_version — and testdata/capabilities_0.8.15.golden
// is the committed snapshot of them, asserted against a live engine by
// TestIntegration_ServerInfo and against these constants by
// TestCapabilityConstantsMatchEngine.
const (
	// --- legacy rows (Geode Auth DDL, GAP-0858 admin surface) ---

	CapAuthDDL         = "auth_ddl"
	CapAdminWritable   = "admin_writable"
	CapRoleDDL         = "role_ddl"
	CapGrantDDL        = "grant_ddl"
	CapShowPermissions = "show_permissions"
	CapShowVersion     = "show_version"
	CapPolicyDDL       = "policy_ddl"
	CapFLEDDL          = "fle_ddl"
	CapTenantDDL       = "tenant_ddl"

	// --- hybrid-RAG discovery rows ---

	CapVectorKNN           = "vector_knn"
	CapVectorIndexMetrics  = "vector_index_metrics"
	CapKNNMetricValidation = "knn_metric_validation"
	CapFTSSearch           = "fts_search"
	CapFTSPhraseProximity  = "fts_phrase_proximity"
	CapFTSRebuild          = "fts_rebuild"
	CapFTSBulkMergeLive    = "fts_bulk_merge_live_maintenance"
	CapFTSRowVisibility    = "fts_row_visibility"
	CapTSProcedures        = "ts_procedures"
	CapReadOnlyExecute     = "read_only_execute"
	CapMCPReadOnly         = "mcp_read_only"
	CapExplain             = "explain"
	CapProfile             = "profile"
	CapExplainPolicy       = "explain_policy"

	// --- Sonny GraphRAG capability profile rows (GEO-UP-001 / ENG-4653) ---

	// CapSonnyProfileVersion is version-valued metadata, NOT a boolean. Its
	// value is the profile version string (currently "1") and it is compared
	// exactly as a string; it never appears in the Capabilities map. See
	// ServerInfo.Metadata.
	CapSonnyProfileVersion = "sonny_profile_version"

	// Required booleans.
	CapReadOnlyCoversAdminDDL         = "read_only_covers_admin_ddl"
	CapStrictTenancyProductionDefault = "strict_tenancy_production_default"
	CapTenantScopeFailClosed          = "tenant_scope_fail_closed"
	CapTenantBindingServerVerified    = "tenant_binding_server_verified"
	CapFTSStrictTenancy               = "fts_strict_tenancy"
	CapFTSMultiGraph                  = "fts_multi_graph"
	CapKNNRowVisibility               = "knn_row_visibility"
	CapKNNDeleteLiveness              = "knn_delete_liveness"
	CapGraphProcedureTenancy          = "graph_procedure_tenancy"
	CapEmbeddingSpacePinning          = "embedding_space_pinning"
	CapVectorIndexGenerations         = "vector_index_generations"
	CapVectorGenerationRollback       = "vector_generation_rollback"
	CapVectorBuildBounded             = "vector_build_bounded"
	CapVectorIndexRestartRebuild      = "vector_index_restart_rebuild"
	CapBulkVectorIndexMaintenance     = "bulk_vector_index_maintenance"
	CapBulkCreateFTSMaintenance       = "bulk_create_fts_maintenance"
	CapBulkDeleteBatch                = "bulk_delete_batch"
	CapIndexBuildTelemetry            = "index_build_telemetry"
	CapRetrievalReady                 = "retrieval_ready"
	CapTraversalMultiEdgeType         = "traversal_multi_edge_type"
	CapTraversalBoundedNodesCostTime  = "traversal_bounded_nodes_cost_time"
	CapTraversalCycleHubControl       = "traversal_cycle_hub_control"
	CapTraversalCancellation          = "traversal_cancellation"
	CapTraversalDeterministicTies     = "traversal_deterministic_ties"
	CapTraversalPathProvenance        = "traversal_path_provenance"
	CapWeightedPathRawProperty        = "weighted_path_raw_property"
	CapTemporalTraversalPredicates    = "temporal_traversal_predicates"
	CapCDCOrderedResumable            = "cdc_ordered_resumable"
	CapCDCTenantBound                 = "cdc_tenant_bound"
	CapCDCBackpressureDLQObservable   = "cdc_backpressure_dlq_observable"

	// Optional booleans. Each carries gate "sonny_feature_gate" in the Sonny
	// profile manifest: unsupported or missing support disables the dependent
	// feature rather than failing the profile.
	CapKNNFilteredANNPushdown           = "knn_filtered_ann_pushdown"
	CapFederationAuthorizationPreserved = "federation_authorization_preserved"
	CapFederationDeterministicTopK      = "federation_deterministic_topk"
	CapFederationPartialShardErrors     = "federation_partial_shard_errors"
	CapGraphCommunitySummaryLayer       = "graph_community_summary_layer"
)

// allCapabilityNames is every feature name this package declares a constant
// for, in the order the pinned engine emits them.
// TestCapabilityConstantsMatchEngine pins it against the committed golden in
// both directions, so a constant with no engine row — or an engine row with no
// constant — fails the unit lane.
//
//nolint:gochecknoglobals // package-level table; the tests walk it
var allCapabilityNames = []string{
	CapAuthDDL, CapAdminWritable, CapRoleDDL, CapGrantDDL, CapShowPermissions,
	CapShowVersion, CapPolicyDDL, CapFLEDDL, CapTenantDDL,
	CapVectorKNN, CapVectorIndexMetrics, CapKNNMetricValidation, CapFTSSearch,
	CapFTSPhraseProximity, CapFTSRebuild, CapFTSBulkMergeLive,
	CapFTSRowVisibility, CapTSProcedures, CapReadOnlyExecute, CapMCPReadOnly,
	CapExplain, CapProfile, CapExplainPolicy,
	CapSonnyProfileVersion,
	CapReadOnlyCoversAdminDDL, CapStrictTenancyProductionDefault,
	CapTenantScopeFailClosed, CapTenantBindingServerVerified,
	CapFTSStrictTenancy, CapFTSMultiGraph, CapKNNRowVisibility,
	CapKNNDeleteLiveness, CapKNNFilteredANNPushdown, CapGraphProcedureTenancy,
	CapEmbeddingSpacePinning, CapVectorIndexGenerations,
	CapVectorGenerationRollback, CapVectorBuildBounded,
	CapVectorIndexRestartRebuild, CapBulkVectorIndexMaintenance,
	CapBulkCreateFTSMaintenance, CapBulkDeleteBatch, CapIndexBuildTelemetry,
	CapRetrievalReady, CapTraversalMultiEdgeType,
	CapTraversalBoundedNodesCostTime, CapTraversalCycleHubControl,
	CapTraversalCancellation, CapTraversalDeterministicTies,
	CapTraversalPathProvenance, CapWeightedPathRawProperty,
	CapTemporalTraversalPredicates, CapCDCOrderedResumable, CapCDCTenantBound,
	CapCDCBackpressureDLQObservable, CapFederationAuthorizationPreserved,
	CapFederationDeterministicTopK, CapFederationPartialShardErrors,
	CapGraphCommunitySummaryLayer,
}

// knownMetadataFeatures is the closed allow-list of feature names whose
// `supported` value is deliberately NOT a boolean.
//
// It is closed on purpose. The Sonny capability profile contract requires that
// "future non-boolean metadata kinds require a negotiated contract update", so
// an unrecognised feature carrying an unrecognised value is a malformed row,
// not a new metadata kind this driver may invent a meaning for. Failing closed
// here is what stops `supported: "1"` on some future row from being read as
// support.
//
//nolint:gochecknoglobals // package-level lookup table
var knownMetadataFeatures = map[string]bool{
	CapSonnyProfileVersion: true,
}

// The two discovery statements. Both are read-only: SHOW is on the read-only
// leading allow-list and neither carries a mutating keyword, so they pass the
// client classifier on a read_only=true connection
// (TestShowStatementsClassifyReadOnly), and the engine agrees — both build
// their results locally with no storage access at all (geode 0.8.15,
// src/execution/admin_ddl.zig:160-165 and :167-263).
const (
	showVersionStatement      = "SHOW VERSION"
	showCapabilitiesStatement = "SHOW CAPABILITIES"
)

// Discovery bounds, mirroring the Sonny capability profile contract's limits
// so a hostile or broken server cannot make a consumer allocate without bound.
const (
	maxCapabilityRows      = 256
	maxCapabilityFieldSize = 128
	// capabilityColumns is the column count SHOW CAPABILITIES must answer
	// with: feature, supported.
	capabilityColumns = 2
)

// The only two values the `supported` column may carry for a boolean row.
// They are matched byte-exactly: no trimming, no case folding, no coercion.
const (
	supportedTrue  = "true"
	supportedFalse = "false"
)

// ErrMalformedCapabilityRows is returned when `SHOW CAPABILITIES` does not
// answer with the shape the capability contract requires: two string columns
// named feature and supported, in that order, with unique non-empty feature
// names, and values that are exactly "true"/"false" except for the
// known-metadata rows.
//
// It is deliberately an error and not a silent false. Discovery and
// requirements are different artifacts: a consumer that silently downgraded an
// unparseable row to "unsupported" would report a capability gap where the
// real fault is a protocol mismatch.
var ErrMalformedCapabilityRows = errors.New("geode: malformed SHOW CAPABILITIES rows")

// CapabilityRow is one raw `SHOW CAPABILITIES` row, exactly as the engine
// emitted it: two owned strings, unparsed and uncoerced.
//
// Consumers that validate against a capability profile manifest
// (devnw.dev/geode/capability) must use these raw rows rather than the derived
// boolean map, because the profile compares metadata values as exact strings
// and treats a non-boolean value as a distinct condition from false.
type CapabilityRow struct {
	// Feature is the value of the `feature` column.
	Feature string
	// Supported is the value of the `supported` column, verbatim. It is
	// "true"/"false" for boolean rows and a version string for metadata rows.
	Supported string
}

// Capabilities is the engine's self-reported boolean feature matrix, keyed by
// the feature name `SHOW CAPABILITIES` emits.
//
// It contains ONLY rows whose `supported` value was exactly "true" or "false".
// Version-valued metadata rows such as sonny_profile_version are never in this
// map; they live in ServerInfo.Metadata and are compared as exact strings.
type Capabilities map[string]bool

// Has reports whether the engine advertises the feature as supported.
// An unknown feature is unsupported — capability gates fail closed.
func (c Capabilities) Has(feature string) bool { return c[feature] }

// Names returns every advertised boolean feature name, supported or not.
func (c Capabilities) Names() []string {
	out := make([]string, 0, len(c))
	for k := range c {
		out = append(out, k)
	}
	return out
}

// ServerInfo is a point-in-time snapshot of the engine a connector talks to.
//
// Nothing here is cached: each Connector.ServerInfo call opens one connection,
// asks the engine, and closes it again.
type ServerInfo struct {
	// Version is the single `version` column of SHOW VERSION, verbatim. The
	// pinned engine answers with a leading "v" (e.g. "v0.8.15"); the string is
	// not normalised, because a change in the reported format is itself worth
	// failing on. Consumers that compare against a SemVer floor are
	// responsible for adapting the producer format.
	Version string

	// Rows is every SHOW CAPABILITIES row in the order the engine emitted
	// them, raw and uncoerced. This is the input a capability-profile
	// validator must use.
	Rows []CapabilityRow

	// Capabilities is the boolean subset of Rows.
	Capabilities Capabilities

	// Metadata is the non-boolean subset of Rows, keyed by feature name — at
	// 0.8.15 exactly one entry, sonny_profile_version. Values are compared as
	// exact strings, never converted to a boolean.
	Metadata map[string]string

	// HelloCapabilities is the repeated capabilities string list the server
	// sent in its HELLO response, in order.
	//
	// HELLO capabilities and SHOW CAPABILITIES are DIFFERENT VOCABULARIES and
	// neither is derived from the other: HELLO advertises session/protocol
	// capabilities negotiated at handshake time, while SHOW CAPABILITIES is
	// the engine's feature matrix. A name present in one says nothing about
	// the other, and an empty HelloCapabilities does not mean an empty feature
	// matrix. Both are surfaced because neither subsumes the other.
	HelloCapabilities []string

	// Graph is the graph the server bound during HELLO, empty when unbound.
	Graph string

	// SessionID is the session identifier the server issued in HELLO.
	SessionID string
}

// ServerInfo probes the engine for its version, capability matrix and HELLO
// metadata.
//
// It opens exactly ONE connection, using the connector's configuration —
// Config.ReadOnly included, since both statements it issues are reads — and
// closes it before returning. Nothing is cached: every call re-reads the
// engine, so a caller that needs a stable snapshot must keep the returned
// value.
//
// The connection is DEDICATED, not borrowed from the connector's shared QUIC
// pool, for two reasons. First, the HELLO fields reported below then come from
// a handshake this call actually performed, rather than from whichever earlier
// stream happened to authenticate the pooled connection. Second, a probe must
// not disturb the application's pool: the engine tears the QUIC connection
// down when the stream that carried HELLO closes as the last stream on it, so
// borrowing a pooled connection and closing it would leave the next stream on
// that pooled connection failing with a remote close. Discovery is
// short-lived and infrequent, so the extra handshake is the right trade.
func (c *Connector) ServerInfo(ctx context.Context) (*ServerInfo, error) {
	conn, err := connect(ctx, c.cfg)
	if err != nil {
		return nil, fmt.Errorf("geode: server info: connect: %w", err)
	}
	defer func() { _ = conn.Close() }()

	return conn.serverInfo(ctx)
}

// serverInfo runs the two discovery statements on one connection and folds the
// HELLO metadata that connection already carries into the result.
func (c *Conn) serverInfo(ctx context.Context) (*ServerInfo, error) {
	version, err := c.showVersion(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := c.showCapabilities(ctx)
	if err != nil {
		return nil, err
	}

	caps, meta, err := parseCapabilityRows(rows)
	if err != nil {
		return nil, err
	}

	return &ServerInfo{
		Version:           version,
		Rows:              rows,
		Capabilities:      caps,
		Metadata:          meta,
		HelloCapabilities: c.HelloCapabilities(),
		Graph:             c.BoundGraph(),
		SessionID:         c.SessionID(),
	}, nil
}

// showVersion reads the single-column, single-row SHOW VERSION result.
func (c *Conn) showVersion(ctx context.Context) (string, error) {
	rows, err := c.QueryContext(ctx, showVersionStatement, nil)
	if err != nil {
		return "", fmt.Errorf("geode: %s: %w", showVersionStatement, err)
	}
	defer func() { _ = rows.Close() }()

	cols := rows.Columns()
	if len(cols) != 1 || cols[0] != "version" {
		return "", fmt.Errorf("geode: %s: unexpected columns %v, want [version]", showVersionStatement, cols)
	}

	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		if errors.Is(err, io.EOF) {
			return "", fmt.Errorf("geode: %s: returned no rows", showVersionStatement)
		}
		return "", fmt.Errorf("geode: %s: %w", showVersionStatement, err)
	}

	version, ok := capabilityString(dest[0])
	if !ok {
		return "", fmt.Errorf("geode: %s: version column is %T, want a string", showVersionStatement, dest[0])
	}
	return version, nil
}

// showCapabilities reads every SHOW CAPABILITIES row verbatim, validating only
// the result SHAPE here — column count, order and value type. Row-level
// semantics (duplicates, empty names, value vocabulary) belong to
// parseCapabilityRows so they can be unit-tested without a server.
func (c *Conn) showCapabilities(ctx context.Context) ([]CapabilityRow, error) {
	rows, err := c.QueryContext(ctx, showCapabilitiesStatement, nil)
	if err != nil {
		return nil, fmt.Errorf("geode: %s: %w", showCapabilitiesStatement, err)
	}
	defer func() { _ = rows.Close() }()

	cols := rows.Columns()
	if len(cols) != capabilityColumns || cols[0] != "feature" || cols[1] != "supported" {
		return nil, fmt.Errorf("%w: columns are %v, want [feature supported]", ErrMalformedCapabilityRows, cols)
	}

	out := make([]CapabilityRow, 0, len(allCapabilityNames))
	dest := make([]driver.Value, capabilityColumns)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("geode: %s: %w", showCapabilitiesStatement, err)
		}
		if len(out) >= maxCapabilityRows {
			return nil, fmt.Errorf("%w: more than %d rows", ErrMalformedCapabilityRows, maxCapabilityRows)
		}
		feature, ok := capabilityString(dest[0])
		if !ok {
			return nil, fmt.Errorf("%w: feature column of row %d is %T, want a string",
				ErrMalformedCapabilityRows, len(out)+1, dest[0])
		}
		supported, ok := capabilityString(dest[1])
		if !ok {
			return nil, fmt.Errorf("%w: supported column of row %q is %T, want a string",
				ErrMalformedCapabilityRows, feature, dest[1])
		}
		out = append(out, CapabilityRow{Feature: feature, Supported: supported})
	}
	return out, nil
}

// capabilityString accepts the two representations a string column can arrive
// as and refuses every other type. A bool is deliberately NOT accepted: the
// contract fixes `supported` as a string column, and silently accepting a
// different wire type would hide exactly the protocol change a consumer needs
// to see.
func capabilityString(v driver.Value) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case []byte:
		return string(x), true
	default:
		return "", false
	}
}

// parseCapabilityRows splits raw rows into the boolean feature matrix and the
// non-boolean metadata map, failing closed on anything malformed.
//
// The rules come from the Sonny capability profile contract ("Pure
// validation"): reject duplicate feature names, empty names and over-long
// fields; keep values uncoerced and untrimmed; treat a value that is not
// exactly "true"/"false" as metadata only when the feature is on the closed
// known-metadata allow-list, and as an error otherwise.
//
// Extra well-formed boolean rows from a newer engine are accepted — a future
// engine may append capabilities — but an unknown feature with an unknown
// value kind is refused, because guessing its meaning is how a gate fails open.
func parseCapabilityRows(rows []CapabilityRow) (Capabilities, map[string]string, error) {
	caps := make(Capabilities, len(rows))
	meta := make(map[string]string)
	seen := make(map[string]bool, len(rows))

	for i, row := range rows {
		switch {
		case row.Feature == "":
			return nil, nil, fmt.Errorf("%w: row %d has an empty feature name", ErrMalformedCapabilityRows, i+1)
		case len(row.Feature) > maxCapabilityFieldSize:
			return nil, nil, fmt.Errorf("%w: feature name of row %d exceeds %d bytes",
				ErrMalformedCapabilityRows, i+1, maxCapabilityFieldSize)
		case len(row.Supported) > maxCapabilityFieldSize:
			return nil, nil, fmt.Errorf("%w: supported value of %q exceeds %d bytes",
				ErrMalformedCapabilityRows, row.Feature, maxCapabilityFieldSize)
		case seen[row.Feature]:
			return nil, nil, fmt.Errorf("%w: feature %q appears more than once", ErrMalformedCapabilityRows, row.Feature)
		}
		seen[row.Feature] = true

		if value, ok := parseSupportedFlag(row.Supported); ok {
			caps[row.Feature] = value
			continue
		}
		if !knownMetadataFeatures[row.Feature] {
			return nil, nil, fmt.Errorf("%w: feature %q has non-boolean value %q and is not known metadata",
				ErrMalformedCapabilityRows, row.Feature, row.Supported)
		}
		meta[row.Feature] = row.Supported
	}
	return caps, meta, nil
}

// parseSupportedFlag converts a `supported` column value to a boolean. It
// reports ok=false for anything that is not exactly "true" or "false".
//
// The strictness is the point. The contract forbids coercion: `1` is the value
// of sonny_profile_version, and a permissive parser that read "1"/"yes"/"on"
// as true would turn a metadata row into a supported feature, while one that
// case-folded "TRUE" would accept a value the profile validator rejects. The
// two implementations must agree on the vocabulary or the driver and the
// validator disagree about what the engine said.
func parseSupportedFlag(supported string) (value, ok bool) {
	switch supported {
	case supportedTrue:
		return true, true
	case supportedFalse:
		return false, true
	default:
		return false, false
	}
}
