package geode

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"geodedb.com/geode/proto"
	gpb "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// GAP-0841: the vendored proto/geode.proto HelloRequest must stay in sync with
// the canonical server proto (geode/src/proto/geode.proto). In particular it
// must carry tenant_id (field 3) and role (field 8) so the Go client can drive
// native multi-tenancy and FLE. The tests below are a drift guard: a future
// regeneration that drops either field will fail here (and the struct-field
// assertions will even fail to compile if the generated Go type loses Role).

// requiredHelloFields is the canonical HelloRequest field set the vendored proto
// must contain. Keep this in sync with geode/src/proto/geode.proto HelloRequest.
var requiredHelloFields = map[protoreflect.FieldNumber]struct {
	name     string
	optional bool
}{
	1: {"username", false},
	2: {"password", false},
	3: {"tenant_id", true},
	4: {"client_name", false},
	5: {"client_version", false},
	6: {"wanted_conformance", false},
	7: {"graph", true},
	8: {"role", true},
}

// TestHelloRequest_HasTenantAndRole asserts, via the runtime protobuf
// descriptor, that the generated HelloRequest still carries every canonical
// field — most importantly tenant_id and role. If a regeneration drops a field
// this fails, catching silent proto drift.
func TestHelloRequest_HasTenantAndRole(t *testing.T) {
	desc := (&proto.HelloRequest{}).ProtoReflect().Descriptor()
	fields := desc.Fields()

	for num, want := range requiredHelloFields {
		fd := fields.ByNumber(num)
		if fd == nil {
			t.Errorf("HelloRequest is missing field %d (%s) — proto drift?", num, want.name)
			continue
		}
		if string(fd.Name()) != want.name {
			t.Errorf("HelloRequest field %d: got name %q, want %q", num, fd.Name(), want.name)
		}
		if fd.HasOptionalKeyword() != want.optional {
			t.Errorf("HelloRequest field %d (%s): optional=%v, want %v", num, want.name, fd.HasOptionalKeyword(), want.optional)
		}
		if fd.Kind() != protoreflect.StringKind {
			t.Errorf("HelloRequest field %d (%s): kind=%v, want string", num, want.name, fd.Kind())
		}
	}

	// Guard against extra/unknown fields creeping in undocumented.
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if _, ok := requiredHelloFields[fd.Number()]; !ok {
			t.Errorf("HelloRequest has unexpected field %d (%s); update requiredHelloFields and the canonical proto", fd.Number(), fd.Name())
		}
	}
}

// TestHelloRequest_TenantAndRoleSettable asserts TenantId and Role are settable
// optional-string pointers that survive a marshal/unmarshal roundtrip. This
// fails to compile if a regeneration drops the Role field from the struct.
func TestHelloRequest_TenantAndRoleSettable(t *testing.T) {
	tenant := "acme"
	role := "analyst"
	original := &proto.HelloRequest{
		Username: "u",
		Password: "p",
		TenantId: &tenant,
		Role:     &role,
	}

	data, err := gpb.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	decoded := &proto.HelloRequest{}
	if err := gpb.Unmarshal(data, decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.GetTenantId() != tenant {
		t.Errorf("tenant_id roundtrip: got %q, want %q", decoded.GetTenantId(), tenant)
	}
	if decoded.GetRole() != role {
		t.Errorf("role roundtrip: got %q, want %q", decoded.GetRole(), role)
	}
}

// helloFieldRe extracts "<number> <name>" pairs from a proto HelloRequest body.
var helloFieldRe = regexp.MustCompile(`(?m)^\s*(?:optional\s+|repeated\s+)?\w+\s+(\w+)\s*=\s*(\d+)\s*;`)

// extractHelloRequestFields returns name->number for the HelloRequest message in
// the given .proto source text.
func extractHelloRequestFields(src string) map[string]string {
	start := strings.Index(src, "message HelloRequest {")
	if start < 0 {
		return nil
	}
	body := src[start:]
	if end := strings.Index(body, "}"); end >= 0 {
		body = body[:end]
	}
	out := map[string]string{}
	for _, m := range helloFieldRe.FindAllStringSubmatch(body, -1) {
		out[m[1]] = m[2]
	}
	return out
}

// TestVendoredProtoMatchesCanonical is a best-effort drift check against the
// server's canonical proto. When the sibling geode repo is not on disk (e.g. in
// CI for this standalone repo) it skips — the reflection-based guard above is
// the always-on backstop. Override the canonical path with GEODE_CANONICAL_PROTO.
func TestVendoredProtoMatchesCanonical(t *testing.T) {
	canonical := os.Getenv("GEODE_CANONICAL_PROTO")
	if canonical == "" {
		canonical = filepath.Join("..", "geode", "src", "proto", "geode.proto")
	}

	// Path is a fixed relative default or a test-controlled env override, not
	// untrusted input; this is a build-time drift guard, not a runtime path.
	canonicalSrc, err := os.ReadFile(canonical) //nolint:gosec // G304/G703: test-controlled path
	if err != nil {
		t.Skipf("canonical server proto not reachable (%s): %v — relying on reflection drift guard", canonical, err)
	}
	vendoredSrc, err := os.ReadFile(filepath.Join("proto", "geode.proto"))
	if err != nil {
		t.Fatalf("read vendored proto: %v", err)
	}

	want := extractHelloRequestFields(string(canonicalSrc))
	got := extractHelloRequestFields(string(vendoredSrc))
	if len(want) == 0 {
		t.Fatalf("could not parse HelloRequest from canonical proto %s", canonical)
	}

	if !equalFieldMaps(want, got) {
		t.Errorf("vendored HelloRequest drifted from canonical %s:\n  canonical: %s\n  vendored:  %s\nrun `make generate-proto` to resync",
			canonical, fmtFieldMap(want), fmtFieldMap(got))
	}
}

func equalFieldMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func fmtFieldMap(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return strings.Join(parts, " ")
}
