package geode

import (
	"strings"
	"testing"
)

// ============================================================================
// The Geode engine pin.
//
// These five references MUST MOVE TOGETHER, in the same commit:
//
//  1. Makefile                  GEODE_IMAGE
//  2. .gitlab-ci.yml            the integration:geode job's services[].name
//  3. defaultGeodeImage         (below) — used by getGeodeImage() when
//     GEODE_IMAGE is unset, i.e. every `go test -tags=integration ./...`
//     run that does not go through make
//  4. pinnedEngineVersion       (below) — asserted against SHOW VERSION by
//     TestIntegration_PinnedEngineVersion
//  5. ../geode/build.zig.zon    .version, in the engine source checkout
//
// The constants live in an untagged _test.go file so the guard below runs in
// the ordinary unit lane (`make test-unit`), not only under -tags=integration.
// ============================================================================

// defaultGeodeImage is the engine image used when GEODE_IMAGE is unset. It is
// pinned by tag *and* digest: the digest makes the pin immutable even if the
// tag is republished, which is what stops a supply-chain swap or a silent
// engine bump from changing what the release gate actually tests.
//
// Never point this at a floating tag such as :latest. TestEnginePinIsImmutable
// enforces that.
const defaultGeodeImage = "geodedb/geode:0.8.22@sha256:867d93375470fd18c60336036196a9ef9715a1922e063e8787dc05d8e798ddce"

// pinnedEngineVersion is the exact Geode engine release this driver is
// qualified against, as the engine reports it.
//
// The engine answers SHOW VERSION with a leading "v" (verified against
// geodedb/geode:0.8.22), so this constant carries it too. Compare the exact
// string the engine returns rather than normalising it away: a change in the
// reported format is itself a signal worth failing on.
const pinnedEngineVersion = "v0.8.22"

// TestEnginePinIsImmutable guards the engine pin without needing Docker: it
// fails if the default image ever becomes a floating reference, or if the
// image tag and pinnedEngineVersion drift apart.
func TestEnginePinIsImmutable(t *testing.T) {
	t.Run("not a floating tag", func(t *testing.T) {
		if strings.HasSuffix(defaultGeodeImage, ":latest") ||
			strings.Contains(defaultGeodeImage, ":latest@") {
			t.Fatalf("defaultGeodeImage = %q: the integration lane is a release gate and must not track a floating tag", defaultGeodeImage)
		}
	})

	t.Run("pinned by digest", func(t *testing.T) {
		idx := strings.Index(defaultGeodeImage, "@sha256:")
		if idx < 0 {
			t.Fatalf("defaultGeodeImage = %q: must be pinned by digest (name:tag@sha256:<64 hex>)", defaultGeodeImage)
		}
		digest := defaultGeodeImage[idx+len("@sha256:"):]
		if len(digest) != 64 {
			t.Fatalf("defaultGeodeImage digest = %q: want 64 hex characters, got %d", digest, len(digest))
		}
		for _, r := range digest {
			if !strings.ContainsRune("0123456789abcdef", r) {
				t.Fatalf("defaultGeodeImage digest = %q: contains non-hex character %q", digest, r)
			}
		}
	})

	t.Run("tag matches pinnedEngineVersion", func(t *testing.T) {
		want := ":" + strings.TrimPrefix(pinnedEngineVersion, "v") + "@"
		if !strings.Contains(defaultGeodeImage, want) {
			t.Fatalf("defaultGeodeImage = %q does not carry tag %q: the image pin and pinnedEngineVersion (%q) must move together",
				defaultGeodeImage, strings.Trim(want, ":@"), pinnedEngineVersion)
		}
	})
}
