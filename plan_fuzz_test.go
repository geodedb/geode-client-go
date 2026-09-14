package geode

import (
	"strconv"
	"strings"
	"testing"
)

// FuzzPlanTextDecode fuzzes the text-row EXPLAIN/PROFILE decoders.
//
// These are the only parsers in the query-plan path: the structured payload
// arrives already typed from protobuf, but the text-row fallback is a string
// parser fed by whatever an engine chose to put in a `plan` or `metric`/`value`
// column. A server that is old, hostile, or simply differently configured can
// put anything there, and a retrieval path must not lose a goroutine to it.
//
// Invariants:
//   - neither decoder panics on any input;
//   - every text row is preserved in Text, so a log reader sees what arrived
//     even when nothing parsed;
//   - no more operators than rows, and no operator with an empty or untrimmed
//     kind (an empty kind would make ExplainPlan.UsesIndex meaningless);
//   - a POSITIVE time_ms never produces a negative TotalTimeNS. That is the
//     overflow this target exists to hold: time_ms is multiplied by 1e6, and
//     an unchecked multiply turns a large millisecond count into a negative
//     duration.
func FuzzPlanTextDecode(f *testing.F) {
	f.Add("EXPLAIN\nMATCH\n  IndexSeek (cost: 120)\nTotal cost: 128")
	f.Add("IndexSeek\nFilter\nProject")
	f.Add("profile\tplanned_query_profile\nrows\t42\ntime_ns\t184213")
	f.Add("time_ms\t9223372036854775807")
	// The M1 false positive: parseProfileMetrics takes the LAST timing row it
	// sees, so a time_ns row after a time_ms row legitimately zeroes
	// TotalTimeNS. The invariant is about the multiply wrapping, not about
	// which row won.
	f.Add("time_ms\t5\ntime_ns\t0")
	f.Add("rows\t-1\ncolumns\t-1\noperators\t-1")
	f.Add("")
	f.Add("\n\n   \n\t\n")
	f.Add("Total cost: not-a-number")
	f.Add("  (cost: 1)")
	f.Add(strings.Repeat("IndexSeek\n", 64))

	f.Fuzz(func(t *testing.T, text string) {
		lines := strings.Split(text, "\n")

		plan := explainPlanFromRows(lines)
		if plan == nil {
			t.Fatal("explainPlanFromRows returned nil")
		}
		if len(plan.Text) != len(lines) {
			t.Fatalf("len(Text) = %d, want every row preserved (%d)", len(plan.Text), len(lines))
		}
		if len(plan.Ops) > len(lines) {
			t.Fatalf("%d operators from %d rows", len(plan.Ops), len(lines))
		}
		for i, op := range plan.Ops {
			if op.Kind == "" {
				t.Fatalf("Ops[%d] has an empty kind", i)
			}
			if strings.TrimSpace(op.Kind) != op.Kind {
				t.Fatalf("Ops[%d].Kind = %q is not trimmed", i, op.Kind)
			}
			if uint32(i) != op.Index {
				t.Fatalf("Ops[%d].Index = %d", i, op.Index)
			}
		}
		_ = plan.UsesIndex()

		checkProfileMetricInvariants(t, lines)
	})
}

// checkProfileMetricInvariants holds the PROFILE half of the fuzz contract.
func checkProfileMetricInvariants(t *testing.T, lines []string) {
	t.Helper()

	pairs := metricPairsFromLines(lines)
	rep := parseProfileMetrics(pairs)
	if len(rep.Text) != len(pairs) {
		t.Fatalf("len(Text) = %d, want %d", len(rep.Text), len(pairs))
	}
	// parseProfileMetrics takes the LAST timing row it parses, so the
	// invariant is checked against the row that actually produced
	// TotalTimeNS. A later time_ns row is allowed to zero it; a time_ms row
	// that WON is not allowed to make it negative or zero.
	if ms, won := lastParsedTiming(pairs); won && ms > 0 && rep.TotalTimeNS <= 0 {
		t.Fatalf("time_ms=%d produced TotalTimeNS=%d — the ms→ns multiply wrapped",
			ms, rep.TotalTimeNS)
	}
	_ = rep.UsesIndex()
	_ = rep.IndexNames()
}

// lastParsedTiming returns the value of the final parseable time_ms/time_ns
// pair and whether that pair was a time_ms one — i.e. whether the millisecond
// conversion is what set TotalTimeNS.
func lastParsedTiming(pairs [][2]string) (value int64, wasMillis bool) {
	for _, kv := range pairs {
		if kv[0] != "time_ms" && kv[0] != "time_ns" {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(kv[1]), 10, 64)
		if err != nil {
			continue
		}
		value, wasMillis = n, kv[0] == "time_ms"
	}
	return value, wasMillis
}

// metricPairsFromLines splits fuzz text into metric/value pairs the way the
// PROFILE text form's two columns would arrive.
func metricPairsFromLines(lines []string) [][2]string {
	out := make([][2]string, 0, len(lines))
	for _, line := range lines {
		metric, value, _ := strings.Cut(line, "\t")
		out = append(out, [2]string{metric, value})
	}
	return out
}
