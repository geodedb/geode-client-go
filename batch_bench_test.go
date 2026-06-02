package geode

import (
	"context"
	"fmt"
	"testing"
)

func buildStmts(n int) []Statement {
	stmts := make([]Statement, n)
	for i := range stmts {
		stmts[i] = Statement{Query: fmt.Sprintf("CREATE (n:Bench%d)", i)}
	}
	return stmts
}

func BenchmarkBatchExec(b *testing.B) {
	sizes := []int{1, 10, 100, 500}
	for _, n := range sizes {
		stmts := buildStmts(n)
		b.Run(fmt.Sprintf("%dstmts", n), func(b *testing.B) {
			md := &mockDriver{}
			p := newMockPool(b, md)
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				if err := p.BatchExec(context.Background(), stmts); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPipelineExec(b *testing.B) {
	type cfg struct {
		stmts   int
		chunk   int
		workers int
	}
	cases := []cfg{
		{100, 10, 4},
		{500, 50, 8},
		{1000, 100, 8},
	}
	for _, tc := range cases {
		stmts := buildStmts(tc.stmts)
		name := fmt.Sprintf("%dstmts_c%d_w%d", tc.stmts, tc.chunk, tc.workers)
		b.Run(name, func(b *testing.B) {
			md := &mockDriver{}
			p := newMockPool(b, md)
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				if err := p.PipelineExec(context.Background(), stmts, tc.chunk, tc.workers); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
