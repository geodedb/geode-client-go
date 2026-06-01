package geode_test

import (
	"testing"

	geode "geodedb.com/geode"
)

func TestAsNode(t *testing.T) {
	validNode := map[string]any{
		"id":         int64(42),
		"labels":     []any{"Person", "User"},
		"properties": map[string]any{"name": "Alice"},
	}
	n, ok := geode.AsNode(validNode)
	if !ok {
		t.Fatal("AsNode(valid node map) returned false")
	}
	if n.ID != 42 {
		t.Errorf("Node.ID = %d, want 42", n.ID)
	}
	if len(n.Labels) != 2 || n.Labels[0] != "Person" || n.Labels[1] != "User" {
		t.Errorf("Node.Labels = %v, want [Person User]", n.Labels)
	}
	if n.Properties["name"] != "Alice" {
		t.Errorf("Node.Properties[name] = %v, want Alice", n.Properties["name"])
	}

	// Edge map must not match (has start_node).
	if _, ok := geode.AsNode(map[string]any{
		"id": int64(1), "start_node": int64(2), "end_node": int64(3),
	}); ok {
		t.Error("AsNode returned true for an edge map")
	}
	// Map without labels must not match.
	if _, ok := geode.AsNode(map[string]any{"id": int64(1)}); ok {
		t.Error("AsNode returned true for map without labels")
	}
	if _, ok := geode.AsNode(nil); ok {
		t.Error("AsNode returned true for nil")
	}
	if _, ok := geode.AsNode("not a map"); ok {
		t.Error("AsNode returned true for a string")
	}
}

func TestAsEdge(t *testing.T) {
	validEdge := map[string]any{
		"id":         int64(7),
		"start_node": int64(1),
		"end_node":   int64(2),
		"type":       "KNOWS",
		"properties": map[string]any{"since": int64(2020)},
	}
	e, ok := geode.AsEdge(validEdge)
	if !ok {
		t.Fatal("AsEdge(valid edge map) returned false")
	}
	if e.ID != 7 || e.StartNode != 1 || e.EndNode != 2 {
		t.Errorf("Edge IDs = %d/%d/%d, want 7/1/2", e.ID, e.StartNode, e.EndNode)
	}
	if e.Type != "KNOWS" {
		t.Errorf("Edge.Type = %q, want KNOWS", e.Type)
	}

	// Node map must not match AsEdge (no start_node).
	if _, ok := geode.AsEdge(map[string]any{"id": int64(1), "labels": []any{"N"}}); ok {
		t.Error("AsEdge returned true for a node map")
	}
	if _, ok := geode.AsEdge(nil); ok {
		t.Error("AsEdge returned true for nil")
	}
}

func TestAsPath(t *testing.T) {
	validPath := map[string]any{
		"nodes": []any{
			map[string]any{
				"id": int64(1), "labels": []any{"A"}, "properties": map[string]any{},
			},
		},
		"edges": []any{},
	}
	p, ok := geode.AsPath(validPath)
	if !ok {
		t.Fatal("AsPath(valid path map) returned false")
	}
	if len(p.Nodes) != 1 {
		t.Errorf("Path.Nodes len = %d, want 1", len(p.Nodes))
	}
	if len(p.Edges) != 0 {
		t.Errorf("Path.Edges len = %d, want 0", len(p.Edges))
	}

	// Plain map without nodes/edges must not match.
	if _, ok := geode.AsPath(map[string]any{"id": int64(1)}); ok {
		t.Error("AsPath returned true for a plain map")
	}
	if _, ok := geode.AsPath(nil); ok {
		t.Error("AsPath returned true for nil")
	}
}
