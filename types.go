package geode

import (
	"reflect"
)

// GQLType represents a GQL data type.
type GQLType string

// GQL type constants.
const (
	GQLTypeInt     GQLType = "INT"
	GQLTypeFloat   GQLType = "FLOAT"
	GQLTypeString  GQLType = "STRING"
	GQLTypeBool    GQLType = "BOOL"
	GQLTypeNull    GQLType = "NULL"
	GQLTypeList    GQLType = "LIST"
	GQLTypeMap     GQLType = "MAP"
	GQLTypeNode    GQLType = "NODE"
	GQLTypeEdge    GQLType = "EDGE"
	GQLTypePath    GQLType = "PATH"
	GQLTypeUnknown GQLType = "UNKNOWN"
)

// GoType returns the Go reflect.Type for a GQL type.
func (t GQLType) GoType() reflect.Type {
	switch t {
	case GQLTypeInt:
		return reflect.TypeOf(int64(0))
	case GQLTypeFloat:
		return reflect.TypeOf(float64(0))
	case GQLTypeString:
		return reflect.TypeOf("")
	case GQLTypeBool:
		return reflect.TypeOf(false)
	case GQLTypeNull:
		return reflect.TypeOf(nil)
	case GQLTypeList:
		return reflect.TypeOf([]any{})
	case GQLTypeMap, GQLTypeNode, GQLTypeEdge, GQLTypePath:
		return reflect.TypeOf(map[string]any{})
	case GQLTypeUnknown:
		return reflect.TypeOf((*any)(nil)).Elem()
	}
	return reflect.TypeOf((*any)(nil)).Elem()
}

// ParseGQLType parses a GQL type string.
func ParseGQLType(s string) GQLType {
	switch GQLType(s) {
	case GQLTypeInt, "INTEGER", "BIGINT":
		return GQLTypeInt
	case GQLTypeFloat, "DOUBLE", "REAL":
		return GQLTypeFloat
	case GQLTypeString, "VARCHAR", "TEXT":
		return GQLTypeString
	case GQLTypeBool, "BOOLEAN":
		return GQLTypeBool
	case GQLTypeNull:
		return GQLTypeNull
	case GQLTypeList, "ARRAY":
		return GQLTypeList
	case GQLTypeMap, "RECORD":
		return GQLTypeMap
	case GQLTypeNode:
		return GQLTypeNode
	case GQLTypeEdge, "RELATIONSHIP":
		return GQLTypeEdge
	case GQLTypePath:
		return GQLTypePath
	case GQLTypeUnknown:
		return GQLTypeUnknown
	default:
		return GQLTypeUnknown
	}
}

// Node is the typed representation of a Geode graph node value as
// returned by the driver when a GQL query projects a node variable
// (e.g. MATCH (n) RETURN n). Use AsNode to convert the driver's
// map[string]any output to this type.
type Node struct {
	ID         int64          `json:"id"`
	Labels     []string       `json:"labels"`
	Properties map[string]any `json:"properties"`
}

// Edge is the typed representation of a Geode graph edge (relationship)
// value. Use AsEdge to convert the driver's map[string]any output.
type Edge struct {
	ID         int64          `json:"id"`
	StartNode  int64          `json:"start_node"`
	EndNode    int64          `json:"end_node"`
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties"`
}

// Path is the typed representation of a Geode graph path value, the
// ordered sequence of nodes and edges along a traversal. Use AsPath
// to convert the driver's map[string]any output.
type Path struct {
	Nodes []Node `json:"nodes"`
	Edges []Edge `json:"edges"`
}

// AsNode converts a driver value (the map[string]any produced when
// scanning a NODE column) into a typed Node. Returns (Node{}, false) if v
// is not a node map (missing "labels" key, or has "start_node" key which
// would make it an edge).
func AsNode(v any) (Node, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return Node{}, false
	}
	if _, hasLabels := m["labels"]; !hasLabels {
		return Node{}, false
	}
	if _, hasStart := m["start_node"]; hasStart {
		return Node{}, false // edges also have "id"
	}
	id, _ := anyToInt64(m["id"])
	labels := anyToStringSlice(m["labels"])
	props, _ := m["properties"].(map[string]any)
	return Node{ID: id, Labels: labels, Properties: props}, true
}

// AsEdge converts a driver value (map[string]any produced when scanning
// an EDGE column) into a typed Edge. Returns (Edge{}, false) if v is not
// an edge map (must have both "start_node" and "end_node" keys).
func AsEdge(v any) (Edge, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return Edge{}, false
	}
	_, hasStart := m["start_node"]
	_, hasEnd := m["end_node"]
	if !hasStart || !hasEnd {
		return Edge{}, false
	}
	id, _ := anyToInt64(m["id"])
	s, _ := anyToInt64(m["start_node"])
	e, _ := anyToInt64(m["end_node"])
	typ, _ := m["type"].(string)
	props, _ := m["properties"].(map[string]any)
	return Edge{ID: id, StartNode: s, EndNode: e, Type: typ, Properties: props}, true
}

// AsPath converts a driver value (map[string]any produced when scanning
// a PATH column) into a typed Path. Returns (Path{}, false) if v is not
// a path map (must have both "nodes" and "edges" keys).
func AsPath(v any) (Path, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return Path{}, false
	}
	nodesRaw, hasNodes := m["nodes"]
	_, hasEdges := m["edges"]
	if !hasNodes || !hasEdges {
		return Path{}, false
	}
	var nodes []Node
	if ns, ok := nodesRaw.([]any); ok {
		for _, n := range ns {
			if node, ok := AsNode(n); ok {
				nodes = append(nodes, node)
			}
		}
	}
	var edges []Edge
	if es, ok := m["edges"].([]any); ok {
		for _, e := range es {
			if edge, ok := AsEdge(e); ok {
				edges = append(edges, edge)
			}
		}
	}
	return Path{Nodes: nodes, Edges: edges}, true
}

// anyToInt64 coerces a driver value to int64. The driver returns int64 for
// integer columns, but tolerates int and float64 (e.g. from JSON-decoded paths).
// The bool reports whether the coercion succeeded; current callers default to
// zero on failure, but it is part of the helper's contract for future use.
//
//nolint:unparam // ok bool is part of the coercion contract for future callers
func anyToInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case float64:
		if x == float64(int64(x)) {
			return int64(x), true
		}
	}
	return 0, false
}

// anyToStringSlice coerces a driver value to []string, tolerating both
// []string and []any (JSON-decoded label arrays deliver []any).
func anyToStringSlice(v any) []string {
	switch x := v.(type) {
	case []string:
		return x
	case []any:
		out := make([]string, 0, len(x))
		for _, item := range x {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}
