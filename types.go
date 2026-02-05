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
	switch s {
	case "INT", "INTEGER", "BIGINT":
		return GQLTypeInt
	case "FLOAT", "DOUBLE", "REAL":
		return GQLTypeFloat
	case "STRING", "VARCHAR", "TEXT":
		return GQLTypeString
	case "BOOL", "BOOLEAN":
		return GQLTypeBool
	case "NULL":
		return GQLTypeNull
	case "LIST", "ARRAY":
		return GQLTypeList
	case "MAP", "RECORD":
		return GQLTypeMap
	case "NODE":
		return GQLTypeNode
	case "EDGE", "RELATIONSHIP":
		return GQLTypeEdge
	case "PATH":
		return GQLTypePath
	default:
		return GQLTypeUnknown
	}
}
