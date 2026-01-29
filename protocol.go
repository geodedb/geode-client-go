package geode

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// Message type constants.
const (
	MsgHello      = "HELLO"
	MsgRunGQL     = "RUN_GQL"
	MsgPull       = "PULL"
	MsgBegin      = "BEGIN"
	MsgCommit     = "COMMIT"
	MsgRollback   = "ROLLBACK"
	MsgSavepoint  = "SAVEPOINT"
	MsgRollbackTo = "ROLLBACK_TO"
	MsgPing       = "PING"
)

// Response type constants.
const (
	RespSchema   = "SCHEMA"
	RespBindings = "BINDINGS"
	RespError    = "ERROR"
	RespExplain  = "EXPLAIN"
	RespProfile  = "PROFILE"
	RespResult   = "RESULT"
	RespStatus   = "STATUS"
	RespHello    = "HELLO"
)

// Frame represents a wire protocol response frame from the server.
type Frame struct {
	StatusClass    string      `json:"status_class"`
	StatusSubclass string      `json:"status_subclass"`
	Result         FrameResult `json:"result"`
}

// FrameResult represents the result portion of a frame.
type FrameResult struct {
	Type       string           `json:"type"`
	Columns    []ColumnDef      `json:"columns,omitempty"`
	Rows       []map[string]any `json:"rows,omitempty"`
	Ordered    bool             `json:"ordered,omitempty"`
	OrderKeys  []string         `json:"order_keys,omitempty"`
	Final      bool             `json:"final,omitempty"`
	Code       string           `json:"code,omitempty"`
	Message    string           `json:"message,omitempty"`
	Anchor     string           `json:"anchor,omitempty"`
	Additional []AdditionalInfo `json:"additional,omitempty"`
	Findings   []string         `json:"flagger_findings,omitempty"`
	JSON       any              `json:"json,omitempty"`
}

// ColumnDef represents a column definition in a schema.
type ColumnDef struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// AdditionalInfo represents additional status information.
type AdditionalInfo struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// ParseFrame parses a JSON response frame from the server.
func ParseFrame(data []byte) (*Frame, error) {
	var frame Frame
	if err := json.Unmarshal(data, &frame); err != nil {
		return nil, &TransportError{
			Op:    "parse_frame",
			cause: err,
		}
	}
	return &frame, nil
}

// ToError converts a frame to a DriverError if it represents an error.
// Returns nil if the frame is not an error frame.
func (f *Frame) ToError() error {
	if f.Result.Type != RespError {
		return nil
	}

	additional := make([]string, 0, len(f.Result.Additional))
	for _, a := range f.Result.Additional {
		if a.Message != "" {
			additional = append(additional, a.Message)
		} else if a.Code != "" {
			additional = append(additional, a.Code)
		}
	}

	return &DriverError{
		Class:      f.StatusClass,
		Subclass:   f.StatusSubclass,
		Code:       f.Result.Code,
		Message:    f.Result.Message,
		Anchor:     f.Result.Anchor,
		Additional: additional,
		Findings:   f.Result.Findings,
	}
}

// IsError returns true if this frame represents an error.
func (f *Frame) IsError() bool {
	return f.Result.Type == RespError
}

// IsSchema returns true if this frame represents a schema.
func (f *Frame) IsSchema() bool {
	return f.Result.Type == RespSchema
}

// IsBindings returns true if this frame represents bindings (rows).
func (f *Frame) IsBindings() bool {
	return f.Result.Type == RespBindings
}

// IsFinal returns true if this is the final frame in a result set.
func (f *Frame) IsFinal() bool {
	return f.Result.Final
}

// BuildHelloMessage creates a HELLO message for the server.
func BuildHelloMessage(cfg *Config) map[string]any {
	return map[string]any{
		"type":               MsgHello,
		"client_name":        cfg.HelloName,
		"client_ver":         cfg.HelloVersion,
		"wanted_conformance": cfg.Conformance,
	}
}

// BuildRunGQLMessage creates a RUN_GQL message for executing a query.
func BuildRunGQLMessage(query string, params map[string]any) map[string]any {
	msg := map[string]any{
		"type": MsgRunGQL,
		"text": query,
	}
	if len(params) > 0 {
		msg["params"] = params
	}
	return msg
}

// BuildPullMessage creates a PULL message for fetching results.
func BuildPullMessage(requestID int, pageSize int) map[string]any {
	return map[string]any{
		"type":       MsgPull,
		"request_id": requestID,
		"page_size":  pageSize,
	}
}

// BuildBeginMessage creates a BEGIN message for starting a transaction.
func BuildBeginMessage() map[string]any {
	return map[string]any{
		"type": MsgBegin,
	}
}

// BuildCommitMessage creates a COMMIT message.
func BuildCommitMessage() map[string]any {
	return map[string]any{
		"type": MsgCommit,
	}
}

// BuildRollbackMessage creates a ROLLBACK message.
func BuildRollbackMessage() map[string]any {
	return map[string]any{
		"type": MsgRollback,
	}
}

// BuildSavepointMessage creates a SAVEPOINT message.
func BuildSavepointMessage(name string) map[string]any {
	return map[string]any{
		"type": MsgSavepoint,
		"name": name,
	}
}

// BuildRollbackToMessage creates a ROLLBACK_TO message.
func BuildRollbackToMessage(name string) map[string]any {
	return map[string]any{
		"type": MsgRollbackTo,
		"name": name,
	}
}

// BuildPingMessage creates a PING message.
func BuildPingMessage() map[string]any {
	return map[string]any{
		"type": MsgPing,
	}
}

// ConvertValue converts a JSON value to a driver-compatible value.
func ConvertValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case bool:
		return x
	case float64:
		// Convert to int64 if it's a whole number
		if x == float64(int64(x)) {
			return int64(x)
		}
		return x
	case string:
		return x
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		if f, err := x.Float64(); err == nil {
			return f
		}
		return string(x)
	case []any:
		// Return arrays as-is for now
		return x
	case map[string]any:
		// Return maps as-is for now
		return x
	default:
		return fmt.Sprintf("%v", v)
	}
}

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
