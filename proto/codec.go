// Package proto provides protobuf message types and gRPC client for Geode.
//
// The generated types come from protoc (geode.pb.go / geode_grpc.pb.go).
// This file provides:
//   - QUIC wire-format helpers: length-prefixed protobuf encode/decode
//   - Convenience constructors for QuicClientMessage/QuicServerMessage
//   - DecodeQuicServerMessage for QUIC transport
//   - Encode() on QuicClientMessage for QUIC transport
//
//nolint:gosec // protobuf encoding uses field numbers and repetitive patterns
package proto

import (
	"errors"
	"fmt"
	"reflect"
	"sort"

	gpb "google.golang.org/protobuf/proto"
)

// ErrInvalidMessage is returned when a protobuf message cannot be decoded.
var ErrInvalidMessage = errors.New("invalid protobuf message")

// Encode serializes a QuicClientMessage using standard protobuf marshaling.
func (x *QuicClientMessage) Encode() ([]byte, error) {
	return gpb.Marshal(x)
}

// DecodeQuicServerMessage decodes a protobuf-encoded QuicServerMessage.
func DecodeQuicServerMessage(data []byte) (*QuicServerMessage, error) {
	msg := &QuicServerMessage{}
	if err := gpb.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return msg, nil
}

// DecodeHelloResponse decodes a protobuf-encoded HelloResponse.
func DecodeHelloResponse(data []byte) (*HelloResponse, error) {
	msg := &HelloResponse{}
	if err := gpb.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return msg, nil
}

// DecodeExecutionResponse decodes a protobuf-encoded ExecutionResponse.
func DecodeExecutionResponse(data []byte) (*ExecutionResponse, error) {
	msg := &ExecutionResponse{}
	if err := gpb.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return msg, nil
}

// DecodePingResponse decodes a protobuf-encoded PingResponse.
func DecodePingResponse(data []byte) (*PingResponse, error) {
	msg := &PingResponse{}
	if err := gpb.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return msg, nil
}

// DecodeBeginResponse decodes a protobuf-encoded BeginResponse.
func DecodeBeginResponse(data []byte) (*BeginResponse, error) {
	msg := &BeginResponse{}
	if err := gpb.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return msg, nil
}

// DecodeCommitResponse decodes a protobuf-encoded CommitResponse.
func DecodeCommitResponse(data []byte) (*CommitResponse, error) {
	msg := &CommitResponse{}
	if err := gpb.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return msg, nil
}

// DecodeRollbackResponse decodes a protobuf-encoded RollbackResponse.
func DecodeRollbackResponse(data []byte) (*RollbackResponse, error) {
	msg := &RollbackResponse{}
	if err := gpb.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return msg, nil
}

// DecodeCdcDiagnosticsResponse decodes a protobuf-encoded CdcDiagnosticsResponse.
func DecodeCdcDiagnosticsResponse(data []byte) (*CdcDiagnosticsResponse, error) {
	msg := &CdcDiagnosticsResponse{}
	if err := gpb.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return msg, nil
}

// DecodeCdcControlResponse decodes a protobuf-encoded CdcControlResponse.
func DecodeCdcControlResponse(data []byte) (*CdcControlResponse, error) {
	msg := &CdcControlResponse{}
	if err := gpb.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return msg, nil
}

// ----------------------------------------------------------------------------
// QuicClientMessage constructors (convenience for QUIC transport callers)
// ----------------------------------------------------------------------------

// NewHelloMessage creates a QuicClientMessage with a HelloRequest.
func NewHelloMessage(req *HelloRequest) *QuicClientMessage {
	return &QuicClientMessage{
		Msg: &QuicClientMessage_Hello{Hello: req},
	}
}

// NewExecuteMessage creates a QuicClientMessage with an ExecuteRequest.
func NewExecuteMessage(req *ExecuteRequest) *QuicClientMessage {
	return &QuicClientMessage{
		Msg: &QuicClientMessage_Execute{Execute: req},
	}
}

// NewPingMessage creates a QuicClientMessage with a PingRequest.
func NewPingMessage(req *PingRequest) *QuicClientMessage {
	return &QuicClientMessage{
		Msg: &QuicClientMessage_Ping{Ping: req},
	}
}

// NewBeginMessage creates a QuicClientMessage with a BeginRequest.
func NewBeginMessage(req *BeginRequest) *QuicClientMessage {
	return &QuicClientMessage{
		Msg: &QuicClientMessage_Begin{Begin: req},
	}
}

// NewCommitMessage creates a QuicClientMessage with a CommitRequest.
func NewCommitMessage(req *CommitRequest) *QuicClientMessage {
	return &QuicClientMessage{
		Msg: &QuicClientMessage_Commit{Commit: req},
	}
}

// NewRollbackMessage creates a QuicClientMessage with a RollbackRequest.
func NewRollbackMessage(req *RollbackRequest) *QuicClientMessage {
	return &QuicClientMessage{
		Msg: &QuicClientMessage_Rollback{Rollback: req},
	}
}

// NewCdcDiagMessage creates a QuicClientMessage with a CdcDiagnosticsRequest.
func NewCdcDiagMessage(req *CdcDiagnosticsRequest) *QuicClientMessage {
	return &QuicClientMessage{
		Msg: &QuicClientMessage_CdcDiag{CdcDiag: req},
	}
}

// NewPullMessage creates a QuicClientMessage with a PullRequest.
func NewPullMessage(req *PullRequest) *QuicClientMessage {
	return &QuicClientMessage{
		Msg: &QuicClientMessage_Pull{Pull: req},
	}
}

// NewCdcCtrlMessage creates a QuicClientMessage with a CdcControlRequest.
func NewCdcCtrlMessage(req *CdcControlRequest) *QuicClientMessage {
	return &QuicClientMessage{
		Msg: &QuicClientMessage_CdcCtrl{CdcCtrl: req},
	}
}

// ----------------------------------------------------------------------------
// QuicServerMessage field accessors (convenience wrappers around oneof getters)
// These provide nil-safe access matching how the old code used direct fields.
// ----------------------------------------------------------------------------

// GetHelloResponse returns the HelloResponse if this is a hello message, or nil.
func (x *QuicServerMessage) GetHelloResponse() *HelloResponse {
	return x.GetHello()
}

// GetExecuteResponse returns the ExecutionResponse if this is an execute message, or nil.
func (x *QuicServerMessage) GetExecuteResponse() *ExecutionResponse {
	return x.GetExecute()
}

// GetPingResp returns the PingResponse if this is a ping message, or nil.
func (x *QuicServerMessage) GetPingResp() *PingResponse {
	return x.GetPing()
}

// GetBeginResp returns the BeginResponse if this is a begin message, or nil.
func (x *QuicServerMessage) GetBeginResp() *BeginResponse {
	return x.GetBegin()
}

// GetCommitResp returns the CommitResponse if this is a commit message, or nil.
func (x *QuicServerMessage) GetCommitResp() *CommitResponse {
	return x.GetCommit()
}

// GetRollbackResp returns the RollbackResponse if this is a rollback message, or nil.
func (x *QuicServerMessage) GetRollbackResp() *RollbackResponse {
	return x.GetRollback()
}

// maxParamDepth bounds container nesting when encoding parameters. It matches
// the server limit (protoValueToValue in proto_util.zig) and the Python
// client's PARAM_MAX_DEPTH. MakeParam cannot report an error, so values nested
// deeper are encoded as null instead of being recursed into; that also keeps
// cyclic structures from exhausting the stack.
const maxParamDepth = 64

// stringValue builds a string_val Value.
func stringValue(val string) *Value {
	return &Value{Kind: &Value_StringVal{StringVal: &StringValue{Value: val}}}
}

// intValue builds an int_val Value.
func intValue(val int64) *Value {
	return &Value{Kind: &Value_IntVal{IntVal: &IntValue{Value: val, Kind: IntKind_INT}}}
}

// floatValue builds a double_val Value.
func floatValue(val float64) *Value {
	return &Value{Kind: &Value_DoubleVal{DoubleVal: &DoubleValue{Value: val, Kind: FloatKind_DOUBLE}}}
}

// boolValue builds a bool_val Value.
func boolValue(val bool) *Value {
	return &Value{Kind: &Value_BoolVal{BoolVal: val}}
}

// bytesValue builds a bytes_val Value.
func bytesValue(val []byte) *Value {
	return &Value{Kind: &Value_BytesVal{BytesVal: &BytesValue{Value: val, Kind: BytesKind_BYTEA}}}
}

// nullValue builds a null_val Value.
func nullValue() *Value {
	return &Value{Kind: &Value_NullVal{NullVal: &NullValue{}}}
}

// StringParam creates a Param with a string value.
func StringParam(name, val string) *Param {
	return &Param{Name: name, Value: stringValue(val)}
}

// IntParam creates a Param with an integer value.
func IntParam(name string, val int64) *Param {
	return &Param{Name: name, Value: intValue(val)}
}

// FloatParam creates a Param with a float value.
func FloatParam(name string, val float64) *Param {
	return &Param{Name: name, Value: floatValue(val)}
}

// BoolParam creates a Param with a bool value.
func BoolParam(name string, val bool) *Param {
	return &Param{Name: name, Value: boolValue(val)}
}

// BytesParam creates a Param with a bytes value.
func BytesParam(name string, val []byte) *Param {
	return &Param{Name: name, Value: bytesValue(val)}
}

// NullParam creates a Param with a null value.
func NullParam(name string) *Param {
	return &Param{Name: name, Value: nullValue()}
}

// MakeParam creates a Param with the appropriate protobuf type for the given Go value.
//
// Slices, arrays and maps are encoded structurally as list_val/map_val (see
// makeValue); every other unsupported type keeps its fmt representation.
func MakeParam(name string, val interface{}) *Param {
	return &Param{Name: name, Value: makeValue(val, 0)}
}

// makeValue converts a Go value into the matching protobuf Value.
//
// Containers are encoded structurally (slice/array -> list_val, map ->
// map_val) so the server receives a real collection instead of a Go fmt
// representation, which made `x IN $list` match nothing and `NOT IN` fail
// open (ENG-3910). Strings and []byte stay scalars. depth tracks container
// nesting for maxParamDepth.
func makeValue(val interface{}, depth int) *Value {
	if val == nil {
		return nullValue()
	}
	switch v := val.(type) {
	case int:
		return intValue(int64(v))
	case int8:
		return intValue(int64(v))
	case int16:
		return intValue(int64(v))
	case int32:
		return intValue(int64(v))
	case int64:
		return intValue(v)
	case uint:
		return intValue(int64(v))
	case uint8:
		return intValue(int64(v))
	case uint16:
		return intValue(int64(v))
	case uint32:
		return intValue(int64(v))
	case uint64:
		return intValue(int64(v))
	case float32:
		return floatValue(float64(v))
	case float64:
		// If the float is a whole number, send as int (YAML parses 35 as int but
		// some paths may produce float64).
		if v == float64(int64(v)) {
			return intValue(int64(v))
		}
		return floatValue(v)
	case bool:
		return boolValue(v)
	case string:
		return stringValue(v)
	case []byte:
		// Bytes are a scalar value, not a list of integers.
		return bytesValue(v)
	}
	return containerValue(val, depth)
}

// containerValue encodes slices, arrays and maps structurally. Any other type
// (time.Time, structs, ...) keeps the historical fmt representation, which the
// server stores as a string; see ScanTime in the geode package.
func containerValue(val interface{}, depth int) *Value {
	rv := reflect.ValueOf(val)
	//nolint:exhaustive // only container kinds are encoded structurally
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		if depth >= maxParamDepth {
			return nullValue()
		}
		values := make([]*Value, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			values = append(values, makeValue(rv.Index(i).Interface(), depth+1))
		}
		return &Value{Kind: &Value_ListVal{ListVal: &ListValue{Values: values}}}
	case reflect.Map:
		if depth >= maxParamDepth {
			return nullValue()
		}
		return mapValue(rv, depth)
	default:
		return stringValue(fmt.Sprintf("%v", val))
	}
}

// mapValue encodes a reflect map as map_val with key-sorted entries so the
// wire bytes are deterministic for a given parameter value.
func mapValue(rv reflect.Value, depth int) *Value {
	keys := rv.MapKeys()
	entries := make([]*MapEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, &MapEntry{
			Key:   mapKeyString(key),
			Value: makeValue(rv.MapIndex(key).Interface(), depth+1),
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].GetKey() < entries[j].GetKey()
	})
	return &Value{Kind: &Value_MapVal{MapVal: &MapValue{Entries: entries}}}
}

// mapKeyString renders a map key as the string required by MapEntry.
func mapKeyString(key reflect.Value) string {
	if key.Kind() == reflect.String {
		return key.String()
	}
	return fmt.Sprintf("%v", key.Interface())
}
