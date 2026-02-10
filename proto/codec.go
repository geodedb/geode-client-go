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

// StringParam creates a Param with a string value.
func StringParam(name, val string) *Param {
	return &Param{
		Name: name,
		Value: &Value{
			Kind: &Value_StringVal{StringVal: &StringValue{Value: val}},
		},
	}
}

// IntParam creates a Param with an integer value.
func IntParam(name string, val int64) *Param {
	return &Param{
		Name: name,
		Value: &Value{
			Kind: &Value_IntVal{IntVal: &IntValue{Value: val, Kind: IntKind_INT}},
		},
	}
}

// FloatParam creates a Param with a float value.
func FloatParam(name string, val float64) *Param {
	return &Param{
		Name: name,
		Value: &Value{
			Kind: &Value_DoubleVal{DoubleVal: &DoubleValue{Value: val, Kind: FloatKind_DOUBLE}},
		},
	}
}

// BoolParam creates a Param with a bool value.
func BoolParam(name string, val bool) *Param {
	return &Param{
		Name: name,
		Value: &Value{
			Kind: &Value_BoolVal{BoolVal: val},
		},
	}
}

// NullParam creates a Param with a null value.
func NullParam(name string) *Param {
	return &Param{
		Name:  name,
		Value: &Value{Kind: &Value_NullVal{NullVal: &NullValue{}}},
	}
}

// MakeParam creates a Param with the appropriate protobuf type for the given Go value.
func MakeParam(name string, val interface{}) *Param {
	if val == nil {
		return NullParam(name)
	}
	switch v := val.(type) {
	case int:
		return IntParam(name, int64(v))
	case int8:
		return IntParam(name, int64(v))
	case int16:
		return IntParam(name, int64(v))
	case int32:
		return IntParam(name, int64(v))
	case int64:
		return IntParam(name, v)
	case uint:
		return IntParam(name, int64(v))
	case uint8:
		return IntParam(name, int64(v))
	case uint16:
		return IntParam(name, int64(v))
	case uint32:
		return IntParam(name, int64(v))
	case uint64:
		return IntParam(name, int64(v))
	case float32:
		return FloatParam(name, float64(v))
	case float64:
		// If the float is a whole number, send as int (YAML parses 35 as int but
		// some paths may produce float64).
		if v == float64(int64(v)) {
			return IntParam(name, int64(v))
		}
		return FloatParam(name, v)
	case bool:
		return BoolParam(name, v)
	case string:
		return StringParam(name, v)
	default:
		return StringParam(name, fmt.Sprintf("%v", v))
	}
}
