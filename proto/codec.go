// Package proto provides protobuf message types and gRPC client for Geode.
//
//nolint:mnd,dupl,gosec,gocyclo,cyclop,funlen // protobuf encoding uses field numbers and repetitive patterns
package proto

import (
	"google.golang.org/grpc/encoding"
	gpb "google.golang.org/protobuf/proto"
)

func init() {
	// Register our custom codec for the proto message types
	encoding.RegisterCodec(GeodeCodec{})
}

// GeodeCodec implements grpc/encoding.Codec for Geode proto messages.
type GeodeCodec struct{}

// Marshal implements encoding.Codec.
func (GeodeCodec) Marshal(v any) ([]byte, error) {
	// Try protobuf.Message first (for google.golang.org/protobuf types)
	if pm, ok := v.(gpb.Message); ok {
		return gpb.Marshal(pm)
	}

	// Handle our custom message types
	switch m := v.(type) {
	case *HelloRequest:
		return m.Encode()
	case *HelloResponse:
		return encodeHelloResponse(m)
	case *ExecuteRequest:
		return m.Encode()
	case *ExecutionResponse:
		return encodeExecutionResponse(m)
	case *PingRequest:
		return m.Encode()
	case *PingResponse:
		return encodePingResponse(m)
	case *BeginRequest:
		return m.Encode()
	case *BeginResponse:
		return encodeBeginResponse(m)
	case *CommitRequest:
		return m.Encode()
	case *CommitResponse:
		return encodeCommitResponse(m)
	case *RollbackRequest:
		return m.Encode()
	case *RollbackResponse:
		return encodeRollbackResponse(m)
	case *CdcDiagnosticsRequest:
		return m.Encode()
	case *CdcDiagnosticsResponse:
		return encodeCdcDiagnosticsResponse(m)
	case *CdcControlRequest:
		return m.Encode()
	case *CdcControlResponse:
		return encodeCdcControlResponse(m)
	default:
		return nil, ErrInvalidMessage
	}
}

// Unmarshal implements encoding.Codec.
func (GeodeCodec) Unmarshal(data []byte, v any) error {
	// Try protobuf.Message first
	if pm, ok := v.(gpb.Message); ok {
		return gpb.Unmarshal(data, pm)
	}

	// Handle our custom message types
	switch m := v.(type) {
	case *HelloRequest:
		req, err := decodeHelloRequest(data)
		if err != nil {
			return err
		}
		*m = *req
		return nil
	case *HelloResponse:
		resp, err := DecodeHelloResponse(data)
		if err != nil {
			return err
		}
		*m = *resp
		return nil
	case *ExecuteRequest:
		req, err := decodeExecuteRequest(data)
		if err != nil {
			return err
		}
		*m = *req
		return nil
	case *ExecutionResponse:
		resp, err := DecodeExecutionResponse(data)
		if err != nil {
			return err
		}
		*m = *resp
		return nil
	case *PingRequest:
		// Empty message
		return nil
	case *PingResponse:
		resp, err := DecodePingResponse(data)
		if err != nil {
			return err
		}
		*m = *resp
		return nil
	case *BeginRequest:
		req, err := decodeBeginRequest(data)
		if err != nil {
			return err
		}
		*m = *req
		return nil
	case *BeginResponse:
		resp, err := DecodeBeginResponse(data)
		if err != nil {
			return err
		}
		*m = *resp
		return nil
	case *CommitRequest:
		req, err := decodeCommitRequest(data)
		if err != nil {
			return err
		}
		*m = *req
		return nil
	case *CommitResponse:
		resp, err := DecodeCommitResponse(data)
		if err != nil {
			return err
		}
		*m = *resp
		return nil
	case *RollbackRequest:
		req, err := decodeRollbackRequest(data)
		if err != nil {
			return err
		}
		*m = *req
		return nil
	case *RollbackResponse:
		resp, err := DecodeRollbackResponse(data)
		if err != nil {
			return err
		}
		*m = *resp
		return nil
	case *CdcDiagnosticsRequest:
		// Empty message
		return nil
	case *CdcDiagnosticsResponse:
		resp, err := DecodeCdcDiagnosticsResponse(data)
		if err != nil {
			return err
		}
		*m = *resp
		return nil
	case *CdcControlRequest:
		req, err := decodeCdcControlRequest(data)
		if err != nil {
			return err
		}
		*m = *req
		return nil
	case *CdcControlResponse:
		resp, err := DecodeCdcControlResponse(data)
		if err != nil {
			return err
		}
		*m = *resp
		return nil
	default:
		return ErrInvalidMessage
	}
}

// Name implements encoding.Codec.
func (GeodeCodec) Name() string {
	return "proto"
}

// ----------------------------------------------------------------------------
// Additional encode functions for response types
// ----------------------------------------------------------------------------

func encodeHelloResponse(m *HelloResponse) ([]byte, error) {
	var buf []byte
	w := newBytesWriter(&buf)
	if err := EncodeBool(w, 1, m.Success); err != nil {
		return nil, err
	}
	if err := EncodeString(w, 2, m.SessionID); err != nil {
		return nil, err
	}
	if err := EncodeString(w, 3, m.ErrorMessage); err != nil {
		return nil, err
	}
	for _, cap := range m.Capabilities {
		if err := EncodeString(w, 4, cap); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func encodeExecutionResponse(m *ExecutionResponse) ([]byte, error) {
	var buf []byte
	w := newBytesWriter(&buf)
	if m.Schema != nil {
		data, err := encodeSchemaDefinition(m.Schema)
		if err != nil {
			return nil, err
		}
		if err := EncodeBytes(w, 1, data); err != nil {
			return nil, err
		}
	}
	if m.Page != nil {
		data, err := encodeDataPage(m.Page)
		if err != nil {
			return nil, err
		}
		if err := EncodeBytes(w, 2, data); err != nil {
			return nil, err
		}
	}
	if m.Error != nil {
		data, err := encodeError(m.Error)
		if err != nil {
			return nil, err
		}
		if err := EncodeBytes(w, 3, data); err != nil {
			return nil, err
		}
	}
	if m.Metrics != nil {
		data, err := encodeExecutionMetrics(m.Metrics)
		if err != nil {
			return nil, err
		}
		if err := EncodeBytes(w, 4, data); err != nil {
			return nil, err
		}
	}
	if m.Heartbeat != nil {
		if err := EncodeBytesAlways(w, 5, []byte{}); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func encodeSchemaDefinition(m *SchemaDefinition) ([]byte, error) {
	var buf []byte
	w := newBytesWriter(&buf)
	for _, col := range m.Columns {
		data, err := encodeColumnDefinition(&col)
		if err != nil {
			return nil, err
		}
		if err := EncodeBytes(w, 1, data); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func encodeColumnDefinition(m *ColumnDefinition) ([]byte, error) {
	var buf []byte
	w := newBytesWriter(&buf)
	if err := EncodeString(w, 1, m.Name); err != nil {
		return nil, err
	}
	if err := EncodeString(w, 2, m.Type); err != nil {
		return nil, err
	}
	return buf, nil
}

func encodeDataPage(m *DataPage) ([]byte, error) {
	var buf []byte
	w := newBytesWriter(&buf)
	for _, row := range m.Rows {
		data, err := encodeRow(&row)
		if err != nil {
			return nil, err
		}
		if err := EncodeBytes(w, 1, data); err != nil {
			return nil, err
		}
	}
	if err := EncodeBool(w, 2, m.LastPage); err != nil {
		return nil, err
	}
	return buf, nil
}

func encodeRow(m *Row) ([]byte, error) {
	var buf []byte
	w := newBytesWriter(&buf)
	for _, val := range m.Values {
		data, err := encodeValue(&val)
		if err != nil {
			return nil, err
		}
		if err := EncodeBytes(w, 1, data); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func encodeValue(m *Value) ([]byte, error) {
	var buf []byte
	w := newBytesWriter(&buf)
	if m.StringVal != nil {
		if err := EncodeString(w, 1, *m.StringVal); err != nil {
			return nil, err
		}
	}
	if m.IntVal != nil {
		if err := EncodeInt64(w, 2, *m.IntVal); err != nil {
			return nil, err
		}
	}
	if m.DoubleVal != nil {
		if err := EncodeDouble(w, 3, *m.DoubleVal); err != nil {
			return nil, err
		}
	}
	if m.BoolVal != nil {
		if err := EncodeBool(w, 4, *m.BoolVal); err != nil {
			return nil, err
		}
	}
	if m.NullVal {
		if err := EncodeBool(w, 5, true); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func encodeError(m *Error) ([]byte, error) {
	var buf []byte
	w := newBytesWriter(&buf)
	if err := EncodeString(w, 1, m.Code); err != nil {
		return nil, err
	}
	if err := EncodeString(w, 2, m.Message); err != nil {
		return nil, err
	}
	if err := EncodeString(w, 3, m.Type); err != nil {
		return nil, err
	}
	return buf, nil
}

func encodeExecutionMetrics(m *ExecutionMetrics) ([]byte, error) {
	var buf []byte
	w := newBytesWriter(&buf)
	if err := EncodeInt64(w, 1, m.ParseDurationNs); err != nil {
		return nil, err
	}
	if err := EncodeInt64(w, 2, m.PlanDurationNs); err != nil {
		return nil, err
	}
	if err := EncodeInt64(w, 3, m.ExecuteDurationNs); err != nil {
		return nil, err
	}
	if err := EncodeInt64(w, 4, m.TotalDurationNs); err != nil {
		return nil, err
	}
	return buf, nil
}

func encodePingResponse(m *PingResponse) ([]byte, error) {
	var buf []byte
	w := newBytesWriter(&buf)
	if err := EncodeBool(w, 1, m.Ok); err != nil {
		return nil, err
	}
	return buf, nil
}

func encodeBeginResponse(m *BeginResponse) ([]byte, error) {
	var buf []byte
	w := newBytesWriter(&buf)
	if err := EncodeString(w, 1, m.SessionID); err != nil {
		return nil, err
	}
	if err := EncodeString(w, 2, m.TxID); err != nil {
		return nil, err
	}
	return buf, nil
}

func encodeCommitResponse(m *CommitResponse) ([]byte, error) {
	var buf []byte
	w := newBytesWriter(&buf)
	if err := EncodeBool(w, 1, m.Success); err != nil {
		return nil, err
	}
	return buf, nil
}

func encodeRollbackResponse(m *RollbackResponse) ([]byte, error) {
	var buf []byte
	w := newBytesWriter(&buf)
	if err := EncodeBool(w, 1, m.Success); err != nil {
		return nil, err
	}
	return buf, nil
}

func encodeCdcDiagnosticsResponse(m *CdcDiagnosticsResponse) ([]byte, error) {
	var buf []byte
	w := newBytesWriter(&buf)
	if err := EncodeString(w, 1, m.JSONPayload); err != nil {
		return nil, err
	}
	return buf, nil
}

func encodeCdcControlResponse(m *CdcControlResponse) ([]byte, error) {
	var buf []byte
	w := newBytesWriter(&buf)
	if err := EncodeBool(w, 1, m.Success); err != nil {
		return nil, err
	}
	return buf, nil
}

// ----------------------------------------------------------------------------
// Additional decode functions for request types
// ----------------------------------------------------------------------------

func decodeHelloRequest(data []byte) (*HelloRequest, error) {
	m := &HelloRequest{}
	pos := 0
	for pos < len(data) {
		tag, n, err := DecodeVarint(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n
		fieldNum := int(tag >> 3)
		wireType := int(tag & 7)

		switch fieldNum {
		case 1: // username
			if wireType != WireBytes {
				return nil, ErrInvalidMessage
			}
			s, n, err := DecodeString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			m.Username = s
		case 2: // password
			if wireType != WireBytes {
				return nil, ErrInvalidMessage
			}
			s, n, err := DecodeString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			m.Password = s
		case 3: // tenant_id
			if wireType != WireBytes {
				return nil, ErrInvalidMessage
			}
			s, n, err := DecodeString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			m.TenantID = s
		default:
			n, err := skipField(data[pos:], wireType)
			if err != nil {
				return nil, err
			}
			pos += n
		}
	}
	return m, nil
}

func decodeExecuteRequest(data []byte) (*ExecuteRequest, error) {
	m := &ExecuteRequest{
		Parameters: make(map[string]string),
	}
	pos := 0
	for pos < len(data) {
		tag, n, err := DecodeVarint(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n
		fieldNum := int(tag >> 3)
		wireType := int(tag & 7)

		switch fieldNum {
		case 1: // session_id
			if wireType != WireBytes {
				return nil, ErrInvalidMessage
			}
			s, n, err := DecodeString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			m.SessionID = s
		case 2: // query
			if wireType != WireBytes {
				return nil, ErrInvalidMessage
			}
			s, n, err := DecodeString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			m.Query = s
		case 3: // parameters (map entry)
			if wireType != WireBytes {
				return nil, ErrInvalidMessage
			}
			length, n, err := DecodeVarint(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			entryData := data[pos : pos+int(length)]
			pos += int(length)
			key, val, err := decodeMapEntry(entryData)
			if err != nil {
				return nil, err
			}
			m.Parameters[key] = val
		default:
			n, err := skipField(data[pos:], wireType)
			if err != nil {
				return nil, err
			}
			pos += n
		}
	}
	return m, nil
}

func decodeMapEntry(data []byte) (string, string, error) {
	var key, val string
	pos := 0
	for pos < len(data) {
		tag, n, err := DecodeVarint(data[pos:])
		if err != nil {
			return "", "", err
		}
		pos += n
		fieldNum := int(tag >> 3)
		wireType := int(tag & 7)

		switch fieldNum {
		case 1: // key
			if wireType != WireBytes {
				return "", "", ErrInvalidMessage
			}
			s, n, err := DecodeString(data[pos:])
			if err != nil {
				return "", "", err
			}
			pos += n
			key = s
		case 2: // value
			if wireType != WireBytes {
				return "", "", ErrInvalidMessage
			}
			s, n, err := DecodeString(data[pos:])
			if err != nil {
				return "", "", err
			}
			pos += n
			val = s
		default:
			n, err := skipField(data[pos:], wireType)
			if err != nil {
				return "", "", err
			}
			pos += n
		}
	}
	return key, val, nil
}

func decodeBeginRequest(data []byte) (*BeginRequest, error) {
	m := &BeginRequest{}
	pos := 0
	for pos < len(data) {
		tag, n, err := DecodeVarint(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n
		fieldNum := int(tag >> 3)
		wireType := int(tag & 7)

		switch fieldNum {
		case 1: // read_only
			if wireType != WireVarint {
				return nil, ErrInvalidMessage
			}
			v, n, err := DecodeVarint(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			m.ReadOnly = v != 0
		case 2: // session_id
			if wireType != WireBytes {
				return nil, ErrInvalidMessage
			}
			s, n, err := DecodeString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			m.SessionID = s
		default:
			n, err := skipField(data[pos:], wireType)
			if err != nil {
				return nil, err
			}
			pos += n
		}
	}
	return m, nil
}

func decodeCommitRequest(data []byte) (*CommitRequest, error) {
	m := &CommitRequest{}
	pos := 0
	for pos < len(data) {
		tag, n, err := DecodeVarint(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n
		fieldNum := int(tag >> 3)
		wireType := int(tag & 7)

		switch fieldNum {
		case 1: // session_id
			if wireType != WireBytes {
				return nil, ErrInvalidMessage
			}
			s, n, err := DecodeString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			m.SessionID = s
		default:
			n, err := skipField(data[pos:], wireType)
			if err != nil {
				return nil, err
			}
			pos += n
		}
	}
	return m, nil
}

func decodeRollbackRequest(data []byte) (*RollbackRequest, error) {
	m := &RollbackRequest{}
	pos := 0
	for pos < len(data) {
		tag, n, err := DecodeVarint(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n
		fieldNum := int(tag >> 3)
		wireType := int(tag & 7)

		switch fieldNum {
		case 1: // session_id
			if wireType != WireBytes {
				return nil, ErrInvalidMessage
			}
			s, n, err := DecodeString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			m.SessionID = s
		default:
			n, err := skipField(data[pos:], wireType)
			if err != nil {
				return nil, err
			}
			pos += n
		}
	}
	return m, nil
}

func decodeCdcControlRequest(data []byte) (*CdcControlRequest, error) {
	m := &CdcControlRequest{}
	pos := 0
	for pos < len(data) {
		tag, n, err := DecodeVarint(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n
		fieldNum := int(tag >> 3)
		wireType := int(tag & 7)

		switch fieldNum {
		case 1: // action
			if wireType != WireBytes {
				return nil, ErrInvalidMessage
			}
			s, n, err := DecodeString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			m.Action = s
		default:
			n, err := skipField(data[pos:], wireType)
			if err != nil {
				return nil, err
			}
			pos += n
		}
	}
	return m, nil
}
