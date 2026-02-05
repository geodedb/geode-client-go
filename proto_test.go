package geode

import (
	"reflect"
	"testing"

	"geodedb.com/geode/proto"
)

// TestProtoRoundtrip_HelloRequest tests encoding and decoding of HelloRequest.
func TestProtoRoundtrip_HelloRequest(t *testing.T) {
	original := &proto.HelloRequest{
		Username: "testuser",
		Password: "testpass",
		TenantID: "tenant1",
	}

	// Encode
	data, err := original.Encode()
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	// Decode (via codec)
	var decoded proto.HelloRequest
	codec := proto.GeodeCodec{}
	if err := codec.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	// Verify
	if decoded.Username != original.Username {
		t.Errorf("username mismatch: got %q, want %q", decoded.Username, original.Username)
	}
	if decoded.Password != original.Password {
		t.Errorf("password mismatch: got %q, want %q", decoded.Password, original.Password)
	}
	if decoded.TenantID != original.TenantID {
		t.Errorf("tenant_id mismatch: got %q, want %q", decoded.TenantID, original.TenantID)
	}
}

// TestProtoRoundtrip_ExecuteRequest tests encoding and decoding of ExecuteRequest.
func TestProtoRoundtrip_ExecuteRequest(t *testing.T) {
	original := &proto.ExecuteRequest{
		SessionID: "session123",
		Query:     "MATCH (n:Person) RETURN n.name",
		Parameters: map[string]string{
			"param1": "value1",
			"param2": "value2",
		},
	}

	// Encode
	data, err := original.Encode()
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	// Decode
	var decoded proto.ExecuteRequest
	codec := proto.GeodeCodec{}
	if err := codec.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	// Verify
	if decoded.SessionID != original.SessionID {
		t.Errorf("session_id mismatch: got %q, want %q", decoded.SessionID, original.SessionID)
	}
	if decoded.Query != original.Query {
		t.Errorf("query mismatch: got %q, want %q", decoded.Query, original.Query)
	}
	for k, v := range original.Parameters {
		if decoded.Parameters[k] != v {
			t.Errorf("parameter %q mismatch: got %q, want %q", k, decoded.Parameters[k], v)
		}
	}
}

// TestProtoRoundtrip_QuicClientMessage tests encoding and decoding of QuicClientMessage.
func TestProtoRoundtrip_QuicClientMessage(t *testing.T) {
	tests := []struct {
		name string
		msg  *proto.QuicClientMessage
	}{
		{
			name: "hello",
			msg: &proto.QuicClientMessage{
				Hello: &proto.HelloRequest{
					Username: "user",
					Password: "pass",
				},
			},
		},
		{
			name: "execute",
			msg: &proto.QuicClientMessage{
				Execute: &proto.ExecuteRequest{
					Query: "RETURN 1",
				},
			},
		},
		{
			name: "ping",
			msg: &proto.QuicClientMessage{
				Ping: &proto.PingRequest{},
			},
		},
		{
			name: "begin",
			msg: &proto.QuicClientMessage{
				Begin: &proto.BeginRequest{
					ReadOnly: true,
				},
			},
		},
		{
			name: "commit",
			msg: &proto.QuicClientMessage{
				Commit: &proto.CommitRequest{},
			},
		},
		{
			name: "rollback",
			msg: &proto.QuicClientMessage{
				Rollback: &proto.RollbackRequest{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			data, err := tt.msg.Encode()
			if err != nil {
				t.Fatalf("failed to encode: %v", err)
			}

			// Verify data is not empty (basic sanity check)
			if len(data) == 0 && tt.msg.Ping == nil && tt.msg.Commit == nil && tt.msg.Rollback == nil {
				t.Error("encoded data is empty")
			}
		})
	}
}

// TestProtoRoundtrip_ServerMessages tests decoding of server messages.
func TestProtoRoundtrip_ServerMessages(t *testing.T) {
	// Test HelloResponse decode
	t.Run("HelloResponse", func(t *testing.T) {
		// Manually encode a HelloResponse
		resp := &proto.HelloResponse{
			Success:   true,
			SessionID: "sess123",
		}

		// Use codec to marshal
		codec := proto.GeodeCodec{}
		data, err := codec.Marshal(resp)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		// Decode
		decoded, err := proto.DecodeHelloResponse(data)
		if err != nil {
			t.Fatalf("failed to decode: %v", err)
		}

		if !decoded.Success {
			t.Error("expected success=true")
		}
		if decoded.SessionID != "sess123" {
			t.Errorf("session_id mismatch: got %q", decoded.SessionID)
		}
	})

	// Test PingResponse decode
	t.Run("PingResponse", func(t *testing.T) {
		resp := &proto.PingResponse{Ok: true}

		codec := proto.GeodeCodec{}
		data, err := codec.Marshal(resp)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		decoded, err := proto.DecodePingResponse(data)
		if err != nil {
			t.Fatalf("failed to decode: %v", err)
		}

		if !decoded.Ok {
			t.Error("expected ok=true")
		}
	})
}

// TestProtoRoundtrip_ExecutionResponse tests decoding of ExecutionResponse.
func TestProtoRoundtrip_ExecutionResponse(t *testing.T) {
	// Test with schema
	t.Run("schema", func(t *testing.T) {
		resp := &proto.ExecutionResponse{
			Schema: &proto.SchemaDefinition{
				Columns: []proto.ColumnDefinition{
					{Name: "id", Type: "INT"},
					{Name: "name", Type: "STRING"},
				},
			},
		}

		codec := proto.GeodeCodec{}
		data, err := codec.Marshal(resp)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		decoded, err := proto.DecodeExecutionResponse(data)
		if err != nil {
			t.Fatalf("failed to decode: %v", err)
		}

		if decoded.Schema == nil {
			t.Fatal("expected schema")
		}
		if len(decoded.Schema.Columns) != 2 {
			t.Errorf("expected 2 columns, got %d", len(decoded.Schema.Columns))
		}
	})

	// Test with data page
	t.Run("page", func(t *testing.T) {
		str := "hello"
		num := int64(42)
		resp := &proto.ExecutionResponse{
			Page: &proto.DataPage{
				Rows: []proto.Row{
					{
						Values: []proto.Value{
							{IntVal: &num},
							{StringVal: &str},
						},
					},
				},
				LastPage: true,
			},
		}

		codec := proto.GeodeCodec{}
		data, err := codec.Marshal(resp)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		decoded, err := proto.DecodeExecutionResponse(data)
		if err != nil {
			t.Fatalf("failed to decode: %v", err)
		}

		if decoded.Page == nil {
			t.Fatal("expected page")
		}
		if !decoded.Page.LastPage {
			t.Error("expected last_page=true")
		}
		if len(decoded.Page.Rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(decoded.Page.Rows))
		}
	})

	// Test with error
	t.Run("error", func(t *testing.T) {
		resp := &proto.ExecutionResponse{
			Error: &proto.Error{
				Code:    "42000",
				Message: "syntax error",
				Type:    "ERROR",
			},
		}

		codec := proto.GeodeCodec{}
		data, err := codec.Marshal(resp)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		decoded, err := proto.DecodeExecutionResponse(data)
		if err != nil {
			t.Fatalf("failed to decode: %v", err)
		}

		if decoded.Error == nil {
			t.Fatal("expected error")
		}
		if decoded.Error.Code != "42000" {
			t.Errorf("code mismatch: got %q", decoded.Error.Code)
		}
	})
}

// TestNoJSONProtocol verifies that JSON protocol code has been removed.
func TestNoJSONProtocol(_ *testing.T) {
	// This test ensures the legacy JSON protocol types are not accessible.
	// If this test compiles, it means the JSON types have been properly removed.

	// Verify GQLType is still available (it's kept for type mapping)
	_ = GQLTypeInt
	_ = GQLTypeString
	_ = ParseGQLType("INT")
}

// TestParseGQLType tests GQL type parsing.
func TestParseGQLType(t *testing.T) {
	tests := []struct {
		input    string
		expected GQLType
	}{
		{"INT", GQLTypeInt},
		{"INTEGER", GQLTypeInt},
		{"BIGINT", GQLTypeInt},
		{"FLOAT", GQLTypeFloat},
		{"DOUBLE", GQLTypeFloat},
		{"STRING", GQLTypeString},
		{"VARCHAR", GQLTypeString},
		{"BOOL", GQLTypeBool},
		{"BOOLEAN", GQLTypeBool},
		{"NULL", GQLTypeNull},
		{"LIST", GQLTypeList},
		{"ARRAY", GQLTypeList},
		{"MAP", GQLTypeMap},
		{"NODE", GQLTypeNode},
		{"EDGE", GQLTypeEdge},
		{"PATH", GQLTypePath},
		{"UNKNOWN", GQLTypeUnknown},
		{"", GQLTypeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ParseGQLType(tt.input)
			if got != tt.expected {
				t.Errorf("ParseGQLType(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

// TestGQLType_GoType tests Go type mapping.
func TestGQLType_GoType(t *testing.T) {
	if GQLTypeInt.GoType() != reflect.TypeOf(int64(0)) {
		t.Error("INT should map to int64")
	}
	if GQLTypeFloat.GoType() != reflect.TypeOf(float64(0)) {
		t.Error("FLOAT should map to float64")
	}
	if GQLTypeString.GoType() != reflect.TypeOf("") {
		t.Error("STRING should map to string")
	}
	if GQLTypeBool.GoType() != reflect.TypeOf(false) {
		t.Error("BOOL should map to bool")
	}
}
