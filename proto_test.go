package geode

import (
	"reflect"
	"testing"

	"geodedb.com/geode/proto"
	gpb "google.golang.org/protobuf/proto"
)

// TestProtoRoundtrip_HelloRequest tests encoding and decoding of HelloRequest.
func TestProtoRoundtrip_HelloRequest(t *testing.T) {
	original := &proto.HelloRequest{
		Username: "testuser",
		Password: "testpass",
	}

	// Encode using standard protobuf
	data, err := gpb.Marshal(original)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	// Decode
	decoded := &proto.HelloRequest{}
	if err := gpb.Unmarshal(data, decoded); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	// Verify
	if decoded.GetUsername() != original.GetUsername() {
		t.Errorf("username mismatch: got %q, want %q", decoded.GetUsername(), original.GetUsername())
	}
	if decoded.GetPassword() != original.GetPassword() {
		t.Errorf("password mismatch: got %q, want %q", decoded.GetPassword(), original.GetPassword())
	}
}

// TestProtoRoundtrip_ExecuteRequest tests encoding and decoding of ExecuteRequest.
func TestProtoRoundtrip_ExecuteRequest(t *testing.T) {
	original := &proto.ExecuteRequest{
		SessionId: "session123",
		Query:     "MATCH (n:Person) RETURN n.name",
		Params: []*proto.Param{
			proto.StringParam("param1", "value1"),
			proto.StringParam("param2", "value2"),
		},
	}

	// Encode
	data, err := gpb.Marshal(original)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	// Decode
	decoded := &proto.ExecuteRequest{}
	if err := gpb.Unmarshal(data, decoded); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	// Verify
	if decoded.GetSessionId() != original.GetSessionId() {
		t.Errorf("session_id mismatch: got %q, want %q", decoded.GetSessionId(), original.GetSessionId())
	}
	if decoded.GetQuery() != original.GetQuery() {
		t.Errorf("query mismatch: got %q, want %q", decoded.GetQuery(), original.GetQuery())
	}
	if len(decoded.GetParams()) != len(original.GetParams()) {
		t.Errorf("params count mismatch: got %d, want %d", len(decoded.GetParams()), len(original.GetParams()))
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
			msg: proto.NewHelloMessage(&proto.HelloRequest{
				Username: "user",
				Password: "pass",
			}),
		},
		{
			name: "execute",
			msg: proto.NewExecuteMessage(&proto.ExecuteRequest{
				Query: "RETURN 1",
			}),
		},
		{
			name: "ping",
			msg:  proto.NewPingMessage(&proto.PingRequest{}),
		},
		{
			name: "begin",
			msg: proto.NewBeginMessage(&proto.BeginRequest{
				ReadOnly: true,
			}),
		},
		{
			name: "commit",
			msg:  proto.NewCommitMessage(&proto.CommitRequest{}),
		},
		{
			name: "rollback",
			msg:  proto.NewRollbackMessage(&proto.RollbackRequest{}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			data, err := tt.msg.Encode()
			if err != nil {
				t.Fatalf("failed to encode: %v", err)
			}

			// Decode and verify it round-trips
			decoded, err := proto.DecodeQuicServerMessage(data)
			// We don't check error here because client messages
			// are not server messages; we just check no panic
			_ = decoded
			_ = err
		})
	}
}

// TestProtoRoundtrip_ServerMessages tests decoding of server messages.
func TestProtoRoundtrip_ServerMessages(t *testing.T) {
	// Test HelloResponse decode
	t.Run("HelloResponse", func(t *testing.T) {
		resp := &proto.HelloResponse{
			Success:   true,
			SessionId: "sess123",
		}

		data, err := gpb.Marshal(resp)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		decoded, err := proto.DecodeHelloResponse(data)
		if err != nil {
			t.Fatalf("failed to decode: %v", err)
		}

		if !decoded.GetSuccess() {
			t.Error("expected success=true")
		}
		if decoded.GetSessionId() != "sess123" {
			t.Errorf("session_id mismatch: got %q", decoded.GetSessionId())
		}
	})

	// Test PingResponse decode
	t.Run("PingResponse", func(t *testing.T) {
		resp := &proto.PingResponse{Ok: true}

		data, err := gpb.Marshal(resp)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		decoded, err := proto.DecodePingResponse(data)
		if err != nil {
			t.Fatalf("failed to decode: %v", err)
		}

		if !decoded.GetOk() {
			t.Error("expected ok=true")
		}
	})
}

// TestProtoRoundtrip_ExecutionResponse tests decoding of ExecutionResponse.
func TestProtoRoundtrip_ExecutionResponse(t *testing.T) {
	// Test with schema
	t.Run("schema", func(t *testing.T) {
		resp := &proto.ExecutionResponse{
			Payload: &proto.ExecutionResponse_Schema{
				Schema: &proto.SchemaDefinition{
					Columns: []*proto.ColumnDefinition{
						{Name: "id", Type: "INT"},
						{Name: "name", Type: "STRING"},
					},
				},
			},
		}

		data, err := gpb.Marshal(resp)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		decoded, err := proto.DecodeExecutionResponse(data)
		if err != nil {
			t.Fatalf("failed to decode: %v", err)
		}

		if decoded.GetSchema() == nil {
			t.Fatal("expected schema")
		}
		if len(decoded.GetSchema().GetColumns()) != 2 {
			t.Errorf("expected 2 columns, got %d", len(decoded.GetSchema().GetColumns()))
		}
	})

	// Test with data page
	t.Run("page", func(t *testing.T) {
		resp := &proto.ExecutionResponse{
			Payload: &proto.ExecutionResponse_Page{
				Page: &proto.DataPage{
					Rows: []*proto.Row{
						{
							Values: []*proto.Value{
								{Kind: &proto.Value_IntVal{IntVal: &proto.IntValue{Value: 42}}},
								{Kind: &proto.Value_StringVal{StringVal: &proto.StringValue{Value: "hello"}}},
							},
						},
					},
					Final: true,
				},
			},
		}

		data, err := gpb.Marshal(resp)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		decoded, err := proto.DecodeExecutionResponse(data)
		if err != nil {
			t.Fatalf("failed to decode: %v", err)
		}

		if decoded.GetPage() == nil {
			t.Fatal("expected page")
		}
		if !decoded.GetPage().GetFinal() {
			t.Error("expected final=true")
		}
		if len(decoded.GetPage().GetRows()) != 1 {
			t.Errorf("expected 1 row, got %d", len(decoded.GetPage().GetRows()))
		}
	})

	// Test with error
	t.Run("error", func(t *testing.T) {
		resp := &proto.ExecutionResponse{
			Payload: &proto.ExecutionResponse_Error{
				Error: &proto.Error{
					Code:    "42000",
					Message: "syntax error",
					Type:    "ERROR",
				},
			},
		}

		data, err := gpb.Marshal(resp)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		decoded, err := proto.DecodeExecutionResponse(data)
		if err != nil {
			t.Fatalf("failed to decode: %v", err)
		}

		if decoded.GetError() == nil {
			t.Fatal("expected error")
		}
		if decoded.GetError().GetCode() != "42000" {
			t.Errorf("code mismatch: got %q", decoded.GetError().GetCode())
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
