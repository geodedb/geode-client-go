package geode

import (
	"testing"

	"geodedb.com/geode/internal/validate"
	"geodedb.com/geode/proto"
	gpb "google.golang.org/protobuf/proto"
)

func BenchmarkParseDSN_Simple(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = ParseDSN("localhost:3141")
	}
}

func BenchmarkParseDSN_QUIC(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = ParseDSN("quic://localhost:3141")
	}
}

func BenchmarkParseDSN_GRPC(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = ParseDSN("grpc://localhost:50051")
	}
}

func BenchmarkParseDSN_URL(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = ParseDSN("quic://localhost:3141")
	}
}

func BenchmarkParseDSN_WithOptions(b *testing.B) {
	dsn := "localhost:3141?page_size=500&hello_name=myapp&hello_ver=1.0&conformance=full"
	for i := 0; i < b.N; i++ {
		_, _ = ParseDSN(dsn)
	}
}

func BenchmarkProtoEncode_HelloRequest(b *testing.B) {
	msg := proto.NewHelloMessage(&proto.HelloRequest{
		Username: "testuser",
		Password: "testpass",
	})
	for i := 0; i < b.N; i++ {
		_, _ = msg.Encode()
	}
}

func BenchmarkProtoEncode_ExecuteRequest(b *testing.B) {
	msg := proto.NewExecuteMessage(&proto.ExecuteRequest{
		SessionId: "session123",
		Query:     "MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.age > 30 RETURN n.name, m.name",
		Params: []*proto.Param{
			proto.StringParam("param1", "value1"),
			proto.StringParam("param2", "value2"),
		},
	})
	for i := 0; i < b.N; i++ {
		_, _ = msg.Encode()
	}
}

func BenchmarkProtoDecode_HelloResponse(b *testing.B) {
	resp := &proto.HelloResponse{
		Success:   true,
		SessionId: "session123",
	}
	data, _ := gpb.Marshal(resp)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = proto.DecodeHelloResponse(data)
	}
}

func BenchmarkProtoDecode_ExecutionResponse(b *testing.B) {
	resp := &proto.ExecutionResponse{
		Payload: &proto.ExecutionResponse_Page{
			Page: &proto.DataPage{
				Rows: []*proto.Row{
					{Values: []*proto.Value{
						{Kind: &proto.Value_IntVal{IntVal: &proto.IntValue{Value: 42}}},
						{Kind: &proto.Value_StringVal{StringVal: &proto.StringValue{Value: "hello"}}},
					}},
					{Values: []*proto.Value{
						{Kind: &proto.Value_IntVal{IntVal: &proto.IntValue{Value: 42}}},
						{Kind: &proto.Value_StringVal{StringVal: &proto.StringValue{Value: "hello"}}},
					}},
				},
				Final: true,
			},
		},
	}
	data, _ := gpb.Marshal(resp)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = proto.DecodeExecutionResponse(data)
	}
}

func BenchmarkValidateQuery_Short(b *testing.B) {
	query := "RETURN 1"
	for i := 0; i < b.N; i++ {
		_ = validate.Query(query)
	}
}

func BenchmarkValidateQuery_Medium(b *testing.B) {
	query := "MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.age > 30 RETURN n.name, m.name ORDER BY n.name LIMIT 100"
	for i := 0; i < b.N; i++ {
		_ = validate.Query(query)
	}
}

func BenchmarkValidateParamName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = validate.ParamName("user_name_123")
	}
}

func BenchmarkParseGQLType(b *testing.B) {
	types := []string{"INT", "STRING", "BOOL", "FLOAT", "NODE", "EDGE", "PATH"}
	for i := 0; i < b.N; i++ {
		for _, t := range types {
			_ = ParseGQLType(t)
		}
	}
}

func BenchmarkDriverError_Error(b *testing.B) {
	err := &DriverError{
		Code:    "US001",
		Message: "unsupported feature in query",
	}
	for i := 0; i < b.N; i++ {
		_ = err.Error()
	}
}

func BenchmarkConfig_Addr(b *testing.B) {
	cfg := &Config{Host: "localhost", Port: "3141"}
	for i := 0; i < b.N; i++ {
		_ = cfg.Addr()
	}
}

func BenchmarkConfig_Clone(b *testing.B) {
	cfg := &Config{
		Host:         "localhost",
		Port:         "3141",
		PageSize:     1000,
		HelloName:    "test",
		HelloVersion: "1.0",
		Conformance:  "min",
		TLSCA:        "/path/to/ca.crt",
		Transport:    TransportQUIC,
	}
	for i := 0; i < b.N; i++ {
		_ = cfg.Clone()
	}
}

func BenchmarkTransportType_String(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = TransportQUIC.String()
		_ = TransportGRPC.String()
	}
}
