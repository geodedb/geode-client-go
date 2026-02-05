package geode

import (
	"testing"

	"geodedb.com/geode/internal/validate"
	"geodedb.com/geode/proto"
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
	msg := &proto.QuicClientMessage{
		Hello: &proto.HelloRequest{
			Username: "testuser",
			Password: "testpass",
		},
	}
	for i := 0; i < b.N; i++ {
		_, _ = msg.Encode()
	}
}

func BenchmarkProtoEncode_ExecuteRequest(b *testing.B) {
	msg := &proto.QuicClientMessage{
		Execute: &proto.ExecuteRequest{
			SessionID: "session123",
			Query:     "MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.age > 30 RETURN n.name, m.name",
			Parameters: map[string]string{
				"param1": "value1",
				"param2": "value2",
			},
		},
	}
	for i := 0; i < b.N; i++ {
		_, _ = msg.Encode()
	}
}

func BenchmarkProtoDecode_HelloResponse(b *testing.B) {
	// Create a sample HelloResponse
	resp := &proto.HelloResponse{
		Success:   true,
		SessionID: "session123",
	}
	codec := proto.GeodeCodec{}
	data, _ := codec.Marshal(resp)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = proto.DecodeHelloResponse(data)
	}
}

func BenchmarkProtoDecode_ExecutionResponse(b *testing.B) {
	// Create a sample ExecutionResponse with schema
	str := "hello"
	num := int64(42)
	resp := &proto.ExecutionResponse{
		Page: &proto.DataPage{
			Rows: []proto.Row{
				{Values: []proto.Value{{IntVal: &num}, {StringVal: &str}}},
				{Values: []proto.Value{{IntVal: &num}, {StringVal: &str}}},
			},
			LastPage: true,
		},
	}
	codec := proto.GeodeCodec{}
	data, _ := codec.Marshal(resp)

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
