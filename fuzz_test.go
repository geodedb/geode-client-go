package geode

import (
	"testing"

	"geodedb.com/geode/internal/validate"
	"geodedb.com/geode/proto"
)

func FuzzParseDSN(f *testing.F) {
	// Add seed corpus
	f.Add("localhost:3141")
	f.Add("geode://localhost:3141")
	f.Add("quic://localhost:3141")
	f.Add("grpc://localhost:50051")
	f.Add("grcp://localhost:50051")
	f.Add("localhost:3141?page_size=100")
	f.Add("quic://host:8080?page_size=500&hello_name=test&insecure=true")
	f.Add("grpc://host:50051?tls=0")
	f.Add("")
	f.Add("host")
	f.Add("192.168.1.1:3141")
	f.Add("[::1]:3141")
	f.Add("quic://[::1]:3141")
	f.Add("grpc://[2001:db8::1]:50051")
	f.Add("localhost:3141?ca=/path/to/ca.crt")
	f.Add("localhost:3141?cert=/path/to/cert&key=/path/to/key")

	f.Fuzz(func(_ *testing.T, dsn string) {
		// ParseDSN should never panic
		cfg, err := ParseDSN(dsn)
		if err != nil {
			return
		}

		// If parsing succeeded, Validate should not panic
		_ = cfg.Validate()

		// Addr should not panic
		_ = cfg.Addr()

		// Clone should not panic
		_ = cfg.Clone()

		// Transport String should not panic
		_ = cfg.Transport.String()
	})
}

func FuzzProtoDecodeServerMessage(f *testing.F) {
	// Add seed corpus with valid protobuf-encoded messages
	// Note: These are simple handcrafted byte sequences
	f.Add([]byte{}) // Empty message

	f.Fuzz(func(_ *testing.T, data []byte) {
		// DecodeQuicServerMessage should never panic
		_, _ = proto.DecodeQuicServerMessage(data)
	})
}

func FuzzProtoDecodeExecutionResponse(f *testing.F) {
	f.Add([]byte{})

	f.Fuzz(func(_ *testing.T, data []byte) {
		// DecodeExecutionResponse should never panic
		_, _ = proto.DecodeExecutionResponse(data)
	})
}

func FuzzValidateQuery(f *testing.F) {
	// Add seed corpus
	f.Add("RETURN 1")
	f.Add("MATCH (n) RETURN n")
	f.Add("MATCH (n:Person)-[:KNOWS]->(m) RETURN n.name")
	f.Add("")
	f.Add("SELECT * FROM users") // Not GQL, but should not panic

	f.Fuzz(func(_ *testing.T, query string) {
		// Should never panic
		_ = validate.Query(query)
	})
}

func FuzzValidateParamName(f *testing.F) {
	// Add seed corpus
	f.Add("name")
	f.Add("_private")
	f.Add("param1")
	f.Add("")
	f.Add("1invalid")
	f.Add("with space")

	f.Fuzz(func(_ *testing.T, name string) {
		// Should never panic
		_ = validate.ParamName(name)
	})
}

func FuzzParseGQLType(f *testing.F) {
	f.Add("INT")
	f.Add("STRING")
	f.Add("BOOL")
	f.Add("")
	f.Add("UNKNOWN_TYPE")

	f.Fuzz(func(_ *testing.T, typeStr string) {
		// Should never panic
		gqlType := ParseGQLType(typeStr)
		_ = gqlType.GoType()
	})
}
