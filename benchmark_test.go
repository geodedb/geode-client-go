package geode

import (
	"testing"

	"geodedb.com/geode/internal/validate"
)

func BenchmarkParseDSN_Simple(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = ParseDSN("localhost:3141")
	}
}

func BenchmarkParseDSN_URL(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = ParseDSN("geode://localhost:3141")
	}
}

func BenchmarkParseDSN_WithOptions(b *testing.B) {
	dsn := "localhost:3141?page_size=500&hello_name=myapp&hello_ver=1.0&conformance=full"
	for i := 0; i < b.N; i++ {
		_, _ = ParseDSN(dsn)
	}
}

func BenchmarkParseFrame_Schema(b *testing.B) {
	data := []byte(`{"status_class":"00000","result":{"type":"SCHEMA","columns":[{"name":"x","type":"INT"},{"name":"y","type":"STRING"}]}}`)
	for i := 0; i < b.N; i++ {
		_, _ = ParseFrame(data)
	}
}

func BenchmarkParseFrame_Bindings(b *testing.B) {
	data := []byte(`{"status_class":"00000","result":{"type":"BINDINGS","rows":[{"x":1,"y":"hello"},{"x":2,"y":"world"}],"final":true}}`)
	for i := 0; i < b.N; i++ {
		_, _ = ParseFrame(data)
	}
}

func BenchmarkParseFrame_LargeBindings(b *testing.B) {
	// Create a frame with many rows
	data := []byte(`{"status_class":"00000","result":{"type":"BINDINGS","rows":[`)
	for i := 0; i < 100; i++ {
		if i > 0 {
			data = append(data, ',')
		}
		data = append(data, `{"x":`...)
		data = append(data, '0'+byte(i%10))
		data = append(data, `}`...)
	}
	data = append(data, `],"final":true}}`...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseFrame(data)
	}
}

func BenchmarkBuildHelloMessage(b *testing.B) {
	cfg := &Config{
		HelloName:    "myapp",
		HelloVersion: "1.0",
		Conformance:  "min",
	}
	for i := 0; i < b.N; i++ {
		_ = BuildHelloMessage(cfg)
	}
}

func BenchmarkBuildRunGQLMessage(b *testing.B) {
	params := map[string]any{"x": 1, "y": "hello"}
	for i := 0; i < b.N; i++ {
		_ = BuildRunGQLMessage("MATCH (n) WHERE n.x = $x AND n.y = $y RETURN n", params)
	}
}

func BenchmarkBuildPullMessage(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = BuildPullMessage(1, 1000)
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

func BenchmarkConvertValue(b *testing.B) {
	values := []any{
		nil,
		true,
		float64(42),
		float64(3.14),
		"hello world",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range values {
			_ = ConvertValue(v)
		}
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
	}
	for i := 0; i < b.N; i++ {
		_ = cfg.Clone()
	}
}
