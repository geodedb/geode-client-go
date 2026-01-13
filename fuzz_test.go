package geode

import (
	"testing"

	"go.codepros.org/geode/internal/validate"
)

func FuzzParseDSN(f *testing.F) {
	// Add seed corpus
	f.Add("localhost:3141")
	f.Add("geode://localhost:3141")
	f.Add("localhost:3141?page_size=100")
	f.Add("geode://host:8080?page_size=500&hello_name=test&insecure=true")
	f.Add("")
	f.Add("host")
	f.Add("192.168.1.1:3141")
	f.Add("::1:3141")
	f.Add("localhost:3141?ca=/path/to/ca.crt")
	f.Add("localhost:3141?cert=/path/to/cert&key=/path/to/key")

	f.Fuzz(func(t *testing.T, dsn string) {
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
	})
}

func FuzzParseFrame(f *testing.F) {
	// Add seed corpus
	f.Add([]byte(`{"status_class":"00000","result":{"type":"SCHEMA"}}`))
	f.Add([]byte(`{"status_class":"00000","result":{"type":"BINDINGS","rows":[],"final":true}}`))
	f.Add([]byte(`{"status_class":"42000","result":{"type":"ERROR","code":"US001"}}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"result":{}}`))
	f.Add([]byte(`{"status_class":"00000","result":{"type":"BINDINGS","rows":[{"x":1}]}}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		// ParseFrame should never panic
		frame, err := ParseFrame(data)
		if err != nil {
			return
		}

		// These methods should never panic
		_ = frame.IsError()
		_ = frame.IsSchema()
		_ = frame.IsBindings()
		_ = frame.IsFinal()
		_ = frame.ToError()
	})
}

func FuzzValidateQuery(f *testing.F) {
	// Add seed corpus
	f.Add("RETURN 1")
	f.Add("MATCH (n) RETURN n")
	f.Add("MATCH (n:Person)-[:KNOWS]->(m) RETURN n.name")
	f.Add("")
	f.Add("SELECT * FROM users") // Not GQL, but should not panic

	f.Fuzz(func(t *testing.T, query string) {
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

	f.Fuzz(func(t *testing.T, name string) {
		// Should never panic
		_ = validate.ParamName(name)
	})
}

func FuzzConvertValue(f *testing.F) {
	// Note: Go's fuzzer only supports basic types
	// We can only fuzz with strings here

	f.Fuzz(func(t *testing.T, s string) {
		// Should never panic
		_ = ConvertValue(s)
		_ = ConvertValue(nil)
		_ = ConvertValue(true)
		_ = ConvertValue(float64(42))
	})
}

func FuzzParseGQLType(f *testing.F) {
	f.Add("INT")
	f.Add("STRING")
	f.Add("BOOL")
	f.Add("")
	f.Add("UNKNOWN_TYPE")

	f.Fuzz(func(t *testing.T, typeStr string) {
		// Should never panic
		gqlType := ParseGQLType(typeStr)
		_ = gqlType.GoType()
	})
}
