package geode

import (
	"reflect"
	"testing"
)

func TestParseFrame_Schema(t *testing.T) {
	data := []byte(`{"status_class":"00000","result":{"type":"SCHEMA","columns":[{"name":"x","type":"INT"},{"name":"y","type":"STRING"}]}}`)

	frame, err := ParseFrame(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if frame.StatusClass != StatusSuccess {
		t.Errorf("expected status_class %q, got %q", StatusSuccess, frame.StatusClass)
	}
	if !frame.IsSchema() {
		t.Error("expected IsSchema() to be true")
	}
	if len(frame.Result.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(frame.Result.Columns))
	}
	if frame.Result.Columns[0].Name != "x" || frame.Result.Columns[0].Type != "INT" {
		t.Errorf("unexpected first column: %+v", frame.Result.Columns[0])
	}
}

func TestParseFrame_Bindings(t *testing.T) {
	data := []byte(`{"status_class":"00000","result":{"type":"BINDINGS","rows":[{"x":1,"y":"hello"}],"ordered":true,"order_keys":["x"],"final":true}}`)

	frame, err := ParseFrame(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !frame.IsBindings() {
		t.Error("expected IsBindings() to be true")
	}
	if !frame.IsFinal() {
		t.Error("expected IsFinal() to be true")
	}
	if !frame.Result.Ordered {
		t.Error("expected ordered to be true")
	}
	if len(frame.Result.OrderKeys) != 1 || frame.Result.OrderKeys[0] != "x" {
		t.Errorf("unexpected order_keys: %v", frame.Result.OrderKeys)
	}
	if len(frame.Result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(frame.Result.Rows))
	}
}

func TestParseFrame_Error(t *testing.T) {
	data := []byte(`{"status_class":"42000","status_subclass":"Syntax","result":{"type":"ERROR","code":"US001","message":"unsupported feature","anchor":"line 1:5"}}`)

	frame, err := ParseFrame(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !frame.IsError() {
		t.Error("expected IsError() to be true")
	}

	derr := frame.ToError()
	if derr == nil {
		t.Fatal("expected ToError() to return non-nil")
	}

	de, ok := derr.(*DriverError)
	if !ok {
		t.Fatalf("expected *DriverError, got %T", derr)
	}

	if de.Code != "US001" {
		t.Errorf("expected code US001, got %q", de.Code)
	}
	if de.Message != "unsupported feature" {
		t.Errorf("expected message 'unsupported feature', got %q", de.Message)
	}
	if de.Anchor != "line 1:5" {
		t.Errorf("expected anchor 'line 1:5', got %q", de.Anchor)
	}
	if de.Class != StatusSyntax {
		t.Errorf("expected class %q, got %q", StatusSyntax, de.Class)
	}
}

func TestParseFrame_Invalid(t *testing.T) {
	_, err := ParseFrame([]byte(`invalid json`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestBuildHelloMessage(t *testing.T) {
	cfg := &Config{
		HelloName:    "myapp",
		HelloVersion: "1.0",
		Conformance:  "full",
	}

	msg := BuildHelloMessage(cfg)

	if msg["type"] != MsgHello {
		t.Errorf("expected type %q, got %q", MsgHello, msg["type"])
	}
	if msg["client_name"] != "myapp" {
		t.Errorf("expected client_name 'myapp', got %q", msg["client_name"])
	}
	if msg["client_ver"] != "1.0" {
		t.Errorf("expected client_ver '1.0', got %q", msg["client_ver"])
	}
	if msg["wanted_conformance"] != "full" {
		t.Errorf("expected wanted_conformance 'full', got %q", msg["wanted_conformance"])
	}
}

func TestBuildRunGQLMessage(t *testing.T) {
	msg := BuildRunGQLMessage("RETURN $x", map[string]any{"x": 42})

	if msg["type"] != MsgRunGQL {
		t.Errorf("expected type %q, got %q", MsgRunGQL, msg["type"])
	}
	if msg["text"] != "RETURN $x" {
		t.Errorf("expected text 'RETURN $x', got %q", msg["text"])
	}
	params, ok := msg["params"].(map[string]any)
	if !ok {
		t.Fatal("expected params to be map[string]any")
	}
	if params["x"] != 42 {
		t.Errorf("expected params[x]=42, got %v", params["x"])
	}
}

func TestBuildRunGQLMessage_NoParams(t *testing.T) {
	msg := BuildRunGQLMessage("RETURN 1", nil)

	if _, ok := msg["params"]; ok {
		t.Error("expected no params key when params is nil")
	}
}

func TestBuildPullMessage(t *testing.T) {
	msg := BuildPullMessage(1, 500)

	if msg["type"] != MsgPull {
		t.Errorf("expected type %q, got %q", MsgPull, msg["type"])
	}
	if msg["request_id"] != 1 {
		t.Errorf("expected request_id 1, got %v", msg["request_id"])
	}
	if msg["page_size"] != 500 {
		t.Errorf("expected page_size 500, got %v", msg["page_size"])
	}
}

func TestBuildTransactionMessages(t *testing.T) {
	begin := BuildBeginMessage()
	if begin["type"] != MsgBegin {
		t.Errorf("expected type %q, got %q", MsgBegin, begin["type"])
	}

	commit := BuildCommitMessage()
	if commit["type"] != MsgCommit {
		t.Errorf("expected type %q, got %q", MsgCommit, commit["type"])
	}

	rollback := BuildRollbackMessage()
	if rollback["type"] != MsgRollback {
		t.Errorf("expected type %q, got %q", MsgRollback, rollback["type"])
	}

	savepoint := BuildSavepointMessage("sp1")
	if savepoint["type"] != MsgSavepoint {
		t.Errorf("expected type %q, got %q", MsgSavepoint, savepoint["type"])
	}
	if savepoint["name"] != "sp1" {
		t.Errorf("expected name 'sp1', got %q", savepoint["name"])
	}
}

func TestBuildPingMessage(t *testing.T) {
	msg := BuildPingMessage()
	if msg["type"] != MsgPing {
		t.Errorf("expected type %q, got %q", MsgPing, msg["type"])
	}
}

func TestConvertValue(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
	}{
		{"nil", nil, nil},
		{"bool", true, true},
		{"int float64", float64(42), int64(42)},
		{"float float64", float64(3.14), float64(3.14)},
		{"string", "hello", "hello"},
		{"array", []any{1, 2}, []any{1, 2}},
		{"map", map[string]any{"a": 1}, map[string]any{"a": 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ConvertValue(tt.input)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("ConvertValue(%v) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

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
