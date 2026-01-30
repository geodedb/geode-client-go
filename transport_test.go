package geode

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

func TestReadLineLimited_OK(t *testing.T) {
	br := bufio.NewReaderSize(bytes.NewBufferString("hello\n"), 4)
	line, err := readLineLimited(br, 16)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(line) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(line))
	}
}

func TestReadLineLimited_TooLargeDiscardsLine(t *testing.T) {
	limit := 10
	payload := strings.Repeat("a", limit+1)
	br := bufio.NewReaderSize(bytes.NewBufferString(payload+"\nnext\n"), 4)

	_, err := readLineLimited(br, limit)
	if err == nil {
		t.Fatalf("expected error for oversized frame")
	}
	se, ok := err.(*SecurityError)
	if !ok {
		t.Fatalf("expected *SecurityError, got %T", err)
	}
	if !strings.Contains(se.Message, "frame exceeds") {
		t.Fatalf("unexpected error message: %q", se.Message)
	}

	line, err := readLineLimited(br, limit)
	if err != nil {
		t.Fatalf("unexpected error reading next line: %v", err)
	}
	if string(line) != "next" {
		t.Fatalf("expected 'next', got %q", string(line))
	}
}
