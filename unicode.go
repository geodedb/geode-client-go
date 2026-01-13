package geode

import (
	"unicode/utf16"
	"unicode/utf8"
)

// Unicode conversion utilities for client-side convenience.

// Utf8ToUtf16 converts a UTF-8 string to UTF-16 code units.
// CANARY: REQ=REQ-GQL-UNICODE-012; FEATURE="UTF-8 -> UTF-16 Conversion"; ASPECT=ClientWrapperGo; STATUS=IMPL; TEST=TestCANARY_ClientGo_Utf8Utf16RoundTrip; OWNER=clients; UPDATED=2025-09-27
func Utf8ToUtf16(s string) []uint16 {
	return utf16.Encode([]rune(s))
}

// Utf16ToUtf8 converts UTF-16 code units to a UTF-8 string.
// CANARY: REQ=REQ-GQL-UNICODE-013; FEATURE="UTF-16 -> UTF-8 Conversion"; ASPECT=ClientWrapperGo; STATUS=IMPL; TEST=TestCANARY_ClientGo_Utf8Utf16RoundTrip; OWNER=clients; UPDATED=2025-09-27
func Utf16ToUtf8(u []uint16) string {
	return string(utf16.Decode(u))
}

// Wtf8Lossy converts a byte slice to a UTF-8 string, replacing
// invalid sequences with the Unicode replacement character (U+FFFD).
// CANARY: REQ=REQ-GQL-UNICODE-014; FEATURE="WTF-8 Lossy Decode"; ASPECT=ClientWrapperGo; STATUS=IMPL; TEST=TestCANARY_ClientGo_Wtf8Lossy; OWNER=clients; UPDATED=2025-09-27
func Wtf8Lossy(b []byte) string {
	var out []rune
	i := 0
	for i < len(b) {
		r, size := utf8.DecodeRune(b[i:])
		if r == utf8.RuneError && size == 1 {
			// Invalid sequence - replace with U+FFFD
			out = append(out, '\uFFFD')
			i++
			continue
		}
		out = append(out, r)
		i += size
	}
	return string(out)
}
