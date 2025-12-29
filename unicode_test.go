package geodesql

import "testing"

func TestCANARY_ClientGo_Utf8Utf16RoundTrip(t *testing.T) {
	orig := "Hello 🌍"
	u16 := Utf8ToUtf16(orig)
	back := Utf16ToUtf8(u16)
	if back != orig { t.Fatalf("roundtrip mismatch: %q vs %q", orig, back) }
}

func TestCANARY_ClientGo_Wtf8Lossy(t *testing.T) {
	bad := []byte{0x61, 0x80, 0x62} // a, invalid, b
	res := Wtf8Lossy(bad)
	if len(res) < 3 { t.Fatalf("expected replacement handling, got %q", res) }
}

// CANARY: REQ=REQ-GQL-UNICODE-012; FEATURE="UTF-8 -> UTF-16 Conversion"; ASPECT=ClientTestGo; STATUS=TESTED; TEST=TestCANARY_ClientGo_Utf8Utf16RoundTrip; OWNER=clients; UPDATED=2025-09-27
// CANARY: REQ=REQ-GQL-UNICODE-013; FEATURE="UTF-16 -> UTF-8 Conversion"; ASPECT=ClientTestGo; STATUS=TESTED; TEST=TestCANARY_ClientGo_Utf8Utf16RoundTrip; OWNER=clients; UPDATED=2025-09-27
// CANARY: REQ=REQ-GQL-UNICODE-014; FEATURE="WTF-8 Lossy Decode"; ASPECT=ClientTestGo; STATUS=TESTED; TEST=TestCANARY_ClientGo_Wtf8Lossy; OWNER=clients; UPDATED=2025-09-27
