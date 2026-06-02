package crypto_test

import (
	"bytes"
	"testing"

	geocrypto "geodedb.com/geode/crypto"
)

func FuzzEncryptDecrypt(f *testing.F) {
	f.Add([]byte("hello"))
	f.Add([]byte(""))
	f.Add([]byte{0x00, 0xFF, 0xAB})
	f.Add(make([]byte, 1024))

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	c, err := geocrypto.NewCipher(key)
	if err != nil {
		f.Fatal(err)
	}

	f.Fuzz(func(t *testing.T, plaintext []byte) {
		ct, err := c.Encrypt(plaintext)
		if err != nil {
			t.Skip()
		}
		got, err := c.Decrypt(ct)
		if err != nil {
			t.Fatalf("Decrypt failed after Encrypt: %v", err)
		}
		if !bytes.Equal(got, plaintext) {
			t.Fatalf("roundtrip mismatch: got %x, want %x", got, plaintext)
		}
	})
}

func FuzzDecrypt(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 11)) // shorter than nonceSize
	f.Add(make([]byte, 12)) // exactly nonceSize
	f.Add(make([]byte, 28)) // nonceSize + 16 (GCM tag size)

	key := make([]byte, 32)
	c, err := geocrypto.NewCipher(key)
	if err != nil {
		f.Fatal(err)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must not panic; error is expected for arbitrary data
		_, _ = c.Decrypt(data)
	})
}

func FuzzHMACHex(f *testing.F) {
	f.Add("")
	f.Add("user@example.com")
	f.Add(string(make([]byte, 1024)))

	key := make([]byte, 32)
	c, err := geocrypto.NewCipher(key)
	if err != nil {
		f.Fatal(err)
	}

	f.Fuzz(func(t *testing.T, data string) {
		h := c.HMACHex(data)
		if len(h) != 64 {
			t.Fatalf("HMACHex(%q) returned %d chars, want 64", data, len(h))
		}
	})
}
