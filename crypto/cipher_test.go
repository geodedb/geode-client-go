package crypto_test

import (
	"bytes"
	"encoding/hex"
	"testing"

	geocrypto "geodedb.com/geode/crypto"
)

var testKey = bytes.Repeat([]byte("k"), 32)

func TestNewCipher(t *testing.T) {
	t.Parallel()

	c, err := geocrypto.NewCipher(testKey)
	if err != nil {
		t.Fatalf("NewCipher with 32-byte key failed: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil Cipher")
	}
}

func TestNewCipher_ShortKey(t *testing.T) {
	t.Parallel()

	_, err := geocrypto.NewCipher([]byte("short"))
	if err == nil {
		t.Fatal("expected error for short key, got nil")
	}
}

func TestNewCipher_NilKey(t *testing.T) {
	t.Parallel()

	_, err := geocrypto.NewCipher(nil)
	if err == nil {
		t.Fatal("expected error for nil key, got nil")
	}
}

func TestEncryptDecrypt_Binary(t *testing.T) {
	t.Parallel()

	c, err := geocrypto.NewCipher(testKey)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}

	plaintext := []byte{0x00, 0x01, 0xFF, 0xFE, 0x80, 0x7F}

	ct, err := c.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	got, err := c.Decrypt(ct)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}

	if !bytes.Equal(plaintext, got) {
		t.Fatalf("roundtrip mismatch: want %x, got %x", plaintext, got)
	}
}

func TestEncryptDecrypt_String(t *testing.T) {
	t.Parallel()

	c, err := geocrypto.NewCipher(testKey)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}

	plaintext := "hello, geode!"

	encoded, err := c.EncryptString(plaintext)
	if err != nil {
		t.Fatalf("EncryptString: %v", err)
	}

	got, err := c.DecryptString(encoded)
	if err != nil {
		t.Fatalf("DecryptString: %v", err)
	}

	if got != plaintext {
		t.Fatalf("roundtrip mismatch: want %q, got %q", plaintext, got)
	}
}

func TestEncrypt_DifferentNonces(t *testing.T) {
	t.Parallel()

	c, err := geocrypto.NewCipher(testKey)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}

	plaintext := []byte("same plaintext")

	ct1, err := c.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt 1: %v", err)
	}

	ct2, err := c.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt 2: %v", err)
	}

	if bytes.Equal(ct1, ct2) {
		t.Fatal("expected different ciphertexts due to random nonces, got identical")
	}
}

func TestDecrypt_TamperedCiphertext(t *testing.T) {
	t.Parallel()

	c, err := geocrypto.NewCipher(testKey)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}

	ct, err := c.Encrypt([]byte("sensitive data"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// Flip a byte in the ciphertext portion (after nonce).
	tampered := make([]byte, len(ct))
	copy(tampered, ct)
	tampered[len(tampered)-1] ^= 0xFF

	_, err = c.Decrypt(tampered)
	if err == nil {
		t.Fatal("expected authentication failure for tampered ciphertext, got nil error")
	}
}

func TestDecrypt_TooShort(t *testing.T) {
	t.Parallel()

	c, err := geocrypto.NewCipher(testKey)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}

	_, err = c.Decrypt([]byte{0x01, 0x02})
	if err == nil {
		t.Fatal("expected error for too-short ciphertext, got nil")
	}
}

func TestHMACHex_Deterministic(t *testing.T) {
	t.Parallel()

	c, err := geocrypto.NewCipher(testKey)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}

	input := "user@example.com"
	h1 := c.HMACHex(input)
	h2 := c.HMACHex(input)

	if h1 != h2 {
		t.Fatalf("HMACHex is not deterministic: %q != %q", h1, h2)
	}

	// Verify it's valid hex.
	if _, err := hex.DecodeString(h1); err != nil {
		t.Fatalf("HMACHex returned invalid hex %q: %v", h1, err)
	}
}

func TestHMACHex_DifferentInputs(t *testing.T) {
	t.Parallel()

	c, err := geocrypto.NewCipher(testKey)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}

	h1 := c.HMACHex("alice@example.com")
	h2 := c.HMACHex("bob@example.com")

	if h1 == h2 {
		t.Fatal("expected different HMAC outputs for different inputs")
	}
}

func TestHMACHex_DifferentKeys(t *testing.T) {
	t.Parallel()

	key1 := bytes.Repeat([]byte("a"), 32)
	key2 := bytes.Repeat([]byte("b"), 32)

	c1, err := geocrypto.NewCipher(key1)
	if err != nil {
		t.Fatalf("NewCipher key1: %v", err)
	}

	c2, err := geocrypto.NewCipher(key2)
	if err != nil {
		t.Fatalf("NewCipher key2: %v", err)
	}

	input := "same-input"
	h1 := c1.HMACHex(input)
	h2 := c2.HMACHex(input)

	if h1 == h2 {
		t.Fatal("expected different HMAC outputs for different keys")
	}
}

func TestEncryptDecrypt_EmptyPlaintext(t *testing.T) {
	t.Parallel()

	c, err := geocrypto.NewCipher(testKey)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}

	plaintext := []byte{}

	ct, err := c.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt empty: %v", err)
	}

	got, err := c.Decrypt(ct)
	if err != nil {
		t.Fatalf("Decrypt empty: %v", err)
	}

	if !bytes.Equal(plaintext, got) {
		t.Fatalf("roundtrip empty mismatch: want %x, got %x", plaintext, got)
	}

	// Sanity: result is empty or nil but not a non-empty slice.
	if len(got) != 0 {
		t.Fatalf("expected empty result, got len=%d", len(got))
	}
}
