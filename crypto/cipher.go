// Package crypto provides field-level encryption for Geode node and edge
// properties. It uses AES-256-GCM for authenticated encryption with
// HKDF-SHA256 for key derivation and HMAC-SHA256 for deterministic
// searchable encryption.
package crypto

import (
	"crypto/aes"
	gocipher "crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/hkdf"
)

const (
	keySize   = 32 // AES-256
	nonceSize = 12 // GCM nonce
)

// Cipher holds derived encryption and HMAC keys for field-level encryption.
type Cipher struct {
	encKey  []byte        // derived encryption key
	hmacKey []byte        // derived HMAC key
	aead    gocipher.AEAD // AES-256-GCM
}

// NewCipher creates a new Cipher from the given master key.
// masterKey must be at least 32 bytes.
func NewCipher(masterKey []byte) (*Cipher, error) {
	if len(masterKey) < keySize {
		return nil, fmt.Errorf("crypto: master key must be at least %d bytes, got %d", keySize, len(masterKey))
	}

	encKey, err := deriveKey(masterKey, "geode-encrypt")
	if err != nil {
		return nil, fmt.Errorf("crypto: derive encryption key: %w", err)
	}

	hmacKey, err := deriveKey(masterKey, "geode-hmac")
	if err != nil {
		return nil, fmt.Errorf("crypto: derive HMAC key: %w", err)
	}

	block, err := aes.NewCipher(encKey)
	if err != nil {
		return nil, fmt.Errorf("crypto: create AES cipher: %w", err)
	}

	aead, err := gocipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: create GCM AEAD: %w", err)
	}

	return &Cipher{
		encKey:  encKey,
		hmacKey: hmacKey,
		aead:    aead,
	}, nil
}

// deriveKey derives a keySize-byte key from masterKey using HKDF-SHA256.
func deriveKey(masterKey []byte, info string) ([]byte, error) {
	r := hkdf.New(sha256.New, masterKey, nil, []byte(info))
	key := make([]byte, keySize)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, err
	}
	return key, nil
}

// Encrypt encrypts plaintext using AES-256-GCM with a random nonce.
// The returned ciphertext has the nonce prepended.
func (c *Cipher) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("crypto: generate nonce: %w", err)
	}

	ciphertext := c.aead.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// Decrypt decrypts a ciphertext produced by Encrypt.
// Returns an error if the input is too short or authentication fails.
func (c *Cipher) Decrypt(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < nonceSize {
		return nil, errors.New("crypto: ciphertext too short")
	}

	nonce := ciphertext[:nonceSize]
	data := ciphertext[nonceSize:]

	plaintext, err := c.aead.Open(nil, nonce, data, nil)
	if err != nil {
		return nil, fmt.Errorf("crypto: decrypt: %w", err)
	}

	return plaintext, nil
}

// EncryptString encrypts the plaintext string and returns a base64-encoded result.
func (c *Cipher) EncryptString(plaintext string) (string, error) {
	ct, err := c.Encrypt([]byte(plaintext))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ct), nil
}

// DecryptString base64-decodes the input and decrypts it, returning the original string.
func (c *Cipher) DecryptString(encoded string) (string, error) {
	ct, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("crypto: base64 decode: %w", err)
	}

	pt, err := c.Decrypt(ct)
	if err != nil {
		return "", err
	}

	return string(pt), nil
}

// HMACHex computes HMAC-SHA256 of data using the derived HMAC key and returns
// a 64-character lowercase hex digest. Use this for deterministic lookups on
// encrypted fields (e.g., storing email_hash alongside an encrypted email
// column so equality queries remain possible without decrypting).
func (c *Cipher) HMACHex(data string) string {
	mac := hmac.New(sha256.New, c.hmacKey)
	mac.Write([]byte(data))
	return hex.EncodeToString(mac.Sum(nil))
}
