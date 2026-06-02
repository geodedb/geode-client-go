package crypto_test

import (
	"fmt"
	"testing"

	geocrypto "geodedb.com/geode/crypto"
)

var benchKey = func() []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = byte(i)
	}
	return k
}()

var benchCipher = func() *geocrypto.Cipher {
	c, err := geocrypto.NewCipher(benchKey)
	if err != nil {
		panic(err)
	}
	return c
}()

func BenchmarkEncrypt(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096}
	for _, n := range sizes {
		pt := make([]byte, n)
		b.Run(fmt.Sprintf("%dB", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := benchCipher.Encrypt(pt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDecrypt(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096}
	for _, n := range sizes {
		pt := make([]byte, n)
		ct, err := benchCipher.Encrypt(pt)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("%dB", n), func(b *testing.B) {
			b.SetBytes(int64(n))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := benchCipher.Decrypt(ct); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkEncryptString(b *testing.B) {
	input := "user@example.com"
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := benchCipher.EncryptString(input); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecryptString(b *testing.B) {
	enc, err := benchCipher.EncryptString("user@example.com")
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := benchCipher.DecryptString(enc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHMACHex(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = benchCipher.HMACHex("user@example.com")
	}
}

func BenchmarkNewCipher(b *testing.B) {
	key := make([]byte, 32)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := geocrypto.NewCipher(key); err != nil {
			b.Fatal(err)
		}
	}
}
