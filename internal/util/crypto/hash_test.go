package crypto

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/sha3"
)

func TestComputeHash_SHA256(t *testing.T) {
	data := []byte("hello world")
	expected := sha256.Sum256(data)
	expectedHex := hex.EncodeToString(expected[:])

	result := ComputeHash(data, HashSHA256)
	assert.Equal(t, expectedHex, result)
}

func TestComputeHash_SHA3(t *testing.T) {
	data := []byte("hello world")
	h := sha3.New256()
	h.Write(data)
	expectedHex := hex.EncodeToString(h.Sum(nil))

	result := ComputeHash(data, HashSHA3)
	assert.Equal(t, expectedHex, result)
}

func TestComputeHash_DefaultFallback(t *testing.T) {
	data := []byte("hello world")
	expected := sha256.Sum256(data)
	expectedHex := hex.EncodeToString(expected[:])

	result := ComputeHash(data, "unknown")
	assert.Equal(t, expectedHex, result)
}

func TestComputeHash_EmptyData(t *testing.T) {
	sha256Result := ComputeHash([]byte{}, HashSHA256)
	sha3Result := ComputeHash([]byte{}, HashSHA3)

	assert.NotEmpty(t, sha256Result)
	assert.NotEmpty(t, sha3Result)
	assert.NotEqual(t, sha256Result, sha3Result)
}

func TestComputeHash_DifferentAlgorithmsProduceDifferentHashes(t *testing.T) {
	data := []byte("test data for hashing")

	sha256Result := ComputeHash(data, HashSHA256)
	sha3Result := ComputeHash(data, HashSHA3)

	assert.Len(t, sha256Result, 64)
	assert.Len(t, sha3Result, 64)
	assert.NotEqual(t, sha256Result, sha3Result)
}

func TestComputeHash_Deterministic(t *testing.T) {
	data := []byte("deterministic test")

	result1 := ComputeHash(data, HashSHA256)
	result2 := ComputeHash(data, HashSHA256)
	assert.Equal(t, result1, result2)

	result3 := ComputeHash(data, HashSHA3)
	result4 := ComputeHash(data, HashSHA3)
	assert.Equal(t, result3, result4)
}

func TestComputeHash_KnownVectors(t *testing.T) {
	// NIST test vector: SHA-256 of empty string
	assert.Equal(t,
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		ComputeHash([]byte{}, HashSHA256),
	)
	// SHA3-256 of empty string (NIST FIPS 202)
	assert.Equal(t,
		"a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a",
		ComputeHash([]byte{}, HashSHA3),
	)
}

func TestComputeHash_SHA3_KnownVector(t *testing.T) {
	// "abc" SHA3-256 from NIST examples
	assert.Equal(t,
		"3a985da74fe225b2045c172d6bd390bd855f086e3e9d525b46bfe24511431532",
		ComputeHash([]byte("abc"), HashSHA3),
	)
}

func TestComputeHash_OutputIsValidHex(t *testing.T) {
	for _, ht := range []HashType{HashSHA256, HashSHA3} {
		result := ComputeHash([]byte("test"), ht)
		_, err := hex.DecodeString(result)
		assert.NoError(t, err, "output for %s should be valid hex", ht)
	}
}

func TestComputeHash_OutputLength(t *testing.T) {
	inputs := [][]byte{
		{},
		{0x00},
		[]byte("short"),
		[]byte(strings.Repeat("a", 10000)),
	}
	for _, input := range inputs {
		for _, ht := range []HashType{HashSHA256, HashSHA3} {
			result := ComputeHash(input, ht)
			assert.Len(t, result, 64,
				"256-bit hash should always produce 64 hex chars (algo=%s, inputLen=%d)", ht, len(input))
		}
	}
}

func TestComputeHash_DifferentInputsProduceDifferentHashes(t *testing.T) {
	for _, ht := range []HashType{HashSHA256, HashSHA3} {
		a := ComputeHash([]byte("input_a"), ht)
		b := ComputeHash([]byte("input_b"), ht)
		assert.NotEqual(t, a, b, "different inputs should hash differently for %s", ht)
	}
}

func TestComputeHash_NilInput(t *testing.T) {
	sha256Result := ComputeHash(nil, HashSHA256)
	sha3Result := ComputeHash(nil, HashSHA3)

	assert.Len(t, sha256Result, 64)
	assert.Len(t, sha3Result, 64)

	// nil and empty slice should produce the same hash
	assert.Equal(t, ComputeHash([]byte{}, HashSHA256), sha256Result)
	assert.Equal(t, ComputeHash([]byte{}, HashSHA3), sha3Result)
}

func TestComputeHash_BinaryData(t *testing.T) {
	data := []byte{0x00, 0xFF, 0x01, 0xFE, 0x80}
	for _, ht := range []HashType{HashSHA256, HashSHA3} {
		result := ComputeHash(data, ht)
		assert.Len(t, result, 64)
		_, err := hex.DecodeString(result)
		assert.NoError(t, err)
	}
}

func TestComputeHash_LargeInput(t *testing.T) {
	data := []byte(strings.Repeat("x", 1<<20)) // 1 MiB
	for _, ht := range []HashType{HashSHA256, HashSHA3} {
		result := ComputeHash(data, ht)
		assert.Len(t, result, 64)
	}
}

func TestComputeHash_FallbackVariants(t *testing.T) {
	data := []byte("fallback test")
	expected := ComputeHash(data, HashSHA256)

	for _, invalid := range []HashType{"", "SHA256", "SHA-256", "md5", "sha512", "SHA3"} {
		result := ComputeHash(data, invalid)
		assert.Equal(t, expected, result,
			"invalid HashType %q should fall back to SHA-256", invalid)
	}
}

func TestComputeHash_TruncatedDigest(t *testing.T) {
	// Mirrors how callers use ComputeHash: truncating to [:16] for fingerprints
	data := []byte("config-id-1" + "persistent" + "payload-bytes")
	for _, ht := range []HashType{HashSHA256, HashSHA3} {
		full := ComputeHash(data, ht)
		truncated := full[:16]
		assert.Len(t, truncated, 16)
		_, err := hex.DecodeString(truncated)
		assert.NoError(t, err, "truncated digest should be valid hex for %s", ht)
	}
}

func TestComputeHash_ConcatenatedFields(t *testing.T) {
	// Mirrors telemetry usage: hashing concatenated config fields
	configID := "cfg-001"
	configType := "persistent"
	payload := []byte(`{"key":"value"}`)

	var data []byte
	data = append(data, []byte(configID)...)
	data = append(data, []byte(configType)...)
	data = append(data, payload...)

	for _, ht := range []HashType{HashSHA256, HashSHA3} {
		result := ComputeHash(data, ht)
		assert.Len(t, result, 64)
		// Same concatenation should always produce same result
		assert.Equal(t, result, ComputeHash(data, ht))
	}
}

func TestComputeHash_LegacyClientID(t *testing.T) {
	// Mirrors manager.go legacy client ID generation
	sdkType := "python"
	sdkVersion := "2.7.0"
	host := "10.0.0.1"
	user := "root"
	seed := fmt.Sprintf("%s|%s|%s|%s", sdkType, sdkVersion, host, user)

	for _, ht := range []HashType{HashSHA256, HashSHA3} {
		hash := ComputeHash([]byte(seed), ht)
		clientID := fmt.Sprintf("legacy:%s:%s", host, hash[:16])
		assert.True(t, strings.HasPrefix(clientID, "legacy:10.0.0.1:"))
		assert.Len(t, hash[:16], 16)
	}
}

func TestComputeHash_OrderSensitivity(t *testing.T) {
	// Verify that field order matters (callers sort before hashing)
	for _, ht := range []HashType{HashSHA256, HashSHA3} {
		ab := ComputeHash([]byte("AB"), ht)
		ba := ComputeHash([]byte("BA"), ht)
		assert.NotEqual(t, ab, ba, "hash should be order-sensitive for %s", ht)
	}
}

func TestHashTypeConstants(t *testing.T) {
	assert.Equal(t, HashType("sha256"), HashSHA256)
	assert.Equal(t, HashType("sha3"), HashSHA3)
}
