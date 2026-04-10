package crypto

import (
	"crypto/sha256"
	"encoding/hex"
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
