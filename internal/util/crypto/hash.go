// Package crypto provides configurable cryptographic hashing utilities.
//
// It centralizes hash algorithm selection so that Milvus components use
// a single, consistent implementation. The algorithm is controlled at
// runtime via the common.security.hashAlgorithm configuration parameter,
// supporting SHA-256 (default) and SHA-3 (SHA3-256) per NIST recommendations.
//
// See: https://github.com/milvus-io/milvus/issues/25970
package crypto

import (
	"crypto/sha256"
	"encoding/hex"

	"golang.org/x/crypto/sha3"
)

// HashType defines the supported hash algorithm identifiers.
type HashType string

const (
	HashSHA256 HashType = "sha256"
	HashSHA3   HashType = "sha3"
)

// ComputeHash returns the hex-encoded digest of data using the specified algorithm.
// Unrecognized hashType values fall back to SHA-256.
func ComputeHash(data []byte, hashType HashType) string {
	switch hashType {
	case HashSHA3:
		h := sha3.New256()
		h.Write(data)
		return hex.EncodeToString(h.Sum(nil))

	case HashSHA256:
		fallthrough
	default:
		h := sha256.Sum256(data)
		return hex.EncodeToString(h[:])
	}
}
