package message

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/hook"
)

// cipher is a global variable that is used to encrypt and decrypt messages.
// It should be initialized at initialization stage.
var (
	cipher hook.Cipher
)

// RegisterCipher registers a cipher to be used for encrypting and decrypting messages.
// It should be called only once when the program starts and initialization stage.
func RegisterCipher(c hook.Cipher) {
	if cipher != nil {
		panic("cipher already registered")
	}
	cipher = c
}

// mustGetCipher returns the registered cipher.
func mustGetCipher() hook.Cipher {
	if cipher == nil {
		panic("cipher not registered")
	}
	return cipher
}

// CipherConfig is the configuration for cipher that is used to encrypt and decrypt messages.
type CipherConfig struct {
	// EzID is the encryption zone ID.
	EzID int64

	// Collection ID
	CollectionID int64
}
