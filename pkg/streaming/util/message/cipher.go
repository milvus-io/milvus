package message

import (
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/hook"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

// cipher is a global variable that is used to encrypt and decrypt messages.
// It should be initialized at initialization stage.
var (
	cipher   hook.Cipher
	initOnce sync.Once
)

// RegisterCipher registers a cipher to be used for encrypting and decrypting messages.
// It should be called only once when the program starts and initialization stage.
func RegisterCipher(c hook.Cipher) {
	initOnce.Do(func() {
		cipher = c
	})
}

// mustGetCipher returns the registered cipher.
func mustGetCipher() hook.Cipher {
	if cipher == nil {
		panic("cipher not registered")
	}
	return cipher
}

// ErrKmsKeyInvalid is the error returned when a KMS key is invalid or revoked.
// This error is also defined in the milvus-cloud-plugin. It is checked using `errors.Is`
// to allow for proper error wrapping and reliable error handling.
var ErrKmsKeyInvalid = errors.New("kms key invalid")

func isKmsKeyInvalidError(err error) bool {
	if err == nil {
		return false
	}
	// Check both errors.Is for local errors and string matching for errors
	// that cross the plugin boundary (which lose type information)
	return errors.Is(err, ErrKmsKeyInvalid) || strings.Contains(err.Error(), "kms key invalid")
}

// getDecryptorWithRetry wraps cipher.GetDecryptor with retry logic for streaming node consumption.
// It retries with exponential backoff if the error is KmsKeyInvalid (retriable).
// For other errors, it returns immediately without retry.
func getDecryptorWithRetry(ezID, collectionID int64, safeKey []byte) (hook.Decryptor, error) {
	cipher := mustGetCipher()

	const (
		initialBackoff = 100 * time.Millisecond
		maxBackoff     = 3 * time.Second
		backoffFactor  = 2.0
	)

	backoff := initialBackoff
	attempt := 0

	for {
		attempt++
		decryptor, err := cipher.GetDecryptor(ezID, collectionID, safeKey)
		if err == nil {
			return decryptor, nil
		}

		// If it's NOT a KMS key invalid error, fail immediately (non-retriable)
		if !isKmsKeyInvalidError(err) {
			log.Error("failed to get decryptor with non-retriable error",
				zap.Int64("ezID", ezID),
				zap.Int64("collectionID", collectionID),
				zap.Int("attempt", attempt),
				zap.Error(err))
			return nil, err
		}

		// KMS key invalid error - log and retry
		log.Warn("KMS key invalid, will retry",
			zap.Int64("ezID", ezID),
			zap.Int64("collectionID", collectionID),
			zap.Int("attempt", attempt),
			zap.Duration("backoff", backoff),
			zap.Error(err))

		time.Sleep(backoff)

		// Exponential backoff with max cap
		backoff = time.Duration(float64(backoff) * backoffFactor)
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// CipherConfig is the configuration for cipher that is used to encrypt and decrypt messages.
type CipherConfig struct {
	// EzID is the encryption zone ID.
	EzID int64

	// Collection ID
	CollectionID int64
}
