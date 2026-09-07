package message

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

func getCipher() (hook.Cipher, error) {
	if cipher == nil {
		return nil, merr.WrapErrServiceInternalMsg("cipher not registered")
	}
	return cipher, nil
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
	return getDecryptorWithRetryContext(context.Background(), ezID, collectionID, safeKey)
}

func getDecryptorWithRetryContext(
	ctx context.Context,
	ezID, collectionID int64,
	safeKey []byte,
) (hook.Decryptor, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cipher, err := getCipher()
	if err != nil {
		return nil, err
	}

	const (
		initialBackoff = 100 * time.Millisecond
		maxBackoff     = 3 * time.Second
		backoffFactor  = 2.0
	)

	backoff := initialBackoff
	attempt := 0

	for {
		attempt++
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		decryptor, err := cipher.GetDecryptor(ezID, collectionID, safeKey)
		if err == nil {
			return decryptor, nil
		}

		// If it's NOT a KMS key invalid error, fail immediately (non-retriable)
		if !isKmsKeyInvalidError(err) {
			mlog.Error(ctx, "failed to get decryptor with non-retriable error",
				mlog.Int64("ezID", ezID),
				mlog.FieldCollectionID(collectionID),
				mlog.Int("attempt", attempt),
				mlog.Err(err))
			return nil, err
		}

		// KMS key invalid error - log and retry
		mlog.Warn(ctx, "KMS key invalid, will retry",
			mlog.Int64("ezID", ezID),
			mlog.FieldCollectionID(collectionID),
			mlog.Int("attempt", attempt),
			mlog.Duration("backoff", backoff),
			mlog.Err(err))

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}

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
