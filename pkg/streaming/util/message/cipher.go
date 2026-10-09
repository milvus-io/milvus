package message

import (
	"context"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// decryptPayloadWithRetry retries only recoverable cipher failures, before any
// consumer applies the message. Reacquire the decryptor after each failure so a
// refreshed key can replace the previous one. There is no per-message goroutine.
func decryptPayloadWithRetry(ctx context.Context, ezID, collectionID int64, safeKey, payload []byte) ([]byte, error) {
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

	backoff := 100 * time.Millisecond
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		decryptor, err := cipher.GetDecryptor(ezID, collectionID, safeKey)
		if err == nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			var decoded []byte
			decoded, err = decryptor.Decrypt(payload)
			if err == nil {
				return decoded, nil
			}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !isRetryableCipherError(err) {
			return nil, err
		}
		mlog.Warn(ctx, "message decryption failed, will retry",
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
		backoff = min(backoff*2, 3*time.Second)
	}
}

// Unknown plugin errors stay terminal: retrying corrupt ciphertext forever
// would hide the failure. Preserve the existing KMS key restoration contract.
func isRetryableCipherError(err error) bool {
	if err == nil {
		return false
	}
	if isKmsKeyInvalidError(err) {
		return true
	}
	if merr.IsMilvusError(err) {
		return merr.IsRetryableErr(err)
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.IsAny(err, context.DeadlineExceeded, syscall.ECONNRESET, syscall.ECONNREFUSED, syscall.EPIPE) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

// CipherConfig is the configuration for cipher that is used to encrypt and decrypt messages.
type CipherConfig struct {
	// EzID is the encryption zone ID.
	EzID int64

	// Collection ID
	CollectionID int64
}
