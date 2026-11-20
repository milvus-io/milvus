package packed

import "github.com/cockroachdb/errors"

// ErrLoonTransient marks any failure surfaced by the loon FFI layer. Some
// milvus-storage paths can still lose their structured error detail and fall
// back to a generic error code, so callers cannot reliably distinguish a
// transient failure from a permanent one. Treat all loon failures as retryable
// for now and rely on a bounded retry budget plus outer error handling to keep
// the worst case finite.
//
// TODO(storage v3): once every milvus-storage FFI path preserves explicit error
// codes end-to-end, narrow this sentinel to the retryable cases and let other
// errors propagate immediately as retry.Unrecoverable.
var ErrLoonTransient = errors.New("loon FFI transient error")

// ErrLoonPermanent marks a loon FFI failure whose err_code the producer
// itself reports as non-retryable (loon_ffi_is_retryable_errcode == 0):
// access denied, malformed input, corrupt data. Retrying cannot succeed;
// callers' retry guards must terminate on it.
var ErrLoonPermanent = errors.New("loon FFI permanent error")

// sourceFFIError keeps a single cause chain for cockroachdb/errors v1.9.1,
// which cannot traverse errors.Join. The retry marker is also visible through
// Is to both the standard library and cockroachdb/errors.
type sourceFFIError struct {
	cause  error
	marker error
}

func (e *sourceFFIError) Error() string        { return e.cause.Error() }
func (e *sourceFFIError) Unwrap() error        { return e.cause }
func (e *sourceFFIError) Is(target error) bool { return target == e.marker }
