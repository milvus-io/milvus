package catalog

import "github.com/cockroachdb/errors"

// Sentinel errors define the catalog contract. Callers should use errors.Is to
// detect them. The milvuscompat adapter only emits ErrNotFound and
// ErrUnsupportedImplementation today; native implementations (catalog service,
// TiKV) are expected to emit the remaining sentinels so callers can branch on
// concrete semantics instead of inspecting backend-specific error types.
var (
	ErrNotFound                  = errors.New("catalog: not found")
	ErrAlreadyExists             = errors.New("catalog: already exists")
	ErrConflict                  = errors.New("catalog: conflict")
	ErrStaleEpoch                = errors.New("catalog: stale epoch")
	ErrUnavailable               = errors.New("catalog: unavailable")
	ErrInvalidArgument           = errors.New("catalog: invalid argument")
	ErrUnsupportedImplementation = errors.New("catalog: unsupported implementation")
	ErrNotWired                  = errors.New("catalog: backend not wired")
)
