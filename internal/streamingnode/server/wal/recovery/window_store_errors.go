package recovery

import "github.com/cockroachdb/errors"

var (
	ErrPChannelWindowStoreCorrupted = errors.New("pchannel window store corrupted")
	// ErrPChannelWindowStoreFenced marks a write refused because the durable
	// store already carries a newer WAL assignment term: this owner is stale
	// (split-brain) and must stop persisting rather than overwrite the current
	// owner's window state. Terminal — never retried.
	ErrPChannelWindowStoreFenced = errors.New("pchannel window store fenced by a newer term")
)

type markedRecoveryError struct {
	err    error
	target error
}

func (e *markedRecoveryError) Error() string {
	return e.err.Error()
}

func (e *markedRecoveryError) Unwrap() error {
	return e.err
}

func (e *markedRecoveryError) Is(target error) bool {
	return target == e.target
}

func markPChannelWindowStoreCorrupted(err error) error {
	if err == nil {
		return nil
	}
	return &markedRecoveryError{
		err:    err,
		target: ErrPChannelWindowStoreCorrupted,
	}
}

func pchannelWindowStoreCorruptedf(format string, args ...any) error {
	return markPChannelWindowStoreCorrupted(errors.Errorf(format, args...))
}

func pchannelWindowStoreFencedf(format string, args ...any) error {
	return &markedRecoveryError{
		err:    errors.Errorf(format, args...),
		target: ErrPChannelWindowStoreFenced,
	}
}
