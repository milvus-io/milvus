package recovery

import "github.com/cockroachdb/errors"

var (
	ErrPChannelSummaryStoreCorrupted = errors.New("pchannel summary store corrupted")
	// ErrPChannelSummaryStoreFenced marks a write refused because the durable
	// store already carries a newer WAL assignment term: this owner is stale
	// (split-brain) and must stop persisting rather than overwrite the current
	// owner's summary state. Terminal — never retried.
	ErrPChannelSummaryStoreFenced = errors.New("pchannel summary store fenced by a newer term")
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

func markPChannelSummaryStoreCorrupted(err error) error {
	if err == nil {
		return nil
	}
	return &markedRecoveryError{
		err:    err,
		target: ErrPChannelSummaryStoreCorrupted,
	}
}

func pchannelSummaryStoreCorruptedf(format string, args ...any) error {
	return markPChannelSummaryStoreCorrupted(errors.Errorf(format, args...))
}

func pchannelSummaryStoreFencedf(format string, args ...any) error {
	return &markedRecoveryError{
		err:    errors.Errorf(format, args...),
		target: ErrPChannelSummaryStoreFenced,
	}
}
