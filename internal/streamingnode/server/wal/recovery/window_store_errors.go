package recovery

import "github.com/cockroachdb/errors"

var (
	ErrPChannelWindowStoreCorrupted = errors.New("pchannel window store corrupted")
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
