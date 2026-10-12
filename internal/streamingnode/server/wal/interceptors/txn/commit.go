package txn

import "github.com/cockroachdb/errors"

var errCommitAdmissionRejected = errors.New("commit admission rejected")

// MarkCommitAdmissionRejected marks an error as a deterministic rejection
// before CommitTxn reaches the WAL.
func MarkCommitAdmissionRejected(err error) error {
	return errors.Mark(err, errCommitAdmissionRejected)
}

// IsCommitAdmissionRejected reports whether CommitTxn was rejected before WAL append.
func IsCommitAdmissionRejected(err error) bool {
	return errors.Is(err, errCommitAdmissionRejected)
}
