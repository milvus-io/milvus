package message

import "github.com/milvus-io/milvus/pkg/v3/util/merr"

// CheckTxnBodyCount validates logical bodies after physical WAL deduplication.
// Zero is the legacy unchecked format, including empty transactions.
func CheckTxnBodyCount(expected uint64, actual int) error {
	if expected != 0 && expected != uint64(actual) {
		return merr.WrapErrDataIntegrityMsg("transaction body count mismatch: expected %d, received %d", expected, actual)
	}
	return nil
}
