package datacoord

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// V3 worker results persist the merr code alongside the human-readable reason.
// Recovery must branch on this code, never parse reason text. The three import
// families below are terminal by contract; other codes keep their normal
// merr retryability (for example, an object-storage throttle remains retryable).
func isImportTerminalError(err error) bool {
	if err == nil {
		return false
	}
	switch merr.Code(err) {
	case merr.Code(merr.ErrImportFailed),
		merr.Code(merr.ErrDataIntegrity),
		merr.Code(merr.ErrImportSysFailed):
		return true
	default:
		return !merr.IsRetryableErr(err)
	}
}

// isImportOwnershipLost is checked before terminal classification on the
// Query/Drop control path. A missing DataNode session means the current worker
// ownership is gone, not that the immutable import input is invalid; the task
// must be rebound with a larger run ID. Object-storage IoKeyNotFound is not in
// this helper and remains a terminal missing-input failure.
func isImportOwnershipLost(err error) bool {
	return errors.Is(err, merr.ErrNodeNotFound)
}

// importFailureCode is the only code extraction point used when persisting a
// V3 task/job terminal failure. It preserves retryable storage/node codes for
// observability instead of flattening every worker error to ImportSysFailed.
func importFailureCode(err error) int32 {
	if err == nil {
		return 0
	}
	return merr.Code(err)
}
