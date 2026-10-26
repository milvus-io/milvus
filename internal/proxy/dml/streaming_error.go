package dml

import (
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// unwrapStreamingMutationError projects missing transaction history to a public
// error. An unknown outcome on any shard must take precedence over a rejection
// on another shard, especially a CAS rejection that could trigger replay.
func unwrapStreamingMutationError(resp streaming.AppendResponses) error {
	var firstErr, casErr error
	for _, response := range resp.Responses {
		if response.Error == nil {
			continue
		}
		se := status.AsStreamingError(response.Error)
		if se.IsTxnNotFound() {
			return merr.ErrServiceWriteOutcomeUnknown
		}
		if se.IsPartialUpdateRetryableCAS() {
			if casErr == nil {
				casErr = response.Error
			}
		} else if firstErr == nil {
			firstErr = response.Error
		}
	}
	if firstErr != nil {
		return firstErr
	}
	return casErr
}
