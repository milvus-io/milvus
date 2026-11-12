package resource

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// PublishIndexes is part of the durable index DDL callback, after index metadata
// persistence. It publishes requirements, not a claim that index files are ready.
func PublishIndexes(ctx context.Context, collectionID int64) error {
	r := Resource()
	if r == nil {
		return merr.WrapErrServiceNotReadyMsg("QueryView load manager is not initialized")
	}
	return r.LoadManager.RefreshIndexes(ctx, collectionID)
}
