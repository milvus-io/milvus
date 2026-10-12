package dql

import (
	"context"
	"slices"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// A missing view can mean release or temporary handoff/recovery. Consult the
// current configuration only on failure; never turn a loaded collection's
// transient view error into a terminal not-loaded error.
func queryViewLoadError(ctx context.Context, node taskmodel.TaskNode, collectionID int64, partitions []int64, queryErr error) error {
	var viewErr *viewerror.ViewError
	var clientStatus *viewerror.ViewClientStatus
	if errors.IsAny(queryErr, context.Canceled, context.DeadlineExceeded) ||
		(!errors.As(queryErr, &viewErr) && !errors.As(queryErr, &clientStatus)) {
		return queryErr
	}
	info, err := node.MixCoord().GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{CollectionID: collectionID})
	if err := merr.CheckRPCCall(info, err); err != nil {
		if errors.IsAny(err, merr.ErrCollectionNotLoaded, merr.ErrCollectionNotFound) {
			return err
		}
		return queryErr
	}
	for _, partitionID := range partitions {
		if !slices.Contains(info.GetPartitionIDs(), partitionID) {
			return merr.WrapErrPartitionNotLoaded(partitionID)
		}
	}
	return queryErr
}
