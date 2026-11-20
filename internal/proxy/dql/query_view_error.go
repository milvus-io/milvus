package dql

import (
	"context"
	"slices"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// EnsureQueryViewReady waits for the bound collection and its expected shards.
func EnsureQueryViewReady(ctx context.Context, node taskmodel.TaskNode, collectionID int64, vchannels []string) error {
	status, err := node.MixCoord().EnsureCollectionReady(ctx, &querypb.EnsureCollectionReadyRequest{
		CollectionID: collectionID, ExpectedVchannels: vchannels,
	})
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return merr.CheckRPCCall(status, err)
}

// QueryViewLoadError preserves the public load-state errors for all query consumers.
// A missing view can mean release or temporary handoff/recovery. Consult the
// current configuration only on failure; never turn a loaded collection's
// transient view error into a terminal not-loaded error.
func QueryViewLoadError(ctx context.Context, node taskmodel.TaskNode, collectionID int64, partitions []int64, queryErr error) error {
	// Recv errors bypass the client's return-time conversion. Retry decisions must
	// use the original error; only this terminal boundary maps it to public codes.
	queryErr = queryclient.ToMilvusError(queryErr)
	if !errors.IsAny(queryErr, merr.ErrQueryViewNotReady, merr.ErrServiceUnavailable) {
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
