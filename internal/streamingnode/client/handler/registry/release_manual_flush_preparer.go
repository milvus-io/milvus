package registry

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var (
	releaseManualFlushPreparerRegistry = syncutil.NewFuture[ReleaseManualFlushPreparer]()
	ErrNoReleaseManualFlushPreparer    = errors.New("no release manual flush preparer")
)

// ReleaseManualFlushPreparer prepares process-local release handoff.
type ReleaseManualFlushPreparer interface {
	PrepareReleaseManualFlush(ctx context.Context, pchannel types.PChannelInfo, collectionID int64, vchannel string, releaseSegmentIDs []int64) error

	// PrepareReleaseSegments reports whether the LOCAL write buffer still owes a
	// growing-source flush for the given segments, and nudges those flushes
	// forward when it does. It never blocks on the drain.
	//
	// Returns true when at least one of the given segments still owes a flush, in
	// which case the caller must NOT drop the segments yet — they hold the only
	// copy of the unflushed rows.
	PrepareReleaseSegments(ctx context.Context, pchannel types.PChannelInfo, collectionID int64, vchannel string, segmentIDs []int64) (bool, error)
}

// RegisterLocalReleaseManualFlushPreparer registers the process-local release handoff preparer.
func RegisterLocalReleaseManualFlushPreparer(preparer ReleaseManualFlushPreparer) {
	if !paramtable.IsLocalComponentEnabled(typeutil.StreamingNodeRole) {
		panic("unreachable: streaming node is not enabled but release manual flush preparer setup")
	}
	releaseManualFlushPreparerRegistry.Set(preparer)
	mlog.Info(context.TODO(), "register local release manual flush preparer done")
}

// GetLocalReleaseManualFlushPreparer returns the process-local release handoff preparer.
func GetLocalReleaseManualFlushPreparer() (ReleaseManualFlushPreparer, error) {
	if !paramtable.IsLocalComponentEnabled(typeutil.StreamingNodeRole) {
		return nil, ErrNoStreamingNodeDeployed
	}
	if !releaseManualFlushPreparerRegistry.Ready() {
		return nil, ErrNoReleaseManualFlushPreparer
	}
	return releaseManualFlushPreparerRegistry.Get(), nil
}
