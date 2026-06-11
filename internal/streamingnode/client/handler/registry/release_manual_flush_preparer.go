package registry

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/log"
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
	PrepareReleaseManualFlush(ctx context.Context, pchannel types.PChannelInfo, collectionID int64, vchannel string, releaseSegmentIDs []int64) (prepared bool, err error)
}

// RegisterLocalReleaseManualFlushPreparer registers the process-local release handoff preparer.
func RegisterLocalReleaseManualFlushPreparer(preparer ReleaseManualFlushPreparer) {
	if !paramtable.IsLocalComponentEnabled(typeutil.StreamingNodeRole) {
		panic("unreachable: streaming node is not enabled but release manual flush preparer setup")
	}
	releaseManualFlushPreparerRegistry.Set(preparer)
	log.Ctx(context.Background()).Info("register local release manual flush preparer done")
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
