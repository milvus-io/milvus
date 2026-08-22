package recovery

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// pchannelSummaryStoreMeta is the decoded etcd meta: which term last owned the
// store, and nothing else.
//
// There is deliberately no checkpoint here. A chunk is written synchronously
// before the WAL checkpoint covering it is persisted, so the WAL checkpoint IS
// the boundary between "already in a chunk" and "still in the WAL". A second
// recorded position could only ever disagree with it.
type pchannelSummaryStoreMeta struct {
	PChannel string
	Term     int64
}

func pchannelSummaryStoreMetaFromCatalog(meta *streamingpb.PChannelSummaryMeta) *pchannelSummaryStoreMeta {
	if meta == nil {
		return nil
	}
	return &pchannelSummaryStoreMeta{
		PChannel: meta.GetPchannel(),
		Term:     meta.GetTerm(),
	}
}

func (meta *pchannelSummaryStoreMeta) intoCatalogMeta() *streamingpb.PChannelSummaryMeta {
	return &streamingpb.PChannelSummaryMeta{
		Pchannel: meta.PChannel,
		Term:     meta.Term,
	}
}

type pchannelSummaryMetaCASCatalog interface {
	CompareAndSwapPChannelSummaryMeta(ctx context.Context, pchannelName string, expected *streamingpb.PChannelSummaryMeta, target *streamingpb.PChannelSummaryMeta) (bool, error)
}

func compareAndSwapPChannelSummaryMeta(
	ctx context.Context,
	logger *mlog.Logger,
	pchannel string,
	expected *streamingpb.PChannelSummaryMeta,
	target *streamingpb.PChannelSummaryMeta,
) (bool, error) {
	catalog := resource.Resource().StreamingNodeCatalog()
	if casCatalog, ok := catalog.(pchannelSummaryMetaCASCatalog); ok {
		return casCatalog.CompareAndSwapPChannelSummaryMeta(ctx, pchannel, expected, target)
	}
	if logger != nil {
		logger.Warn(ctx, "pchannel summary meta CAS is unavailable; falling back to plain save for test catalog",
			mlog.String("pchannel", pchannel))
	}
	if err := catalog.SavePChannelSummaryMeta(ctx, pchannel, target); err != nil {
		return false, err
	}
	return true, nil
}

func updatePChannelSummaryMetaWithCAS(
	ctx context.Context,
	logger *mlog.Logger,
	pchannel string,
	update func(currentPB *streamingpb.PChannelSummaryMeta, current *pchannelSummaryStoreMeta) (*streamingpb.PChannelSummaryMeta, error),
) error {
	return retryOperationWithBackoff(ctx, logger, func(ctx context.Context) error {
		currentPB, err := resource.Resource().StreamingNodeCatalog().GetPChannelSummaryMeta(ctx, pchannel)
		if err != nil {
			return err
		}
		current := pchannelSummaryStoreMetaFromCatalog(currentPB)
		targetPB, err := update(currentPB, current)
		if err != nil || targetPB == nil {
			return err
		}
		swapped, err := compareAndSwapPChannelSummaryMeta(ctx, logger, pchannel, currentPB, targetPB)
		if err != nil {
			return err
		}
		if !swapped {
			return merr.WrapErrServiceUnavailable("pchannel summary meta CAS conflict")
		}
		return nil
	})
}
