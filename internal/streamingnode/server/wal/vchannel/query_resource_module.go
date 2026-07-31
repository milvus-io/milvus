package vchannel

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func (m *VChannelRecoveryModule) AcquireQueryResource(req snview.AcquireResource) {
	if req.Meta == nil || req.Meta.GetVchannel() != m.vchannel {
		panic("query view vchannel does not match recovery module")
	}
	m.mu.Lock()
	m.queryResources.AcquireLocked(req, m.queryWALViewLocked)
	m.mu.Unlock()
}

func (m *VChannelRecoveryModule) ReleaseQueryResource(req snview.ReleaseResource) {
	if m == nil || m.queryResources == nil {
		return
	}
	m.queryResources.Release(req)
	m.mu.Lock()
	changed := m.tryFinalizeSegmentsLocked()
	m.mu.Unlock()
	if changed && m.runtime.Notifier != nil {
		m.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameSegment)
		m.runtime.Notifier.NotifyBarrierUpdated()
	}
}

func (m *VChannelRecoveryModule) QueryRuntime(key qviews.QueryViewKey) (*queryresource.QueryRuntime, bool) {
	if m == nil || m.queryResources == nil {
		return nil, false
	}
	return m.queryResources.QueryRuntime(key)
}

func (m *VChannelRecoveryModule) CloseQueryResources() {
	if m == nil || m.queryResources == nil {
		return
	}
	m.queryResources.Close()
}

func (m *VChannelRecoveryModule) observeQueryResourceEvent(ctx context.Context, event walview.VChannelResourceEvent) {
	if m == nil || m.queryResources == nil {
		return
	}
	m.queryResources.ObserveEvent(ctx, event)
}

func (m *VChannelRecoveryModule) queryWALViewLocked(meta *viewpb.QueryViewMeta) (walview.VChannelWALView, bool) {
	if m == nil || m.vchannelView == nil || m.transformLog == nil || m.queryTransformLogStream == nil {
		return walview.VChannelWALView{}, false
	}
	vchannelSnapshot, ok := m.vchannelView.WALViewSnapshot()
	if !ok {
		return walview.VChannelWALView{}, false
	}
	baseTransformTimeTick := m.transformLog.LatestTimeTick()
	baseGrowingTimeTick := max(m.latestInsertTimeTick, baseTransformTimeTick)
	dataVersion := qviews.FromProtoDataVersion(meta.GetVersion().GetDataVersion())
	segmentSnapshot := m.visibleSnapshot(baseGrowingTimeTick, dataVersion)
	return walview.VChannelWALView{
		PChannel:                       m.pchannel,
		VChannel:                       m.vchannel,
		CollectionID:                   vchannelSnapshot.CollectionID,
		BaseGrowingTimeTick:            baseGrowingTimeTick,
		BaseTransformTimeTick:          baseTransformTimeTick,
		LoadInfoVersion:                meta.GetLoadInfoVersion(),
		Schema:                         vchannelSnapshot.Schema,
		SegmentSnapshot:                segmentSnapshot,
		TransformLogStream:             m.queryTransformLogStream,
		DeleteReplayStartAfterTimeTick: max(deleteReplayStartAfter(segmentSnapshot), meta.GetTransformStartAfterTimetick()),
	}, true
}
