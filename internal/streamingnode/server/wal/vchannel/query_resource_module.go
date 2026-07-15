package vchannel

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func (m *VChannelRecoveryModule) AcquireQueryResource(req snview.AcquireResource) {
	if req.Meta == nil || req.Meta.GetVchannel() != m.vchannel {
		panic("query view vchannel does not match recovery module")
	}
	m.mu.Lock()
	epoch := m.queryResources.AcquireLocked(req, m.queryWALViewLocked)
	m.mu.Unlock()
	go m.queryResources.WaitReady(req.Key, epoch, req.OnReady)
}

func (m *VChannelRecoveryModule) ReleaseQueryResource(req snview.ReleaseResource) {
	if m == nil || m.queryResources == nil {
		return
	}
	m.queryResources.Release(req)
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
	deleteReplay := newDeleteReplayScanner(
		context.Background(),
		m.queryTransformLogStream,
		m.pchannel,
		m.vchannel,
		max(deleteReplayStartAfter(segmentSnapshot), meta.GetTransformStartAfterTimetick()),
		baseTransformTimeTick,
	)
	settings := cloneQueryViewSettings(meta.GetSettings())
	return walview.VChannelWALView{
		PChannel:              m.pchannel,
		VChannel:              m.vchannel,
		CollectionID:          vchannelSnapshot.CollectionID,
		BaseGrowingTimeTick:   baseGrowingTimeTick,
		BaseTransformTimeTick: baseTransformTimeTick,
		LoadConfig:            queryViewLoadConfig(meta, settings),
		Settings:              settings,
		Schema:                vchannelSnapshot.Schema,
		SegmentSnapshot:       segmentSnapshot,
		DeleteReplay:          deleteReplay,
	}, true
}

func cloneQueryViewSettings(settings *viewpb.QueryViewSettings) *viewpb.QueryViewSettings {
	if settings == nil {
		return &viewpb.QueryViewSettings{}
	}
	return proto.Clone(settings).(*viewpb.QueryViewSettings)
}

func queryViewLoadConfig(meta *viewpb.QueryViewMeta, settings *viewpb.QueryViewSettings) *streamingpb.VChannelLoadConfig {
	loadFields := make([]*messagespb.LoadFieldConfig, 0, len(settings.GetRequiredFields()))
	for _, fieldID := range settings.GetRequiredFields() {
		loadFields = append(loadFields, &messagespb.LoadFieldConfig{FieldId: fieldID})
	}
	return &streamingpb.VChannelLoadConfig{
		Header: &messagespb.AlterLoadConfigMessageHeader{
			CollectionId: meta.GetCollectionId(),
			PartitionIds: append([]int64(nil), settings.GetRequiredPartitions()...),
			LoadFields:   loadFields,
		},
	}
}
