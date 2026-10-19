package vchannel

import (
	"context"
	"math"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func (m *VChannelRecoveryModule) AcquireQueryResource(req snview.AcquireResource) {
	if req.Meta == nil || req.Meta.GetVchannel() != m.vchannel {
		panic("query view vchannel does not match recovery module")
	}
	m.mu.Lock()
	requested := qviews.FromProtoDataVersion(req.Meta.GetVersion().GetDataVersion())
	if m.removed || m.vchannelView == nil || !m.vchannelView.IsActive() || !requested.GTE(m.vchannelView.SegmentDataVersionSummary()) {
		m.queryResources.Reject(req)
		m.mu.Unlock()
		return
	}
	m.queryResources.AcquireLocked(req, m.queryWALViewLocked)
	m.mu.Unlock()
}

func (m *VChannelRecoveryModule) ReleaseQueryResource(req snview.ReleaseResource) {
	if m == nil || m.queryResources == nil {
		return
	}
	m.queryResources.Release(req)
	if m.runtime.Notifier != nil {
		m.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameSegment)
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
	if m.vchannelView == nil || m.queryTransformLogStream == nil {
		return walview.VChannelWALView{}, false
	}
	state, ok := m.vchannelView.WritePathRecoveryState()
	if !ok {
		return walview.VChannelWALView{}, false
	}
	if !m.ensureFinalCommitsLocked() {
		return walview.VChannelWALView{}, false
	}
	version := qviews.FromProtoDataVersion(meta.GetVersion().GetDataVersion())
	snapshot := m.visibleSnapshot(m.queryObservedTimeTick, version)
	start := max(deleteReplayStartAfter(snapshot), meta.GetTransformStartAfterTimetick())
	if len(snapshot.Segments) == 0 {
		start = m.queryObservedTimeTick
	}
	return walview.VChannelWALView{
		PChannel: m.pchannel, VChannel: m.vchannel, CollectionID: state.CollectionID,
		WithResourceEventLock: func(fn func()) { m.mu.Lock(); defer m.mu.Unlock(); fn() },
		PrepareQueryView:      m.prepareQueryViewLocked,
		BaseGrowingTimeTick:   m.queryObservedTimeTick, BaseTransformTimeTick: m.queryObservedTimeTick,
		LoadInfoVersion: meta.GetLoadInfoVersion(), Schema: state.Schema,
		SegmentSnapshot: snapshot, TransformLogStream: m.queryTransformLogStream,
		DeleteReplayStartAfterTimeTick: start,
	}, true
}

// Open growing segments need no commit. Closing segments must complete their
// final commit before either capturing a snapshot or serving a new query view.
func (m *VChannelRecoveryModule) ensureFinalCommitsLocked() bool {
	ready := true
	for _, view := range m.segments {
		if !view.EnsureFinalCommit() {
			ready = false
		}
	}
	return ready
}

func (m *VChannelRecoveryModule) prepareQueryViewLocked() bool {
	if !m.ensureFinalCommitsLocked() {
		return false
	}
	// The commit installs its version before calling NotifyDataUpdated. Publish
	// here too so the ready barrier cannot overtake that owner notification.
	for id := range m.segments {
		m.publishSegmentSealedLocked(id)
	}
	return true
}

func (m *VChannelRecoveryModule) visibleSnapshot(baseGrowingTimeTick uint64, dataVersion qviews.DataVersion) walview.VisibleSegmentSnapshot {
	snapshot := walview.VisibleSegmentSnapshot{
		VChannel:            m.vchannel,
		DataVersion:         dataVersion,
		BaseGrowingTimeTick: baseGrowingTimeTick,
	}
	for _, view := range m.segments {
		visible, ok := view.VisibleSnapshot(m.vchannel, dataVersion)
		if ok {
			if snapshot.CollectionID == 0 {
				snapshot.CollectionID = visible.Assignment.GetCollectionId()
			}
			snapshot.Segments = append(snapshot.Segments, visible)
			continue
		}
		flushed, ok := view.FlushedSegmentSnapshot(m.vchannel, dataVersion)
		if ok {
			snapshot.FlushedSegments = append(snapshot.FlushedSegments, flushed)
		}
	}
	return snapshot
}

func deleteReplayStartAfter(snapshot walview.VisibleSegmentSnapshot) uint64 {
	if len(snapshot.Segments) == 0 {
		return 0
	}
	minCreateTimeTick := uint64(0)
	for _, segment := range snapshot.Segments {
		createTimeTick := segment.Assignment.GetStat().GetCreateSegmentTimeTick()
		if createTimeTick == 0 {
			// Legacy metadata may not carry a creation timestamp. No later
			// segment can establish a safe lower bound for its delete replay.
			return 0
		}
		if minCreateTimeTick == 0 || createTimeTick < minCreateTimeTick {
			minCreateTimeTick = createTimeTick
		}
	}
	if minCreateTimeTick == 0 {
		return 0
	}
	return minCreateTimeTick - 1
}

// refreshQueryRetentionLocked pins deletes needed to rebuild every retained
// segment, including old-view segments already committed to DataCoord.
func (m *VChannelRecoveryModule) refreshQueryRetentionLocked() {
	if m.summaryManager == nil {
		return
	}
	floor := uint64(math.MaxUint64)
	for _, segment := range m.segments {
		created := segment.CreateTimeTick()
		if created == 0 {
			floor = 0
			break
		}
		floor = min(floor, created-1)
	}
	m.summaryManager.SetQueryRetention(m.vchannel, floor)
}

func (m *VChannelRecoveryModule) publishSegmentSealedLocked(id int64) {
	view := m.segments[id]
	if view == nil {
		return
	}
	meta := view.AssignmentMeta()
	if meta.GetState() != streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED || meta.GetSealedAtDataVersion() == nil {
		return
	}
	if m.querySealed[id] {
		return
	}
	m.querySealed[id] = true
	m.observeQueryResourceEvent(context.TODO(), walview.VChannelResourceEvent{SegmentSealed: &walview.SegmentSealedEvent{SegmentID: id, VChannel: m.vchannel, SealedAtDataVersion: qviews.FromProtoDataVersion(meta.GetSealedAtDataVersion())}})
}
