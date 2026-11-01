package coordview

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// PublishedRecovery captures the latest view while data preparation for the
// target schema is pending. Recovery preserves its exact base;
// once a newer view exists, only that view may be replaced.
type PublishedRecovery struct {
	View  *viewpb.QueryViewOfShard
	Sizes map[int64]int64
}

func (m *ShardViewManager) PublishedRecovery(targetSchema int32) *PublishedRecovery {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.releaseRequested || m.closed || m.publishedSchema == nil || *m.publishedSchema >= targetSchema {
		return nil
	}
	source := m.latestViewLocked()
	if source == nil || source.View().GetMeta().GetSchemaVersion() != *m.publishedSchema || !recoverablePlacementState(source.State()) {
		return nil
	}
	snapshot := &PublishedRecovery{View: source.viewWithState(source.State()), Sizes: make(map[int64]int64)}
	if ref := source.Ref(); ref != nil {
		for _, node := range source.View().GetQueryNode() {
			for _, partition := range node.GetPartitions() {
				for _, id := range partition.GetSegmentIds() {
					if stats, ok := ref.Stats(id); ok {
						snapshot.Sizes[id] = stats.LogicalSize
					}
				}
			}
		}
	}
	return snapshot
}

func recoverablePlacementState(state qviews.QueryViewState) bool {
	switch state {
	case qviews.QueryViewStatePreparing, qviews.QueryViewStateReady, qviews.QueryViewStateUp, qviews.QueryViewStateUnrecoverable:
		return true
	default:
		return false
	}
}

func (m *ShardViewManager) latestViewLocked() *CoordQueryViewStateMachine {
	var result *CoordQueryViewStateMachine
	for _, sm := range m.views {
		if result == nil || sm.Version().GT(result.Version()) {
			result = sm
		}
	}
	return result
}

func (m *ShardViewManager) refreshNewestPreparingLocked() {
	m.preparingView = nil
	for _, sm := range m.views {
		if (sm.State() == qviews.QueryViewStatePreparing || sm.State() == qviews.QueryViewStateReady) &&
			(m.preparingView == nil || sm.Version().GT(m.preparingView.Version())) {
			m.preparingView = sm
		}
	}
}

func (m *ShardViewManager) validatePreparationLocked(view *viewpb.QueryViewOfShard, source *qviews.QueryViewVersion) error {
	meta := view.GetMeta()
	version := qviews.FromProtoDataVersion(meta.GetVersion().GetDataVersion())
	if err := m.validatePreparingLocked(version); err != nil {
		return err
	}
	if source == nil {
		return nil
	}
	retained := m.views[*source]
	if retained == nil || retained != m.latestViewLocked() || m.publishedSchema == nil ||
		!recoverablePlacementState(retained.State()) ||
		meta.GetSchemaVersion() != *m.publishedSchema || retained.View().GetMeta().GetSchemaVersion() != *m.publishedSchema ||
		!version.EQ(retained.Version().DataVersion) || meta.GetLoadInfoVersion() != retained.View().GetMeta().GetLoadInfoVersion() {
		return merr.WrapErrServiceUnavailableMsg("published schema recovery source for shard %s changed; replan", m.shardID.String())
	}
	return nil
}
