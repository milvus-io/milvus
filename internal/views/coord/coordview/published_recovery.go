package coordview

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// PublishedRecovery captures the exact old serving obligation. Its membership,
// manifests and load snapshot must not be replaced with the target schema's base.
type PublishedRecovery struct {
	View      *viewpb.QueryViewOfShard
	Preparing *viewpb.QueryViewOfShard
	Sizes     map[int64]int64
}

func (m *ShardViewManager) PublishedRecovery(targetSchema int32) *PublishedRecovery {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.releaseRequested || m.closed || m.publishedSchema == nil || *m.publishedSchema >= targetSchema {
		return nil
	}
	var source *CoordQueryViewStateMachine
	for _, sm := range m.views {
		if sm.View().GetMeta().GetSchemaVersion() != *m.publishedSchema ||
			(sm.State() != qviews.QueryViewStateUp && sm.State() != qviews.QueryViewStateUnrecoverable) {
			continue
		}
		if source == nil || sm.Version().GT(source.Version()) {
			source = sm
		}
	}
	if source == nil {
		return nil
	}
	snapshot := &PublishedRecovery{View: source.viewWithState(source.State()), Sizes: make(map[int64]int64)}
	if preparing := m.preparingAtSchemaLocked(*m.publishedSchema); preparing != nil {
		snapshot.Preparing = preparing.viewWithState(preparing.State())
	}
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

func (m *ShardViewManager) preparingAtSchemaLocked(schema int32) *CoordQueryViewStateMachine {
	var result *CoordQueryViewStateMachine
	for _, sm := range m.views {
		if sm.View().GetMeta().GetSchemaVersion() == schema &&
			(sm.State() == qviews.QueryViewStatePreparing || sm.State() == qviews.QueryViewStateReady) &&
			(result == nil || sm.Version().GT(result.Version())) {
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

// A target placement can later have to recover the published schema. Subscribe
// from its retained floor on first acquisition so that repair never rewinds a
// live node's shared TransformLogBuffer or starts a second replay.
func (m *ShardViewManager) publishedRetentionStartLocked(meta *viewpb.QueryViewMeta) uint64 {
	start := meta.GetTransformStartAfterTimetick()
	if m.publishedSchema == nil || meta.GetSchemaVersion() <= *m.publishedSchema {
		return start
	}
	for _, sm := range m.views {
		if sm.View().GetMeta().GetSchemaVersion() != *m.publishedSchema {
			continue
		}
		switch sm.State() {
		case qviews.QueryViewStateUp, qviews.QueryViewStatePreparing, qviews.QueryViewStateReady, qviews.QueryViewStateUnrecoverable:
			start = min(start, sm.View().GetMeta().GetTransformStartAfterTimetick())
		}
	}
	return start
}

func (m *ShardViewManager) keepPublishedRecoverySourceLocked(source *CoordQueryViewStateMachine) bool {
	if m.releaseRequested || m.publishedSchema == nil || source.View().GetMeta().GetSchemaVersion() != *m.publishedSchema {
		return false
	}
	targetExists := false
	for _, sm := range m.views {
		schema := sm.View().GetMeta().GetSchemaVersion()
		if schema == *m.publishedSchema {
			if sm.State() == qviews.QueryViewStateUp ||
				(sm.State() == qviews.QueryViewStateUnrecoverable && sm.Version().GT(source.Version())) {
				return false
			}
		}
		if schema > *m.publishedSchema {
			targetExists = true
		}
	}
	return targetExists
}

func (m *ShardViewManager) validatePreparationLocked(view *viewpb.QueryViewOfShard, source *qviews.QueryViewVersion) error {
	meta := view.GetMeta()
	version := qviews.FromProtoDataVersion(meta.GetVersion().GetDataVersion())
	if source == nil {
		return m.validatePreparingLocked(version)
	}
	retained := m.views[*source]
	if m.releaseRequested || m.closed || retained == nil || m.publishedSchema == nil ||
		meta.GetSchemaVersion() != *m.publishedSchema || retained.View().GetMeta().GetSchemaVersion() != *m.publishedSchema ||
		!version.EQ(retained.Version().DataVersion) || meta.GetLoadInfoVersion() != retained.View().GetMeta().GetLoadInfoVersion() {
		return merr.WrapErrServiceUnavailableMsg("published schema recovery source for shard %s changed; replan", m.shardID.String())
	}
	return nil
}
