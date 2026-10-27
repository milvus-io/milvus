package dataview

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// AdvanceSchema publishes the DDL's supported read schema exactly once. A nil
// projector is a direct logical transition. Otherwise, the complete current
// projection must have finished schema-bump before it can be published. Recompute
// wakes this waiter even for schema-only SegmentMeta changes. No locks are held
// while waiting; a replacement transfers the obligation to its live successor.
func (m *dataViewManager) AdvanceSchema(ctx context.Context, collectionID int64, target int32, project Projector) (*viewpb.DataVersion, error) {
	for {
		state, unlock := m.lockStateForMutation(collectionID)
		if state == nil {
			return nil, merr.WrapErrCollectionNotFound(collectionID)
		}
		version, changed, err := m.advanceSchemaLocked(ctx, state, target, project)
		unlock()
		if err != nil || changed == nil {
			return version, err
		}
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-changed:
		}
	}
}

func (m *dataViewManager) advanceSchemaLocked(ctx context.Context, state *collectionState, target int32, project Projector) (*viewpb.DataVersion, <-chan struct{}, error) {
	base := latestView(state)
	if base == nil {
		return nil, nil, merr.WrapErrServiceNotReadyMsg("collection %d DataView is not initialized", state.id)
	}
	if base.GetSchemaVersion() >= target {
		return dataVersionFromView(base), nil, nil
	}
	var segments []LoadableSegment
	if project != nil {
		changed := state.changed.Listen(syncutil.VersionedListenAtLatest)
		var err error
		segments, err = project(ctx, state.id)
		if err != nil {
			return nil, nil, err
		}
		for _, segment := range segments {
			if segment.SchemaVersion < target {
				return nil, changed.WaitChan(), nil
			}
		}
	}
	next := canonicalDataViewClone(base)
	var stats map[int64]SegmentStats
	if project != nil {
		if err := rebuildSegments(next, segments); err != nil {
			return nil, nil, err
		}
		stats = buildSegmentStats(segments)
	} else {
		// Cost normalization mutates its input, never share an old entry's map.
		stats = make(map[int64]SegmentStats, len(state.latest.stats))
		for id, value := range state.latest.stats {
			stats[id] = value
		}
	}
	next.SchemaVersion = target
	next.DataVersion = nextDataVersion(base, dataViewAdvanceCompact)
	if err := m.persistLockedWithStats(ctx, state, next, stats); err != nil {
		return nil, nil, err
	}
	return dataVersionFromView(next), nil, nil
}
