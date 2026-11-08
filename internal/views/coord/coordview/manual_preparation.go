package coordview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// Preparation observes the first completion of one exact submitted version.
// It owns no View/DataView references and is not persisted.
type Preparation struct {
	Version  qviews.QueryViewVersion
	source   qviews.QueryViewVersion
	lifetime context.Context
	result   *syncutil.Future[PreparationResult]
}

type PreparationResult struct {
	State qviews.QueryViewState
	Err   error
}

func (p *Preparation) Wait(ctx context.Context) PreparationResult {
	// Preserve a completed outcome even if the caller's deadline also expired.
	if p.result.Ready() {
		return p.result.Get()
	}
	select {
	case <-p.result.Done():
		return p.result.Get()
	case <-ctx.Done():
		if p.result.Ready() {
			return p.result.Get()
		}
		return PreparationResult{Err: context.Cause(ctx)}
	case <-p.lifetime.Done():
		if p.result.Ready() {
			return p.result.Get()
		}
		return PreparationResult{Err: merr.WrapErrServiceUnavailableMsg("QueryView coordinator stopped")}
	}
}

func (sm *CoordQueryViewStateMachine) completePreparation(err error) {
	if sm.preparation != nil {
		sm.preparation.result.Set(PreparationResult{State: sm.State(), Err: err})
		sm.preparation = nil
	}
}

func (sm *CoordQueryViewStateMachine) failPreparation(reason string) {
	if sm.preparation != nil {
		sm.completePreparation(merr.WrapErrServiceUnavailableMsg("%s", reason))
	}
}

func (m *ShardViewManager) placementSourceLocked() *CoordQueryViewStateMachine {
	source := m.upView
	if preparing := m.preparingView; preparing != nil && (source == nil || preparing.Version().GT(source.Version())) {
		source = preparing
	}
	return source
}

// PlacementBuilder copies the newest usable placement, never mutating a live
// view. A released/empty manager has no snapshot; the caller decides whether
// an absent shard is relevant to its operation.
func (m *ShardViewManager) PlacementBuilder() *qviews.QueryViewAtCoordBuilder {
	m.mu.Lock()
	defer m.mu.Unlock()
	source := m.placementSourceLocked()
	if m.releaseRequested || m.closed || source == nil {
		return nil
	}
	return qviews.NewQueryViewAtCoordBuilderFromView(source.View())
}

func (m *ShardViewManager) AddManualPreparing(ctx context.Context, builder *qviews.QueryViewAtCoordBuilder, source qviews.QueryViewVersion) (*Preparation, error) {
	p := &Preparation{source: source, lifetime: m.ctx, result: syncutil.NewFuture[PreparationResult]()}
	if err := m.addPreparing(ctx, builder, nil, p); err != nil {
		return nil, err
	}
	return p, nil
}

func (m *ShardViewManager) validateManualPreparationLocked(view *viewpb.QueryViewOfShard, recovery *qviews.QueryViewVersion, p *Preparation) error {
	if err := m.validatePreparationLocked(view, recovery); err != nil {
		return err
	}
	if p != nil {
		source := m.placementSourceLocked()
		if source == nil || source.Version() != p.source {
			return merr.WrapErrServiceUnavailableMsg("manual placement source for shard %s changed; replan", m.shardID.String())
		}
	}
	return nil
}
