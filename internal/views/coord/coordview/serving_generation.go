package coordview

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ResolveServingGeneration publishes the desired shard set only after every
// shard is Up on every requested replica. Prior publication remains readable
// during preparation. A crash after Save but before preemption resumes it here.
func (r *ShardViewRegistry) ResolveServingGeneration(ctx context.Context, desired *viewpb.DataViewOfCollection, replicas []int64) (*viewpb.DataViewOfCollection, error) {
	r.servingMu.Lock()
	defer r.servingMu.Unlock()
	id := desired.GetCollectionId()
	current := r.serving[id]
	if current == nil && desired.GetShardSetEpoch() > 1 {
		previous := proto.Clone(desired).(*viewpb.DataViewOfCollection)
		previous.ShardSetEpoch = 0
		targets := make(map[string]bool)
		for _, shard := range previous.Shards {
			for _, target := range shard.GetLineage().GetVchannels() {
				targets[target] = true
			}
		}
		previous.Shards = nil
		for _, shard := range desired.GetShards() {
			if !targets[shard.GetVchannel()] {
				copy := proto.Clone(shard).(*viewpb.DataViewOfShard)
				copy.Lineage = nil
				copy.Partitions = nil
				previous.Shards = append(previous.Shards, copy)
			}
		}
		if r.generationReady(previous, replicas) && r.servingCatalog != nil {
			if err := r.servingCatalog.SaveServingGeneration(ctx, previous); err != nil {
				return nil, err
			}
			if r.serving == nil {
				r.serving = make(map[int64]*viewpb.DataViewOfCollection)
			}
			r.serving[id], current = previous, previous
		}
	}
	if current != nil && current.GetShardSetEpoch() > desired.GetShardSetEpoch() {
		return proto.Clone(current).(*viewpb.DataViewOfCollection), nil
	}
	ready := r.generationReady(desired, replicas)
	if ready && (current == nil || current.GetShardSetEpoch() < desired.GetShardSetEpoch()) {
		if r.servingCatalog == nil {
			return nil, merr.WrapErrServiceNotReadyMsg("serving generation catalog is not initialized")
		}
		next := proto.Clone(desired).(*viewpb.DataViewOfCollection)
		// Publication stores only membership and lineage, never a segment snapshot.
		for _, shard := range next.Shards {
			shard.Partitions = nil
		}
		if err := r.servingCatalog.SaveServingGeneration(ctx, next); err != nil {
			return nil, err
		}
		if r.serving == nil {
			r.serving = make(map[int64]*viewpb.DataViewOfCollection)
		}
		r.serving[id], current = next, next
	}
	if current == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d has no published serving generation", id)
	}
	// The durable generation controls every resident shard, including S after
	// adoption removes it from the desired set.
	for _, sid := range r.CollectionShards(id) {
		if manager := r.Get(sid); manager != nil {
			manager.PublishServingGeneration(current.GetShardSetEpoch(), current.GetSchemaVersion())
			manager.preemptOlderGenerations(current.GetShardSetEpoch())
		}
	}
	return proto.Clone(current).(*viewpb.DataViewOfCollection), nil
}

func (m *ShardViewManager) servesGeneration(epoch uint64, schema int32, lineage *viewpb.QueryViewLineage) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sm := range m.views {
		meta := sm.View().GetMeta()
		if sm.State() == qviews.QueryViewStateUp && sm.snState == qviews.QueryViewStateUp && meta.GetShardSetEpoch() == epoch && meta.GetSchemaVersion() == schema && proto.Equal(meta.GetLineage(), lineage) {
			return true
		}
	}
	return false
}

func (m *ShardViewManager) preemptOlderGenerations(epoch uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sm := range m.views {
		if sm.View().GetMeta().GetShardSetEpoch() >= epoch {
			continue
		}
		switch sm.State() {
		case qviews.QueryViewStateUp, qviews.QueryViewStateDown:
			sm.EnterDownWithControl(&viewpb.QueryViewControl{PreemptServingLease: true})
			m.processStateMachine(sm)
		}
	}
	m.refreshNewestUpLocked()
	event := m.consumeDirtyEventLocked()
	m.publishStatsLocked()
	m.submitDirtyEvent(event)
}

func (r *ShardViewRegistry) generationReady(desired *viewpb.DataViewOfCollection, replicas []int64) bool {
	if len(replicas) == 0 || len(desired.GetShards()) == 0 {
		return false
	}
	for _, replica := range replicas {
		for _, shard := range desired.GetShards() {
			manager := r.Get(qviews.ShardID{ReplicaID: replica, VChannel: shard.GetVchannel()})
			if manager == nil || !manager.servesGeneration(desired.GetShardSetEpoch(), desired.GetSchemaVersion(), shard.GetLineage()) {
				return false
			}
		}
	}
	return true
}

// ServingTransitionComplete is a consuming-side fence: persisted Down intent
// alone is insufficient, since an SN may not yet have received preemption.
func (r *ShardViewRegistry) ServingTransitionComplete(collectionID int64, epoch uint64, unloaded bool) bool {
	for _, sid := range r.CollectionShards(collectionID) {
		manager := r.Get(sid)
		if manager == nil {
			continue
		}
		manager.mu.Lock()
		complete := true
		for _, sm := range manager.views {
			if !unloaded && sm.View().GetMeta().GetShardSetEpoch() >= epoch {
				continue
			}
			if sm.State() == qviews.QueryViewStateUp || sm.State() == qviews.QueryViewStatePreparing || sm.State() == qviews.QueryViewStateReady ||
				sm.snState == qviews.QueryViewStateUp || sm.snState == qviews.QueryViewStateReady || sm.snState == qviews.QueryViewStateNil {
				complete = false
				break
			}
		}
		manager.mu.Unlock()
		if !complete {
			return false
		}
	}
	return true
}

// RemoveServingGeneration follows logical DataView deletion, not Release: a
// later Load of a live collection must retain its published shard-set fence.
func (r *ShardViewRegistry) RemoveServingGeneration(ctx context.Context, id int64) error {
	r.servingMu.Lock()
	defer r.servingMu.Unlock()
	if r.servingCatalog != nil {
		if err := r.servingCatalog.RemoveServingGeneration(ctx, id); err != nil {
			return err
		}
	}
	delete(r.serving, id)
	return nil
}

func (m *ShardViewManager) PublishServingGeneration(epoch uint64, schema int32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishedServingEpoch != nil && *m.publishedServingEpoch > epoch {
		return
	}
	m.publishedServingEpoch = &epoch
	m.publishedServingSchema = schema
}

func (r *ShardViewRegistry) ServingCollectionIDs() []int64 {
	r.servingMu.Lock()
	defer r.servingMu.Unlock()
	ids := make([]int64, 0, len(r.serving))
	for id := range r.serving {
		ids = append(ids, id)
	}
	return ids
}
