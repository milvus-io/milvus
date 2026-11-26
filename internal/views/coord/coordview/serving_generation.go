package coordview

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ResolveServingGeneration publishes one complete read identity only when every
// replica serves its exact load, public read schema, shard set and lineage.
func (r *ShardViewRegistry) ResolveServingGeneration(ctx context.Context, desired *viewpb.DataViewOfCollection, loadVersion uint64, replicas []int64) (*viewpb.ServingGeneration, error) {
	r.servingMu.Lock()
	defer r.servingMu.Unlock()
	id := desired.GetCollectionId()
	r.mu.RLock()
	readSchema, published := r.publishedSchemas[id]
	r.mu.RUnlock()
	if !published || loadVersion == 0 {
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d has no active published read identity", id)
	}
	current := r.serving[id]
	next := &viewpb.ServingGeneration{
		CollectionId: id, LoadInfoVersion: loadVersion, ReadSchemaVersion: readSchema,
		ShardSetEpoch: desired.GetShardSetEpoch(),
	}
	for _, shard := range desired.GetShards() {
		// A serving publication owns logical membership, not DataView watermarks
		// or segment membership. The view selected by SN owns those resources.
		copy := &viewpb.DataViewOfShard{Vchannel: shard.GetVchannel()}
		if shard.GetLineage() != nil {
			copy.Lineage = proto.Clone(shard.GetLineage()).(*viewpb.QueryViewLineage)
		}
		next.Shards = append(next.Shards, copy)
	}
	if current == nil && next.GetShardSetEpoch() > 1 {
		previous := proto.Clone(next).(*viewpb.ServingGeneration)
		previous.ShardSetEpoch = 0
		targets := make(map[string]bool)
		for _, shard := range previous.Shards {
			for _, target := range shard.GetLineage().GetVchannels() {
				targets[target] = true
			}
		}
		previous.Shards = nil
		for _, shard := range next.Shards {
			if !targets[shard.GetVchannel()] {
				previous.Shards = append(previous.Shards, &viewpb.DataViewOfShard{Vchannel: shard.GetVchannel()})
			}
		}
		if r.generationReady(previous, replicas) {
			if err := r.saveServingGeneration(ctx, previous); err != nil {
				return nil, err
			}
			current = previous
		}
	}
	// Load versions and shard epochs are monotonic. A request that captured an
	// older load snapshot must never regress a concurrent Reload publication.
	if current != nil && (current.GetLoadInfoVersion() > loadVersion || current.GetShardSetEpoch() > next.GetShardSetEpoch()) {
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d serving snapshot changed", id)
	}
	if !proto.Equal(current, next) && r.generationReady(next, replicas) {
		if err := r.saveServingGeneration(ctx, next); err != nil {
			return nil, err
		}
		current = next
	}
	// Retaining the shard-set fence across Release must not make its obsolete
	// load requirements serve a later Load, even if the epoch is unchanged.
	if current == nil || current.GetLoadInfoVersion() != loadVersion || current.GetReadSchemaVersion() != readSchema {
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d has no published serving generation for load %d and schema %d", id, loadVersion, readSchema)
	}
	for _, sid := range r.CollectionShards(id) {
		if manager := r.Get(sid); manager != nil {
			manager.PublishServingGeneration(current.GetShardSetEpoch(), current.GetReadSchemaVersion(), current.GetLoadInfoVersion())
			manager.preemptOlderGenerations(current.GetShardSetEpoch())
		}
	}
	return proto.Clone(current).(*viewpb.ServingGeneration), nil
}

func (r *ShardViewRegistry) saveServingGeneration(ctx context.Context, generation *viewpb.ServingGeneration) error {
	if err := r.servingCatalog.SaveServingGeneration(ctx, generation); err != nil {
		return err
	}
	r.serving[generation.GetCollectionId()] = generation
	return nil
}

func (m *ShardViewManager) servesGeneration(generation *viewpb.ServingGeneration, lineage *viewpb.QueryViewLineage) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, sm := range m.views {
		meta := sm.View().GetMeta()
		if sm.State() == qviews.QueryViewStateUp && sm.snState == qviews.QueryViewStateUp &&
			meta.GetLoadInfoVersion() == generation.GetLoadInfoVersion() &&
			meta.GetShardSetEpoch() == generation.GetShardSetEpoch() &&
			meta.GetSchemaVersion() == generation.GetReadSchemaVersion() && proto.Equal(meta.GetLineage(), lineage) {
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

func (r *ShardViewRegistry) generationReady(desired *viewpb.ServingGeneration, replicas []int64) bool {
	if len(replicas) == 0 || len(desired.GetShards()) == 0 {
		return false
	}
	for _, replica := range replicas {
		for _, shard := range desired.GetShards() {
			manager := r.Get(qviews.ShardID{ReplicaID: replica, VChannel: shard.GetVchannel()})
			if manager == nil || !manager.servesGeneration(desired, shard.GetLineage()) {
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
	r.mu.Lock()
	delete(r.publishedSchemas, id)
	r.mu.Unlock()
	return nil
}

func (m *ShardViewManager) PublishServingGeneration(epoch uint64, schema int32, loadVersion uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.publishedServingEpoch != nil && *m.publishedServingEpoch > epoch {
		return
	}
	m.publishedServingEpoch = &epoch
	m.publishedServingSchema = schema
	m.publishedServingLoadInfoVersion = loadVersion
}

// ServingCollectionIDs includes schema-only publications so the existing GC
// pass can collect a schema callback that completes after logical deletion.
func (r *ShardViewRegistry) ServingCollectionIDs() []int64 {
	r.servingMu.Lock()
	defer r.servingMu.Unlock()
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]int64, 0, len(r.serving)+len(r.publishedSchemas))
	for id := range r.serving {
		ids = append(ids, id)
	}
	for id := range r.publishedSchemas {
		if _, exists := r.serving[id]; !exists {
			ids = append(ids, id)
		}
	}
	return ids
}
