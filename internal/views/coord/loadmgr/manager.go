package loadmgr

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

// ReplicaNodeAllocator fills the runtime node assignment for each replica in a
// LoadConfig before it is persisted.
type ReplicaNodeAllocator interface {
	AssignNodes(ctx context.Context, cfg *LoadConfig) (*LoadConfig, error)
}

// CollectionShardProvider lists vchannels that belong to a collection. The
// manager uses it to create ShardViewManagers for every replica+vchannel pair
// after a load config update.
type CollectionShardProvider interface {
	VChannels(ctx context.Context, collectionID int64) ([]string, error)
}

// ShardEnsurer creates the actual shard view manager for a replica+vchannel
// pair. It is injected so loadmgr does not depend on the concrete coordview
// registry package.
type ShardEnsurer func(qviews.ShardID)

// DirtyCollectionNotifier is called after desired load state changes. The
// outer Balancer adapter can translate this into TriggerScope{DirtyCollections}.
type DirtyCollectionNotifier func(collectionID int64)

// CollectionLoadManager is the Coord-side facade over desired load config
// lifecycle.
type CollectionLoadManager struct {
	store       *LoadConfigStore
	ensureShard ShardEnsurer
	alloc       ReplicaNodeAllocator
	shards      CollectionShardProvider
	notify      DirtyCollectionNotifier
}

func NewCollectionLoadManager(
	store *LoadConfigStore,
	ensureShard ShardEnsurer,
	alloc ReplicaNodeAllocator,
	shards CollectionShardProvider,
	notify DirtyCollectionNotifier,
) *CollectionLoadManager {
	return &CollectionLoadManager{
		store:       store,
		ensureShard: ensureShard,
		alloc:       alloc,
		shards:      shards,
		notify:      notify,
	}
}

// UpdateLoadConfig applies an AlterLoadConfig WAL ack to desired state, creates
// managers for the configured replica+vchannel shards, and notifies the
// reconciler.
func (m *CollectionLoadManager) UpdateLoadConfig(
	ctx context.Context,
	msg *messagespb.AlterLoadConfigMessageHeader,
) error {
	if msg == nil {
		return nil
	}
	cfg := FromAlterLoadConfigMessage(msg)
	if m.alloc != nil {
		next, err := m.alloc.AssignNodes(ctx, cfg.Clone())
		if err != nil {
			return err
		}
		if next != nil {
			cfg = next
		}
	}
	if err := m.store.Put(ctx, cfg); err != nil {
		return err
	}
	if err := m.ensureConfiguredShards(ctx, cfg); err != nil {
		return err
	}
	m.notifyCollection(cfg.CollectionID)
	return nil
}

// ReleaseCollection removes desired state. Existing views are intentionally
// left in the registry; the Balancer sees "desired absent + current exists"
// and releases them through ShardViewManager.RequestRelease.
func (m *CollectionLoadManager) ReleaseCollection(
	ctx context.Context,
	msg *messagespb.DropLoadConfigMessageHeader,
) error {
	if msg == nil {
		return nil
	}
	collectionID := msg.GetCollectionId()
	if err := m.store.Remove(ctx, collectionID); err != nil {
		return err
	}
	m.notifyCollection(collectionID)
	return nil
}

func (m *CollectionLoadManager) ensureConfiguredShards(ctx context.Context, cfg *LoadConfig) error {
	if m.shards == nil || m.ensureShard == nil {
		return nil
	}
	vchannels, err := m.shards.VChannels(ctx, cfg.CollectionID)
	if err != nil {
		return err
	}
	for _, replica := range cfg.Replicas {
		for _, vchannel := range vchannels {
			m.ensureShard(qviews.ShardID{
				ReplicaID: replica.ReplicaID,
				VChannel:  vchannel,
			})
		}
	}
	return nil
}

func (m *CollectionLoadManager) notifyCollection(collectionID int64) {
	if m.notify != nil {
		m.notify(collectionID)
	}
}
