package loadmgr

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

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
	notify      DirtyCollectionNotifier
}

func NewCollectionLoadManager(
	store *LoadConfigStore,
	ensureShard ShardEnsurer,
	notify DirtyCollectionNotifier,
) *CollectionLoadManager {
	return &CollectionLoadManager{
		store:       store,
		ensureShard: ensureShard,
		notify:      notify,
	}
}

// UpdateLoadConfig applies an AlterLoadConfig WAL ack to desired state and
// notifies the reconciler. The ack result already contains every vchannel that
// received the broadcast, so no extra collection-shard provider is needed.
func (m *CollectionLoadManager) UpdateLoadConfig(
	ctx context.Context,
	result message.BroadcastResultAlterLoadConfigMessageV2,
) error {
	msg := result.Message.Header()
	if msg == nil {
		return nil
	}
	cfg := FromAlterLoadConfigMessage(msg)
	if err := m.store.Put(ctx, cfg); err != nil {
		return err
	}
	m.ensureConfiguredShards(cfg, result.GetVChannelsWithoutControlChannel())
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

func (m *CollectionLoadManager) ensureConfiguredShards(cfg *LoadConfig, vchannels []string) {
	if m.ensureShard == nil {
		return
	}
	for _, replica := range cfg.Replicas {
		for _, vchannel := range vchannels {
			m.ensureShard(qviews.ShardID{
				ReplicaID: replica.ReplicaID,
				VChannel:  vchannel,
			})
		}
	}
}

func (m *CollectionLoadManager) notifyCollection(collectionID int64) {
	if m.notify != nil {
		m.notify(collectionID)
	}
}
