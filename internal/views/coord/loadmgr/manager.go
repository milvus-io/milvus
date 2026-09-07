package loadmgr

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// DirtyCollectionNotifier is called after desired load state changes. The
// outer Balancer adapter can translate this into TriggerScope{DirtyCollections}.
type DirtyCollectionNotifier func(collectionID int64)

// CollectionLoadManager is the Coord-side facade over desired load config
// lifecycle.
type CollectionLoadManager struct {
	store  *LoadConfigStore
	notify DirtyCollectionNotifier
}

func NewCollectionLoadManager(
	store *LoadConfigStore,
	notify DirtyCollectionNotifier,
) *CollectionLoadManager {
	return &CollectionLoadManager{
		store:  store,
		notify: notify,
	}
}

// UpdateLoadConfig applies an AlterLoadConfig WAL ack to desired state and
// notifies the reconciler. The Balancer expands collection shards from the
// latest DataView and creates shard managers when it applies the plan.
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

func (m *CollectionLoadManager) notifyCollection(collectionID int64) {
	if m.notify != nil {
		m.notify(collectionID)
	}
}
