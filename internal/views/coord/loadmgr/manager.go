package loadmgr

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	return m.ApplyLoadConfig(ctx, result.Message.Header())
}

// ApplyLoadConfig applies desired state under the caller's collection DDL lock.
func (m *CollectionLoadManager) ApplyLoadConfig(ctx context.Context, msg *messagespb.AlterLoadConfigMessageHeader) error {
	if msg == nil {
		return nil
	}
	return m.apply(ctx, FromAlterLoadConfigMessage(msg))
}

// AddPartition extends an existing config under the CreatePartition DDL lock.
func (m *CollectionLoadManager) AddPartition(ctx context.Context, collectionID, partitionID int64) error {
	cfg := m.store.GetConfig(collectionID)
	if cfg == nil {
		return nil
	}
	cfg = cfg.Clone()
	partitions := typeutil.NewUniqueSet(cfg.PartitionIDs...)
	partitions.Insert(partitionID)
	cfg.PartitionIDs = partitions.Collect()
	return m.apply(ctx, cfg)
}

func (m *CollectionLoadManager) apply(ctx context.Context, cfg *LoadConfig) error {
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
