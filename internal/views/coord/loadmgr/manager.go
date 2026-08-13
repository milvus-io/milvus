package loadmgr

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// DirtyCollectionNotifier is called after desired load state changes. The
// outer Balancer adapter can translate this into TriggerScope{DirtyCollections}.
type DirtyCollectionNotifier func(collectionID int64)

// ShardAssignmentNotifier is called after the pchannel-scoped discoverable
// shard assignment changes. The caller can translate it into an assignment
// discovery watch update without coupling loadmgr to StreamingCoord.
type ShardAssignmentNotifier func()

// CollectionLoadManager is the Coord-side facade over desired load config
// lifecycle.
type CollectionLoadManager struct {
	store  *LoadConfigStore
	notify DirtyCollectionNotifier

	mu                      sync.RWMutex
	discoverableShards      map[qviews.ShardID]discoverableShard
	shardAssignmentNotifier ShardAssignmentNotifier
}

type discoverableShard struct {
	collectionID int64
	pchannel     string
	shardIndex   int32
	replicaID    int64
}

func NewCollectionLoadManager(
	store *LoadConfigStore,
	notify DirtyCollectionNotifier,
) *CollectionLoadManager {
	return &CollectionLoadManager{
		store:              store,
		notify:             notify,
		discoverableShards: make(map[qviews.ShardID]discoverableShard),
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
	if m.removeDiscoverableCollection(collectionID) {
		m.notifyShardAssignmentsChanged()
	}
	m.notifyCollection(collectionID)
	return nil
}

// SetShardAssignmentNotifier installs the callback used to publish
// discoverable shard assignment changes.
func (m *CollectionLoadManager) SetShardAssignmentNotifier(notifier ShardAssignmentNotifier) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shardAssignmentNotifier = notifier
}

// ObserveShardUp marks a shard as discoverable the first time it reaches Up.
// Later QueryView version changes for the same shard do not change assignment
// discovery; the shard remains discoverable until the collection is released.
func (m *CollectionLoadManager) ObserveShardUp(shardID qviews.ShardID) {
	if m.markDiscoverable(shardID) {
		m.notifyShardAssignmentsChanged()
	}
}

// MarkShardDiscoverable marks a shard as discoverable. It is exported for the
// coordview registry adapter and tests; callers should pass a shard that has
// reached Up.
func (m *CollectionLoadManager) MarkShardDiscoverable(shardID qviews.ShardID) bool {
	return m.markDiscoverable(shardID)
}

// ShardAssignmentsByPChannel returns a snapshot of discoverable shard replicas,
// grouped by pchannel. StreamingCoord maps these pchannel groups onto the
// current SN owners when publishing assignment discovery.
func (m *CollectionLoadManager) ShardAssignmentsByPChannel() map[string][]types.ShardAssignmentEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	assignments := make(map[string][]types.ShardAssignmentEntry)
	for _, shard := range m.discoverableShards {
		assignments[shard.pchannel] = append(assignments[shard.pchannel], types.ShardAssignmentEntry{
			CollectionID: shard.collectionID,
			ShardIndex:   shard.shardIndex,
			ReplicaID:    shard.replicaID,
		})
	}
	return assignments
}

func (m *CollectionLoadManager) markDiscoverable(shardID qviews.ShardID) bool {
	shard, ok := newDiscoverableShard(shardID)
	if !ok {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.discoverableShards[shardID]; exists {
		return false
	}
	m.discoverableShards[shardID] = shard
	return true
}

func newDiscoverableShard(shardID qviews.ShardID) (discoverableShard, bool) {
	ch, err := metautil.ParseChannel(shardID.VChannel, metautil.NewDynChannelMapper())
	if err != nil {
		return discoverableShard{}, false
	}
	return discoverableShard{
		collectionID: ch.CollectionID(),
		pchannel:     ch.PhysicalName(),
		shardIndex:   int32(ch.ShardIdx()),
		replicaID:    shardID.ReplicaID,
	}, true
}

func (m *CollectionLoadManager) removeDiscoverableCollection(collectionID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	changed := false
	for shardID, shard := range m.discoverableShards {
		if shard.collectionID == collectionID {
			delete(m.discoverableShards, shardID)
			changed = true
		}
	}
	return changed
}

func (m *CollectionLoadManager) notifyShardAssignmentsChanged() {
	m.mu.RLock()
	notifier := m.shardAssignmentNotifier
	m.mu.RUnlock()
	if notifier != nil {
		notifier()
	}
}

func (m *CollectionLoadManager) notifyCollection(collectionID int64) {
	if m.notify != nil {
		m.notify(collectionID)
	}
}
