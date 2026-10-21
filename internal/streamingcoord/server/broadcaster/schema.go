package broadcaster

import (
	"maps"
	"sync"

	"github.com/google/btree"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// SchemaTarget is a snapshot of the earliest unfinished durable schema
// ALTER for a collection. It is readable after task persistence, before WAL
// append or ACK completion. This does not authorize publication or prove that
// any channel has applied the schema. Completion of its callback retires it.
type SchemaTarget struct {
	CollectionID int64
	BroadcastID  uint64
	// Schema is shared and immutable; callers must not modify it.
	Schema *schemapb.CollectionSchema
	// TimeTicks contains only persisted, known business-channel ACK checkpoints.
	// Missing entries are unknown; the initial snapshot may have none.
	TimeTicks map[string]uint64
}

// schemaTargetCollectionID is captured before publishing a task. MarkIgnore can
// replace a task's message later, but only for AlterReplicateConfig messages.
// Read only the header: completed tasks need not have their bodies decoded
// during recovery.
func schemaTargetCollectionID(msg message.BroadcastMutableMessage) int64 {
	if msg.MessageTypeWithVersion() != message.MessageTypeAlterCollectionV2 {
		return 0
	}
	alter, err := message.AsMutableAlterCollectionMessageV2(msg)
	if err != nil || !messageutil.IsSchemaChange(alter.Header()) {
		return 0
	}
	return alter.Header().GetCollectionId()
}

// schemaTaskIndex contains only successfully persisted task snapshots. Code
// holding its lock never acquires a manager or task lock, so persistence can
// update it under the task lock without blocking readers on metadata I/O.
//
// ALTERs for a collection are serial: withResourceKeys allocates BroadcastID
// after acquiring the resource lock, and replication preserves that ID. Keep the
// earliest unfinished ID, including on secondaries with several pending ALTERs.
// ACK arrival order and missing control-channel checkpoints do not change order.
type schemaTaskIndex struct {
	mu          sync.RWMutex
	collections map[int64]*collectionSchemaTasks
}

type collectionSchemaTasks struct {
	pending *btree.BTreeG[*SchemaTarget]
	head    *SchemaTarget
}

func newSchemaTaskIndex() *schemaTaskIndex {
	return &schemaTaskIndex{collections: make(map[int64]*collectionSchemaTasks)}
}

// update is called with the task lock held after a successful catalog save, or
// before recovery publishes the manager. Never index an unpersisted mutation.
func (i *schemaTaskIndex) update(task *broadcastTask) {
	collectionID := task.schemaCollectionID
	if collectionID == 0 {
		return
	}
	snapshot := task.pendingSchemaTarget()
	i.mu.Lock()
	defer i.mu.Unlock()
	tasks := i.collections[collectionID]
	if snapshot == nil {
		if tasks != nil {
			tasks.pending.Delete(&SchemaTarget{BroadcastID: task.header().BroadcastID})
			if tasks.pending.Len() == 0 {
				delete(i.collections, collectionID)
			} else {
				tasks.head, _ = tasks.pending.Min()
			}
		}
		return
	}
	if tasks == nil {
		tasks = &collectionSchemaTasks{pending: btree.NewG(2, func(a, b *SchemaTarget) bool {
			return a.BroadcastID < b.BroadcastID
		})}
		i.collections[collectionID] = tasks
	}
	tasks.pending.ReplaceOrInsert(snapshot)
	tasks.head, _ = tasks.pending.Min()
}

// pendingSchemaTarget snapshots the task under its lock (or before publication).
func (b *broadcastTask) pendingSchemaTarget() *SchemaTarget {
	if b.task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE || b.task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE {
		b.schemaForTarget = nil
		return nil
	}
	if b.schemaForTarget == nil {
		msg := message.MustAsMutableAlterCollectionMessageV2(b.msg)
		schema := msg.MustBody().GetUpdates().GetSchema()
		if schema == nil {
			return nil
		}
		// A schema ALTER's body never changes. Decode it once and share it across
		// immutable index snapshots; only the ACK checkpoints change on save.
		b.schemaForTarget = schema
	}
	snapshot := &SchemaTarget{
		CollectionID: b.schemaCollectionID,
		BroadcastID:  b.header().BroadcastID,
		Schema:       b.schemaForTarget,
		TimeTicks:    make(map[string]uint64),
	}
	for i, channel := range b.header().VChannels {
		if tick := b.task.AckedCheckpoints[i].GetTimeTick(); !funcutil.IsControlChannel(channel) && tick != 0 {
			snapshot.TimeTicks[channel] = tick
		}
	}
	return snapshot
}

func (bm *broadcastTaskManager) GetPendingSchemaTarget(collectionID int64) (*SchemaTarget, error) {
	bm.schemaTasks.mu.RLock()
	tasks := bm.schemaTasks.collections[collectionID]
	if tasks == nil {
		bm.schemaTasks.mu.RUnlock()
		return nil, nil
	}
	snapshot := tasks.head
	bm.schemaTasks.mu.RUnlock()
	// Index snapshots are immutable. Copy only the selected target, without
	// acquiring any task lock or inspecting pending successors.
	return &SchemaTarget{
		CollectionID: snapshot.CollectionID,
		BroadcastID:  snapshot.BroadcastID,
		Schema:       snapshot.Schema,
		TimeTicks:    maps.Clone(snapshot.TimeTicks),
	}, nil
}
