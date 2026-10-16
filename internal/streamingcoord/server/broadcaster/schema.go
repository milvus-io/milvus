package broadcaster

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SchemaTarget is an owned snapshot of durable ALTER intent. It is available
// only after the ordered ACK callback may prepare resources. RootCoord's
// published schema takes over when that callback completes and is tombstoned.
type SchemaTarget struct {
	CollectionID int64
	BroadcastID  uint64
	Schema       *schemapb.CollectionSchema
	TimeTicks    map[string]uint64
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

// schemaTaskIndex tracks pending membership and the target admitted by the ACK
// scheduler. Conflicting collection callbacks are serialized by the scheduler;
// only its current callback may expose a schema, independent of pending ACK order.
// All accesses are protected by broadcastTaskManager.mu.
type schemaTaskIndex map[int64]*collectionSchemaTasks

type collectionSchemaTasks struct {
	pending map[uint64]*broadcastTask
	target  *broadcastTask
}

// add is called only before the task is published to concurrent readers.
func (i schemaTaskIndex) add(id uint64, task *broadcastTask) {
	if task.schemaCollectionID == 0 ||
		task.task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE ||
		task.task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE {
		return
	}
	tasks := i[task.schemaCollectionID]
	if tasks == nil {
		tasks = &collectionSchemaTasks{pending: make(map[uint64]*broadcastTask)}
		i[task.schemaCollectionID] = tasks
	}
	tasks.pending[id] = task
}

func (i schemaTaskIndex) remove(id uint64, collectionID int64) {
	tasks := i[collectionID]
	if tasks == nil {
		return
	}
	if tasks.target == tasks.pending[id] {
		tasks.target = nil
	}
	delete(tasks.pending, id)
	if len(tasks.pending) == 0 {
		delete(i, collectionID)
	}
}

func (bm *broadcastTaskManager) removeSchemaTask(broadcastID uint64, collectionID int64) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	bm.schemaTasks.remove(broadcastID, collectionID)
}

// admitSchemaTarget is called with the callback's resource locks held and all
// channels ACKed. Recovery rebuilds pending membership only; the scheduler must
// grant admission again before a recovered target becomes readable.
func (bm *broadcastTaskManager) admitSchemaTarget(task *broadcastTask) {
	if task.schemaCollectionID == 0 {
		return
	}
	bm.mu.Lock()
	defer bm.mu.Unlock()
	bm.schemaTasks[task.schemaCollectionID].target = task
}

func (b *broadcastTask) pendingSchemaTarget(collectionID int64) *SchemaTarget {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE || b.task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE || b.msg.MessageTypeWithVersion() != message.MessageTypeAlterCollectionV2 {
		return nil
	}
	msg, err := message.AsMutableAlterCollectionMessageV2(b.msg)
	if err != nil || msg.Header().GetCollectionId() != collectionID || !messageutil.IsSchemaChange(msg.Header()) {
		return nil
	}
	schema := msg.MustBody().GetUpdates().GetSchema()
	if schema == nil {
		return nil
	}
	snapshot := &SchemaTarget{
		CollectionID: collectionID,
		BroadcastID:  b.header().BroadcastID,
		Schema:       proto.Clone(schema).(*schemapb.CollectionSchema),
		TimeTicks:    make(map[string]uint64),
	}
	for i, channel := range b.header().VChannels {
		tick := b.task.AckedCheckpoints[i].GetTimeTick()
		if !funcutil.IsControlChannel(channel) {
			snapshot.TimeTicks[channel] = tick
		}
	}
	return snapshot
}

func (bm *broadcastTaskManager) GetPendingSchemaTarget(collectionID int64) (*SchemaTarget, error) {
	bm.mu.Lock()
	tasks := bm.schemaTasks[collectionID]
	if tasks == nil {
		bm.mu.Unlock()
		return nil, nil
	}
	target := tasks.target
	bm.mu.Unlock()
	if target == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("schema target for collection %d awaits its ordered ACK callback", collectionID)
	}
	// Recheck completion under the task lock: the callback may have finished
	// since the index lookup. Clone only the selected target for the caller.
	return target.pendingSchemaTarget(collectionID), nil
}
