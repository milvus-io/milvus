package broadcaster

import (
	"sort"

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

type pendingSchemaTarget struct {
	target          *SchemaTarget
	controlTimeTick uint64
	eligible        bool
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

// schemaTaskIndex narrows reads to one collection's unfinished ALTER tasks.
// Tombstoning removes membership; recovery excludes completed tasks. Readers
// holding an earlier snapshot still check task state and callback eligibility
// under the task lock.
// All accesses are protected by broadcastTaskManager.mu.
type schemaTaskIndex map[int64]map[uint64]*broadcastTask

// add is called only before the task is published to concurrent readers.
func (i schemaTaskIndex) add(id uint64, task *broadcastTask) {
	if task.schemaCollectionID == 0 ||
		task.task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE ||
		task.task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_DONE {
		return
	}
	tasks := i[task.schemaCollectionID]
	if tasks == nil {
		tasks = make(map[uint64]*broadcastTask)
		i[task.schemaCollectionID] = tasks
	}
	tasks[id] = task
}

func (i schemaTaskIndex) remove(id uint64, collectionID int64) {
	tasks := i[collectionID]
	delete(tasks, id)
	if len(tasks) == 0 {
		delete(i, collectionID)
	}
}

func (bm *broadcastTaskManager) removeSchemaTask(broadcastID uint64, collectionID int64) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	bm.schemaTasks.remove(broadcastID, collectionID)
}

func (bm *broadcastTaskManager) snapshotSchemaTasks(collectionID int64) []*broadcastTask {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	tasks := bm.schemaTasks[collectionID]
	if len(tasks) == 0 {
		return nil
	}
	snapshot := make([]*broadcastTask, 0, len(tasks))
	for _, task := range tasks {
		snapshot = append(snapshot, task)
	}
	return snapshot
}

func (b *broadcastTask) pendingSchemaTarget(collectionID int64) *pendingSchemaTarget {
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
	snapshot := &pendingSchemaTarget{
		target:   &SchemaTarget{CollectionID: collectionID, BroadcastID: b.header().BroadcastID, Schema: proto.Clone(schema).(*schemapb.CollectionSchema), TimeTicks: make(map[string]uint64)},
		eligible: b.schemaTargetEligible,
	}
	for i, channel := range b.header().VChannels {
		tick := b.task.AckedCheckpoints[i].GetTimeTick()
		if funcutil.IsControlChannel(channel) {
			snapshot.controlTimeTick = tick
		} else {
			snapshot.target.TimeTicks[channel] = tick
		}
	}
	return snapshot
}

func (bm *broadcastTaskManager) GetPendingSchemaTarget(collectionID int64) (*SchemaTarget, error) {
	var pending []*pendingSchemaTarget
	for _, task := range bm.snapshotSchemaTasks(collectionID) {
		if snapshot := task.pendingSchemaTarget(collectionID); snapshot != nil {
			pending = append(pending, snapshot)
		}
	}
	if len(pending) == 0 {
		return nil, nil
	}
	// Match ACK callback recovery order, never choose the largest pending DDL.
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].controlTimeTick != pending[j].controlTimeTick {
			// Replication may register a successor via a business-channel ACK
			// before its control-channel ACK arrives. Its unknown order must
			// not hide an already admitted predecessor.
			if pending[i].controlTimeTick == 0 {
				return false
			}
			if pending[j].controlTimeTick == 0 {
				return true
			}
			return pending[i].controlTimeTick < pending[j].controlTimeTick
		}
		return pending[i].target.BroadcastID < pending[j].target.BroadcastID
	})
	if !pending[0].eligible {
		return nil, merr.WrapErrServiceNotReadyMsg("schema target for collection %d awaits its ordered ACK callback", collectionID)
	}
	return pending[0].target, nil
}
