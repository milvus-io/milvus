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

func (b *broadcastTask) pendingSchemaTarget(collectionID int64) *pendingSchemaTarget {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.task.State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE || b.messageTypeWithVersion != message.MessageTypeAlterCollectionV2 {
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
	for _, task := range bm.snapshotBroadcastTasks() {
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
			return pending[i].controlTimeTick < pending[j].controlTimeTick
		}
		return pending[i].target.BroadcastID < pending[j].target.BroadcastID
	})
	if !pending[0].eligible {
		return nil, merr.WrapErrServiceNotReadyMsg("schema target for collection %d awaits its ordered ACK callback", collectionID)
	}
	return pending[0].target, nil
}
