// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package l0materializer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func walDelete(tt uint64) message.ImmutableMessage {
	return message.NewDeleteMessageBuilderV1().WithVChannel("v1").WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).WithBody(&msgpb.DeleteRequest{CollectionID: 1, PartitionID: 10, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}, Timestamps: []uint64{tt}}).MustBuildMutable().WithTimeTick(tt).WithLastConfirmed(rmq.NewRmqID(int64(tt - 1))).IntoImmutableMessage(rmq.NewRmqID(int64(tt)))
}

func walFlush(tt uint64) message.ImmutableMessage {
	return message.NewManualFlushMessageBuilderV2().WithVChannel("v1").WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable().WithTimeTick(tt).WithLastConfirmed(rmq.NewRmqID(int64(tt - 1))).IntoImmutableMessage(rmq.NewRmqID(int64(tt)))
}

func TestWALMaterializerExplicitMessagesSplitQueuedDeletes(t *testing.T) {
	for name, build := range map[string]func() message.MutableMessage{
		"ManualFlush": func() message.MutableMessage {
			return message.NewManualFlushMessageBuilderV2().WithVChannel("v1").WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable()
		},
		"FlushAll": func() message.MutableMessage {
			return message.NewFlushAllMessageBuilderV2().WithVChannel("v1").WithHeader(&message.FlushAllMessageHeader{}).WithBody(&message.FlushAllMessageBody{}).MustBuildMutable()
		},
		"DropCollection": func() message.MutableMessage {
			return message.NewDropCollectionMessageBuilderV1().WithVChannel("v1").WithHeader(&message.DropCollectionMessageHeader{}).WithBody(&msgpb.DropCollectionRequest{}).MustBuildMutable()
		},
		"DropPartition": func() message.MutableMessage {
			return message.NewDropPartitionMessageBuilderV1().WithVChannel("v1").WithHeader(&message.DropPartitionMessageHeader{}).WithBody(&msgpb.DropPartitionRequest{}).MustBuildMutable()
		},
		"TruncateCollection": func() message.MutableMessage {
			return message.NewTruncateCollectionMessageBuilderV2().WithVChannel("v1").WithHeader(&message.TruncateCollectionMessageHeader{}).WithBody(&message.TruncateCollectionMessageBody{}).MustBuildMutable()
		},
		"AlterWAL": func() message.MutableMessage {
			return message.NewAlterWALMessageBuilderV2().WithVChannel("v1").WithHeader(&message.AlterWALMessageHeader{}).WithBody(&message.AlterWALMessageBody{}).MustBuildMutable()
		},
		"CreateSnapshot": func() message.MutableMessage {
			return message.NewCreateSnapshotMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CreateSnapshotMessageHeader{}).WithBody(&message.CreateSnapshotMessageBody{}).MustBuildMutable()
		},
	} {
		t.Run(name, func(t *testing.T) {
			boundary := func(tt uint64) message.ImmutableMessage {
				return build().WithTimeTick(tt).WithLastConfirmed(rmq.NewRmqID(int64(tt - 1))).IntoImmutableMessage(rmq.NewRmqID(int64(tt)))
			}
			m, tasks, batches := testWALMaterializer(t, 0, 1<<20)
			tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
			walObserve(m, tracker, walDelete(100))
			m.RequestPersistThrough(100) // Keep the first output in flight.
			walObserve(m, tracker, walDelete(110))
			walObserve(m, tracker, boundary(120))
			walObserve(m, tracker, walDelete(130))
			walObserve(m, tracker, boundary(140))
			walObserve(m, tracker, walDelete(150))
			require.Len(t, *tasks, 1)
			require.NoError(t, (*tasks)[0].Execute(context.Background()))
			require.Len(t, *tasks, 2)
			require.NoError(t, (*tasks)[1].Execute(context.Background()))
			require.Equal(t, uint64(120), tracker.CompletedPoint().TimeTick)
			require.Equal(t, uint64(120), (*batches)[1].TargetTimeTick)
			require.Len(t, (*batches)[1].Entries, 1)
			require.Equal(t, uint64(110), (*batches)[1].Entries[0].GetTimeTick())
			require.Len(t, *tasks, 3)
			require.NoError(t, (*tasks)[2].Execute(context.Background()))
			require.Equal(t, uint64(140), tracker.CompletedPoint().TimeTick)
			require.Len(t, (*batches)[2].Entries, 1)
			require.Equal(t, uint64(130), (*batches)[2].Entries[0].GetTimeTick())
			require.Len(t, *tasks, 3, "the small post-boundary tail must not be forced")
			require.Equal(t, deleteBytes(walDelete(150)), m.pendingBytes)
			m.RequestPersistThrough(150)
			require.NoError(t, (*tasks)[3].Execute(context.Background()))
			require.Zero(t, m.pendingBytes)
			require.Equal(t, uint64(150), tracker.CompletedPoint().TimeTick)
		})
	}
}

// Non-message triggers retain their existing admission policy: pending work can
// grow while another task is active, unless an explicit WAL boundary separates it.
func TestWALMaterializerImplicitTriggersKeepBatching(t *testing.T) {
	for _, trigger := range []string{"capacity", "age", "persist"} {
		t.Run(trigger, func(t *testing.T) {
			m, tasks, batches := testWALMaterializer(t, 0, 1<<20)
			tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
			walObserve(m, tracker, walDelete(100))
			m.RequestPersistThrough(100)
			walObserve(m, tracker, walDelete(110))
			switch trigger {
			case "capacity":
				m.maxBytes = 1
			case "age":
				m.FlushStale(time.Now().Add(2*time.Hour), time.Hour)
			case "persist":
				m.RequestPersistThrough(110)
			}
			walObserve(m, tracker, walDelete(150))
			require.NoError(t, (*tasks)[0].Execute(context.Background()))
			require.Len(t, *tasks, 2)
			require.NoError(t, (*tasks)[1].Execute(context.Background()))
			require.Len(t, (*batches)[1].Entries, 2)
			require.Equal(t, uint64(150), (*batches)[1].TargetTimeTick)
			require.Equal(t, uint64(150), tracker.CompletedPoint().TimeTick)
		})
	}
}

func walObserve(m *WALMaterializer, tracker *messageack.Tracker, msg message.ImmutableMessage) {
	owner := tracker.Track(msg)
	retained := owner.Clone()
	m.ObserveMessage(retained)
	retained.Release()
	owner.Release()
}

func testWALMaterializer(t *testing.T, initial, maxBytes uint64, errs ...*error) (*WALMaterializer, *[]nodescheduler.Task, *[]MaterializeRequest) {
	old, tasks, batches := testMaterializer(t, initial, errs...)
	return NewWALMaterializer(WALConfig{VChannel: "v1", MaterializedTimeTick: initial, MaterializeMaxBytes: maxBytes, Runtime: old.runtime, Materializer: old.materializer}), tasks, batches
}

func TestWALMaterializerRetainsDeleteThroughFailureAndDirtySnapshot(t *testing.T) {
	outputErr := context.DeadlineExceeded
	m, tasks, batches := testWALMaterializer(t, 0, 1, &outputErr)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	walObserve(m, tracker, walDelete(100))
	require.Len(t, *tasks, 1)
	require.Zero(t, tracker.CompletedPoint().TimeTick)
	require.Error(t, (*tasks)[0].Execute(context.Background()))
	require.Zero(t, m.MaterializedTimeTick())
	require.Zero(t, tracker.CompletedPoint().TimeTick)
	m.onMaterialized = func(tt uint64) { require.Equal(t, uint64(100), tt); require.Zero(t, tracker.CompletedPoint().TimeTick) }
	outputErr = nil
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Equal(t, uint64(100), tracker.CompletedPoint().TimeTick)
	require.Equal(t, uint64(100), m.MaterializedTimeTick())
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Len(t, *batches, 2)
}

func TestWALMaterializerStaleCapacityAndFrozenBatch(t *testing.T) {
	m, tasks, batches := testWALMaterializer(t, 0, 1<<20)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	walObserve(m, tracker, walDelete(100))
	m.FlushStale(time.Now(), time.Hour)
	require.Empty(t, *tasks)
	m.FlushStale(time.Now().Add(2*time.Hour), time.Hour)
	require.Len(t, *tasks, 1)
	walObserve(m, tracker, walDelete(200))
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Len(t, *tasks, 1, "small tail waits")
	require.Equal(t, uint64(100), tracker.CompletedPoint().TimeTick)
	m.RequestPersistThrough(150)
	require.Len(t, *tasks, 1)
	m.RequestPersistThrough(200)
	require.Len(t, *tasks, 2)
	require.NoError(t, (*tasks)[1].Execute(context.Background()))
	require.Equal(t, uint64(200), tracker.CompletedPoint().TimeTick)
	require.Equal(t, uint64(100), (*batches)[0].TargetTimeTick)
	require.Equal(t, uint64(200), (*batches)[1].TargetTimeTick)
	m.FlushStale(time.Now().Add(3*time.Hour), time.Hour)
	require.Len(t, *tasks, 2)
}

func TestWALMaterializerFlushQueuedDuringOutput(t *testing.T) {
	m, tasks, batches := testWALMaterializer(t, 0, 1)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	walObserve(m, tracker, walDelete(100))
	walObserve(m, tracker, walFlush(200))
	require.Len(t, *tasks, 1)
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Equal(t, uint64(100), tracker.CompletedPoint().TimeTick)
	require.Len(t, *tasks, 2)
	require.NoError(t, (*tasks)[1].Execute(context.Background()))
	require.Equal(t, uint64(200), tracker.CompletedPoint().TimeTick)
	require.Len(t, *batches, 1, "empty Flush advances metadata without empty L0")
}

func TestWALMaterializerReplayUsesMaterializedFrontier(t *testing.T) {
	m, tasks, _ := testWALMaterializer(t, 100, 1)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	walObserve(m, tracker, walDelete(100))
	require.Equal(t, uint64(100), tracker.CompletedPoint().TimeTick)
	require.Empty(t, *tasks)
	walObserve(m, tracker, walDelete(200))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, (*tasks)[0].Execute(ctx), context.Canceled)
	require.Equal(t, uint64(100), tracker.CompletedPoint().TimeTick)
	// Simulate restart without executing the old process's pending task.
	recovered := NewWALMaterializer(WALConfig{VChannel: "v1", MaterializedTimeTick: 100, MaterializeMaxBytes: 1, Runtime: m.runtime, Materializer: m.writer})
	recoveredTracker := messageack.NewTracker(tracker.CompletedPoint(), nil, nil)
	walObserve(recovered, recoveredTracker, walDelete(200))
	require.Len(t, *tasks, 2)
	require.NoError(t, (*tasks)[1].Execute(context.Background()))
	require.Equal(t, uint64(200), recoveredTracker.CompletedPoint().TimeTick)
}

func walTxn(t *testing.T, children ...message.ImmutableMessage) message.ImmutableMessage {
	txnContext := message.TxnContext{TxnID: 1}
	id := rmq.NewRmqID(1)
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnContext).WithTimeTick(1).WithLastConfirmed(id).IntoImmutableMessage(id)
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
	for _, child := range children {
		builder.Add(child)
	}
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnContext).WithTimeTick(300).WithLastConfirmed(id).IntoImmutableMessage(rmq.NewRmqID(300))
	txn, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commit))
	require.NoError(t, err)
	return txn
}

func TestWALMaterializerWholeTxnAndRegistrationGate(t *testing.T) {
	m, tasks, batches := testWALMaterializer(t, 0, 1)
	registered := false
	m.growingSegmentsRegistered = func(tt uint64) bool { require.Equal(t, uint64(300), tt); return registered }
	insert := message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(&message.InsertMessageHeader{}).WithBody(&msgpb.InsertRequest{}).MustBuildMutable().WithTimeTick(50).WithLastConfirmed(rmq.NewRmqID(49)).IntoImmutableMessage(rmq.NewRmqID(50))
	txn := walTxn(t, insert, walDelete(100), walDelete(200))
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	walObserve(m, tracker, txn)
	require.Len(t, *tasks, 1)
	require.ErrorIs(t, (*tasks)[0].Execute(context.Background()), nodescheduler.ErrDelay)
	require.Empty(t, *batches, "unregistered L1 must block L0 output")
	require.Zero(t, tracker.CompletedPoint().TimeTick)
	registered = true
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Equal(t, uint64(300), tracker.CompletedPoint().TimeTick)
	require.Len(t, *batches, 1)
	require.Len(t, (*batches)[0].Entries, 1)
	require.Len(t, (*batches)[0].Entries[0].GetDelete().GetBlocks(), 2)
	require.Equal(t, uint64(300), (*batches)[0].Entries[0].GetTimeTick())
	require.Len(t, (*batches)[0].StartPositions, 1)
	position := utility.NewMessagePosition(txn, "v1")
	require.True(t, proto.Equal(position, (*batches)[0].StartPositions[300]))
	require.True(t, proto.Equal(position, (*batches)[0].Checkpoint))
}

func TestWALMaterializerInsertAndBarrierDoNotFlush(t *testing.T) {
	m, tasks, _ := testWALMaterializer(t, 0, 1)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	insert := message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(&message.InsertMessageHeader{}).WithBody(&msgpb.InsertRequest{}).MustBuildMutable().WithTimeTick(100).WithLastConfirmed(rmq.NewRmqID(99)).IntoImmutableMessage(rmq.NewRmqID(100))
	walObserve(m, tracker, insert)
	walObserve(m, tracker, walTxn(t, insert))
	require.Equal(t, uint64(300), tracker.CompletedPoint().TimeTick)
	require.Empty(t, *tasks)
	require.Zero(t, m.MaterializedTimeTick(), "no empty L0 needed to advance the global prefix")
}

func TestWALMaterializerUsesFlushBoundaryPosition(t *testing.T) {
	m, tasks, batches := testWALMaterializer(t, 0, 1<<20)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	deleted, flushed := walDelete(100), walFlush(200)
	walObserve(m, tracker, deleted)
	walObserve(m, tracker, flushed)
	require.Len(t, *tasks, 1)
	require.NoError(t, (*tasks)[0].Execute(context.Background()))
	require.Len(t, *batches, 1)
	req := (*batches)[0]
	require.True(t, proto.Equal(utility.NewMessagePosition(deleted, "v1"), req.StartPositions[100]))
	require.True(t, proto.Equal(utility.NewMessagePosition(flushed, "v1"), req.Checkpoint))
	require.Equal(t, uint64(200), tracker.CompletedPoint().TimeTick)
}
