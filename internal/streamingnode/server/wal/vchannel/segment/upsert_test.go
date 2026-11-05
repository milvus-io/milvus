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

package segment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/l0materializer"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	upsertfixture "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/testutils"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func persistenceUpsert(t *testing.T) message.ImmutableMessage {
	t.Helper()
	insert := message.MustAsImmutableInsertMessageV1(buildCrashTestInsertMessage(t, "v1", 1, 2, 3, 999, 20))
	body := proto.Clone(insert.MustBody()).(*msgpb.InsertRequest)
	body.Version = msgpb.InsertDataVersion_ColumnBased
	body.CollectionID = 1
	body.PartitionID = 2
	return message.NewUpsertMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.UpsertMessageHeader{CollectionId: 1, SchemaVersion: func() *int32 { v := int32(1); return &v }(), Partitions: []*message.PartitionSegmentAssignment{insert.Header().GetPartitions()[0]}, DeleteRows: 1}).
		WithBody(upsertfixture.Body(body, &msgpb.DeleteRequest{
			CollectionID: 1, PartitionID: -1, NumRows: 1, Timestamps: []uint64{999},
			PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
		})).MustBuildMutable().WithTimeTick(20).WithLastConfirmed(rmq.NewRmqID(10)).IntoImmutableMessage(rmq.NewRmqID(20))
}

func persistenceTxn(t *testing.T, body message.ImmutableMessage) message.ImmutableMessage {
	t.Helper()
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 1}).WithTimeTick(10).
		WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(10))
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).
		WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 1}).WithTimeTick(30).
		WithLastConfirmed(rmq.NewRmqID(10)).IntoImmutableMessage(rmq.NewRmqID(30))
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
	builder.Add(body)
	txn, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commit))
	require.NoError(t, err)
	return txn
}

func TestBuildGrowingInsertDataUpsert(t *testing.T) {
	upsert := persistenceUpsert(t)
	before := proto.Clone(message.MustAsImmutableUpsertMessageV2(upsert).MustBody())
	for name, raw := range map[string]message.ImmutableMessage{"autocommit": upsert, "transaction": persistenceTxn(t, upsert)} {
		t.Run(name, func(t *testing.T) {
			batches, err := BuildInsertBatches(raw)
			require.NoError(t, err)
			require.Len(t, batches, 1)
			require.Equal(t, uint64(1), batches[3].rows)
			pack := crashTestPack(1, 2, 3, "v1", raw.TimeTick(), crashTestSchema(), raw)
			data, _, err := buildGrowingInsertData(pack.Schema, pack)
			require.NoError(t, err)
			require.Len(t, data, 1)
			timestamps, err := storage.GetTimestampFromInsertData(data[0])
			require.NoError(t, err)
			require.Equal(t, []int64{int64(raw.TimeTick())}, timestamps.Data)
			require.Equal(t, []int64{1}, data[0].Data[100].(*storage.Int64FieldData).Data)
			require.Equal(t, []float32{1, 2, 3, 4, 5, 6, 7, 8}, data[0].Data[101].(*storage.FloatVectorFieldData).Data)
			pack.SegmentID = 99
			_, _, err = buildGrowingInsertData(pack.Schema, pack)
			require.Error(t, err, "rows must not be copied to an unassigned segment")
		})
	}
	require.True(t, proto.Equal(before, message.MustAsImmutableUpsertMessageV2(upsert).MustBody()), "L1 preparation must preserve both cached requests")
}

type upsertPersistenceLifecycle struct{ Lifecycle }

func (upsertPersistenceLifecycle) PersistGrowingSegment(context.Context, *streamingpb.SegmentAssignmentMeta, *msgpb.MsgPosition, *msgpb.MsgPosition) error {
	return nil
}

type upsertPersistenceWriter struct {
	t     *testing.T
	rows  int
	fail  bool
	write int
}

func (w *upsertPersistenceWriter) FlushInsertBuffer(_ context.Context, pack *flushPack) (*flushResult, error) {
	if w.fail {
		return nil, context.DeadlineExceeded
	}
	data, _, err := buildGrowingInsertData(pack.Schema, pack)
	if err != nil {
		return nil, err
	}
	w.write++
	for _, rows := range data {
		pks := rows.Data[100].(*storage.Int64FieldData).Data
		require.Equal(w.t, []int64{1}, pks)
		timestamps, err := storage.GetTimestampFromInsertData(rows)
		require.NoError(w.t, err)
		require.Equal(w.t, []int64{int64(pack.ToTimeTick)}, timestamps.Data)
		w.rows += len(pks)
	}
	return &flushResult{PersistedStorage: &streamingpb.L1SegmentPersistedStorage{Binlogs: []*streamingpb.L1SegmentBinLogs{{FromTimeTick: pack.FromTimeTick, ToTimeTick: pack.ToTimeTick}}}}, nil
}

type upsertDeleteWriter struct {
	t      *testing.T
	fail   bool
	writes int
}

func (w *upsertDeleteWriter) Materialize(_ context.Context, request l0materializer.MaterializeRequest) error {
	if w.fail {
		return context.DeadlineExceeded
	}
	require.Len(w.t, request.Entries, 1)
	entry := request.Entries[0]
	require.Equal(w.t, request.TargetTimeTick, entry.GetTimeTick())
	require.Len(w.t, entry.GetDelete().GetBlocks(), 1)
	block := entry.GetDelete().GetBlocks()[0]
	require.Equal(w.t, int64(-1), block.GetPartitionId())
	require.Equal(w.t, []int64{1}, block.GetPrimaryKeys().GetIntId().GetData())
	w.writes++
	return nil
}

func TestUpsertPersistenceJoinsL1AndL0AndReplaysIndependentSnapshots(t *testing.T) {
	for _, transaction := range []bool{false, true} {
		for _, l0First := range []bool{false, true} {
			name := "autocommit"
			if transaction {
				name = "transaction"
			}
			if l0First {
				name += "/L0-first"
			} else {
				name += "/L1-first"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				raw := persistenceUpsert(t)
				if transaction {
					raw = persistenceTxn(t, raw)
				}
				through := raw.TimeTick()
				insertWriter := &upsertPersistenceWriter{t: t}
				deleteWriter := &upsertDeleteWriter{t: t}
				scheduler := &recordingSegmentScheduler{}
				config := runtimeConfig{lifecycle: upsertPersistenceLifecycle{}, packWriter: insertWriter, owner: testSegmentOwner{}, runtime: moduleapi.Runtime{Scheduler: scheduler}, flushPolicy: newWriteOnlyFlushPolicy(0, 0, 0)}
				meta := &streamingpb.SegmentAssignmentMeta{CollectionId: 1, PartitionId: 2, SegmentId: 3, Vchannel: "v1", CheckpointTimeTick: 10, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING, Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 5}}
				view := newSegmentViewFromMeta(meta, crashTestSchema(), config)
				l0 := l0materializer.NewWALMaterializer(l0materializer.WALConfig{VChannel: "v1", Runtime: config.runtime, Materializer: deleteWriter, MaterializeMaxBytes: 1})
				observe := func(view *SegmentView, l0 *l0materializer.WALMaterializer, tracker *messageack.Tracker) {
					owner := tracker.Track(raw)
					retained := owner.Clone()
					batches, err := BuildInsertBatches(raw)
					require.NoError(t, err)
					view.ObserveInsert(ctx, retained, batches[3])
					l0.ObserveMessage(retained)
					retained.Release()
					owner.Release()
				}
				tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
				observe(view, l0, tracker)
				require.Zero(t, tracker.CompletedPoint().TimeTick)
				require.True(t, view.RequestPersistThrough(through))
				require.Len(t, scheduler.tasks, 2, "both components retain the same outer WAL owner")
				first, second := 1, 0 // Segment persistence was scheduled after L0.
				if l0First {
					first, second = 0, 1
				}
				if l0First {
					deleteWriter.fail = true
				} else {
					insertWriter.fail = true
				}
				require.Error(t, scheduler.tasks[first].Execute(ctx))
				require.Zero(t, tracker.CompletedPoint().TimeTick, "a failed component cannot complete the compound message")
				deleteWriter.fail, insertWriter.fail = false, false
				require.NoError(t, scheduler.tasks[first].Execute(ctx))
				require.Zero(t, tracker.CompletedPoint().TimeTick, "the other component must still pin completion")

				// Recover only the completed component's snapshot. Replay must retain
				// the unfinished effect, while skipping the persisted effect.
				recoveryMeta := meta
				if !l0First {
					recoveryMeta = view.ConsumeDirtyAndGetSnapshot()
					require.Equal(t, through, recoveryMeta.GetCheckpointTimeTick())
				}
				recoveredView := newSegmentViewFromMeta(recoveryMeta, crashTestSchema(), config)
				recoveredL0 := l0materializer.NewWALMaterializer(l0materializer.WALConfig{VChannel: "v1", Runtime: config.runtime, Materializer: deleteWriter, MaterializeMaxBytes: 1, MaterializedTimeTick: l0.MaterializedTimeTick()})
				recoveredTracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
				beforeTasks := len(scheduler.tasks)
				observe(recoveredView, recoveredL0, recoveredTracker)
				recoveredView.RequestPersistThrough(through)
				require.Zero(t, recoveredTracker.CompletedPoint().TimeTick)
				require.Len(t, scheduler.tasks, beforeTasks+1, "only the missing persistence effect needs replay")
				require.NoError(t, scheduler.tasks[beforeTasks].Execute(ctx))
				require.Equal(t, through, recoveredTracker.CompletedPoint().TimeTick)
				require.Equal(t, 1, insertWriter.write)
				require.Equal(t, 1, insertWriter.rows)
				require.Equal(t, 1, deleteWriter.writes)
				// Drain the first process too, so no retained handles outlive the test.
				require.NoError(t, scheduler.tasks[second].Execute(ctx))
				require.Equal(t, through, tracker.CompletedPoint().TimeTick)
			})
		}
	}
}
