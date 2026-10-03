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

package recovery

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	istorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	sealTestSource  = "test-pchannel_1v0"
	sealTestTarget0 = "test-pchannel_1v1"
	sealTestTarget1 = "test-pchannel_1v2"
)

// newSealTestKeyedInsert is an insert appended with a client idempotency key,
// which is what stages a WAL summary record. A plain unkeyed insert stages
// nothing (see walsummary.TestLastAckedIsHeldByAStagedRecordWhateverStagedIt),
// which is exactly why the e2e's "clean" shape never hit this.
func newSealTestKeyedInsert(vchannel string, timetick uint64) message.ImmutableMessage {
	header := &message.InsertMessageHeader{
		CollectionId: 1,
		Partitions:   []*message.PartitionSegmentAssignment{{PartitionId: 10, Rows: 1}},
	}
	message.SetInsertHeaderIdempotentInsertResult(header, &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
	})
	return message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(header).
		WithBody(&msgpb.InsertRequest{
			Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert}, CollectionID: 1,
			PartitionID: 10, NumRows: 1,
		}).
		WithIdempotencyKey(message.IdempotencyKey("seal-test-key")).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick) + 1))
}

func newSealTestSplitShardSource(timetick uint64) message.ImmutableMessage {
	return message.NewSplitShardMessageBuilderV2().
		WithVChannel(sealTestSource).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    1,
			SplitTaskId:     7,
			SourceVchannel:  sealTestSource,
			TargetVchannels: []string{sealTestTarget0, sealTestTarget1},
			PartitionIds:    []int64{10},
		}).
		WithBody(&message.SplitShardMessageBody{
			Genesis: &message.CreateCollectionRequest{
				CollectionSchema: &schemapb.CollectionSchema{Name: "collection"},
			},
		}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick) + 1))
}

// newSealTestStorage wires a recovery storage with a REAL summary manager over
// a local store, a real vchannel manager holding the split source, and a
// FlushMaxBytes high water mark nothing in the test can reach -- the production
// shape, where the only seal that can happen is one somebody asks for.
func newSealTestStorage(t *testing.T) (*recoveryStorageImpl, *walsummary.Manager, *[]nodescheduler.Task) {
	t.Helper()
	initial := &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 10}
	storage := newTestRecoveryStorage(t, initial)
	t.Cleanup(storage.metrics.Close)

	tasks := &[]nodescheduler.Task{}
	submit := mockey.Mock(mockey.GetMethod(storage.taskScheduler, "Submit")).To(
		func(task nodescheduler.Task) nodescheduler.TaskHandle {
			*tasks = append(*tasks, task)
			return nil
		}).Build()
	t.Cleanup(func() { submit.UnPatch() })

	cm := istorage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	summary := walsummary.NewManager(walsummary.ManagerConfig{
		Runtime:       moduleapi.Runtime{Scheduler: storage.taskScheduler},
		PChannel:      storage.channel.Name,
		Term:          1,
		Store:         walsummary.NewStore(cm, storage.channel.Name, 1),
		FlushMaxBytes: 1 << 30,
	})
	summary.InitLastAcked(initial.TimeTick)

	manager, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{
		PChannel: storage.channel.Name,
		VChannelMetas: map[string]*streamingpb.VChannelMeta{
			sealTestSource: {
				Vchannel: sealTestSource, State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
				CheckpointTimeTick: 10, TransformMaterializedTimeTick: 10,
				CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
					CollectionId: 1,
					Partitions: []*streamingpb.PartitionInfoOfVChannel{
						{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
					},
					Schemas: []*streamingpb.CollectionSchemaOfVChannel{{
						Schema:             &schemapb.CollectionSchema{Name: "collection"},
						State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
						CheckpointTimeTick: 1,
					}},
				},
			},
		},
		SummaryManager: summary,
		Runtime:        moduleapi.Runtime{Scheduler: storage.taskScheduler, Notifier: storage},
	})
	require.NoError(t, err)
	t.Cleanup(manager.Close)
	storage.vchannelManager = manager
	storage.summaryManager = summary
	return storage, summary, tasks
}

// drainSealTestTasks runs every scheduled task to completion, re-running the
// growing set until nothing makes progress: a chunk write schedules the
// manifest publish that actually releases LastAcked.
func drainSealTestTasks(t *testing.T, ctx context.Context, tasks *[]nodescheduler.Task) {
	t.Helper()
	for round := 0; round < 16; round++ {
		progress := false
		for i := 0; i < len(*tasks); i++ {
			task := (*tasks)[i]
			if done, ok := task.(interface{ Done() bool }); ok && done.Done() {
				continue
			}
			if err := task.Execute(ctx); err == nil {
				progress = true
			}
		}
		if !progress {
			return
		}
	}
}

// TestFencedSourceReleasesThePublishedCheckpointPastTSwitch is the regression
// test for e2e finding F-r5-1.
//
// A shard split's source takes no message after its fence, so nothing of its
// own can ever seal the WAL summary span its pre-fence traffic staged. The
// published checkpoint is capped at summaryManager.LastAcked()
// (consumeDirtySnapshot -> ackTracker.CheckpointThrough), and that is the only
// checkpoint DataCoord ever sees -- L3's drain gate compares exactly it to
// T_switch. Without the fence asking for a seal the gate never clears and the
// split stays in the fence state forever, which is what the e2e measured twice
// (persisted frontier frozen 13s before T_switch, in-memory frontier minutes
// past it, publish_lag_bytes constant).
func TestFencedSourceReleasesThePublishedCheckpointPastTSwitch(t *testing.T) {
	ctx := context.Background()
	storage, summary, tasks := newSealTestStorage(t)

	// Pre-fence traffic that stages a summary record on the source.
	storage.observeMessage(ctx, newSealTestKeyedInsert(sealTestSource, 20))
	require.Less(t, summary.LastAcked(), uint64(20), "the staged record pins the summary frontier")

	// The fence. T_switch is its own tick.
	const tSwitch = uint64(30)
	storage.observeMessage(ctx, newSealTestSplitShardSource(tSwitch))
	// The pchannel keeps ticking; the fenced source never takes another message.
	storage.observeMessage(ctx, newAckTestTimeTickMessage(t, 40, 4))
	drainSealTestTasks(t, ctx, tasks)

	require.GreaterOrEqual(t, summary.LastAcked(), tSwitch,
		"the fence must ask the WAL summary to seal, or nothing ever will")

	batch := storage.consumeDirtySnapshot()
	require.NotNil(t, batch)
	require.GreaterOrEqual(t, batch.Checkpoint.TimeTick, tSwitch,
		"the published checkpoint must pass T_switch: it is what L3's drain gate waits on")
}

// newSealTestRoutingCommit is the adoption's AlterCollection whose post-image
// no longer names the source: the commit that retires it.
func newSealTestRoutingCommit(vchannel string, timetick uint64, vchannels []string) message.ImmutableMessage {
	return message.NewAlterCollectionMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: 1,
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{message.FieldMaskCollectionShardSplitRouting},
			},
		}).
		WithBody(&message.AlterCollectionMessageBody{
			Updates: &message.AlterCollectionMessageUpdates{
				VirtualChannelNames: vchannels,
				SplitTaskId:         7,
			},
		}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick) + 1))
}

// TestRetiringCommitReleasesThePublishedCheckpointPastTheDrop: the retire has
// the same hole as the fence and needs the same seal.
//
// After the routing commit that delists it, the source takes no message of any
// kind. Its catalog row is collected only once the summary confirms the drop
// tick -- ConsumeCleanupSnapshots passes CanCleanupVChannel as
// CleanupContext.SummaryRetired, and that reads LastAcked. Without a seal the
// row never goes, the module is never removed, and so the GC frontier that
// module removal would hand the summary (DroppedVChannelTimeTick) is never
// handed over either: the source's transform history is retained for good.
func TestRetiringCommitReleasesThePublishedCheckpointPastTheDrop(t *testing.T) {
	ctx := context.Background()
	storage, summary, tasks := newSealTestStorage(t)

	storage.observeMessage(ctx, newSealTestKeyedInsert(sealTestSource, 20))
	storage.observeMessage(ctx, newSealTestSplitShardSource(30))
	drainSealTestTasks(t, ctx, tasks)
	require.GreaterOrEqual(t, summary.LastAcked(), uint64(30))

	// Traffic of ANOTHER vchannel of the same pchannel stages a record the
	// retire must not be allowed to wait on either.
	storage.observeMessage(ctx, newSealTestKeyedInsert(sealTestTarget0, 40))
	require.Less(t, summary.LastAcked(), uint64(50), "the new record pins the frontier again")

	const retireTick = uint64(50)
	storage.observeMessage(ctx, newSealTestRoutingCommit(sealTestSource, retireTick,
		[]string{sealTestTarget0, sealTestTarget1}))
	drainSealTestTasks(t, ctx, tasks)

	require.GreaterOrEqual(t, summary.LastAcked(), retireTick,
		"the retiring commit must ask the WAL summary to seal, or the source's row never collects")
	require.True(t, summary.CanCleanupVChannel(sealTestSource, retireTick),
		"the summary must confirm the drop tick, which is what gates the catalog cleanup")
}

// TestNonRetiringRoutingCommitAsksForNoSeal: the same message type commits
// routing on every shard of the collection, and the seal request belongs only
// to the one it retires. A commit that keeps the vchannel is ordinary DDL.
func TestNonRetiringRoutingCommitAsksForNoSeal(t *testing.T) {
	keeps := newSealTestRoutingCommit(sealTestSource, 50,
		[]string{sealTestSource, sealTestTarget0, sealTestTarget1})
	require.False(t, vchannel.RequiresSummarySeal(keeps))
	retires := newSealTestRoutingCommit(sealTestSource, 50,
		[]string{sealTestTarget0, sealTestTarget1})
	require.True(t, vchannel.RequiresSummarySeal(retires))

	// The SOURCE replica of a split does ask for one; a plain insert does not.
	require.True(t, vchannel.RequiresSummarySeal(newSealTestSplitShardSource(30)))
	require.False(t, vchannel.RequiresSummarySeal(newSealTestKeyedInsert(sealTestSource, 20)))
}
