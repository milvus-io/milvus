// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package shard

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

const (
	splitTestCollectionID int64 = 1
	splitTestPartitionID  int64 = 2
	splitTestSegmentID    int64 = 1001
	splitTestTaskID       int64 = 77
	splitTestSource             = "p0_1v0"
	splitTestTarget1            = "p1_1v1"
	splitTestTarget2            = "p2_1v2"
)

func splitTestSchema(version int32) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:    "c",
		Version: version,
		Fields:  []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
	}
}

// newSplitTestSourceManager recovers a shard manager that holds the source
// vchannel with one growing segment.
func newSplitTestSourceManager(t *testing.T, splitTaskID int64) shards.ShardManager {
	t.Helper()
	initSplitTest(t)
	manager := shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
		ChannelInfo: types.PChannelInfo{Name: "p0", Term: 1},
		WAL:         syncutil.NewFuture[wal.WAL](),
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			Checkpoint: &recovery.WALCheckpoint{TimeTick: 100},
			WritePathRecovery: &moduleapi.WritePathRecoveryModuleSnapshot{
				VChannels: map[string]moduleapi.VChannelWritePathRecoveryState{
					splitTestSource: {
						VChannel:       splitTestSource,
						CollectionID:   splitTestCollectionID,
						PartitionIDs:   []int64{splitTestPartitionID},
						Schema:         splitTestSchema(1),
						SchemaTimeTick: 1,
						SplitTaskID:    splitTaskID,
					},
				},
				GrowingSegments: map[int64]moduleapi.SegmentWritePathRecoveryState{
					splitTestSegmentID: {
						VChannel:     splitTestSource,
						CollectionID: splitTestCollectionID,
						PartitionID:  splitTestPartitionID,
						SegmentID:    splitTestSegmentID,
						Stat: &streamingpb.SegmentAssignmentStat{
							MaxRows:               1000000,
							MaxBinarySize:         1 << 30,
							CreateSegmentTimeTick: 50,
						},
					},
				},
			},
		},
	})
	t.Cleanup(manager.Close)
	return manager
}

// initSplitTest initializes the process resources once for a test. A second
// initialization would drop the seal operators a manager registered.
func initSplitTest(t *testing.T) {
	t.Helper()
	if splitTestInitialized[t] {
		return
	}
	splitTestInitialized[t] = true
	t.Cleanup(func() { delete(splitTestInitialized, t) })
	paramtable.Init()
	resource.InitForTest(t)
}

var splitTestInitialized = map[*testing.T]bool{}

func newSplitTestInterceptor(manager shards.ShardManager) *shardInterceptor {
	interceptor := &shardInterceptor{shardManager: manager}
	interceptor.initOpTable()
	return interceptor
}

func newSplitShardCopy(vchannel string, taskID int64, timetick uint64, mutate ...func(*message.SplitShardMessageBody)) message.MutableMessage {
	body := &message.SplitShardMessageBody{
		GenesisHeader: &message.CreateCollectionMessageHeader{CollectionId: splitTestCollectionID, PartitionIds: []int64{splitTestPartitionID}},
		Genesis:       &message.CreateCollectionRequest{CollectionID: splitTestCollectionID, CollectionSchema: splitTestSchema(1)},
		Routing:       &message.SplitShardRouting{RoutingModulus: 2},
	}
	for _, m := range mutate {
		m(body)
	}
	return message.NewSplitShardMessageBuilderV2().WithVChannel(vchannel).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    splitTestCollectionID,
			SplitTaskId:     taskID,
			SourceVchannel:  splitTestSource,
			TargetVchannels: []string{splitTestTarget1, splitTestTarget2},
		}).
		WithBody(body).MustBuildMutable().WithTimeTick(timetick).WithLastConfirmedUseMessageID()
}

func newSplitTestInsert(vchannel string, timetick uint64) message.MutableMessage {
	return message.NewInsertMessageBuilderV1().WithVChannel(vchannel).
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: splitTestCollectionID,
			Partitions:   []*messagespb.PartitionSegmentAssignment{{PartitionId: splitTestPartitionID, Rows: 1, BinarySize: 100}},
		}).
		WithBody(&msgpb.InsertRequest{}).MustBuildMutable().WithTimeTick(timetick)
}

func newSplitTestDelete(vchannel string, timetick uint64) message.MutableMessage {
	return message.NewDeleteMessageBuilderV1().WithVChannel(vchannel).
		WithHeader(&messagespb.DeleteMessageHeader{CollectionId: splitTestCollectionID, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{}).MustBuildMutable().WithTimeTick(timetick)
}

// appendRecorder is the WAL below the interceptor. It records what reached it.
type appendRecorder struct {
	appended []message.MutableMessage
	err      error
}

func (r *appendRecorder) append(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
	if r.err != nil {
		return nil, r.err
	}
	r.appended = append(r.appended, msg)
	return walimplstest.NewTestMessageID(int64(msg.TimeTick())), nil
}

func requireShardFenced(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	streamingErr := status.AsStreamingError(err)
	require.True(t, streamingErr.IsShardFenced(), err.Error())
	// The refusal must stay distinguishable from a terminal error: the caller
	// retries the rows on the new shards.
	require.False(t, streamingErr.IsUnrecoverable())
}

func TestSplitShardFenceRefusesDMLAndAdmitsLocalMessages(t *testing.T) {
	manager := newSplitTestSourceManager(t, 0)
	interceptor := newSplitTestInterceptor(manager)
	wal := &appendRecorder{}
	ctx := context.Background()

	// Before the fence the vchannel takes DML.
	_, err := interceptor.DoAppend(ctx, newSplitTestInsert(splitTestSource, 150), wal.append)
	require.NoError(t, err)
	_, err = interceptor.DoAppend(ctx, newSplitTestDelete(splitTestSource, 160), wal.append)
	require.NoError(t, err)
	require.Len(t, wal.appended, 2)

	// The fence seals the growing segment (shards.TestFenceForSplit) and is
	// appended as it was built: segment ids are local to a cluster and are not
	// written into the message.
	fenceCopy := newSplitShardCopy(splitTestSource, splitTestTaskID, 200)
	builtHeader := message.MustAsMutableSplitShardMessageV2(fenceCopy).Header()
	_, err = interceptor.DoAppend(ctx, fenceCopy, wal.append)
	require.NoError(t, err)
	require.Len(t, wal.appended, 3)
	fence := message.MustAsMutableSplitShardMessageV2(wal.appended[2])
	require.True(t, proto.Equal(builtHeader, fence.Header()))

	// DML after the fence is refused with the retriable fence error, and never
	// reaches the WAL.
	_, err = interceptor.DoAppend(ctx, newSplitTestInsert(splitTestSource, 210), wal.append)
	requireShardFenced(t, err)
	_, err = interceptor.DoAppend(ctx, newSplitTestDelete(splitTestSource, 211), wal.append)
	requireShardFenced(t, err)
	require.Len(t, wal.appended, 3)

	// The vchannel is not a tombstone: its registration is kept.
	require.NoError(t, manager.CheckIfCollectionExists(splitTestCollectionID))
	holder, ok := manager.CollectionVChannel(splitTestCollectionID)
	require.True(t, ok)
	require.Equal(t, splitTestSource, holder)

	// A message that is local to the cluster is still admitted: an
	// unreplicable flush ...
	flush := message.NewManualFlushMessageBuilderV2().WithUnreplicable().
		WithHeader(&message.ManualFlushMessageHeader{CollectionId: splitTestCollectionID}).
		WithBody(&message.ManualFlushMessageBody{}).
		WithBroadcast([]string{splitTestSource}, message.OptBuildBroadcastAckSyncUp()).MustBuildBroadcast().
		WithBroadcastID(1).SplitIntoMutableMessage()[0].WithTimeTick(220)
	_, err = interceptor.DoAppend(utility.WithExtraAppendResult(ctx, &utility.ExtraAppendResult{}), flush, wal.append)
	require.NoError(t, err)
	require.Len(t, wal.appended, 4)
	require.True(t, wal.appended[3].IsUnreplicable())
	// The fence left nothing to flush.
	require.Empty(t, message.MustAsMutableManualFlushMessageV2(wal.appended[3]).Header().GetSegmentIds())

	// ... and an unreplicable schema change, which the write path applies.
	alter := message.NewAlterCollectionMessageBuilderV2().WithVChannel(splitTestSource).WithUnreplicable().
		WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: splitTestCollectionID,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
		}).
		WithBody(&message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{Schema: splitTestSchema(2)}}).
		MustBuildMutable().WithTimeTick(230)
	_, err = interceptor.DoAppend(ctx, alter, wal.append)
	require.NoError(t, err)
	require.Len(t, wal.appended, 5)
	require.Equal(t, int32(2), manager.GetCollectionSchemaSnapshot(splitTestCollectionID).GetVersion())
	require.Empty(t, message.MustAsMutableAlterCollectionMessageV2(wal.appended[4]).Header().GetFlushedSegmentIds())

	// The deprecated SchemaChange message is admitted as well.
	schemaChange := message.NewSchemaChangeMessageBuilderV2().WithVChannel(splitTestSource).WithUnreplicable().
		WithHeader(&message.SchemaChangeMessageHeader{CollectionId: splitTestCollectionID}).
		WithBody(&message.SchemaChangeMessageBody{Schema: splitTestSchema(3)}).
		MustBuildMutable().WithTimeTick(235)
	_, err = interceptor.DoAppend(ctx, schemaChange, wal.append)
	require.NoError(t, err)
	require.Len(t, wal.appended, 6)

	// A new segment is refused: no insert can ever reach it.
	createSegment := message.NewCreateSegmentMessageBuilderV2().WithVChannel(splitTestSource).
		WithHeader(&message.CreateSegmentMessageHeader{CollectionId: splitTestCollectionID, PartitionId: splitTestPartitionID, SegmentId: 2002}).
		WithBody(&message.CreateSegmentMessageBody{}).MustBuildMutable().WithTimeTick(240)
	_, err = interceptor.DoAppend(ctx, createSegment, wal.append)
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
	require.Len(t, wal.appended, 6)

	// DML is still refused after all of that.
	_, err = interceptor.DoAppend(ctx, newSplitTestInsert(splitTestSource, 250), wal.append)
	requireShardFenced(t, err)
}

func TestSplitShardFenceIsIdempotentAndKeptWhenAppendFails(t *testing.T) {
	manager := newSplitTestSourceManager(t, 0)
	interceptor := newSplitTestInterceptor(manager)
	ctx := context.Background()

	// The append reports an error. The message may be persisted all the same,
	// so the fence must hold.
	failing := &appendRecorder{err: context.DeadlineExceeded}
	_, err := interceptor.DoAppend(ctx, newSplitShardCopy(splitTestSource, splitTestTaskID, 200), failing.append)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	wal := &appendRecorder{}
	_, err = interceptor.DoAppend(ctx, newSplitTestInsert(splitTestSource, 210), wal.append)
	requireShardFenced(t, err)

	// The broadcaster appends the copy again. It is admitted and changes
	// nothing: there is nothing left to seal.
	_, err = interceptor.DoAppend(ctx, newSplitShardCopy(splitTestSource, splitTestTaskID, 220), wal.append)
	require.NoError(t, err)
	require.Len(t, wal.appended, 1)
	_, err = interceptor.DoAppend(ctx, newSplitTestInsert(splitTestSource, 230), wal.append)
	requireShardFenced(t, err)

	// A fence by another split is refused, and not appended.
	_, err = interceptor.DoAppend(ctx, newSplitShardCopy(splitTestSource, splitTestTaskID+1, 240), wal.append)
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
	require.Len(t, wal.appended, 1)
}

func TestSplitShardFenceSurvivesRecovery(t *testing.T) {
	// The recovery state says the vchannel was split. The recovered write path
	// refuses DML and admits a local flush, with no SplitShard message replayed
	// through the interceptor.
	manager := newSplitTestSourceManager(t, splitTestTaskID)
	interceptor := newSplitTestInterceptor(manager)
	wal := &appendRecorder{}
	ctx := context.Background()

	_, err := interceptor.DoAppend(ctx, newSplitTestInsert(splitTestSource, 210), wal.append)
	requireShardFenced(t, err)
	_, err = interceptor.DoAppend(ctx, newSplitTestDelete(splitTestSource, 211), wal.append)
	requireShardFenced(t, err)

	flush := message.NewManualFlushMessageBuilderV2().WithUnreplicable().WithVChannel(splitTestSource).
		WithHeader(&message.ManualFlushMessageHeader{CollectionId: splitTestCollectionID}).
		WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable().WithTimeTick(220)
	_, err = interceptor.DoAppend(utility.WithExtraAppendResult(ctx, &utility.ExtraAppendResult{}), flush, wal.append)
	require.NoError(t, err)
	require.Len(t, wal.appended, 1)

	// The copy of the same split is still admitted after recovery.
	_, err = interceptor.DoAppend(ctx, newSplitShardCopy(splitTestSource, splitTestTaskID, 230), wal.append)
	require.NoError(t, err)
	require.Len(t, wal.appended, 2)
}

func TestSplitShardFenceRecoveredFromTheObservedMessage(t *testing.T) {
	// The recovery side observes the fence, and the write path is recovered
	// from what it recorded.
	paramtable.Init()
	resource.InitForTest(t)
	view := vchannel.NewVChannelViewFromMeta(&streamingpb.VChannelMeta{
		Vchannel: splitTestSource, State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, CheckpointTimeTick: 1,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: splitTestCollectionID,
			Partitions:   []*streamingpb.PartitionInfoOfVChannel{{PartitionId: splitTestPartitionID, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL}},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{{
				Schema: splitTestSchema(1), State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL, CheckpointTimeTick: 1,
			}},
		},
	})
	fence := newSplitShardCopy(splitTestSource, splitTestTaskID, 200).IntoImmutableMessage(walimplstest.NewTestMessageID(200))
	require.True(t, view.ObserveSplitShardFenceMessageV2(message.MustAsImmutableSplitShardMessageV2(fence)))
	state, ok := view.WritePathRecoveryState()
	require.True(t, ok)
	require.Equal(t, splitTestTaskID, state.SplitTaskID)

	manager := shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
		ChannelInfo: types.PChannelInfo{Name: "p0", Term: 1},
		WAL:         syncutil.NewFuture[wal.WAL](),
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			Checkpoint: &recovery.WALCheckpoint{TimeTick: 200},
			WritePathRecovery: &moduleapi.WritePathRecoveryModuleSnapshot{
				VChannels: map[string]moduleapi.VChannelWritePathRecoveryState{splitTestSource: state},
			},
		},
	})
	defer manager.Close()
	interceptor := newSplitTestInterceptor(manager)
	wal := &appendRecorder{}
	_, err := interceptor.DoAppend(context.Background(), newSplitTestInsert(splitTestSource, 210), wal.append)
	requireShardFenced(t, err)
	require.Empty(t, wal.appended)
}

func TestSplitShardRefusesMalformedCopies(t *testing.T) {
	manager := newSplitTestSourceManager(t, 0)
	interceptor := newSplitTestInterceptor(manager)
	wal := &appendRecorder{}
	ctx := context.Background()

	requireRefused := func(msg message.MutableMessage) {
		t.Helper()
		_, err := interceptor.DoAppend(ctx, msg, wal.append)
		require.Error(t, err)
		require.True(t, status.AsStreamingError(err).IsUnrecoverable(), err.Error())
		require.Empty(t, wal.appended)
	}
	// No split task id.
	requireRefused(newSplitShardCopy(splitTestSource, 0, 200))
	// A vchannel the message does not name.
	requireRefused(newSplitShardCopy("p0_1v9", splitTestTaskID, 200))
	// A target without a creation payload, or with one of another collection.
	requireRefused(newSplitShardCopy(splitTestTarget1, splitTestTaskID, 200, func(b *message.SplitShardMessageBody) { b.GenesisHeader = nil }))
	requireRefused(newSplitShardCopy(splitTestTarget1, splitTestTaskID, 200, func(b *message.SplitShardMessageBody) { b.Genesis.CollectionSchema = nil }))
	requireRefused(newSplitShardCopy(splitTestTarget1, splitTestTaskID, 200, func(b *message.SplitShardMessageBody) { b.GenesisHeader.CollectionId = 9 }))
	// A target on a pchannel that already holds a vchannel of the collection:
	// this manager holds the source.
	requireRefused(newSplitShardCopy(splitTestTarget1, splitTestTaskID, 200))

	// None of the refusals fenced the source.
	_, err := interceptor.DoAppend(ctx, newSplitTestInsert(splitTestSource, 210), wal.append)
	require.NoError(t, err)

	// A source this pchannel does not hold.
	empty := newSplitTestTargetManager(t)
	_, err = newSplitTestInterceptor(empty).DoAppend(ctx, newSplitShardCopy(splitTestSource, splitTestTaskID, 200), (&appendRecorder{}).append)
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

// newSplitTestTargetManager recovers a shard manager of a pchannel that holds
// no vchannel yet.
func newSplitTestTargetManager(t *testing.T) shards.ShardManager {
	t.Helper()
	initSplitTest(t)
	manager := shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
		ChannelInfo: types.PChannelInfo{Name: "p1", Term: 1},
		WAL:         syncutil.NewFuture[wal.WAL](),
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			Checkpoint:        &recovery.WALCheckpoint{TimeTick: 100},
			WritePathRecovery: &moduleapi.WritePathRecoveryModuleSnapshot{},
		},
	})
	t.Cleanup(manager.Close)
	return manager
}

func TestSplitShardGenesisCreatesTheTargetVChannel(t *testing.T) {
	manager := newSplitTestTargetManager(t)
	interceptor := newSplitTestInterceptor(manager)
	ctx := context.Background()

	// A genesis whose append fails creates nothing.
	failing := &appendRecorder{err: errors.New("append failed")}
	_, err := interceptor.DoAppend(ctx, newSplitShardCopy(splitTestTarget1, splitTestTaskID, 300), failing.append)
	require.Error(t, err)
	require.ErrorIs(t, manager.CheckIfCollectionExists(splitTestCollectionID), shards.ErrCollectionNotFound)
	conflicting := &appendRecorder{}
	_, err = interceptor.DoAppend(ctx, newSplitShardCopy(splitTestTarget1, splitTestTaskID+1, 305), conflicting.append)
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
	require.Empty(t, conflicting.appended)

	wal := &appendRecorder{}
	_, err = interceptor.DoAppend(ctx, newSplitShardCopy(splitTestTarget1, splitTestTaskID, 310), wal.append)
	require.NoError(t, err)
	require.Len(t, wal.appended, 1)

	// The collection is registered on this pchannel with the target vchannel,
	// its partitions and its schema, exactly as after a CreateCollection.
	holder, ok := manager.CollectionVChannel(splitTestCollectionID)
	require.True(t, ok)
	require.Equal(t, splitTestTarget1, holder)
	require.NoError(t, manager.CheckIfPartitionExists(shards.PartitionUniqueKey{CollectionID: splitTestCollectionID, PartitionID: splitTestPartitionID}))
	require.Equal(t, "c", manager.GetCollectionSchemaSnapshot(splitTestCollectionID).GetName())

	// The local read generation has published and opened target admission.
	interceptor.splitGates[splitTestTarget1].opened.Store(true)
	// The target takes DML. A delete is appended.
	_, err = interceptor.DoAppend(ctx, newSplitTestDelete(splitTestTarget1, 320), wal.append)
	require.NoError(t, err)
	require.Len(t, wal.appended, 2)
	// The first insert is admitted by the fence check and is told to wait for
	// a growing segment, as on any vchannel that has none yet. The redo
	// interceptor above this one handles that; the insert is not refused.
	ready, err := manager.WaitUntilGrowingSegmentReady(shards.PartitionUniqueKey{CollectionID: splitTestCollectionID, PartitionID: splitTestPartitionID})
	require.NoError(t, err)
	<-ready
	correct, err := manager.CheckIfCollectionSchemaVersionMatch(message.MustAsMutableInsertMessageV1(newSplitTestInsert(splitTestTarget1, 330)).Header())
	require.NoError(t, err)
	require.Equal(t, int32(1), correct)

	// The same genesis appended again is admitted and changes nothing.
	_, err = interceptor.DoAppend(ctx, newSplitShardCopy(splitTestTarget1, splitTestTaskID, 340), wal.append)
	require.NoError(t, err)
	require.Len(t, wal.appended, 3)
	holder, _ = manager.CollectionVChannel(splitTestCollectionID)
	require.Equal(t, splitTestTarget1, holder)

	// A different task or parent is a conflict even with the same target name.
	otherSource := newSplitShardCopy(splitTestTarget1, splitTestTaskID, 346)
	changed := message.MustAsMutableSplitShardMessageV2(otherSource)
	header := changed.Header()
	header.SourceVchannel = "other-source"
	changed.OverwriteHeader(header)
	for _, conflict := range []message.MutableMessage{newSplitShardCopy(splitTestTarget1, splitTestTaskID+1, 345), otherSource} {
		_, err := interceptor.DoAppend(ctx, conflict, wal.append)
		require.True(t, status.AsStreamingError(err).IsUnrecoverable())
	}
	require.Len(t, wal.appended, 3)

	// The second target cannot be created on the same pchannel.
	_, err = interceptor.DoAppend(ctx, newSplitShardCopy(splitTestTarget2, splitTestTaskID, 350), wal.append)
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
	require.Len(t, wal.appended, 3)
}

func TestSplitShardCopyOnControlChannelHasNoEffect(t *testing.T) {
	manager := newSplitTestSourceManager(t, 0)
	interceptor := newSplitTestInterceptor(manager)
	wal := &appendRecorder{}
	_, err := interceptor.DoAppend(context.Background(), newSplitShardCopy("p0_vcchan", splitTestTaskID, 200), wal.append)
	require.NoError(t, err)
	require.Len(t, wal.appended, 1)
	// The source on the same pchannel is not fenced by the control copy.
	_, err = interceptor.DoAppend(context.Background(), newSplitTestInsert(splitTestSource, 210), wal.append)
	require.NoError(t, err)
}

// A Begin on a fenced vchannel is refused with the fence error and the session
// the time tick interceptor opened for it is ended at once; on a vchannel that
// is not fenced it is appended and its session lives.
func TestSplitShardFenceRefusesBeginTxn(t *testing.T) {
	ctx := context.Background()
	newBegin := func(timetick uint64) message.MutableMessage {
		return message.NewBeginTxnMessageBuilderV2().WithVChannel(splitTestSource).
			WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 60_000}).
			WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTimeTick(timetick)
	}
	open := func(t *testing.T, txnManager *txn.TxnManager, msg message.MutableMessage) message.TxnID {
		session, err := txnManager.BeginNewTxn(ctx, message.MustAsMutableBeginTxnMessageV2(msg))
		require.NoError(t, err)
		return session.TxnContext().TxnID
	}

	t.Run("fenced", func(t *testing.T) {
		manager := newSplitTestSourceManager(t, splitTestTaskID)
		txnManager := txn.NewTxnManager(types.PChannelInfo{Name: "p0"}, nil)
		interceptor := newSplitTestInterceptor(manager)
		interceptor.txnManager = txnManager
		wal := &appendRecorder{}

		require.True(t, shards.IsSplitFenced(manager, splitTestSource))
		msg := newBegin(300)
		id := open(t, txnManager, msg)
		_, err := interceptor.DoAppend(ctx, msg, wal.append)
		requireShardFenced(t, err)
		require.Empty(t, wal.appended)
		_, err = txnManager.GetSessionOfTxn(id)
		require.True(t, status.AsStreamingError(err).IsTxnExpired())

		// Without a transaction manager the Begin is still refused.
		interceptor.txnManager = nil
		_, err = interceptor.DoAppend(ctx, newBegin(301), wal.append)
		requireShardFenced(t, err)
		require.Empty(t, wal.appended)
	})

	t.Run("not fenced", func(t *testing.T) {
		manager := newSplitTestSourceManager(t, 0)
		txnManager := txn.NewTxnManager(types.PChannelInfo{Name: "p0"}, nil)
		interceptor := newSplitTestInterceptor(manager)
		interceptor.txnManager = txnManager
		wal := &appendRecorder{}

		require.False(t, shards.IsSplitFenced(manager, splitTestSource))
		require.False(t, shards.IsSplitFenced(manager, "another-vchannel"))
		msg := newBegin(300)
		id := open(t, txnManager, msg)
		_, err := interceptor.DoAppend(ctx, msg, wal.append)
		require.NoError(t, err)
		require.Len(t, wal.appended, 1)
		_, err = txnManager.GetSessionOfTxn(id)
		require.NoError(t, err)
	})

	t.Run("a shard manager that cannot tell", func(t *testing.T) {
		require.False(t, shards.IsSplitFenced(nil, splitTestSource))
	})
}

func TestSplitShardGenesisRestoresOriginalTaskAndSource(t *testing.T) {
	ctx := context.Background()
	manager := newSplitTestTargetManager(t)
	rec := &appendRecorder{}
	_, err := newSplitTestInterceptor(manager).DoAppend(ctx, newSplitShardCopy(splitTestTarget1, splitTestTaskID, 300), rec.append)
	require.NoError(t, err)
	recoverTarget := func(task int64, source string) interceptors.Interceptor {
		return NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
			ShardManager: manager,
			InitialRecoverSnapshot: &recovery.RecoverySnapshot{WritePathRecovery: &moduleapi.WritePathRecoveryModuleSnapshot{
				VChannels: map[string]moduleapi.VChannelWritePathRecoveryState{splitTestTarget1: {
					CollectionID: splitTestCollectionID, SplitGenesisTaskID: task, SplitSourceVChannel: source,
				}},
			}},
		})
	}
	restored := recoverTarget(splitTestTaskID, splitTestSource)
	_, err = restored.DoAppend(ctx, newSplitShardCopy(splitTestTarget1, splitTestTaskID, 310), rec.append)
	require.NoError(t, err)
	for _, node := range []interceptors.Interceptor{recoverTarget(splitTestTaskID+1, splitTestSource), recoverTarget(splitTestTaskID, "other-source"), recoverTarget(0, "")} {
		_, err = node.DoAppend(ctx, newSplitShardCopy(splitTestTarget1, splitTestTaskID, 320), rec.append)
		require.True(t, status.AsStreamingError(err).IsUnrecoverable())
	}
	require.Len(t, rec.appended, 2)
}
