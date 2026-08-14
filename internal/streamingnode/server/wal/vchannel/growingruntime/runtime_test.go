package growingruntime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newTestInsertMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutable, err := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		BuildMutable()
	require.NoError(t, err)
	return mutable.WithTimeTick(timetick).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
}

func newTestAssignedInsertMessage(t *testing.T, vchannel string, segmentID int64, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutable, err := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId: 10,
					Rows:        1,
					SegmentAssignment: &messagespb.SegmentAssignment{
						SegmentId: segmentID,
					},
				},
			},
		}).
		WithBody(&msgpb.InsertRequest{}).
		BuildMutable()
	require.NoError(t, err)
	return mutable.WithTimeTick(timetick).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
}

func initSegcoreForRuntimeTest(t *testing.T) {
	t.Helper()
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	localDataRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
	initcore.InitLocalChunkManager(localDataRootPath)
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	require.NoError(t, initcore.InitTieredStorage(paramtable.Get()))
}

func newTestSegmentInsertMessage(t *testing.T, vchannel string, segmentID int64, rowCount int, timetick uint64, schema *schemapb.CollectionSchema) message.ImmutableMessage {
	t.Helper()
	collection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID:  1,
		Schema:        schema,
		LoadFieldList: nil,
	})
	require.NoError(t, err)
	defer collection.Release()

	insertMsg, err := mock_segcore.GenInsertMsg(collection, 10, segmentID, rowCount)
	require.NoError(t, err)
	insertMsg.ShardName = vchannel
	insertMsg.CollectionID = 1
	insertMsg.PartitionID = 10
	insertMsg.SegmentID = segmentID

	mutable, err := message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{
					PartitionId: 10,
					Rows:        uint64(rowCount),
					SegmentAssignment: &messagespb.SegmentAssignment{
						SegmentId: segmentID,
					},
				},
			},
		}).
		WithBody(insertMsg.InsertRequest).
		BuildMutable()
	require.NoError(t, err)
	return mutable.WithTimeTick(timetick).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
}

func newTestFlushMessage(t *testing.T, vchannel string, segmentID int64, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutable := message.NewFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.FlushMessageHeader{
			CollectionId: 1,
			PartitionId:  10,
			SegmentId:    segmentID,
		}).
		WithBody(&message.FlushMessageBody{}).
		MustBuildMutable()
	return mutable.WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick + 1)))
}

func newTestManualFlushMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutable := message.NewManualFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).
		WithBody(&message.ManualFlushMessageBody{}).
		MustBuildMutable()
	return mutable.WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick + 1)))
}

func newTestRecoveryBarrierMessage(t *testing.T, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutable := message.NewRecoveryBarrierMessageBuilderV2().
		WithHeader(&message.RecoveryBarrierMessageHeader{}).
		WithBody(&message.RecoveryBarrierMessageBody{}).
		WithAllVChannel().
		MustBuildMutable()
	return mutable.WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick + 1)))
}

func newTestTransformDeleteMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutable := message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  10,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
			Timestamps:   []uint64{timetick},
		}).
		MustBuildMutable()
	return mutable.WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick + 1)))
}

func newTestInsertDeleteTxnMessage(t *testing.T, vchannel string, segmentID int64, timetick uint64) message.ImmutableMessage {
	t.Helper()
	txnContext := message.TxnContext{TxnID: message.TxnID(timetick), Keepalive: time.Second}
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnContext).
		WithTimeTick(timetick - 2).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick - 2))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick - 2)))
	beginMessage, err := message.AsImmutableBeginTxnMessageV2(begin)
	require.NoError(t, err)

	insert := newTestAssignedInsertMessage(t, vchannel, segmentID, timetick-1)
	deleteMessage := newTestTransformDeleteMessage(t, vchannel, timetick-1)

	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnContext).
		WithTimeTick(timetick).
		WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
		IntoImmutableMessage(rmq.NewRmqID(int64(timetick + 1)))
	commitMessage, err := message.AsImmutableCommitTxnMessageV2(commit)
	require.NoError(t, err)

	txn, err := message.NewImmutableTxnMessageBuilder(beginMessage).
		Add(insert).
		Add(deleteMessage).
		Build(commitMessage)
	require.NoError(t, err)
	return txn
}

func TestDrainDeleteReplayUsesSharedTransformLogStream(t *testing.T) {
	ctx := context.Background()
	manager := transformlog.NewStreamManager("p1")
	for _, vchannel := range []string{"v1", "v2"} {
		log := transformlog.New(transformlog.Config{VChannel: vchannel})
		log.SwitchIntoMetaAndData()
		raw := newTestTransformDeleteMessage(t, vchannel, 10)
		owner := message.NewOwnedImmutableMessage(raw, nil)
		dispatch := owner.Clone()
		log.ObserveMessage(ctx, dispatch)
		dispatch.Release()
		owner.Release()
		manager.Register(vchannel, log)
	}
	stream, err := manager.AcquireStream(ctx, "p1")
	require.NoError(t, err)
	defer stream.Close()

	for _, vchannel := range []string{"v1", "v2"} {
		entries, err := drainDeleteReplay(ctx, walview.VChannelWALView{
			VChannel:                       vchannel,
			BaseTransformTimeTick:          10,
			TransformLogStream:             stream,
			DeleteReplayStartAfterTimeTick: 0,
		})
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, uint64(10), entries[0].GetTimeTick())
		select {
		case <-stream.Done():
			t.Fatal("shared transform log stream was closed by vchannel replay")
		default:
		}
	}
}

func TestRecoveryBarrierAdvancesBothRuntimeFrontiers(t *testing.T) {
	runtime := newRuntime()

	runtime.applyLiveMessage(context.Background(), newTestRecoveryBarrierMessage(t, 30))

	require.Equal(t, uint64(30), runtime.AppliedGrowingTimeTick())
	require.Equal(t, uint64(30), runtime.AppliedTransformTimeTick())
}

func TestRuntimeSkipsInsertAtOrBelowGrowingFrontier(t *testing.T) {
	runtime := newRuntime()
	runtime.markGrowingTimeTick(30)

	runtime.applyLiveMessage(context.Background(), newTestAssignedInsertMessage(t, "ch", 100, 20))

	require.Empty(t, runtime.SegmentIDs())
	require.Equal(t, uint64(30), runtime.AppliedGrowingTimeTick())
}

func TestRuntimeSkipsFlushAtOrBelowGrowingFrontier(t *testing.T) {
	runtime := newRuntime()
	runtime.addSegment(newGrowingSegment(nil, 100, 10))
	runtime.markGrowingTimeTick(30)

	runtime.applyLiveMessage(context.Background(), newTestFlushMessage(t, "ch", 100, 20))

	require.False(t, runtime.SegmentFlushed(100))
	require.Equal(t, uint64(30), runtime.AppliedGrowingTimeTick())
}

func TestRuntimeTxnGatesGrowingAndTransformEffectsIndependently(t *testing.T) {
	runtime := newRuntime()
	runtime.markGrowingTimeTick(50)
	runtime.markTransformTimeTick(20)

	runtime.applyLiveMessage(context.Background(), newTestInsertDeleteTxnMessage(t, "ch", 100, 40))

	require.Empty(t, runtime.SegmentIDs())
	require.Equal(t, uint64(50), runtime.AppliedGrowingTimeTick())
	require.Equal(t, uint64(40), runtime.AppliedTransformTimeTick())
}

func TestRuntimeDeleteAdvancesBothFrontiers(t *testing.T) {
	runtime := newRuntime()

	runtime.applyLiveMessage(context.Background(), newTestTransformDeleteMessage(t, "ch", 40))

	require.Equal(t, uint64(40), runtime.AppliedGrowingTimeTick())
	require.Equal(t, uint64(40), runtime.AppliedTransformTimeTick())
}

func TestTransformBarrierMessagesAdvanceRuntimeTransformFrontier(t *testing.T) {
	runtime := newRuntime()

	runtime.applyLiveMessage(context.Background(), newTestFlushMessage(t, "ch", 10, 40))
	require.Equal(t, uint64(40), runtime.AppliedGrowingTimeTick())
	require.Equal(t, uint64(40), runtime.AppliedTransformTimeTick())

	runtime.applyLiveMessage(context.Background(), newTestManualFlushMessage(t, "ch", 50))
	require.Equal(t, uint64(50), runtime.AppliedGrowingTimeTick())
	require.Equal(t, uint64(50), runtime.AppliedTransformTimeTick())
}

func TestRuntimeRejectsLiveInsertAfterFlush(t *testing.T) {
	initSegcoreForRuntimeTest(t)

	schema := mock_segcore.GenTestCollectionSchema("snview-resource-flush", schemapb.DataType_Int64, false)
	collection, err := newCollection(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		Schema:       schema,
	})
	require.NoError(t, err)
	runtime := newRuntime()
	runtime.collection = collection
	defer runtime.Close()

	segmentID := int64(21)
	runtime.applyLiveMessage(context.Background(), newTestFlushMessage(t, "ch", segmentID, 40))
	require.True(t, runtime.SegmentFlushed(segmentID))
	require.Panics(t, func() {
		runtime.applyLiveMessage(context.Background(), newTestSegmentInsertMessage(t, "ch", segmentID, 2, 41, schema))
	})
	_, ok := runtime.Segment(segmentID)
	require.False(t, ok)
}

func TestRuntimeFlushedSegmentSkipsReplayUntilSafeToRelease(t *testing.T) {
	initSegcoreForRuntimeTest(t)

	schema := mock_segcore.GenTestCollectionSchema("snview-flushed-replay", schemapb.DataType_Int64, false)
	runtime := newRuntime()
	err := runtime.Prepare(context.Background(), walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			CollectionID:        1,
			VChannel:            "ch",
			DataVersion:         qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1},
			BaseGrowingTimeTick: 30,
			FlushedSegments: []walview.FlushedSegment{
				{
					SegmentID:           100,
					PartitionID:         10,
					FlushTimeTick:       50,
					SealedAtDataVersion: qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1},
				},
			},
		},
	})
	require.NoError(t, err)
	defer runtime.Close()

	_, ok := runtime.Segment(100)
	require.False(t, ok)
	require.Equal(t, []int64{100}, runtime.SegmentIDs())

	require.Panics(t, func() {
		runtime.applyLiveMessage(context.Background(), newTestSegmentInsertMessage(t, "ch", 100, 2, 52, schema))
	})

	require.NotPanics(t, func() {
		runtime.applyLiveMessage(context.Background(), newTestSegmentInsertMessage(t, "ch", 100, 2, 40, schema))
	})
	_, ok = runtime.Segment(100)
	require.False(t, ok)
	require.Equal(t, []int64{100}, runtime.SegmentIDs())

	runtime.Truncate(qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1})
	require.Equal(t, []int64{100}, runtime.SegmentIDs())

	runtime.applyLiveMessage(context.Background(), newTestRecoveryBarrierMessage(t, 51))
	require.Empty(t, runtime.SegmentIDs())
}

func TestRuntimeSkipsInsertCoveredBySegmentDataCheckpoint(t *testing.T) {
	initSegcoreForRuntimeTest(t)

	schema := mock_segcore.GenTestCollectionSchema("snview-segment-replay", schemapb.DataType_Int64, false)
	runtime := newRuntime()
	err := runtime.Prepare(context.Background(), walview.VChannelWALView{
		CollectionID:        1,
		VChannel:            "ch",
		Schema:              schema,
		BaseGrowingTimeTick: 30,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			CollectionID:        1,
			VChannel:            "ch",
			BaseGrowingTimeTick: 30,
			Segments: []walview.VisibleSegment{
				{
					SegmentID:   100,
					PartitionID: 10,
					Assignment: &streamingpb.SegmentAssignmentMeta{
						CollectionId:           1,
						Vchannel:               "ch",
						DataCheckpointTimeTick: 100,
					},
				},
			},
		},
	})
	require.NoError(t, err)
	defer runtime.Close()

	require.NotPanics(t, func() {
		runtime.applyLiveMessage(context.Background(), newTestSegmentInsertMessage(t, "ch", 100, 2, 80, schema))
	})
	_, ok := runtime.Segment(100)
	require.False(t, ok)

	runtime.applyLiveMessage(context.Background(), newTestSegmentInsertMessage(t, "ch", 100, 2, 120, schema))
	segment, ok := runtime.Segment(100)
	require.True(t, ok)
	require.Equal(t, int64(2), segment.RowNum())
}

func TestNewCollectionAppliesIndexMetaFromWALView(t *testing.T) {
	initSegcoreForRuntimeTest(t)

	schema := mock_segcore.GenTestCollectionSchema("snview-resource-index-meta", schemapb.DataType_Int64, false)
	collection, err := newCollection(walview.VChannelWALView{
		CollectionID: 1,
		VChannel:     "ch",
		Schema:       schema,
		IndexInfos:   mock_segcore.GenTestIndexInfoList(1, schema),
	})
	require.NoError(t, err)
	require.NotNil(t, collection)
	defer collection.Release()
	require.NotNil(t, collection.IndexMeta())
	require.NotEmpty(t, collection.IndexMeta().GetIndexMetas())
}

func TestRuntimeTruncateWatermarkAppliesToLateSegmentSealed(t *testing.T) {
	runtime := newRuntime()
	runtime.addSegment(newGrowingSegment(nil, 10, 0))

	runtime.Truncate(qviews.DataVersion{StreamingVersion: 20, CompactVersion: 1})
	require.Equal(t, []int64{10}, runtime.SegmentIDs())

	runtime.ApplyLiveEvent(context.Background(), walview.VChannelResourceEvent{
		SegmentSealed: &walview.SegmentSealedEvent{
			SegmentID:           10,
			SealedAtDataVersion: qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1},
		},
	})
	require.False(t, runtime.SegmentFlushed(10))
	require.Empty(t, runtime.SegmentIDs())
}

func TestInsertTimestampsFromRequestUsesMessageTimeTick(t *testing.T) {
	request := &msgpb.InsertRequest{
		Timestamps: []uint64{11, 12, 13},
		NumRows:    3,
	}

	require.Equal(t, []uint64{100, 100, 100}, insertTimestampsFromRequest(100, request))
}

func TestDeleteTimestampsFromRequestUsesMessageTimeTick(t *testing.T) {
	request := &msgpb.DeleteRequest{
		PrimaryKeys: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		},
		Timestamps: []uint64{11, 12, 13},
	}

	require.Equal(t, []uint64{100, 100, 100}, deleteTimestampsFromRequest(100, request))
}

func TestDeleteTimestampsFromRequestFallsBackToMessageTimeTick(t *testing.T) {
	request := &msgpb.DeleteRequest{
		PrimaryKeys: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2}},
			},
		},
	}

	require.Equal(t, []uint64{100, 100}, deleteTimestampsFromRequest(100, request))
}

func TestDeleteTimestampsFromTransformLogBlockUsesEntryTimeTick(t *testing.T) {
	block := &streamingpb.TransformDeleteBlock{
		PrimaryKeys: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		},
	}

	require.Equal(t, []uint64{200, 200, 200}, deleteTimestampsFromTransformLogBlock(200, block))
}
