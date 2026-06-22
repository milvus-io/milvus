package growingruntime

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
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

func TestDeleteTimestampsFromRequestUsesPerRowTimestamps(t *testing.T) {
	request := &msgpb.DeleteRequest{
		PrimaryKeys: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}},
			},
		},
		Timestamps: []uint64{11, 12, 13},
	}

	require.Equal(t, []uint64{11, 12, 13}, deleteTimestampsFromRequest(100, request))
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
