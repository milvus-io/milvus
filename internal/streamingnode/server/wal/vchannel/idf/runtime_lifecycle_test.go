package idf

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func bm25Insert(t *testing.T, segmentID int64, tf float32) message.ImmutableMessage {
	t.Helper()
	raw, err := message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(&message.InsertMessageHeader{
		CollectionId: 1, Partitions: []*messagespb.PartitionSegmentAssignment{{PartitionId: 10, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: segmentID}}},
	}).WithBody(&msgpb.InsertRequest{
		CollectionID: 1, NumRows: 1, RowIDs: []int64{1}, Timestamps: []uint64{10}, Version: msgpb.InsertDataVersion_ColumnBased,
		FieldsData: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"already materialized"}}}}}}, {FieldId: 102, Type: schemapb.DataType_SparseFloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Contents: [][]byte{typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{7: tf})}}}}}}},
	}).BuildMutable()
	require.NoError(t, err)
	return raw.WithTimeTick(10).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(10))
}

// Persisted stats, snapshot/live inserts and sealed handoff contribute exactly once.
func TestRuntimeRecoversAndAdvancesBM25(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar}, {FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector}}, Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}}}}
	cm := storage.NewLocalChunkManager()
	writeStats := func(name string, values ...float32) []*datapb.FieldBinlog {
		stats := storage.NewBM25Stats()
		for _, v := range values {
			stats.Append(map[uint32]float32{7: v})
		}
		b, err := stats.Serialize()
		require.NoError(t, err)
		path := t.TempDir() + "/" + name
		require.NoError(t, cm.Write(ctx, path, b))
		return []*datapb.FieldBinlog{{FieldID: 102, Binlogs: []*datapb.Binlog{{LogPath: path}}}}
	}
	growingLogs := writeStats("growing", 2)
	sealedLogs := writeStats("sealed", 2, 4, 6)
	resource := &datapb.StreamingNodeBM25Resource{SegmentId: 20, PartitionId: 10, StorageVersion: storage.StorageV2, Bm25Binlogs: sealedLogs}
	client := &mocks.MockMixCoordClient{}
	patch := mockey.Mock((*mocks.MockMixCoordClient).GetStreamingNodeQueryViewResources).To(func(_ *mocks.MockMixCoordClient, _ context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest, _ ...grpc.CallOption) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
		require.Equal(t, int64(1), req.GetCollectionId())
		require.Equal(t, "v1", req.GetVchannel())
		require.Equal(t, []int64{10}, req.GetPartitionIds())
		require.Equal(t, uint64(5), req.GetLoadInfoVersion())
		resp := &datapb.GetStreamingNodeQueryViewResourcesResponse{Status: merr.Success(), CollectionId: 1, Vchannel: "v1", DataVersion: req.GetDataVersion()}
		if req.GetDataVersion().GetStreamingVersion() >= 11 {
			resp.Bm25Resources = []*datapb.StreamingNodeBM25Resource{resource}
		}
		return resp, nil
	}).Build()
	defer patch.UnPatch()
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	future := syncutil.NewFuture[types.MixCoordClient]()
	future.Set(client)
	provider := NewFutureProvider(future, WithChunkManager(cm), WithNodeScheduler(scheduler))
	module, err := provider.NewRuntime()
	require.NoError(t, err)
	runtime := module.(*Runtime)
	defer runtime.Close()
	current := qviews.DataVersion{StreamingVersion: 10}
	next := qviews.DataVersion{StreamingVersion: 11}
	snapshot := walview.VChannelWALView{CollectionID: 1, VChannel: "v1", Schema: schema, PartitionIDs: []int64{10}, LoadInfoVersion: 5, LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 102}}, SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: current, Segments: []walview.VisibleSegment{{SegmentID: 20, PartitionID: 10, Data: walview.SegmentSnapshotData{PersistedStorage: &streamingpb.L1SegmentPersistedStorage{Binlogs: []*streamingpb.L1SegmentBinLogs{{Bm25Binlog: growingLogs}}}, InsertMessages: []message.ImmutableMessage{bm25Insert(t, 20, 4)}}}}}}
	require.NoError(t, runtime.Prepare(ctx, snapshot))
	require.NoError(t, runtime.Prepare(ctx, snapshot))
	query := &schemapb.SparseFloatArray{Contents: [][]byte{typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{7: 1})}}
	check := func(version qviews.DataVersion, avg float64) {
		ids, actual, err := runtime.BuildIDF(version, 102, query)
		require.NoError(t, err)
		require.Len(t, ids, 1)
		require.Equal(t, avg, actual)
	}
	check(current, 3)
	runtime.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: bm25Insert(t, 20, 6)})
	check(current, 4)
	flush := message.NewFlushMessageBuilderV2().WithVChannel("v1").WithHeader(&message.FlushMessageHeader{CollectionId: 1, PartitionId: 10, SegmentId: 20}).WithBody(&message.FlushMessageBody{}).MustBuildMutable().WithTimeTick(11).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(11))
	runtime.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: flush})
	runtime.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{SegmentSealed: &walview.SegmentSealedEvent{SegmentID: 20, SealedAtDataVersion: next}})
	require.Eventually(t, func() bool { return runtime.oracle.BeforeRelease(ctx, next) == nil }, time.Second, time.Millisecond)
	check(next, 4)
	check(current, 4)
	// A segment created after both versions were prepared belongs to both.
	created := message.NewCreateSegmentMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CreateSegmentMessageHeader{CollectionId: 1, PartitionId: 10, SegmentId: 21}).WithBody(&message.CreateSegmentMessageBody{}).MustBuildMutable().WithTimeTick(12).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(12))
	runtime.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: created})
	runtime.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: bm25Insert(t, 21, 8)})
	check(current, 5)
	check(next, 5)
	check(next, 5)
	check(current, 5)
	require.NotContains(t, runtime.oracle.growingStore.segments, int64(20))
	target := qviews.DataVersion{StreamingVersion: 12}
	require.Eventually(t, func() bool { return runtime.oracle.BeforeRelease(ctx, target) == nil }, time.Second, time.Millisecond)
	check(current, 5)
	check(target, 5)
	runtime.Close()
	runtime.Close()
	require.ErrorIs(t, runtime.Prepare(ctx, snapshot), context.Canceled)
	require.Error(t, runtime.RequestRefresh(ctx, target))
}
