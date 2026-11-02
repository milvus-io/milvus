package idf

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRetainedOracleIgnoresDroppedFunctionInLiveWrites(t *testing.T) {
	schema := &schemapb.CollectionSchema{Version: 1, Fields: []*schemapb.FieldSchema{
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		{FieldID: 103, Name: "other_text", DataType: schemapb.DataType_VarChar},
		{FieldID: 104, Name: "other_sparse", DataType: schemapb.DataType_SparseFloatVector},
		{FieldID: 105, Name: "removed_required_column", DataType: schemapb.DataType_Int64},
	}, Functions: []*schemapb.FunctionSchema{
		{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}},
		{Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{103}, OutputFieldIds: []int64{104}},
	}}
	r, err := newOracleRuntime(t.Context(), nil, walview.VChannelWALView{
		Schema: schema, WriteSchema: schema,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{DataVersion: qviews.DataVersion{StreamingVersion: 1}},
	}, nil, true)
	require.NoError(t, err)
	defer r.Close()
	insert := func(segmentID int64, tick uint64, both bool) message.ImmutableMessage {
		original := message.MustAsImmutableInsertMessageV1(bm25Insert(t, segmentID, 2, tick))
		body := proto.Clone(original.MustBody()).(*msgpb.InsertRequest)
		otherText := proto.Clone(body.FieldsData[0]).(*schemapb.FieldData)
		otherText.FieldId = 103
		otherSparse := proto.Clone(body.FieldsData[1]).(*schemapb.FieldData)
		otherSparse.FieldId = 104
		if !both {
			body.FieldsData = nil
		}
		body.FieldsData = append(body.FieldsData, otherText, otherSparse)
		return message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(original.Header()).WithBody(body).
			MustBuildMutable().WithTimeTick(tick).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(int64(tick)))
	}
	r.ApplyLiveEvent(t.Context(), walview.VChannelResourceEvent{Message: insert(20, 10, true)})
	require.EqualValues(t, 1, r.growingStore.segments[20].stats[102].NumRow())
	next := proto.Clone(schema).(*schemapb.CollectionSchema)
	next.Version = 2
	next.Fields = next.Fields[2:4]
	next.Functions = next.Functions[1:]
	alter := message.NewAlterCollectionMessageBuilderV2().WithVChannel("v1").WithHeader(&message.AlterCollectionMessageHeader{
		CollectionId: 1, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
	}).WithBody(&message.AlterCollectionMessageBody{Updates: &messagespb.AlterCollectionMessageUpdates{Schema: next}}).
		MustBuildMutable().WithTimeTick(11).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(11))
	r.ApplyLiveEvent(t.Context(), walview.VChannelResourceEvent{Message: alter})
	raw := insert(21, 12, false)
	before := proto.Clone(message.MustAsImmutableInsertMessageV1(raw).MustBody())
	require.NotPanics(t, func() { r.ApplyLiveEvent(t.Context(), walview.VChannelResourceEvent{Message: raw}) })
	require.EqualValues(t, 1, r.schema.GetVersion(), "read semantics cannot follow the write frontier")
	require.EqualValues(t, 1, r.growingStore.segments[20].stats[102].NumRow(), "old contribution remains retained")
	require.Zero(t, r.growingStore.segments[21].stats[102].NumRow(), "removed function cannot consume new writes")
	require.EqualValues(t, 1, r.growingStore.segments[21].stats[104].NumRow(), "unchanged function continues consuming")
	require.True(t, proto.Equal(before, message.MustAsImmutableInsertMessageV1(raw).MustBody()))
	recovered, err := newOracleRuntime(t.Context(), &Provider{}, walview.VChannelWALView{
		Schema: schema, WriteSchema: next,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{Segments: []walview.VisibleSegment{
			{SegmentID: 20, PartitionID: 10, Schema: schema, Data: walview.SegmentSnapshotData{InsertMessages: []message.ImmutableMessage{insert(20, 10, true)}}},
			{SegmentID: 21, PartitionID: 10, Schema: next, Data: walview.SegmentSnapshotData{InsertMessages: []message.ImmutableMessage{raw}}},
		}},
	}, nil, true)
	require.NoError(t, err)
	defer recovered.Close()
	require.EqualValues(t, 1, recovered.growingStore.segments[20].stats[102].NumRow())
	require.Zero(t, recovered.growingStore.segments[21].stats[102].NumRow())
	require.EqualValues(t, 1, recovered.growingStore.segments[21].stats[104].NumRow())
}

func TestGrowingBM25StatsRejectIncompleteOutput(t *testing.T) {
	original := message.MustAsImmutableInsertMessageV1(bm25Insert(t, 20, 2))
	body := proto.Clone(original.MustBody()).(*msgpb.InsertRequest)
	body.NumRows = 2
	raw := message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(original.Header()).WithBody(body).
		MustBuildMutable().WithTimeTick(10).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(10))
	insert := walview.SegmentInsertMessage{Message: message.MustAsImmutableInsertMessageV1(raw), Assignment: original.Header().GetPartitions()[0]}
	stats := bm25Stats{}
	stats.getOrCreate(102)
	require.ErrorIs(t, collectGrowingInsertStats(stats, &schemapb.CollectionSchema{}, insert), merr.ErrDataIntegrity)
	require.Zero(t, stats[102].NumRow())
}

func TestGrowingStatsMaterializesOnlyRequiredBM25(t *testing.T) {
	original := message.MustAsImmutableInsertMessageV1(bm25Insert(t, 20, 2))
	body := proto.Clone(original.MustBody()).(*msgpb.InsertRequest)
	body.FieldsData = body.FieldsData[:1]
	raw := message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(original.Header()).WithBody(body).
		MustBuildMutable().WithTimeTick(10).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(10))
	insert := walview.SegmentInsertMessage{Message: message.MustAsImmutableInsertMessageV1(raw), Assignment: original.Header().GetPartitions()[0]}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}, {Key: "max_length", Value: "256"}}},
		{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		{FieldID: 103, Name: "other_text", DataType: schemapb.DataType_VarChar},
		{FieldID: 104, Name: "minhash", DataType: schemapb.DataType_BinaryVector, IsFunctionOutput: true},
	}, Functions: []*schemapb.FunctionSchema{
		{Name: "bm25", Type: schemapb.FunctionType_BM25, InputFieldNames: []string{"text"}, InputFieldIds: []int64{101}, OutputFieldNames: []string{"sparse"}, OutputFieldIds: []int64{102}},
		{Name: "minhash", Type: schemapb.FunctionType_MinHash, InputFieldNames: []string{"other_text"}, InputFieldIds: []int64{103}, OutputFieldNames: []string{"minhash"}, OutputFieldIds: []int64{104}},
	}}
	stats := bm25Stats{}
	stats.getOrCreate(102)
	require.NoError(t, collectGrowingInsertStats(stats, schema, insert))
	require.EqualValues(t, 1, stats[102].NumRow())
	require.Positive(t, stats[102].GetAvgdl())
	require.Len(t, insert.Message.MustBody().GetFieldsData(), 1, "materialization must preserve retained WAL body")
}
