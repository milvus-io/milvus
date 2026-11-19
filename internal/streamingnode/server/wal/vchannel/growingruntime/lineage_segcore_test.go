package growingruntime

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// This test requires a matching native core. It must not be counted as covered
// when the borrowed local core aborts in the test environment's FFI stub.
func TestResidualLineageSegcoreDeleteVisibility(t *testing.T) {
	initSegcoreForRuntimeTest(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	schema := &schemapb.CollectionSchema{Name: "split_residual", Version: 1, Fields: []*schemapb.FieldSchema{
		{FieldID: 0, Name: "RowID", DataType: schemapb.DataType_Int64},
		{FieldID: 1, Name: "Timestamp", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	m := &lineageTestManager{requests: make(chan *lineageTestStream, 2)}
	r := newRuntime()
	defer r.Close()
	require.NoError(t, r.Prepare(ctx, walview.VChannelWALView{CollectionID: 1, VChannel: "source", Schema: schema, LineageStreams: m}))
	insert := message.NewInsertMessageBuilderV1().WithVChannel("source").WithHeader(&message.InsertMessageHeader{CollectionId: 1, Partitions: []*messagespb.PartitionSegmentAssignment{{PartitionId: 10, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 1}}}}).WithBody(&msgpb.InsertRequest{CollectionID: 1, PartitionID: 10, SegmentID: 1, Version: msgpb.InsertDataVersion_ColumnBased, NumRows: 1, RowIDs: []int64{1}, Timestamps: []uint64{80}, FieldsData: []*schemapb.FieldData{{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}}}}}).MustBuildMutable().WithTimeTick(80).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(80))
	r.applyLiveMessage(ctx, insert)
	r.observeSplitLineage(&lineageSpec{vchannels: []string{"target"}, switchTimeTick: 90})
	stream := takeLineageStream(t, m)
	// The target delete flows through the real growing-segment engine.
	entry := messageutil.BuildTransformLogEntry(newPartitionDeleteMessage(t, 10, false), messageutil.TransformEntryOption{})
	stream.opt.Handler.Handle(wal.TransformLogStreamEvent{Entry: entry})
	require.NoError(t, r.WaitLineageVisible(ctx, map[string]uint64{"target": 100}))
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	plan, err := planparserv2.CreateRetrievePlan(helper, "pk == 1", nil)
	require.NoError(t, err)
	plan.OutputFieldIds = []int64{100}
	blob, err := proto.Marshal(plan)
	require.NoError(t, err)
	segment, ok := r.Segment(1)
	require.True(t, ok)
	for _, test := range []struct {
		point uint64
		rows  int
	}{{95, 1}, {105, 0}} {
		retrieve, err := segcore.NewRetrievePlan(r.collection, blob, test.point, 0, commonpb.ConsistencyLevel_Strong, 0, 0)
		require.NoError(t, err)
		result, err := segment.Retrieve(context.Background(), retrieve)
		require.NoError(t, err)
		data, err := result.GetResult()
		require.NoError(t, err)
		require.Len(t, data.GetIds().GetIntId().GetData(), test.rows)
		result.Release()
		retrieve.Delete()
	}
}
