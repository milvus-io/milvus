package segcore_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A QueryView creates an immutable planning collection, rather than updating
// the collection that originally encoded a growing segment.
func TestGrowingRetrieveWithNewReadCollection(t *testing.T) {
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitLocalChunkManager(t.TempDir())
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	initcore.InitTieredStorage(paramtable.Get())
	schema := mock_segcore.GenTestCollectionSchema("versioned-read", schemapb.DataType_Int64, false)
	schema.Version = 3 // Recovery may start from a nonzero encoding version.
	old, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{CollectionID: 100, Schema: schema})
	require.NoError(t, err)
	defer old.Release()
	segment, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{Collection: old, SegmentID: 100, SegmentType: segcore.SegmentTypeGrowing})
	require.NoError(t, err)
	defer segment.Release()
	msg, err := mock_segcore.GenInsertMsg(old, 1, 100, 5)
	require.NoError(t, err)
	_, err = segment.Insert(t.Context(), &segcore.InsertRequest{RowIDs: msg.RowIDs, Timestamps: msg.Timestamps, Record: &segcorepb.InsertRecord{FieldsData: msg.FieldsData, NumRows: 5}})
	require.NoError(t, err)
	next := proto.Clone(schema).(*schemapb.CollectionSchema)
	next.Version++
	next.Fields = append(next.Fields, &schemapb.FieldSchema{FieldID: 200, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true, DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 7}}})
	current, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{CollectionID: 100, Schema: next})
	require.NoError(t, err)
	defer current.Release()
	blob, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Limit: 10}}, OutputFieldIds: []int64{200}})
	require.NoError(t, err)
	plan, err := segcore.NewRetrievePlan(current, blob, typeutil.MaxTimestamp, 1, 0, 0, 0)
	require.NoError(t, err)
	defer plan.Delete()
	result, err := segment.Retrieve(t.Context(), plan)
	require.NoError(t, err)
	defer result.Release()
	data, err := result.GetResult()
	require.NoError(t, err)
	require.Len(t, data.GetFieldsData(), 1)
	require.Equal(t, []int64{7, 7, 7, 7, 7}, data.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	require.EqualValues(t, 3, old.Schema().GetVersion(), "reading must retain the old encoding schema")
	assertEqualCount(t, old, segment, 5)
}
