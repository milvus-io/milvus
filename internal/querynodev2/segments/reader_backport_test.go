package segments

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The fixture was marshaled with the 3.1 messagespb definitions at 3f2044a753.
// Reading fixed bytes avoids constructing the input with the backported decoder.
func TestNewWALReaderNativeReplacementAndReplay(t *testing.T) {
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitLocalChunkManager(t.TempDir())
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	require.NoError(t, initcore.InitTieredStorage(paramtable.Get()))
	ctx := context.Background()
	fixture, err := os.ReadFile("testdata/insert_upsert_v2_3_1.json")
	require.NoError(t, err)
	var records []struct {
		Name       string            `json:"name"`
		Payload    []byte            `json:"payload"`
		Properties map[string]string `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(fixture, &records))
	require.Len(t, records, 2)
	schema := &schemapb.CollectionSchema{Name: "reader_backport", Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		{FieldID: 102, Name: "value", DataType: schemapb.DataType_Int64},
	}}
	for _, phase := range []string{"initial", "replay"} {
		t.Run(phase, func(t *testing.T) {
			collection, err := NewCollection(1, schema, nil, &querypb.LoadMetaInfo{PartitionIDs: []int64{10}})
			require.NoError(t, err)
			t.Cleanup(func() { DeleteCollection(collection) })
			manager := NewManager()
			segment, err := NewSegment(ctx, collection, manager.Segment, SegmentTypeGrowing, 0, &querypb.SegmentLoadInfo{SegmentID: 100, CollectionID: 1, PartitionID: 10, InsertChannel: "by-dev-rootcoord-dml_0_1v0", Level: datapb.SegmentLevel_Legacy})
			require.NoError(t, err)
			t.Cleanup(func() { segment.Release(ctx) })
			for index, record := range records {
				msg := message.NewImmutableMesasge(rmq.NewRmqID(int64(index+1)), record.Payload, record.Properties)
				pack, err := adaptor.NewMsgPackFromMessage(msg)
				require.NoError(t, err)
				require.NotNil(t, pack)
				for _, effect := range pack.Msgs {
					switch effect := effect.(type) {
					case *msgstream.InsertMsg:
						require.NoError(t, effect.MaterializePrimaryKey(schema))
						insert, _, err := storage.TransferInsertMsgToInsertRecord(schema, effect)
						require.NoError(t, err)
						require.NoError(t, segment.Insert(ctx, effect.RowIDs, effect.Timestamps, insert))
					case *msgstream.DeleteMsg:
						require.NoError(t, segment.Delete(ctx, storage.ParseIDs2PrimaryKeysBatch(effect.PrimaryKeys), effect.Timestamps))
					default:
						t.Fatalf("unexpected adapted type %T", effect)
					}
				}
				require.Equal(t, int64(index+1), segment.InsertCount())
				require.Equal(t, int64(1), segment.RowNum(), "new version must survive its own delete")
				planBytes, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_Predicates{Predicates: &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}, Values: []*planpb.GenericValue{{Val: &planpb.GenericValue_Int64Val{Int64Val: 101}}}}}}}, OutputFieldIds: []int64{100, 102}})
				require.NoError(t, err)
				plan, err := segcore.NewRetrievePlan(collection.GetCCollection(), planBytes, msg.TimeTick(), 100, 0, 0, 0)
				require.NoError(t, err)
				result, err := segment.Retrieve(ctx, plan)
				plan.Delete()
				require.NoError(t, err)
				require.Equal(t, []int64{101}, result.GetIds().GetIntId().GetData())
				require.Len(t, result.FieldsData, 2)
				require.Equal(t, []int64{int64((index + 1) * 10)}, result.FieldsData[1].GetScalars().GetLongData().GetData())
			}
		})
	}
}
