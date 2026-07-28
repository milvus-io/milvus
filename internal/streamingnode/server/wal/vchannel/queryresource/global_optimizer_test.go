package queryresource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestGlobalOptimizerBuildsBM25IDF(t *testing.T) {
	const (
		collectionID  = int64(100)
		inputFieldID  = int64(101)
		outputFieldID = int64(102)
		functionKey   = "query-optimizer-test"
	)
	require.NoError(t, function.GetManager().Alloc(collectionID, functionKey, testBM25Schema(inputFieldID, outputFieldID)))
	defer function.GetManager().Release(collectionID, functionKey)

	idf := typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{7: 3})
	req := testBM25SearchRequest(t, collectionID, inputFieldID, outputFieldID)
	runtime := NewQueryRuntime(fakeIDFModule{vectors: [][]byte{idf}, avgdl: 9})
	optimizer := NewGlobalOptimizer(runtime, qviews.DataVersion{StreamingVersion: 1}, functionKey)

	result, err := optimizer.OptimizeSearch(context.Background(), req)
	require.NoError(t, err)
	require.False(t, result.Skip)

	placeholder := &commonpb.PlaceholderGroup{}
	require.NoError(t, proto.Unmarshal(req.GetPlaceholderGroup(), placeholder))
	require.Equal(t, commonpb.PlaceholderType_SparseFloatVector, placeholder.GetPlaceholders()[0].GetType())

	plan := &planpb.PlanNode{}
	require.NoError(t, proto.Unmarshal(req.GetSerializedExprPlan(), plan))
	require.Equal(t, float64(9), plan.GetVectorAnns().GetQueryInfo().GetBm25Avgdl())
}

func TestGlobalOptimizerSkipsPreparedEmptyBM25Corpus(t *testing.T) {
	const (
		collectionID  = int64(200)
		inputFieldID  = int64(201)
		outputFieldID = int64(202)
		functionKey   = "query-optimizer-empty-test"
	)
	require.NoError(t, function.GetManager().Alloc(collectionID, functionKey, testBM25Schema(inputFieldID, outputFieldID)))
	defer function.GetManager().Release(collectionID, functionKey)

	req := testBM25SearchRequest(t, collectionID, inputFieldID, outputFieldID)
	runtime := NewQueryRuntime(fakeIDFModule{})
	optimizer := NewGlobalOptimizer(runtime, qviews.DataVersion{StreamingVersion: 1}, functionKey)

	result, err := optimizer.OptimizeSearch(context.Background(), req)
	require.NoError(t, err)
	require.True(t, result.Skip)
}

func testBM25SearchRequest(t *testing.T, collectionID int64, inputFieldID int64, outputFieldID int64) *internalpb.SearchRequest {
	t.Helper()
	placeholder, err := funcutil.FieldDataToPlaceholderGroupBytes(&schemapb.FieldData{
		Type:    schemapb.DataType_VarChar,
		FieldId: inputFieldID,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"query text"}},
				},
			},
		},
	})
	require.NoError(t, err)

	plan, err := proto.Marshal(&planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{QueryInfo: &planpb.QueryInfo{}},
		},
	})
	require.NoError(t, err)
	return &internalpb.SearchRequest{
		CollectionID:       collectionID,
		MetricType:         metric.BM25,
		FieldId:            outputFieldID,
		PlaceholderGroup:   placeholder,
		SerializedExprPlan: plan,
	}
}

func testBM25Schema(inputFieldID int64, outputFieldID int64) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:    "test",
		Version: 1,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: inputFieldID, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: outputFieldID, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:             "bm25",
			Type:             schemapb.FunctionType_BM25,
			InputFieldIds:    []int64{inputFieldID},
			InputFieldNames:  []string{"text"},
			OutputFieldIds:   []int64{outputFieldID},
			OutputFieldNames: []string{"sparse"},
		}},
	}
}

type fakeIDFModule struct {
	vectors [][]byte
	avgdl   float64
}

func (m fakeIDFModule) Prepare(context.Context, walview.VChannelWALView) error { return nil }
func (m fakeIDFModule) ApplyLiveEvent(context.Context, walview.VChannelResourceEvent) {
}
func (m fakeIDFModule) Advance(qviews.DataVersion) {}
func (m fakeIDFModule) Close()                     {}
func (m fakeIDFModule) BuildIDF(qviews.DataVersion, int64, *schemapb.SparseFloatArray) ([][]byte, float64, error) {
	return m.vectors, m.avgdl, nil
}
