package queryresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
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
	optimizer := NewGlobalOptimizer(runtime, functionKey)

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

func TestGlobalOptimizerDoesNotSkipEmptyLatestBM25Corpus(t *testing.T) {
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
	optimizer := NewGlobalOptimizer(runtime, functionKey)

	result, err := optimizer.OptimizeSearch(context.Background(), req)
	require.NoError(t, err)
	require.False(t, result.Skip)
}

func TestGlobalOptimizerOptimizesAdvancedBM25SubSearch(t *testing.T) {
	const (
		collectionID  = int64(300)
		inputFieldID  = int64(301)
		outputFieldID = int64(302)
		functionKey   = "query-optimizer-advanced-test"
	)
	require.NoError(t, function.GetManager().Alloc(collectionID, functionKey, testBM25Schema(inputFieldID, outputFieldID)))
	defer function.GetManager().Release(collectionID, functionKey)

	bm25Req := testBM25SearchRequest(t, collectionID, inputFieldID, outputFieldID)
	req := &internalpb.SearchRequest{
		CollectionID: collectionID,
		IsAdvanced:   true,
		SubReqs: []*internalpb.SubSearchRequest{
			{
				FieldId:            bm25Req.GetFieldId(),
				MetricType:         bm25Req.GetMetricType(),
				PlaceholderGroup:   bm25Req.GetPlaceholderGroup(),
				SerializedExprPlan: bm25Req.GetSerializedExprPlan(),
			},
		},
	}
	idf := typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{7: 3})
	optimizer := NewGlobalOptimizer(NewQueryRuntime(fakeIDFModule{vectors: [][]byte{idf}, avgdl: 9}), functionKey)

	result, err := optimizer.OptimizeSearch(context.Background(), req)
	require.NoError(t, err)
	require.False(t, result.Skip)
	require.False(t, req.GetSubReqs()[0].GetSkip())

	placeholder := &commonpb.PlaceholderGroup{}
	require.NoError(t, proto.Unmarshal(req.GetSubReqs()[0].GetPlaceholderGroup(), placeholder))
	require.Equal(t, commonpb.PlaceholderType_SparseFloatVector, placeholder.GetPlaceholders()[0].GetType())
	plan := &planpb.PlanNode{}
	require.NoError(t, proto.Unmarshal(req.GetSubReqs()[0].GetSerializedExprPlan(), plan))
	require.Equal(t, float64(9), plan.GetVectorAnns().GetQueryInfo().GetBm25Avgdl())
}

func TestGlobalOptimizerDoesNotSkipOldViewSubSearches(t *testing.T) {
	const (
		collectionID  = int64(400)
		inputFieldID  = int64(401)
		outputFieldID = int64(402)
		functionKey   = "query-optimizer-advanced-empty-test"
	)
	require.NoError(t, function.GetManager().Alloc(collectionID, functionKey, testBM25Schema(inputFieldID, outputFieldID)))
	defer function.GetManager().Release(collectionID, functionKey)

	bm25Req := testBM25SearchRequest(t, collectionID, inputFieldID, outputFieldID)
	bm25SubReq := func() *internalpb.SubSearchRequest {
		return &internalpb.SubSearchRequest{
			FieldId:            bm25Req.GetFieldId(),
			MetricType:         bm25Req.GetMetricType(),
			PlaceholderGroup:   bm25Req.GetPlaceholderGroup(),
			SerializedExprPlan: bm25Req.GetSerializedExprPlan(),
		}
	}
	optimizer := NewGlobalOptimizer(NewQueryRuntime(fakeIDFModule{}), functionKey)

	mixedReq := &internalpb.SearchRequest{
		CollectionID: collectionID,
		IsAdvanced:   true,
		SubReqs: []*internalpb.SubSearchRequest{
			{FieldId: inputFieldID, MetricType: metric.IP},
			bm25SubReq(),
		},
	}
	result, err := optimizer.OptimizeSearch(context.Background(), mixedReq)
	require.NoError(t, err)
	require.False(t, result.Skip)
	require.False(t, mixedReq.GetSubReqs()[0].GetSkip())
	require.False(t, mixedReq.GetSubReqs()[1].GetSkip())

	allBM25Req := &internalpb.SearchRequest{
		CollectionID: collectionID,
		IsAdvanced:   true,
		SubReqs:      []*internalpb.SubSearchRequest{bm25SubReq(), bm25SubReq()},
	}
	result, err = optimizer.OptimizeSearch(context.Background(), allBM25Req)
	require.NoError(t, err)
	require.False(t, result.Skip)
	require.False(t, allBM25Req.GetSubReqs()[0].GetSkip())
	require.False(t, allBM25Req.GetSubReqs()[1].GetSkip())
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
func (m fakeIDFModule) BuildIDFBatch(requests []IDFRequest) ([]IDFResult, error) {
	results := make([]IDFResult, len(requests))
	for i := range results {
		results[i] = IDFResult{Vectors: m.vectors, Avgdl: m.avgdl}
	}
	return results, nil
}

func TestGlobalOptimizerBatchesHybridIDFRead(t *testing.T) {
	const key = "hybrid-atomic-idf"
	const collection = int64(500)
	require.NoError(t, function.GetManager().Alloc(collection, key, testBM25Schema(101, 102)))
	defer function.GetManager().Release(collection, key)
	req := testBM25SearchRequest(t, collection, 101, 102)
	sub := &internalpb.SubSearchRequest{FieldId: 102, MetricType: metric.BM25, PlaceholderGroup: req.PlaceholderGroup, SerializedExprPlan: req.SerializedExprPlan}
	hybrid := &internalpb.SearchRequest{CollectionID: collection, IsAdvanced: true, SubReqs: []*internalpb.SubSearchRequest{sub, proto.Clone(sub).(*internalpb.SubSearchRequest)}}
	calls := 0
	patch := mockey.Mock(fakeIDFModule.BuildIDFBatch).To(func(_ fakeIDFModule, requests []IDFRequest) ([]IDFResult, error) {
		calls++
		require.Len(t, requests, 2, "all hybrid tokenization must precede the single aggregate read")
		for _, request := range requests {
			require.NotEmpty(t, request.TFs.GetContents())
		}
		return []IDFResult{{Avgdl: 3}, {Avgdl: 3}}, nil
	}).Build()
	defer patch.UnPatch()
	opt := NewGlobalOptimizer(NewQueryRuntime(fakeIDFModule{}), key)
	_, err := opt.OptimizeSearch(context.Background(), hybrid)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}

func TestGlobalOptimizerBM25InvalidRequests(t *testing.T) {
	const key = "invalid-bm25-idf"
	const collection = int64(501)
	require.NoError(t, function.GetManager().Alloc(collection, key, testBM25Schema(101, 102)))
	defer function.GetManager().Release(collection, key)
	opt := NewGlobalOptimizer(NewQueryRuntime(fakeIDFModule{}), key)
	_, err := opt.OptimizeSearch(context.Background(), nil)
	require.Error(t, err)
	_, err = opt.OptimizeSearch(context.Background(), &internalpb.SearchRequest{IsAdvanced: true})
	require.Error(t, err)
	require.NoError(t, opt.OptimizeRetrieve(context.Background(), nil))
	for _, tc := range []struct {
		name   string
		mutate func(*internalpb.SearchRequest)
	}{
		{"metric", func(r *internalpb.SearchRequest) { r.MetricType = metric.L2 }},
		{"unknown function output", func(r *internalpb.SearchRequest) { r.FieldId = 101 }},
		{"malformed placeholder", func(r *internalpb.SearchRequest) { r.PlaceholderGroup = []byte{0xff} }},
		{"missing placeholder", func(r *internalpb.SearchRequest) { r.PlaceholderGroup = nil }},
		{"missing plan", func(r *internalpb.SearchRequest) { r.SerializedExprPlan = nil }},
		{"malformed plan", func(r *internalpb.SearchRequest) { r.SerializedExprPlan = []byte{0xff} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := testBM25SearchRequest(t, collection, 101, 102)
			tc.mutate(req)
			_, err := opt.OptimizeSearch(context.Background(), req)
			require.Error(t, err)
		})
	}
	req := testBM25SearchRequest(t, collection, 101, 102)
	_, err = NewGlobalOptimizer(nil, key).OptimizeSearch(context.Background(), req)
	require.Error(t, err)
	patch := mockey.Mock(fakeIDFModule.BuildIDFBatch).Return(nil, context.Canceled).Build()
	defer patch.UnPatch()
	_, err = opt.OptimizeSearch(context.Background(), req)
	require.ErrorIs(t, err, context.Canceled)
}
