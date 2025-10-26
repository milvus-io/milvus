package rerank

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestApplyRerankOnSearchResultData_NilOrNotQueryNode(t *testing.T) {
	ctx := context.Background()
	// nil funcSchema should return input unchanged
	data := &schemapb.SearchResultData{NumQueries: 1, TopK: 1, Topks: []int64{1}, Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}, Scores: []float32{0.1}}
	out, err := ApplyRerankOnSearchResultData(ctx, data, &schemapb.CollectionSchema{}, nil, "COSINE", 1, 1)
	require.NoError(t, err)
	require.Equal(t, data, out)

	// non-query-node reranker (weighted) should be a no-op
	funcSchema := &schemapb.FunctionSchema{Type: schemapb.FunctionType_Rerank, Params: []*commonpb.KeyValuePair{{Key: reranker, Value: WeightedName}}}
	out2, err := ApplyRerankOnSearchResultData(ctx, data, &schemapb.CollectionSchema{}, funcSchema, "COSINE", 1, 1)
	require.NoError(t, err)
	require.Equal(t, data, out2)
}

func TestApplyRerankOnSearchResultData_EdgeCases(t *testing.T) {
	ctx := context.Background()
	schema := &schemapb.CollectionSchema{Name: "c", Fields: []*schemapb.FieldSchema{{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}
	// nil IDs should be a no-op
	rd := &schemapb.SearchResultData{NumQueries: 1, TopK: 1, Topks: []int64{1}}
	out, err := ApplyRerankOnSearchResultData(ctx, rd, schema, &schemapb.FunctionSchema{Type: schemapb.FunctionType_Rerank, Params: []*commonpb.KeyValuePair{{Key: reranker, Value: ExprName}}}, "COSINE", 1, 1)
	require.NoError(t, err)
	require.Equal(t, rd, out)
}

func TestApplyRerankOnSearchResultData_SuccessAndErrors(t *testing.T) {
	ctx := context.Background()

	// minimal schema with pk and a float field used in expr
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "pop", DataType: schemapb.DataType_Float},
		},
	}

	srd := &schemapb.SearchResultData{NumQueries: 1, TopK: 3, Topks: []int64{3}}
	srd.Ids = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}
	srd.Scores = []float32{0.1, 0.2, 0.3}
	srd.FieldsData = []*schemapb.FieldData{{
		FieldId: 101, Type: schemapb.DataType_Float,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{
			FloatData: &schemapb.FloatArray{Data: []float32{1.0, 2.0, 3.0}},
		}}},
	}}

	expr := "score + fields[\"pop\"]"
	funcSchema := &schemapb.FunctionSchema{
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"pop"},
		Params:          []*commonpb.KeyValuePair{{Key: reranker, Value: ExprName}, {Key: ExprCodeKey, Value: expr}},
	}

	out, err := ApplyRerankOnSearchResultData(ctx, srd, schema, funcSchema, "COSINE", 1, 3)
	require.NoError(t, err)
	// verify output result has scores length equal to topk
	require.Equal(t, int(out.TopK), len(out.Scores))

	// If Ids is nil, should be a no-op
	srd2 := &schemapb.SearchResultData{NumQueries: 1, TopK: 1}
	out2, err := ApplyRerankOnSearchResultData(ctx, srd2, schema, funcSchema, "COSINE", 1, 1)
	require.NoError(t, err)
	require.Equal(t, srd2, out2)

	// createFunction should error when function type is not rerank
	badFunc := &schemapb.FunctionSchema{Type: schemapb.FunctionType_BM25}
	_, err = ApplyRerankOnSearchResultData(ctx, srd, schema, badFunc, "COSINE", 1, 3)
	require.Error(t, err)

	// Wasm path: ensure missing wasm_code yields error via createFunction (NewFunctionScore tests already cover wasm)
	wasmFunc := &schemapb.FunctionSchema{Type: schemapb.FunctionType_Rerank, Params: []*commonpb.KeyValuePair{{Key: reranker, Value: WasmName}}}
	_, err = ApplyRerankOnSearchResultData(ctx, srd, schema, wasmFunc, "COSINE", 1, 3)
	require.Error(t, err)

	// Also check that a compiled-but-empty wasm (invalid base64) fails earlier
	wasmFunc2 := &schemapb.FunctionSchema{Type: schemapb.FunctionType_Rerank, Params: []*commonpb.KeyValuePair{{Key: reranker, Value: WasmName}, {Key: wasmCodeKey, Value: base64.StdEncoding.EncodeToString([]byte("badwasm"))}}}
	_, err = ApplyRerankOnSearchResultData(ctx, srd, schema, wasmFunc2, "COSINE", 1, 3)
	require.Error(t, err)
}
