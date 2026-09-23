package dql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRerankMetaInterface(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "intField", DataType: schemapb.DataType_Int64},
		},
	}

	t.Run("nil funcScore returns nil", func(t *testing.T) {
		meta, err := newRerankMeta(collSchema, nil)
		require.NoError(t, err)
		assert.Nil(t, meta)
	})

	t.Run("empty functions returns nil", func(t *testing.T) {
		meta, err := newRerankMeta(collSchema, &schemapb.FunctionScore{})
		require.NoError(t, err)
		assert.Nil(t, meta)
	})

	t.Run("RRF function score returns funcScoreRerankMeta", func(t *testing.T) {
		funcScore := &schemapb.FunctionScore{
			Functions: []*schemapb.FunctionSchema{
				{
					Type:             schemapb.FunctionType_Rerank,
					InputFieldNames:  []string{},
					OutputFieldNames: []string{},
					Params:           []*commonpb.KeyValuePair{{Key: "reranker", Value: "rrf"}},
				},
			},
		}
		meta, err := newRerankMeta(collSchema, funcScore)
		require.NoError(t, err)
		assert.NotNil(t, meta)

		fsm, ok := meta.(*funcScoreRerankMeta)
		assert.True(t, ok)
		assert.Equal(t, funcScore, fsm.funcScore)
		assert.Empty(t, meta.GetInputFieldNames())
		assert.Empty(t, meta.GetInputFieldIDs())
	})

	t.Run("decay function score with input fields", func(t *testing.T) {
		funcScore := &schemapb.FunctionScore{
			Functions: []*schemapb.FunctionSchema{
				{
					Type:             schemapb.FunctionType_Rerank,
					InputFieldNames:  []string{"intField"},
					OutputFieldNames: []string{},
					Params: []*commonpb.KeyValuePair{
						{Key: "reranker", Value: "decay"},
						{Key: "origin", Value: "4"},
						{Key: "scale", Value: "4"},
					},
				},
			},
		}
		meta, err := newRerankMeta(collSchema, funcScore)
		require.NoError(t, err)
		assert.NotNil(t, meta)
		assert.Equal(t, []string{"intField"}, meta.GetInputFieldNames())
		assert.Equal(t, []int64{101}, meta.GetInputFieldIDs())
		require.NotNil(t, meta.GetInputPlan())
		require.Len(t, meta.GetInputPlan().Inputs, 1)
		assert.Equal(t, "intField", meta.GetInputPlan().Inputs[0].LogicalName)
		assert.Equal(t, int64(101), meta.GetInputPlan().Inputs[0].SourceFieldID)
	})

	t.Run("all boost functions returns nil", func(t *testing.T) {
		funcScore := &schemapb.FunctionScore{
			Functions: []*schemapb.FunctionSchema{
				{
					Type:             schemapb.FunctionType_Rerank,
					InputFieldNames:  []string{},
					OutputFieldNames: []string{},
					Params: []*commonpb.KeyValuePair{
						{Key: "reranker", Value: "boost"},
						{Key: "weight", Value: "2.0"},
					},
				},
			},
		}
		meta, err := newRerankMeta(collSchema, funcScore)
		require.NoError(t, err)
		assert.Nil(t, meta)
	})

	t.Run("multiple boost functions returns nil", func(t *testing.T) {
		funcScore := &schemapb.FunctionScore{
			Functions: []*schemapb.FunctionSchema{
				{
					Type:             schemapb.FunctionType_Rerank,
					InputFieldNames:  []string{},
					OutputFieldNames: []string{},
					Params: []*commonpb.KeyValuePair{
						{Key: "reranker", Value: "boost"},
						{Key: "weight", Value: "2.0"},
					},
				},
				{
					Type:             schemapb.FunctionType_Rerank,
					InputFieldNames:  []string{},
					OutputFieldNames: []string{},
					Params: []*commonpb.KeyValuePair{
						{Key: "reranker", Value: "boost"},
						{Key: "weight", Value: "3.0"},
						{Key: "filter", Value: "intField > 100"},
					},
				},
			},
		}
		meta, err := newRerankMeta(collSchema, funcScore)
		require.NoError(t, err)
		assert.Nil(t, meta)
	})

	t.Run("boost mixed with non-boost returns non-nil", func(t *testing.T) {
		funcScore := &schemapb.FunctionScore{
			Functions: []*schemapb.FunctionSchema{
				{
					Type:             schemapb.FunctionType_Rerank,
					InputFieldNames:  []string{},
					OutputFieldNames: []string{},
					Params: []*commonpb.KeyValuePair{
						{Key: "reranker", Value: "boost"},
						{Key: "weight", Value: "2.0"},
					},
				},
				{
					Type:             schemapb.FunctionType_Rerank,
					InputFieldNames:  []string{},
					OutputFieldNames: []string{},
					Params: []*commonpb.KeyValuePair{
						{Key: "reranker", Value: "rrf"},
					},
				},
			},
		}
		meta, err := newRerankMeta(collSchema, funcScore)
		require.NoError(t, err)
		assert.NotNil(t, meta)
	})

	t.Run("missing input field returns error", func(t *testing.T) {
		funcScore := &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{{
			Type:            schemapb.FunctionType_Rerank,
			InputFieldNames: []string{"rank_score"},
			Params: []*commonpb.KeyValuePair{
				{Key: "reranker", Value: "decay"},
			},
		}}}
		meta, err := newRerankMeta(collSchema, funcScore)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.Nil(t, meta)
		// Preserve the substring checked by the Drop Field runtime-reference E2E.
		assert.ErrorContains(t, err, "input field rank_score not found in collection schema")
		assert.Equal(t, int32(1100), merr.Status(err).GetCode())
	})

	t.Run("legacy params returns legacyRerankMeta", func(t *testing.T) {
		params := []*commonpb.KeyValuePair{
			{Key: "strategy", Value: "rrf"},
			{Key: "params", Value: `{"k": 60}`},
		}
		meta := newRerankMetaFromLegacy(params)
		assert.NotNil(t, meta)

		lm, ok := meta.(*legacyRerankMeta)
		assert.True(t, ok)
		assert.Equal(t, params, lm.legacyParams)
		assert.Nil(t, meta.GetInputFieldNames())
		assert.Nil(t, meta.GetInputFieldIDs())
	})
}

func mustNewRerankMeta(t testing.TB, schema *schemapb.CollectionSchema, funcScore *schemapb.FunctionScore) rerankMeta {
	t.Helper()
	meta, err := newRerankMeta(schema, funcScore)
	require.NoError(t, err)
	return meta
}

func TestFunctionScoreInputPlanValidation(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, Name: "a", DataType: schemapb.DataType_Int64},
		{FieldID: 102, Name: "b", DataType: schemapb.DataType_VarChar, Nullable: true},
	}}
	plan, err := newDataFrameInputPlanFromFieldNames(schema, []string{"b", "a", "b", "a"})
	require.NoError(t, err)
	require.Len(t, plan.Inputs, 2)
	assert.Equal(t, []int64{102, 101}, plan.PhysicalFieldIDs())
	assert.Equal(t, "b", plan.Inputs[0].LogicalName)
	assert.True(t, plan.Inputs[0].Nullable)
	for _, dataType := range []schemapb.DataType{schemapb.DataType_JSON, schemapb.DataType_Array, schemapb.DataType_FloatVector, schemapb.DataType_Geometry, schemapb.DataType_None} {
		t.Run(dataType.String(), func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 103, Name: "unsupported", DataType: dataType}}}
			meta, err := newRerankMeta(schema, &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_Rerank, InputFieldNames: []string{"unsupported"}, Params: []*commonpb.KeyValuePair{{Key: "reranker", Value: "rrf"}}}}})
			require.Nil(t, meta)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.ErrorContains(t, err, "unsupported field type")
		})
	}
}
