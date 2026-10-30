// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func kv(key, value string) *commonpb.KeyValuePair {
	return &commonpb.KeyValuePair{Key: key, Value: value}
}

func TestParseCalcDistance(t *testing.T) {
	t.Run("absent key", func(t *testing.T) {
		spec, err := parseCalcDistance([]*commonpb.KeyValuePair{kv(LimitKey, "10")})
		require.NoError(t, err)
		assert.Nil(t, spec)
	})

	t.Run("invalid json", func(t *testing.T) {
		_, err := parseCalcDistance([]*commonpb.KeyValuePair{kv(CalcDistanceKey, "not-json")})
		require.Error(t, err)
	})

	t.Run("missing fields", func(t *testing.T) {
		for _, body := range []string{
			`{"field":"vec","vector":[1,2]}`,
			`{"metric_type":"L2","vector":[1,2]}`,
			`{"metric_type":"L2","field":"vec"}`,
		} {
			_, err := parseCalcDistance([]*commonpb.KeyValuePair{kv(CalcDistanceKey, body)})
			require.Error(t, err)
		}
	})

	t.Run("array form", func(t *testing.T) {
		spec, err := parseCalcDistance([]*commonpb.KeyValuePair{kv(CalcDistanceKey, `{"metric_type":"cosine","field":"vec","vector":[1,2,3]}`)})
		require.NoError(t, err)
		assert.Equal(t, metric.COSINE, spec.metricType)
		assert.Equal(t, "vec", spec.fieldName)
		assert.JSONEq(t, `[1,2,3]`, string(spec.vectorInput))
		assert.Empty(t, spec.expression)
	})

	t.Run("object form", func(t *testing.T) {
		spec, err := parseCalcDistance([]*commonpb.KeyValuePair{kv(CalcDistanceKey, `{"metric_type":"IP","field":"sparse","vector":{"1":0.5}}`)})
		require.NoError(t, err)
		assert.JSONEq(t, `{"1":0.5}`, string(spec.vectorInput))
	})

	t.Run("expression form", func(t *testing.T) {
		spec, err := parseCalcDistance([]*commonpb.KeyValuePair{kv(CalcDistanceKey, `{"metric_type":"L2","field":"vec","vector":"id == 2"}`)})
		require.NoError(t, err)
		assert.Equal(t, "id == 2", spec.expression)
		assert.Empty(t, spec.vectorInput)
	})

	t.Run("unsupported first byte", func(t *testing.T) {
		_, err := parseCalcDistance([]*commonpb.KeyValuePair{kv(CalcDistanceKey, `{"metric_type":"L2","field":"vec","vector":null}`)})
		require.Error(t, err)
	})
}

func TestEncodeCalcDistanceVector(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		got, err := encodeCalcDistanceVector([]byte(`[1,2,3,4]`), schemapb.DataType_FloatVector, 4)
		require.NoError(t, err)
		assert.Equal(t, typeutil.Float32ArrayToBytes([]float32{1, 2, 3, 4}), got)
		_, err = encodeCalcDistanceVector([]byte(`[1,2]`), schemapb.DataType_FloatVector, 4)
		require.Error(t, err)
	})

	t.Run("int8", func(t *testing.T) {
		got, err := encodeCalcDistanceVector([]byte(`[1,-2,3,4]`), schemapb.DataType_Int8Vector, 4)
		require.NoError(t, err)
		assert.Equal(t, typeutil.Int8ArrayToBytes([]int8{1, -2, 3, 4}), got)
		_, err = encodeCalcDistanceVector([]byte(`[1.5]`), schemapb.DataType_Int8Vector, 1)
		require.Error(t, err)
	})

	t.Run("float16", func(t *testing.T) {
		got, err := encodeCalcDistanceVector([]byte(`[1,2]`), schemapb.DataType_Float16Vector, 2)
		require.NoError(t, err)
		assert.Equal(t, typeutil.Float32ArrayToFloat16Bytes([]float32{1, 2}), got)
	})

	t.Run("bfloat16", func(t *testing.T) {
		got, err := encodeCalcDistanceVector([]byte(`[1,2]`), schemapb.DataType_BFloat16Vector, 2)
		require.NoError(t, err)
		assert.Equal(t, typeutil.Float32ArrayToBFloat16Bytes([]float32{1, 2}), got)
	})

	t.Run("binary", func(t *testing.T) {
		got, err := encodeCalcDistanceVector([]byte(`[0,255,1]`), schemapb.DataType_BinaryVector, 24)
		require.NoError(t, err)
		assert.Equal(t, []byte{0x00, 0xff, 0x01}, got)
		_, err = encodeCalcDistanceVector([]byte(`[0,256]`), schemapb.DataType_BinaryVector, 16)
		require.Error(t, err)
		_, err = encodeCalcDistanceVector([]byte(`[0,1]`), schemapb.DataType_BinaryVector, 24)
		require.Error(t, err)
		_, err = encodeCalcDistanceVector([]byte(`[0,1]`), schemapb.DataType_BinaryVector, 12)
		require.Error(t, err)
	})

	t.Run("sparse object", func(t *testing.T) {
		got, err := encodeCalcDistanceVector([]byte(`{"1":0.5,"0":0.1}`), schemapb.DataType_SparseFloatVector, 0)
		require.NoError(t, err)
		indices, values := typeutil.SortSparseFloatRow([]uint32{0, 1}, []float32{0.1, 0.5})
		assert.Equal(t, typeutil.CreateSparseFloatRow(indices, values), got)
	})

	t.Run("sparse pairs", func(t *testing.T) {
		got, err := encodeCalcDistanceVector([]byte(`[{"index":1,"value":0.5},{"index":0,"value":0.1}]`), schemapb.DataType_SparseFloatVector, 0)
		require.NoError(t, err)
		indices, values := typeutil.SortSparseFloatRow([]uint32{1, 0}, []float32{0.5, 0.1})
		assert.Equal(t, typeutil.CreateSparseFloatRow(indices, values), got)
		_, err = encodeCalcDistanceVector([]byte(`[1,2,3]`), schemapb.DataType_SparseFloatVector, 0)
		require.Error(t, err)
	})
}

func TestValidateMetricTypeForVector(t *testing.T) {
	require.NoError(t, func() error {
		_, err := validateMetricTypeForVector("l2", schemapb.DataType_FloatVector)
		return err
	}())
	for _, dt := range []schemapb.DataType{
		schemapb.DataType_FloatVector, schemapb.DataType_Int8Vector,
		schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector,
	} {
		for _, m := range []string{"L2", "IP", "COSINE"} {
			got, err := validateMetricTypeForVector(m, dt)
			require.NoError(t, err, "%s/%s", dt, m)
			assert.Equal(t, m, got)
		}
		_, err := validateMetricTypeForVector("HAMMING", dt)
		require.Error(t, err)
	}
	for _, m := range []string{"HAMMING", "JACCARD"} {
		got, err := validateMetricTypeForVector(m, schemapb.DataType_BinaryVector)
		require.NoError(t, err)
		assert.Equal(t, m, got)
	}
	_, err := validateMetricTypeForVector("L2", schemapb.DataType_BinaryVector)
	require.Error(t, err)
	for _, m := range []string{"IP", "COSINE"} {
		got, err := validateMetricTypeForVector(m, schemapb.DataType_SparseFloatVector)
		require.NoError(t, err)
		assert.Equal(t, m, got)
	}
	_, err = validateMetricTypeForVector("L2", schemapb.DataType_SparseFloatVector)
	require.Error(t, err)
}

func TestValidateCosineRefVector(t *testing.T) {
	require.Error(t, validateCosineRefVector(typeutil.Float32ArrayToBytes([]float32{0, 0}), schemapb.DataType_FloatVector))
	require.NoError(t, validateCosineRefVector(typeutil.Float32ArrayToBytes([]float32{0, 1}), schemapb.DataType_FloatVector))
	require.NoError(t, validateCosineRefVector(typeutil.Int8ArrayToBytes([]int8{0, 1}), schemapb.DataType_Int8Vector))
	require.Error(t, validateCosineRefVector(typeutil.Int8ArrayToBytes([]int8{0, 0}), schemapb.DataType_Int8Vector))
	// sparse and binary are handled by their own metric semantics
	require.NoError(t, validateCosineRefVector([]byte{1}, schemapb.DataType_SparseFloatVector))
	require.NoError(t, validateCosineRefVector([]byte{0}, schemapb.DataType_BinaryVector))
}

func TestIsSupportedCalcDistanceVectorType(t *testing.T) {
	for _, dt := range []schemapb.DataType{
		schemapb.DataType_FloatVector, schemapb.DataType_Int8Vector,
		schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector,
		schemapb.DataType_BinaryVector, schemapb.DataType_SparseFloatVector,
	} {
		assert.True(t, isSupportedCalcDistanceVectorType(dt), dt.String())
	}
	assert.False(t, isSupportedCalcDistanceVectorType(schemapb.DataType_Array))
	assert.False(t, isSupportedCalcDistanceVectorType(schemapb.DataType_VarChar))
}

func TestResolveCalcDistance_DirectVector(t *testing.T) {
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: "cd",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
		},
	})
	task := &QueryTask{baseTask: baseTask{}, schema: schema}

	spec := &calcDistanceSpec{fieldName: "vec", metricType: "L2", vectorInput: []byte(`[1,2,3,4]`)}
	require.NoError(t, task.resolveCalcDistance(spec, context.Background(), nil))
	assert.Equal(t, schemapb.DataType_FloatVector, spec.refVectorType)
	assert.Equal(t, typeutil.Float32ArrayToBytes([]float32{1, 2, 3, 4}), spec.refVector)
	assert.NotNil(t, spec.vectorField)
	assert.Equal(t, metric.L2, spec.metricType)

	// zero-norm reference with COSINE is rejected
	spec = &calcDistanceSpec{fieldName: "vec", metricType: "COSINE", vectorInput: []byte(`[0,0,0,0]`)}
	require.Error(t, task.resolveCalcDistance(spec, context.Background(), nil))

	// dimension mismatch
	spec = &calcDistanceSpec{fieldName: "vec", metricType: "L2", vectorInput: []byte(`[1,2]`)}
	require.Error(t, task.resolveCalcDistance(spec, context.Background(), nil))

	// unknown field
	spec = &calcDistanceSpec{fieldName: "nope", metricType: "L2", vectorInput: []byte(`[1,2,3,4]`)}
	require.Error(t, task.resolveCalcDistance(spec, context.Background(), nil))
}

func TestResolveCalcDistance_UnsupportedFieldType(t *testing.T) {
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: "cd",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "name", DataType: schemapb.DataType_VarChar},
		},
	})
	task := &QueryTask{baseTask: baseTask{}, schema: schema}
	spec := &calcDistanceSpec{fieldName: "name", metricType: "L2", vectorInput: []byte(`["x"]`)}
	require.Error(t, task.resolveCalcDistance(spec, context.Background(), nil))
}

func calcDistanceTask() *QueryTask {
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: "cd",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
		},
	})
	var vectorField *schemapb.FieldSchema
	for _, f := range schema.CollectionSchema.GetFields() {
		if f.GetFieldID() == 101 {
			vectorField = f
		}
	}
	return &QueryTask{
		baseTask: baseTask{},
		schema:   schema,
		plan: &planpb.PlanNode{Node: &planpb.PlanNode_Query{
			Query: &planpb.QueryPlanNode{},
		}},
		queryParams: &queryParams{calcDistance: &calcDistanceSpec{
			fieldName:     "vec",
			metricType:    metric.L2,
			refVector:     typeutil.Float32ArrayToBytes([]float32{1, 2, 3, 4}),
			refVectorType: schemapb.DataType_FloatVector,
			vectorField:   vectorField,
		}},
	}
}

func TestApplyCalcDistanceToPlan(t *testing.T) {
	task := calcDistanceTask()
	require.NoError(t, task.applyCalcDistanceToPlan())

	cd := task.plan.GetQuery().GetCalcDistance()
	require.NotNil(t, cd)
	assert.True(t, cd.GetReturnDistance())
	assert.Equal(t, metric.L2, cd.GetMetricType())
	assert.Equal(t, int64(101), cd.GetVectorFieldId())
	assert.Equal(t, "distance", cd.GetDistanceOutputName())
	assert.Equal(t, schemapb.DataType_FloatVector, cd.GetRefVectorType())
	assert.Equal(t, [][]byte{typeutil.Float32ArrayToBytes([]float32{1, 2, 3, 4})}, cd.GetRefVectors())
	// the target vector field is injected into the plan output ids so segcore
	// fetches its raw data
	assert.Equal(t, []int64{101}, task.plan.GetOutputFieldIds())
	assert.Equal(t, "distance", task.queryParams.calcDistance.distanceOutputName)
}

func TestApplyCalcDistanceToPlan_ColumnNameConflict(t *testing.T) {
	task := calcDistanceTask()
	task.userOutputFields = []string{"distance"}
	// an existing user output column named "distance" forces the "_distance" name
	require.NoError(t, task.applyCalcDistanceToPlan())
	assert.Equal(t, "_distance", task.queryParams.calcDistance.distanceOutputName)
	assert.Equal(t, "_distance", task.plan.GetQuery().GetCalcDistance().GetDistanceOutputName())

	// a schema field (via translatedOutputFields) named "distance" is also a conflict
	task = calcDistanceTask()
	task.translatedOutputFields = []string{"distance"}
	require.NoError(t, task.applyCalcDistanceToPlan())
	assert.Equal(t, "_distance", task.plan.GetQuery().GetCalcDistance().GetDistanceOutputName())

	// names that do not collide leave the default column name untouched
	task = calcDistanceTask()
	task.translatedOutputFields = []string{"vec"}
	require.NoError(t, task.applyCalcDistanceToPlan())
	assert.Equal(t, "distance", task.plan.GetQuery().GetCalcDistance().GetDistanceOutputName())
}

func TestApplyCalcDistanceToPlan_InjectNoDuplicate(t *testing.T) {
	task := calcDistanceTask()
	task.plan.OutputFieldIds = []int64{100, 101}
	require.NoError(t, task.applyCalcDistanceToPlan())
	ids := task.plan.GetOutputFieldIds()
	count := 0
	for _, id := range ids {
		if id == 101 {
			count++
		}
	}
	assert.Equal(t, 1, count, "vector field id must be injected at most once")
	assert.Equal(t, []int64{100, 101}, ids)
}
