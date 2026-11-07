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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
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
	got, err := validateMetricTypeForVector("IP", schemapb.DataType_SparseFloatVector)
	require.NoError(t, err)
	assert.Equal(t, "IP", got)
	for _, m := range []string{"L2", "COSINE"} {
		_, err = validateMetricTypeForVector(m, schemapb.DataType_SparseFloatVector)
		require.Error(t, err)
	}
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

func TestValidateRefVectorValues(t *testing.T) {
	require.NoError(t, validateRefVectorValues(
		typeutil.Float32ArrayToBytes([]float32{1, 2, 3, 4}), schemapb.DataType_FloatVector))
	require.Error(t, validateRefVectorValues(
		typeutil.Float32ArrayToBytes([]float32{1, float32(math.NaN())}), schemapb.DataType_FloatVector))
	require.Error(t, validateRefVectorValues(
		typeutil.Float32ArrayToBytes([]float32{1, float32(math.Inf(1))}), schemapb.DataType_FloatVector))
	// a large finite float32 narrows to +Inf in float16
	require.Error(t, validateRefVectorValues(
		typeutil.Float32ArrayToFloat16Bytes([]float32{1, 1e20}), schemapb.DataType_Float16Vector))
	require.Error(t, validateRefVectorValues(
		typeutil.Float32ArrayToBFloat16Bytes([]float32{1, float32(math.NaN())}), schemapb.DataType_BFloat16Vector))
	// int8 / sparse / binary carry no NaN concept
	require.NoError(t, validateRefVectorValues([]byte{1, 2}, schemapb.DataType_Int8Vector))
	require.NoError(t, validateRefVectorValues([]byte{1}, schemapb.DataType_SparseFloatVector))
	require.NoError(t, validateRefVectorValues([]byte{1}, schemapb.DataType_BinaryVector))
}

// TestResolveCalcDistance_Float16InfRejected pins the NaN/Inf validation on the
// resolve path: a large finite input becomes Inf when narrowed to float16 and
// must be rejected with an explicit error instead of yielding an all-Inf
// distance column.
func TestResolveCalcDistance_Float16InfRejected(t *testing.T) {
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: "cd",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_Float16Vector,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
		},
	})
	task := &QueryTask{baseTask: baseTask{}, schema: schema}
	spec := &calcDistanceSpec{fieldName: "vec", metricType: "L2", vectorInput: []byte(`[1,2,3,1e20]`)}
	require.Error(t, task.resolveCalcDistance(spec, context.Background(), nil))

	spec = &calcDistanceSpec{fieldName: "vec", metricType: "L2", vectorInput: []byte(`[1,2,3,4]`)}
	require.NoError(t, task.resolveCalcDistance(spec, context.Background(), nil))
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

func makeTestFloatField(fieldID int64, name string, vals []float32) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Float,
		FieldId:   fieldID,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: vals}},
			},
		},
	}
}

// TestCalcDistanceColumn_SurvivesProxyReduce verifies the synthetic distance
// column (common.CalcDistanceFieldID) survives the proxy's cross-QN reduce:
// the plain pipeline merges rows by PK keeping every column, and the ORDER BY
// pipeline's FieldIDRemapOperator keeps it because the proxy appends the
// reserved id to the remap target list. Losing or misaligning the column would
// break SDK reconstruction (the class of bug the ORDER BY thread described).
func TestCalcDistanceColumn_SurvivesProxyReduce(t *testing.T) {
	schema := testSchema()
	newResult := func(ids []int64, dis []float32) *internalpb.RetrieveResults {
		return &internalpb.RetrieveResults{
			Ids: makeTestIntIDs(ids),
			FieldsData: []*schemapb.FieldData{
				makeTestInt64Field(100, "pk", ids),
				makeTestInt64Field(common.TimeStampField, "timestamp", make([]int64, len(ids))),
				makeTestFloatField(common.CalcDistanceFieldID, "distance", dis),
			},
		}
	}
	r1 := newResult([]int64{1, 3}, []float32{1.5, 3.5})
	r2 := newResult([]int64{2, 4}, []float32{2.5, 4.5})

	getDistance := func(t *testing.T, result *milvuspb.QueryResults) []float32 {
		t.Helper()
		for _, fd := range result.GetFieldsData() {
			if fd.GetFieldId() == common.CalcDistanceFieldID {
				return fd.GetScalars().GetFloatData().GetData()
			}
		}
		return nil
	}

	t.Run("plain", func(t *testing.T) {
		pipeline, err := NewQueryPipeline(schema, 10, 0, reduce.IReduceNoOrder,
			nil, nil, nil, nil, []int64{100, common.CalcDistanceFieldID})
		require.NoError(t, err)
		result, err := pipeline.Execute(context.Background(), []*internalpb.RetrieveResults{r1, r2})
		require.NoError(t, err)
		dis := getDistance(t, result)
		require.NotNil(t, dis, "synthetic column must survive the plain reduce")
		assert.Equal(t, []float32{1.5, 2.5, 3.5, 4.5}, dis)
	})

	t.Run("order by", func(t *testing.T) {
		pipeline, err := NewQueryPipeline(schema, 10, 0, reduce.IReduceNoOrder,
			[]*orderby.OrderByField{{FieldID: 100, Ascending: true}}, nil, nil, nil,
			[]int64{100, common.CalcDistanceFieldID})
		require.NoError(t, err)
		result, err := pipeline.Execute(context.Background(), []*internalpb.RetrieveResults{r1, r2})
		require.NoError(t, err)
		dis := getDistance(t, result)
		require.NotNil(t, dis, "ORDER BY remap must keep the synthetic column")
		assert.Equal(t, []float32{1.5, 2.5, 3.5, 4.5}, dis)
	})

	t.Run("nullable valid data", func(t *testing.T) {
		// a nullable target field mirrors ValidData onto the distance column;
		// the merge must preserve nullness (the column is not in the schema, so
		// this exercises the template-ValidData path of buildMergedFieldDataRange)
		nr1 := newResult([]int64{1, 3}, []float32{1.5, 0})
		nr1.FieldsData[2].ValidData = []bool{true, false}
		nr2 := newResult([]int64{2, 4}, []float32{2.5, 4.5})
		nr2.FieldsData[2].ValidData = []bool{true, true}

		pipeline, err := NewQueryPipeline(schema, 10, 0, reduce.IReduceNoOrder,
			nil, nil, nil, nil, []int64{100, common.CalcDistanceFieldID})
		require.NoError(t, err)
		result, err := pipeline.Execute(context.Background(), []*internalpb.RetrieveResults{nr1, nr2})
		require.NoError(t, err)
		var dis []float32
		var vd []bool
		for _, fd := range result.GetFieldsData() {
			if fd.GetFieldId() == common.CalcDistanceFieldID {
				dis = fd.GetScalars().GetFloatData().GetData()
				vd = fd.GetValidData()
			}
		}
		require.NotNil(t, dis)
		require.Equal(t, []bool{true, true, false, true}, vd, "nullness must survive the cross-shard merge")
		assert.Equal(t, []float32{1.5, 2.5, 0, 4.5}, dis)
	})
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

func TestApplyCalcDistanceToPlan_DynamicAndPkCollision(t *testing.T) {
	// a requested dynamic-field key literally named "distance" ($meta["distance"])
	// must force the synthetic column to "_distance"
	task := calcDistanceTask()
	task.userDynamicFields = []string{"distance"}
	require.NoError(t, task.applyCalcDistanceToPlan())
	assert.Equal(t, "_distance", task.plan.GetQuery().GetCalcDistance().GetDistanceOutputName())

	// a primary-key field literally named "distance" must also collide, since
	// the PK column is always present in the output even when not requested
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: "cd",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "distance", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
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
	task = &QueryTask{
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
	require.NoError(t, task.applyCalcDistanceToPlan())
	assert.Equal(t, "_distance", task.plan.GetQuery().GetCalcDistance().GetDistanceOutputName())
}

// mockQueryRunner is a taskmodel.TaskNode stub that also implements
// taskmodel.QueryRunner, returning a canned QueryResults for the internal
// reference-vector query of resolveExpressionVector.
type mockQueryRunner struct {
	results *milvuspb.QueryResults
	err     error
}

func (m *mockQueryRunner) GetMetaCache() metacache.Cache        { return nil }
func (m *mockQueryRunner) MixCoord() types.MixCoordClient       { return nil }
func (m *mockQueryRunner) LBPolicy() shardclient.LBPolicy       { return nil }
func (m *mockQueryRunner) ShardMgr() shardclient.ShardClientMgr { return nil }
func (m *mockQueryRunner) ChMgr() channelmgr.ChannelsMgr        { return nil }
func (m *mockQueryRunner) TsoAllocator() taskmodel.TsoAllocator { return nil }
func (m *mockQueryRunner) CheckClusterPrivilege(context.Context, interface{}, string, string) error {
	return nil
}

func (m *mockQueryRunner) ExecuteQuery(context.Context, taskmodel.Task, trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
	return m.results, segcore.StorageCost{}, m.err
}

// expressionQueryTask builds a QueryTask whose node is a mockQueryRunner that
// returns the canned results, with a float vector field named "vec" (id 101).
func expressionQueryTask(results *milvuspb.QueryResults) *QueryTask {
	schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
		Name: "cd",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
		},
	})
	return &QueryTask{
		baseTask: baseTask{},
		schema:   schema,
		request: &milvuspb.QueryRequest{
			DbName: "db", CollectionName: "cd", PartitionNames: []string{},
		},
		node: &mockQueryRunner{results: results},
	}
}

func exprResults(fieldsData ...*schemapb.FieldData) *milvuspb.QueryResults {
	return &milvuspb.QueryResults{
		Status:     &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		FieldsData: fieldsData,
	}
}

func vectorFieldData(dataType schemapb.DataType, vf *schemapb.VectorField) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:    dataType,
		FieldId: 101,
		Field:   &schemapb.FieldData_Vectors{Vectors: vf},
	}
}

// TestResolveExpressionVector covers the expression-form reference-vector
// resolution: 0-row / multi-row / status errors and per-type canonical byte
// extraction from the internal query result.
func TestResolveExpressionVector(t *testing.T) {
	ctx := context.Background()

	t.Run("zero rows", func(t *testing.T) {
		task := expressionQueryTask(exprResults(vectorFieldData(schemapb.DataType_FloatVector, &schemapb.VectorField{
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{}}},
		})))
		_, err := task.resolveExpressionVector("pk == 0", "vec", 4, ctx, nil)
		require.Error(t, err)
	})

	t.Run("multi rows", func(t *testing.T) {
		task := expressionQueryTask(exprResults(vectorFieldData(schemapb.DataType_FloatVector, &schemapb.VectorField{
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6, 7, 8}}},
		})))
		_, err := task.resolveExpressionVector("pk >= 0", "vec", 4, ctx, nil)
		require.Error(t, err)
	})

	t.Run("status error", func(t *testing.T) {
		task := expressionQueryTask(&milvuspb.QueryResults{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "boom"},
		})
		_, err := task.resolveExpressionVector("pk == 1", "vec", 4, ctx, nil)
		require.Error(t, err)
	})

	t.Run("float vector", func(t *testing.T) {
		task := expressionQueryTask(exprResults(vectorFieldData(schemapb.DataType_FloatVector, &schemapb.VectorField{
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}},
		})))
		got, err := task.resolveExpressionVector("pk == 1", "vec", 4, ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, typeutil.Float32ArrayToBytes([]float32{1, 2, 3, 4}), got)
	})

	t.Run("binary vector", func(t *testing.T) {
		task := expressionQueryTask(exprResults(vectorFieldData(schemapb.DataType_BinaryVector, &schemapb.VectorField{
			Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0x0F, 0xAA}},
		})))
		got, err := task.resolveExpressionVector("pk == 1", "vec", 16, ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, []byte{0x0F, 0xAA}, got)
	})

	t.Run("float16 vector", func(t *testing.T) {
		task := expressionQueryTask(exprResults(vectorFieldData(schemapb.DataType_Float16Vector, &schemapb.VectorField{
			Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
		})))
		got, err := task.resolveExpressionVector("pk == 1", "vec", 4, ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8}, got)
	})

	t.Run("bfloat16 vector", func(t *testing.T) {
		task := expressionQueryTask(exprResults(vectorFieldData(schemapb.DataType_BFloat16Vector, &schemapb.VectorField{
			Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{1, 2, 3, 4}},
		})))
		got, err := task.resolveExpressionVector("pk == 1", "vec", 2, ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3, 4}, got)
	})

	t.Run("int8 vector", func(t *testing.T) {
		task := expressionQueryTask(exprResults(vectorFieldData(schemapb.DataType_Int8Vector, &schemapb.VectorField{
			Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{1, 2, 3, 4}},
		})))
		got, err := task.resolveExpressionVector("pk == 1", "vec", 4, ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3, 4}, got)
	})

	t.Run("sparse vector", func(t *testing.T) {
		row := typeutil.CreateSparseFloatRow([]uint32{0, 2}, []float32{1, 2})
		task := expressionQueryTask(exprResults(vectorFieldData(schemapb.DataType_SparseFloatVector, &schemapb.VectorField{
			Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Contents: [][]byte{row}}},
		})))
		got, err := task.resolveExpressionVector("pk == 1", "vec", 0, ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, row, got)
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		task := expressionQueryTask(exprResults(vectorFieldData(schemapb.DataType_FloatVector, &schemapb.VectorField{
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}},
		})))
		_, err := task.resolveExpressionVector("pk == 1", "vec", 4, ctx, nil)
		require.Error(t, err)
	})
}
