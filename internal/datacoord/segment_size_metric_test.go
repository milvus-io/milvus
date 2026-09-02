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

package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func setMetricForTest(t *testing.T, metric string, ceilingMB string) {
	t.Helper()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SizeMetric.Key, metric)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.MaxFullSegmentSize.Key, ceilingMB)
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SizeMetric.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.MaxFullSegmentSize.Key)
	})
}

func TestCapByCeiling(t *testing.T) {
	setMetricForTest(t, typeutil.SizeMetricWholeRow, "64")
	// wholeRow metric: ceiling is not consulted, no-op.
	assert.Equal(t, int64(1024*1024*1024), capByCeiling(1024*1024*1024))

	setMetricForTest(t, typeutil.SizeMetricMainIndex, "64")
	assert.Equal(t, int64(64*1024*1024), capByCeiling(1024*1024*1024))
	assert.Equal(t, int64(32*1024*1024), capByCeiling(32*1024*1024))

	// Disabled ceiling (-1) is a no-op under mainIndex.
	setMetricForTest(t, typeutil.SizeMetricMainIndex, "-1")
	assert.Equal(t, int64(1024*1024*1024), capByCeiling(1024*1024*1024))
}

func TestGetExpectedSegmentSizeCappedByCeiling(t *testing.T) {
	setMetricForTest(t, typeutil.SizeMetricMainIndex, "64")
	meta := &meta{indexMeta: &indexMeta{}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 10, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}}},
	}}
	// SegmentMaxSize default 1024MB is capped to the 64MB ceiling.
	assert.Equal(t, int64(64*1024*1024), getExpectedSegmentSize(meta, 1, schema))
}

func TestCalBySchemaPolicyMainIndex(t *testing.T) {
	setMetricForTest(t, typeutil.SizeMetricMainIndex, "-1")
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 10, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}}},
		{FieldID: 11, Name: "tag", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "256"}}},
	}}
	rows, err := calBySchemaPolicy(schema)
	require.NoError(t, err)
	threshold := Params.DataCoordCfg.SegmentMaxSize.GetAsFloat() * 1024 * 1024
	proportion := Params.DataCoordCfg.SegmentSealProportion.GetAsFloat()
	// budgetRows = proportion × maxSize / (dim × elem).
	assert.Equal(t, int(threshold*proportion/32), rows)
}

func TestCalBySchemaPolicyMainIndexCeiling(t *testing.T) {
	setMetricForTest(t, typeutil.SizeMetricMainIndex, "1")
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 10, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}}},
		{FieldID: 11, Name: "tag", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "256"}}},
	}}
	rows, err := calBySchemaPolicy(schema)
	require.NoError(t, err)
	// 1MB ceiling / (8+32+256 whole-row bytes per record) dominates the budget.
	assert.Less(t, rows, int(1024*1024/8))
}

func TestCalBySchemaPolicyMainIndexSparseOnly(t *testing.T) {
	setMetricForTest(t, typeutil.SizeMetricMainIndex, "-1")
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 11, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
	}}
	rows, err := calBySchemaPolicy(schema)
	require.NoError(t, err)
	// No dense vector field → whole-row fallback.
	assert.Equal(t, int(Params.DataCoordCfg.SegmentMaxSize.GetAsFloat()*1024*1024/float64(8+typeutil.GetSparseFloatVectorEstimateLength())), rows)
}
