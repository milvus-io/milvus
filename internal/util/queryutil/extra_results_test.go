// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queryutil

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/stretchr/testify/require"
)

func extraFixture(ids, timestamps []int64, values ...uint64) *internalpb.RetrieveResults {
	rows := make([]*commonpb.ResultValue, len(values))
	for i, v := range values {
		rows[i] = &commonpb.ResultValue{Value: &commonpb.ResultValue_Uint64Value{Uint64Value: v}}
	}
	return &internalpb.RetrieveResults{
		Ids:          &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}},
		FieldsData:   []*schemapb.FieldData{{FieldId: 1, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: timestamps}}}}}},
		ExtraResults: &commonpb.ResultData{Fields: map[string]*commonpb.ResultValue{"scan": {Value: &commonpb.ResultValue_ObjectValue{ObjectValue: &commonpb.ResultData{Fields: map[string]*commonpb.ResultValue{"matches": {Value: &commonpb.ResultValue_ListValue{ListValue: &commonpb.ResultList{Values: rows}}}}}}}}},
	}
}
func extraValues(r *internalpb.RetrieveResults) []*commonpb.ResultValue {
	return r.GetExtraResults().GetFields()["scan"].GetObjectValue().GetFields()["matches"].GetListValue().GetValues()
}

func TestQueryExtraResultsMergeReplaceAndSlice(t *testing.T) {
	a := extraFixture([]int64{1, 3}, []int64{10, 10}, 100, 300)
	b := extraFixture([]int64{2, 3}, []int64{20, 20}, 200, 1<<63+1)
	op := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, 1<<20, 10, nil)
	outputs, err := op.Run(context.Background(), nil, []*internalpb.RetrieveResults{a, b})
	require.NoError(t, err)
	merged := outputs[0].(*internalpb.RetrieveResults)
	require.Equal(t, []int64{1, 2, 3}, merged.GetIds().GetIntId().GetData())
	require.Equal(t, uint64(1<<63+1), extraValues(merged)[2].GetUint64Value())
	sliced, err := rangeSliceRetrieveResults(merged, 1, 3)
	require.NoError(t, err)
	require.Equal(t, uint64(200), extraValues(sliced)[0].GetUint64Value())
	require.Equal(t, uint64(1<<63+1), extraValues(sliced)[1].GetUint64Value())
	outputs, err = NewSliceOperator(1, 100).Run(context.Background(), nil, merged)
	require.NoError(t, err)
	require.NotNil(t, outputs[0].(*internalpb.RetrieveResults).ExtraResults)
	require.Empty(t, extraValues(outputs[0].(*internalpb.RetrieveResults)))
	require.Len(t, extraValues(merged), 3)
}
func TestQueryExtraResultsEmptyAndMalformed(t *testing.T) {
	op := NewSortAndCheckPKOperator(reduce.IReduceNoOrder, nil)
	outputs, err := op.Run(context.Background(), nil, []*internalpb.RetrieveResults{extraFixture(nil, nil)})
	require.NoError(t, err)
	require.NotNil(t, outputs[0].(*internalpb.RetrieveResults).ExtraResults)
	malformed := extraFixture([]int64{1}, []int64{1})
	_, err = op.Run(context.Background(), nil, []*internalpb.RetrieveResults{malformed})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	first := extraFixture([]int64{1}, []int64{1}, 100)
	second := extraFixture([]int64{2}, []int64{1}, 200)
	second.ExtraResults = nil
	_, err = op.Run(context.Background(), nil, []*internalpb.RetrieveResults{first, second})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}
