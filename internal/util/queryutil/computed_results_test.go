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
	"encoding/json"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func computedFixture(ids, timestamps []int64, values ...int64) *internalpb.RetrieveResults {
	data := &schemapb.FieldData{FieldId: 1, FieldName: "matches", Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}}}}
	return &internalpb.RetrieveResults{
		Ids:             &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}},
		FieldsData:      []*schemapb.FieldData{{FieldId: 1, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: timestamps}}}}}},
		ComputedResults: []*schemapb.FieldData{data},
	}
}

func computedValues(r *internalpb.RetrieveResults) []int64 {
	return r.ComputedResults[0].GetScalars().GetLongData().GetData()
}

func TestQueryComputedResultsMergeReplaceAndSlice(t *testing.T) {
	a := computedFixture([]int64{1, 3}, []int64{10, 10}, 100, 300)
	b := computedFixture([]int64{2, 3}, []int64{20, 20}, 200, 1<<62+1)
	op := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, 1<<20, 10, nil)
	outputs, err := op.Run(context.Background(), nil, []*internalpb.RetrieveResults{a, b})
	require.NoError(t, err)
	merged := outputs[0].(*internalpb.RetrieveResults)
	require.Equal(t, []int64{1, 2, 3}, merged.GetIds().GetIntId().GetData())
	require.Equal(t, int64(1<<62+1), computedValues(merged)[2])
	sliced, err := rangeSliceRetrieveResults(merged, 1, 3)
	require.NoError(t, err)
	require.Equal(t, int64(200), computedValues(sliced)[0])
	require.Equal(t, int64(1<<62+1), computedValues(sliced)[1])
	outputs, err = NewSliceOperator(1, 100).Run(context.Background(), nil, merged)
	require.NoError(t, err)
	require.NotNil(t, outputs[0].(*internalpb.RetrieveResults).ComputedResults)
	require.Empty(t, computedValues(outputs[0].(*internalpb.RetrieveResults)))
	require.Len(t, computedValues(merged), 3)
}

func TestQueryComputedResultsEmptyAndMalformed(t *testing.T) {
	op := NewSortAndCheckPKOperator(reduce.IReduceNoOrder, nil)
	for _, input := range []*internalpb.RetrieveResults{computedFixture(nil, nil), computedJSONFixture(nil, nil, "")} {
		t.Run(input.ComputedResults[0].Type.String(), func(t *testing.T) {
			outputs, err := op.Run(context.Background(), nil, []*internalpb.RetrieveResults{input})
			require.NoError(t, err)
			wire, err := proto.Marshal(outputs[0].(*internalpb.RetrieveResults))
			require.NoError(t, err)
			decoded := &internalpb.RetrieveResults{}
			require.NoError(t, proto.Unmarshal(wire, decoded))
			// Empty reduction must preserve the column's name, type and payload
			// oneof through RPC serialization, including Grep's zero-row JSON.
			require.Len(t, decoded.ComputedResults, 1)
			require.True(t, proto.Equal(input.ComputedResults[0], decoded.ComputedResults[0]))
		})
	}
	malformed := computedFixture([]int64{1}, []int64{1})
	_, err := op.Run(context.Background(), nil, []*internalpb.RetrieveResults{malformed})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	first := computedFixture([]int64{1}, []int64{1}, 100)
	second := computedFixture([]int64{2}, []int64{1}, 200)
	second.ComputedResults = nil
	_, err = op.Run(context.Background(), nil, []*internalpb.RetrieveResults{first, second})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestQueryComputedResultsPageMaterialization(t *testing.T) {
	for _, tc := range []struct {
		name          string
		inputs        []*internalpb.RetrieveResults
		limit, offset int64
		ids           []int64
		values        []int64
	}{
		{"multiple shards", []*internalpb.RetrieveResults{computedFixture([]int64{1, 3}, []int64{10, 30}, 100, 300), computedFixture([]int64{2, 4}, []int64{20, 40}, 200, 400)}, 2, 1, []int64{2, 3}, []int64{200, 300}},
		{"single shard", []*internalpb.RetrieveResults{computedFixture([]int64{1, 3}, []int64{10, 30}, 100, 300)}, 1, 1, []int64{3}, []int64{300}},
		{"empty shards", []*internalpb.RetrieveResults{nil, computedFixture(nil, nil)}, 2, 0, nil, nil},
		{"single shard empty page", []*internalpb.RetrieveResults{computedFixture([]int64{1}, []int64{10}, 100)}, 1, 10, nil, nil},
		{"multiple shards empty page", []*internalpb.RetrieveResults{computedFixture([]int64{1}, []int64{10}, 100), computedFixture([]int64{2}, []int64{20}, 200)}, 1, 10, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inputs := clonePageInputs(tc.inputs)
			out, err := NewSortAndCheckPKPageOperator(reduce.IReduceNoOrder, nil, tc.limit, tc.offset).Run(context.Background(), nil, inputs)
			require.NoError(t, err)
			got := out[0].(*internalpb.RetrieveResults)
			require.Equal(t, tc.ids, got.GetIds().GetIntId().GetData())
			require.NotNil(t, got.GetComputedResults()[0])
			require.Len(t, computedValues(got), len(tc.values))
			for i, value := range tc.values {
				require.Equal(t, value, computedValues(got)[i])
			}
			// Compare against the separate reduce and slice operators, including
			// their empty-page contract, and verify source results were not mutated.
			merged, err := NewSortAndCheckPKOperator(reduce.IReduceNoOrder, nil).Run(context.Background(), nil, clonePageInputs(tc.inputs))
			require.NoError(t, err)
			want, err := NewSliceOperator(tc.limit, tc.offset).Run(context.Background(), nil, merged[0])
			require.NoError(t, err)
			require.True(t, proto.Equal(want[0].(*internalpb.RetrieveResults), got))
			for i := range inputs {
				require.True(t, proto.Equal(tc.inputs[i], inputs[i]))
			}
		})
	}
}

func TestQueryComputedResultsPageValidatesDiscardedData(t *testing.T) {
	for _, name := range []string{"single", "outside page", "filtered empty shard"} {
		t.Run(name, func(t *testing.T) {
			good := computedFixture([]int64{1}, []int64{10}, 100)
			bad := computedFixture([]int64{2}, []int64{20}, 200)
			bad.ComputedResults[0] = nil
			inputs := []*internalpb.RetrieveResults{good, bad}
			switch name {
			case "single":
				inputs = []*internalpb.RetrieveResults{bad}
			case "filtered empty shard":
				inputs[1] = computedFixture(nil, nil, 200)
			}
			_, err := NewSortAndCheckPKPageOperator(reduce.IReduceNoOrder, nil, 1, 0).Run(context.Background(), nil, inputs)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
		})
	}
}

func TestQueryComputedResultsElementPage(t *testing.T) {
	for _, offset := range []int64{1, 100} {
		inputs := []*internalpb.RetrieveResults{computedFixture([]int64{1}, []int64{10}, 100), computedFixture([]int64{2}, []int64{20}, 200)}
		for _, r := range inputs {
			r.ElementLevel = true
			r.ElementIndices = []*internalpb.ElementIndices{{Indices: []int32{0, 1}}}
		}
		out, err := NewSortAndCheckPKPageOperator(reduce.IReduceNoOrder, nil, 2, offset).Run(context.Background(), nil, inputs)
		require.NoError(t, err)
		got := out[0].(*internalpb.RetrieveResults)
		require.True(t, got.ElementLevel)
		require.NotNil(t, got.ComputedResults)
		if offset == 1 {
			require.Equal(t, []int64{1, 2}, got.GetIds().GetIntId().GetData())
			require.Equal(t, []int32{1}, got.ElementIndices[0].Indices)
			require.Equal(t, []int32{0}, got.ElementIndices[1].Indices)
			require.Equal(t, int64(100), computedValues(got)[0])
			require.Equal(t, int64(200), computedValues(got)[1])
		} else {
			require.Empty(t, computedValues(got))
		}
	}
	input := computedFixture([]int64{1}, []int64{10}, 100)
	input.ElementLevel = true
	input.ElementIndices = []*internalpb.ElementIndices{{Indices: []int32{0}}}
	out, err := NewSliceOperator(1, 100).Run(context.Background(), nil, input)
	require.NoError(t, err)
	got := out[0].(*internalpb.RetrieveResults)
	require.True(t, got.ElementLevel)
	require.NotNil(t, got.ComputedResults)
	require.Empty(t, computedValues(got))
}

// Computed values consume the same merge budget as ordinary fields, including
// when a later row replaces the same PK with a larger or smaller computed value.
func TestQueryComputedResultsIncludedInSizeBudget(t *testing.T) {
	for _, lengths := range [][]int{{1, 200, 20}, {200, 1, 20}} {
		input := computedFixture([]int64{1, 1, 1, 2}, []int64{1, 2, 3, 1}, 0, 0, 0, 0)
		values := make([]string, 4)
		for i, n := range append(append([]int(nil), lengths...), 100) {
			values[i] = strings.Repeat("x", n)
		}
		column := input.ComputedResults[0]
		column.Type = schemapb.DataType_VarChar
		column.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}}}
		// Each row contains one 8-byte timestamp; only two PKs are retained.
		require.Equal(t, int64(8+lengths[0]), newRowSizeCalculator(input).rowSize(0))
		peak := int64(208) // Largest intermediate retained row (8-byte timestamp + 200 bytes).
		for _, limit := range []int64{peak, 0, -1} {
			result, err := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, limit, -1, nil).Run(context.Background(), nil, []*internalpb.RetrieveResults{input})
			require.NoError(t, err)
			got := result[0].(*internalpb.RetrieveResults)
			require.Equal(t, []int64{1, 2}, got.GetIds().GetIntId().GetData())
			require.Equal(t, values[2], got.ComputedResults[0].GetScalars().GetStringData().Data[0])
		}
		_, err := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, peak-1, -1, nil).Run(context.Background(), nil, []*internalpb.RetrieveResults{input})
		require.Error(t, err)
	}
}

func computedJSONFixture(ids, timestamps []int64, label string) *internalpb.RetrieveResults {
	result := computedFixture(ids, timestamps)
	rows := make([][]byte, len(ids))
	for i, id := range ids {
		payload, err := json.Marshal([]map[string]any{{"content": label, "starts": []int64{id}, "ends": []int64{id + 1}}})
		if err != nil {
			panic(err)
		}
		rows[i] = payload
	}
	result.ComputedResults[0] = &schemapb.FieldData{FieldName: "matches", Type: schemapb.DataType_JSON, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: rows}}}}}
	return result
}

func TestComputedColumnOrderAndValidity(t *testing.T) {
	a := computedFixture([]int64{1}, []int64{1}, 10)
	b := computedFixture([]int64{2}, []int64{2}, 20)
	for _, r := range []*internalpb.RetrieveResults{a, b} {
		second := proto.Clone(r.ComputedResults[0]).(*schemapb.FieldData)
		second.FieldName = "other"
		r.ComputedResults = append(r.ComputedResults, second)
	}
	b.ComputedResults[0], b.ComputedResults[1] = b.ComputedResults[1], b.ComputedResults[0]
	b.ComputedResults[1].GetScalars().ValidData = []bool{false}
	result, err := selectComputedResults([]*internalpb.RetrieveResults{a, b}, []rowRef{{0, 0}, {1, 0}, {0, 0}})
	require.NoError(t, err)
	require.Equal(t, []int64{10, 20, 10}, result[0].GetScalars().GetLongData().Data)
	require.Equal(t, []bool{true, false, true}, result[0].GetScalars().ValidData)
	require.Equal(t, "matches", result[0].FieldName)
	require.Equal(t, "other", result[1].FieldName)
}

func TestComputedMalformedContracts(t *testing.T) {
	for name, mutate := range map[string]func(*internalpb.RetrieveResults){
		"duplicate column": func(r *internalpb.RetrieveResults) {
			r.ComputedResults = append(r.ComputedResults, r.ComputedResults[0])
		},
		"missing name": func(r *internalpb.RetrieveResults) { r.ComputedResults[0].FieldName = "" },
		"unsupported struct": func(r *internalpb.RetrieveResults) {
			r.ComputedResults[0].Type = schemapb.DataType_ArrayOfStruct
			r.ComputedResults[0].Field = &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{}}
		},
		"unsupported array": func(r *internalpb.RetrieveResults) {
			r.ComputedResults[0].Type = schemapb.DataType_Array
			r.ComputedResults[0].Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{}}}}
		},
		"wrong payload type": func(r *internalpb.RetrieveResults) { r.ComputedResults[0].Type = schemapb.DataType_Int64 },
		"row validity":       func(r *internalpb.RetrieveResults) { r.ComputedResults[0].GetScalars().ValidData = []bool{true, false} },
	} {
		t.Run(name, func(t *testing.T) {
			r := computedJSONFixture([]int64{1}, []int64{1}, "a")
			mutate(r)
			_, err := prepareComputedResults([]*internalpb.RetrieveResults{r})
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		})
	}
	for _, op := range []Operator{NewConcatAndCheckPKOperator(nil), NewDeduplicatePKOperator(0, nil)} {
		empty := computedJSONFixture(nil, nil, "")
		out, err := op.Run(context.Background(), nil, []*internalpb.RetrieveResults{nil, empty})
		require.NoError(t, err)
		require.Len(t, out[0].(*internalpb.RetrieveResults).ComputedResults, 1)
	}
}

func TestComputedConflictingValidity(t *testing.T) {
	input := computedFixture([]int64{1}, []int64{1}, 10)
	data := input.ComputedResults[0]
	data.ValidData = []bool{true}
	data.GetScalars().ValidData = []bool{false}
	_, err := prepareComputedResults([]*internalpb.RetrieveResults{input})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestComputedValidationAtReduceBoundary(t *testing.T) {
	for _, op := range []Operator{
		NewSortAndCheckPKOperator(reduce.IReduceNoOrder, nil),
		NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, 0, -1, nil),
		NewConcatAndCheckPKOperator(nil),
		NewDeduplicatePKOperator(0, nil),
		NewSortAndCheckPKPageOperator(reduce.IReduceNoOrder, nil, 1, 0),
	} {
		t.Run(op.Name(), func(t *testing.T) {
			good := computedJSONFixture([]int64{1}, []int64{1}, "good")
			// This input would be discarded by empty-result filtering. Its bad
			// payload/type mismatch must still be rejected at the reduce boundary.
			bad := computedJSONFixture([]int64{2}, []int64{2}, "bad")
			bad.FieldsData = nil
			bad.ComputedResults[0].Type = schemapb.DataType_Int64
			_, err := op.Run(context.Background(), nil, []*internalpb.RetrieveResults{good, bad})
			require.ErrorIs(t, err, merr.ErrServiceInternal)
		})
	}
}

func TestComputedSelectionDoesNotRevalidatePayloads(t *testing.T) {
	calls := 0
	var original func(*schemapb.FieldData, int) error
	patch := mockey.Mock(validateComputedField).To(func(fd *schemapb.FieldData, rows int) error {
		calls++
		return original(fd, rows)
	}).Origin(&original).Build()
	defer patch.UnPatch()
	a := computedJSONFixture([]int64{1}, []int64{1}, "first")
	b := computedJSONFixture([]int64{2}, []int64{2}, "second")
	out, err := NewConcatAndCheckPKOperator(nil).Run(context.Background(), nil, []*internalpb.RetrieveResults{a, b})
	require.NoError(t, err)
	// Each input contains one opaque JSON computed column.
	require.Equal(t, 2, calls)
	merged := out[0].(*internalpb.RetrieveResults)
	reordered, err := (&OrderByLimitOperator{}).reorderResult(merged, []int{1, 0})
	require.NoError(t, err)
	page, err := rangeSliceRetrieveResults(reordered, 0, 1)
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Equal(t, []int64{2}, page.Ids.GetIntId().Data)
	require.Equal(t, b.ComputedResults[0].GetScalars().GetJsonData().Data[0], page.ComputedResults[0].GetScalars().GetJsonData().Data[0])
}

func TestComputedJSONResultsMergeReplaceAndPage(t *testing.T) {
	makeInput := func(ids, timestamps []int64, content string) *internalpb.RetrieveResults {
		r := computedFixture(ids, timestamps)
		payloads := make([][]byte, len(ids))
		for i := range payloads {
			payloads[i] = []byte(`[{"content":"` + content + `","starts":[0,8],"ends":[3,11]}]`)
		}
		r.ComputedResults = []*schemapb.FieldData{{
			FieldName: "matches", Type: schemapb.DataType_JSON,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: payloads}}}},
		}}
		return r
	}
	a := makeInput([]int64{1, 3}, []int64{1, 1}, "old")
	b := makeInput([]int64{2, 3}, []int64{2, 2}, "new")
	b.ComputedResults[0].FieldId = 99 // Producer-local IDs are not computed-column identity.
	before := proto.Clone(b)
	out, err := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, 0, -1, nil).Run(context.Background(), nil, []*internalpb.RetrieveResults{a, b})
	require.NoError(t, err)
	wire, err := proto.Marshal(out[0].(*internalpb.RetrieveResults))
	require.NoError(t, err)
	decoded := &internalpb.RetrieveResults{}
	require.NoError(t, proto.Unmarshal(wire, decoded))
	page, err := rangeSliceRetrieveResults(decoded, 1, 3)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3}, page.Ids.GetIntId().Data)
	require.Equal(t, schemapb.DataType_JSON, page.ComputedResults[0].Type)
	for _, payload := range page.ComputedResults[0].GetScalars().GetJsonData().Data {
		require.Equal(t, b.ComputedResults[0].GetScalars().GetJsonData().Data[0], payload)
	}
	require.True(t, proto.Equal(before, b))
}

func TestComputedJSONMergeSizeBudget(t *testing.T) {
	inputs := []*internalpb.RetrieveResults{computedJSONFixture([]int64{1}, []int64{1}, "first"), computedJSONFixture([]int64{2}, []int64{1}, "second")}
	var total int64
	for _, input := range inputs {
		total += 8 + int64(len(input.ComputedResults[0].GetScalars().GetJsonData().Data[0]))
	}
	for _, ordered := range []bool{false, true} {
		for _, budget := range []int64{total, total - 1, 0, -1} {
			var op Operator = NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, budget, -1, nil)
			if ordered {
				op = NewDeduplicatePKOperator(budget, nil)
			}
			result, err := op.Run(context.Background(), nil, inputs)
			if budget == total-1 {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.Len(t, result[0].(*internalpb.RetrieveResults).ComputedResults[0].GetScalars().GetJsonData().Data, 2)
			}
		}
	}
}
