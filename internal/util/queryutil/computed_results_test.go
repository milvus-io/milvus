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
	outputs, err := op.Run(context.Background(), nil, []*internalpb.RetrieveResults{computedFixture(nil, nil)})
	require.NoError(t, err)
	require.NotNil(t, outputs[0].(*internalpb.RetrieveResults).ComputedResults)
	malformed := computedFixture([]int64{1}, []int64{1})
	_, err = op.Run(context.Background(), nil, []*internalpb.RetrieveResults{malformed})
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
			if name == "single" {
				inputs = []*internalpb.RetrieveResults{bad}
			} else if name == "filtered empty shard" {
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

// Computed values do not consume the ordinary-field merge budget, including
// when a later row replaces the same PK with a larger or smaller computed value.
func TestQueryComputedResultsExcludedFromSizeBudget(t *testing.T) {
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
		require.Equal(t, int64(8), newRowSizeCalculator(input).rowSize(0))
		peak := int64(16)
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

func computedStructFixture(ids, timestamps []int64, label string) *internalpb.RetrieveResults {
	result := computedFixture(ids, timestamps)
	fields := make([]*schemapb.FieldData, 3)
	for i, name := range []string{"content", "starts", "ends"} {
		element := schemapb.DataType_Array
		if i == 0 {
			element = schemapb.DataType_VarChar
		}
		rows := make([]*schemapb.ScalarField, len(ids))
		for j := range ids {
			if i == 0 {
				rows[j] = &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{label}}}}
			} else {
				rows[j] = &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int64, Data: []*schemapb.ScalarField{{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{ids[j], ids[j] + 1}}}}}}}}
			}
		}
		fields[i] = &schemapb.FieldData{FieldId: int64(i + 1), FieldName: name, Type: schemapb.DataType_Array, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: element, Data: rows}}}}}
	}
	result.ComputedResults[0] = &schemapb.FieldData{FieldId: 0, FieldName: "matches", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: fields}}}
	return result
}

func TestComputedStructWorkerRPCProxyPage(t *testing.T) {
	ctx := context.Background()
	a := computedStructFixture([]int64{1, 3}, []int64{1, 1}, "old")
	b := computedStructFixture([]int64{2, 3}, []int64{2, 2}, "new")
	out, err := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, 0, -1, nil).Run(ctx, nil, []*internalpb.RetrieveResults{a, b})
	require.NoError(t, err)
	worker := out[0].(*internalpb.RetrieveResults)
	wire, err := proto.Marshal(worker)
	require.NoError(t, err)
	decoded := &internalpb.RetrieveResults{}
	require.NoError(t, proto.Unmarshal(wire, decoded))
	other := computedStructFixture([]int64{4}, []int64{1}, "other")
	page, err := NewSortAndCheckPKPageOperator(reduce.IReduceNoOrder, nil, 2, 1).Run(ctx, nil, []*internalpb.RetrieveResults{decoded, other})
	require.NoError(t, err)
	result := page[0].(*internalpb.RetrieveResults)
	require.Equal(t, []int64{2, 3}, result.GetIds().GetIntId().Data)
	fields := result.ComputedResults[0].GetStructArrays().Fields
	for _, row := range fields[0].GetScalars().GetArrayData().Data {
		require.Equal(t, []string{"new"}, row.GetStringData().Data)
	}
	require.Equal(t, []int64{3, 4}, fields[1].GetScalars().GetArrayData().Data[1].GetArrayData().Data[0].GetLongData().Data)
	// A permutation follows the same source rows and does not modify its input.
	snapshot := proto.Clone(result)
	reordered, err := (&OrderByLimitOperator{}).reorderResult(result, []int{1, 0})
	require.NoError(t, err)
	require.Equal(t, []int64{3, 2}, reordered.GetIds().GetIntId().Data)
	require.Equal(t, []int64{3, 4}, reordered.ComputedResults[0].GetStructArrays().Fields[1].GetScalars().GetArrayData().Data[0].GetArrayData().Data[0].GetLongData().Data)
	require.True(t, proto.Equal(snapshot, result))
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
		"duplicate child name": func(r *internalpb.RetrieveResults) {
			r.ComputedResults[0].GetStructArrays().Fields[1].FieldName = "content"
		},
		"nested element type": func(r *internalpb.RetrieveResults) {
			r.ComputedResults[0].GetStructArrays().Fields[1].GetScalars().GetArrayData().Data[0].GetArrayData().ElementType = schemapb.DataType_VarChar
		},
		"different line counts": func(r *internalpb.RetrieveResults) {
			r.ComputedResults[0].GetStructArrays().Fields[1].GetScalars().GetArrayData().Data[0].GetArrayData().Data = nil
		},
		"row validity": func(r *internalpb.RetrieveResults) {
			r.ComputedResults[0].GetStructArrays().Fields[0].GetScalars().ValidData = []bool{true, false}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := computedStructFixture([]int64{1}, []int64{1}, "a")
			mutate(r)
			_, err := prepareComputedResults([]*internalpb.RetrieveResults{r})
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		})
	}
	for _, op := range []Operator{NewConcatAndCheckPKOperator(nil), NewDeduplicatePKOperator(0, nil)} {
		empty := computedStructFixture(nil, nil, "")
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
			good := computedStructFixture([]int64{1}, []int64{1}, "good")
			// This input would be discarded by empty-result filtering. Its bad
			// nested payload must still be rejected at the reduce boundary.
			bad := computedStructFixture([]int64{2}, []int64{2}, "bad")
			bad.FieldsData = nil
			bad.ComputedResults[0].GetStructArrays().Fields[1].GetScalars().GetArrayData().Data[0].GetArrayData().ElementType = schemapb.DataType_VarChar
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
	a := computedStructFixture([]int64{1}, []int64{1}, "first")
	b := computedStructFixture([]int64{2}, []int64{2}, "second")
	out, err := NewConcatAndCheckPKOperator(nil).Run(context.Background(), nil, []*internalpb.RetrieveResults{a, b})
	require.NoError(t, err)
	// Each input contains one parent and three child FieldData columns.
	require.Equal(t, 8, calls)
	merged := out[0].(*internalpb.RetrieveResults)
	reordered, err := (&OrderByLimitOperator{}).reorderResult(merged, []int{1, 0})
	require.NoError(t, err)
	page, err := rangeSliceRetrieveResults(reordered, 0, 1)
	require.NoError(t, err)
	require.Equal(t, 8, calls)
	require.Equal(t, []int64{2}, page.Ids.GetIntId().Data)
	content := page.ComputedResults[0].GetStructArrays().Fields[0].GetScalars().GetArrayData().Data
	require.Equal(t, []string{"second"}, content[0].GetStringData().Data)
}

func TestComputedStructMergeUsesNamesNotIDsOrOrder(t *testing.T) {
	for _, ids := range []string{"positional", "zero", "unrelated"} {
		t.Run(ids, func(t *testing.T) {
			a := computedStructFixture([]int64{1, 3}, []int64{1, 1}, "old")
			b := computedStructFixture([]int64{2, 3}, []int64{2, 2}, "new")
			for _, result := range []*internalpb.RetrieveResults{a, b} {
				// starts and ends have the same type but different values, so a
				// positional/ID-based swap cannot accidentally pass this test.
				ends := result.ComputedResults[0].GetStructArrays().Fields[2]
				for _, row := range ends.GetScalars().GetArrayData().Data {
					for i := range row.GetArrayData().Data[0].GetLongData().Data {
						row.GetArrayData().Data[0].GetLongData().Data[i] += 100
					}
				}
				other := proto.Clone(result.ComputedResults[0]).(*schemapb.FieldData)
				other.FieldName = "another"
				result.ComputedResults = append(result.ComputedResults, other)
			}
			for _, column := range b.ComputedResults {
				children := column.GetStructArrays().Fields
				column.GetStructArrays().Fields = []*schemapb.FieldData{children[2], children[0], children[1]}
				for i, child := range column.GetStructArrays().Fields {
					switch ids {
					case "positional":
						child.FieldId = int64(i + 1)
					case "zero":
						child.FieldId = 0
					case "unrelated":
						child.FieldId = int64(100 - i)
					}
				}
			}
			b.ComputedResults[0], b.ComputedResults[1] = b.ComputedResults[1], b.ComputedResults[0]
			beforeA, beforeB := proto.Clone(a), proto.Clone(b)
			out, err := NewReduceByPKWithTimestampOperator(reduce.IReduceNoOrder, 0, -1, nil).Run(context.Background(), nil, []*internalpb.RetrieveResults{a, b})
			require.NoError(t, err)
			merged := out[0].(*internalpb.RetrieveResults)
			require.Equal(t, []int64{1, 2, 3}, merged.GetIds().GetIntId().Data)
			wire, err := proto.Marshal(merged)
			require.NoError(t, err)
			decoded := &internalpb.RetrieveResults{}
			require.NoError(t, proto.Unmarshal(wire, decoded))
			reordered, err := (&OrderByLimitOperator{}).reorderResult(decoded, []int{2, 0, 1})
			require.NoError(t, err)
			page, err := rangeSliceRetrieveResults(reordered, 0, 1)
			require.NoError(t, err)
			for i, column := range page.ComputedResults {
				require.Equal(t, []string{"matches", "another"}[i], column.FieldName)
				fields := column.GetStructArrays().Fields
				require.Equal(t, "content", fields[0].FieldName)
				require.Equal(t, []string{"new"}, fields[0].GetScalars().GetArrayData().Data[0].GetStringData().Data)
				require.Equal(t, "starts", fields[1].FieldName)
				require.Equal(t, []int64{3, 4}, fields[1].GetScalars().GetArrayData().Data[0].GetArrayData().Data[0].GetLongData().Data)
				require.Equal(t, "ends", fields[2].FieldName)
				require.Equal(t, []int64{103, 104}, fields[2].GetScalars().GetArrayData().Data[0].GetArrayData().Data[0].GetLongData().Data)
			}
			require.True(t, proto.Equal(beforeA, a))
			require.True(t, proto.Equal(beforeB, b))
		})
	}
}

func TestComputedStructRejectsMismatchedNamedChildren(t *testing.T) {
	for name, mutate := range map[string]func(*schemapb.StructArrayField){
		"missing":   func(s *schemapb.StructArrayField) { s.Fields = s.Fields[:2] },
		"renamed":   func(s *schemapb.StructArrayField) { s.Fields[2].FieldName = "other" },
		"duplicate": func(s *schemapb.StructArrayField) { s.Fields[2].FieldName = "starts" },
		"same name different type": func(s *schemapb.StructArrayField) {
			s.Fields[2] = proto.Clone(s.Fields[0]).(*schemapb.FieldData)
			s.Fields[2].FieldName = "ends"
		},
	} {
		t.Run(name, func(t *testing.T) {
			a := computedStructFixture([]int64{1}, []int64{1}, "a")
			b := computedStructFixture([]int64{2}, []int64{1}, "b")
			mutate(b.ComputedResults[0].GetStructArrays())
			_, err := prepareComputedResults([]*internalpb.RetrieveResults{a, b})
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		})
	}
}
