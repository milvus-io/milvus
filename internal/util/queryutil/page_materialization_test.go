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

package queryutil

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// legacyPageOracle deliberately keeps the pre-change two-operator composition.
// It must not call the candidate's row selection or pagination helpers.
func legacyPageOracle(ctx context.Context, inputs []*internalpb.RetrieveResults, schema *schemapb.CollectionSchema, mode reduce.IReduceType, limit, offset int64) (*internalpb.RetrieveResults, error) {
	merged, err := NewSortAndCheckPKOperator(mode, schema).Run(ctx, nil, inputs)
	if err != nil {
		return nil, err
	}
	result := merged[0].(*internalpb.RetrieveResults)
	if result == nil || len(result.GetFieldsData()) == 0 {
		return result, nil
	}
	op := legacyPageSliceOperator{limit: limit, offset: offset}
	if result.GetElementLevel() {
		return op.applyElementLevelSlice(result)
	}
	return op.applySlice(result)
}

func clonePageInputs(inputs []*internalpb.RetrieveResults) []*internalpb.RetrieveResults {
	out := make([]*internalpb.RetrieveResults, len(inputs))
	for i, input := range inputs {
		if input != nil {
			out[i] = proto.Clone(input).(*internalpb.RetrieveResults)
		}
	}
	return out
}

func requirePageParity(t *testing.T, inputs []*internalpb.RetrieveResults, schema *schemapb.CollectionSchema, mode reduce.IReduceType, limit, offset int64) {
	t.Helper()
	want, wantErr := legacyPageOracle(context.Background(), clonePageInputs(inputs), schema, mode, limit, offset)
	got, gotErr := NewSortAndCheckPKPageOperator(mode, schema, limit, offset).Run(context.Background(), nil, clonePageInputs(inputs))
	if wantErr != nil {
		require.Error(t, gotErr)
		require.Equal(t, merr.Code(wantErr), merr.Code(gotErr))
		return
	}
	require.NoError(t, gotErr)
	require.Len(t, got, 1)
	actual := got[0].(*internalpb.RetrieveResults)
	require.True(t, proto.Equal(want, actual), "old Reduce+Slice=%v\npage materialization=%v", want, actual)
}

// The fixture uses several scalar/Array columns and optional nullable compact
// vectors. All values depend on PK, so an ID/payload alignment mistake is visible.
func pageFixture(pks []int64, stringPK bool, vectorType schemapb.DataType, dim int, allNull bool) (*internalpb.RetrieveResults, *schemapb.CollectionSchema) {
	strings := make([]string, len(pks))
	texts := make([]string, len(pks))
	bools := make([]bool, len(pks))
	arrays := make([]*schemapb.ScalarField, len(pks))
	jsons := make([][]byte, len(pks))
	valid := make([]bool, len(pks))
	for i, pk := range pks {
		strings[i] = fmt.Sprintf("pk-%08d", pk)
		texts[i] = fmt.Sprintf("row/%d/资料", pk)
		bools[i] = pk%2 == 0
		values := make([]string, int(pk%3))
		for j := range values {
			values[j] = fmt.Sprintf("%d:%d", pk, j)
		}
		arrays[i] = &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}}
		jsons[i] = []byte(fmt.Sprintf(`{"row":%d}`, pk))
		valid[i] = !allNull && pk%4 != 1
	}
	longField := &schemapb.FieldData{FieldId: 100, FieldName: "pk", Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: append([]int64(nil), pks...)}}}}}
	ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: append([]int64(nil), pks...)}}}
	pkType := schemapb.DataType_Int64
	if stringPK {
		pkType = schemapb.DataType_VarChar
		ids.IdField = &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: strings}}
		longField.Type = pkType
		longField.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: append([]string(nil), strings...)}}}}
	}
	r := &internalpb.RetrieveResults{
		Base: &commonpb.MsgBase{MsgID: 17, SourceID: 9}, ReqID: 22,
		Status: &commonpb.Status{Reason: "fixture"}, AllRetrieveCount: 901,
		ScannedRemoteBytes: 902, ScannedTotalBytes: 903, MvccTimestamp: 904,
		CostAggregation: &internalpb.CostAggregation{ServiceTime: 11},
		Ids:             ids,
		FieldsData: []*schemapb.FieldData{
			longField,
			{FieldId: 101, FieldName: "text", Type: schemapb.DataType_VarChar, IsDynamic: true, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: texts}}}}},
			{FieldId: 102, FieldName: "labels", Type: schemapb.DataType_Array, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_VarChar, Data: arrays}}}}},
			{FieldId: 103, FieldName: "flag", Type: schemapb.DataType_Bool, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{ValidData: append([]bool(nil), valid...), Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: bools}}}}},
			{FieldId: 104, FieldName: "json", Type: schemapb.DataType_JSON, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: jsons}}}}},
		},
	}
	schema := &schemapb.CollectionSchema{Name: "page_test", Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: pkType, IsPrimaryKey: true},
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 102, Name: "labels", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
		{FieldID: 103, Name: "flag", DataType: schemapb.DataType_Bool, Nullable: true},
		{FieldID: 104, Name: "json", DataType: schemapb.DataType_JSON},
	}}
	if vectorType == schemapb.DataType_None {
		return r, schema
	}
	vf := &schemapb.VectorField{Dim: int64(dim), ValidData: valid}
	floats := make([]float32, 0)
	bytes := make([]byte, 0)
	contents := make([][]byte, 0)
	var sparseDim int64
	for i, pk := range pks {
		if !valid[i] {
			continue
		}
		for j := 0; j < dim; j++ {
			floats = append(floats, float32(pk*100+int64(j)))
		}
		byteCount := dim
		switch vectorType {
		case schemapb.DataType_BinaryVector:
			byteCount = dim / 8
		case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
			byteCount = dim * 2
		}
		for j := 0; j < byteCount; j++ {
			bytes = append(bytes, byte(pk+int64(j)))
		}
		row := make([]byte, 8)
		binary.LittleEndian.PutUint32(row, uint32(pk))
		binary.LittleEndian.PutUint32(row[4:], math.Float32bits(float32(pk)))
		contents = append(contents, row)
		if pk+1 > sparseDim {
			sparseDim = pk + 1
		}
	}
	switch vectorType {
	case schemapb.DataType_FloatVector:
		vf.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: floats}}
	case schemapb.DataType_BinaryVector:
		vf.Data = &schemapb.VectorField_BinaryVector{BinaryVector: bytes}
	case schemapb.DataType_Float16Vector:
		vf.Data = &schemapb.VectorField_Float16Vector{Float16Vector: bytes}
	case schemapb.DataType_BFloat16Vector:
		vf.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: bytes}
	case schemapb.DataType_Int8Vector:
		vf.Data = &schemapb.VectorField_Int8Vector{Int8Vector: bytes}
	case schemapb.DataType_SparseFloatVector:
		vf.Dim = sparseDim
		vf.Data = &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: sparseDim, Contents: contents}}
	default:
		panic("unexpected page fixture vector type")
	}
	r.FieldsData = append(r.FieldsData, &schemapb.FieldData{FieldId: 105, FieldName: "vector", Type: vectorType, Field: &schemapb.FieldData_Vectors{Vectors: vf}})
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{FieldID: 105, Name: "vector", DataType: vectorType, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: fmt.Sprint(dim)}}})
	return r, schema
}

func TestPageMaterializationParity(t *testing.T) {
	r1, schema := pageFixture([]int64{1, 3, 5, 7}, false, schemapb.DataType_FloatVector, 4, false)
	r2, _ := pageFixture([]int64{2, 4, 6, 8}, false, schemapb.DataType_FloatVector, 4, false)
	r1.HasMoreResult = true
	inputs := []*internalpb.RetrieveResults{r1, r2}
	for _, single := range []bool{false, true} {
		for _, noSchema := range []bool{false, true} {
			for _, mode := range []reduce.IReduceType{reduce.IReduceNoOrder, reduce.IReduceInOrder, reduce.IReduceInOrderForBest} {
				for _, page := range [][2]int64{{-1, 0}, {0, 0}, {1, 0}, {3, 2}, {2, 6}, {3, 7}, {2, 8}, {2, 20}, {-1, 3}} {
					name := fmt.Sprintf("single=%v/nilSchema=%v/mode=%d/limit=%d/offset=%d", single, noSchema, mode, page[0], page[1])
					t.Run(name, func(t *testing.T) {
						src, sch := inputs, schema
						if single {
							src = inputs[:1]
						}
						if noSchema {
							sch = nil
						}
						requirePageParity(t, src, sch, mode, page[0], page[1])
					})
				}
			}
		}
	}
	for _, inputs := range [][]*internalpb.RetrieveResults{nil, {nil, {}}, {nil, r1, {}}} {
		requirePageParity(t, inputs, schema, reduce.IReduceNoOrder, 1, 0)
	}
}

func TestPageMaterializationNullableVectors(t *testing.T) {
	for _, typ := range []schemapb.DataType{schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector, schemapb.DataType_SparseFloatVector} {
		for _, allNull := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/allNull=%v", typ, allNull), func(t *testing.T) {
				r1, schema := pageFixture([]int64{1, 3, 5, 7}, false, typ, 8, allNull)
				r2, _ := pageFixture([]int64{2, 4, 6, 8}, false, typ, 8, allNull)
				for _, page := range [][2]int64{{1, 0}, {3, 1}, {2, 3}, {3, 5}, {-1, 0}} {
					requirePageParity(t, []*internalpb.RetrieveResults{r1, r2}, schema, reduce.IReduceNoOrder, page[0], page[1])
				}
			})
		}
	}
}

func TestPageMaterializationNilSchemaValidPayload(t *testing.T) {
	for _, typ := range []schemapb.DataType{schemapb.DataType_None, schemapb.DataType_FloatVector, schemapb.DataType_SparseFloatVector} {
		t.Run(typ.String(), func(t *testing.T) {
			// Multiples of four make these fixture vectors fully populated, which
			// is the supported schema-unaware interpretation of vector payloads.
			r1, _ := pageFixture([]int64{4, 12, 20}, false, typ, 8, false)
			r2, _ := pageFixture([]int64{8, 16, 24}, false, typ, 8, false)
			inputs := []*internalpb.RetrieveResults{r1, r2}
			_, err := legacyPageOracle(context.Background(), clonePageInputs(inputs), nil, reduce.IReduceNoOrder, 2, 1)
			require.NoError(t, err)
			requirePageParity(t, inputs, nil, reduce.IReduceNoOrder, 2, 1)
		})
	}
}

// This oracle reconstructs fixture payloads directly from the expected PK page;
// unlike Reduce+Slice it shares no production gather/materialization helper.
func TestPageMaterializationIndependentPayloadOracle(t *testing.T) {
	for _, typ := range []schemapb.DataType{schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector, schemapb.DataType_SparseFloatVector} {
		t.Run(typ.String(), func(t *testing.T) {
			keys := [][]int64{{1, 4, 6, 10}, {2, 5, 9, 13}, {3, 7, 8, 12}}
			inputs := make([]*internalpb.RetrieveResults, len(keys))
			var schema *schemapb.CollectionSchema
			var sorted []int64
			var sparseDim int64
			for i, pks := range keys {
				inputs[i], schema = pageFixture(pks, true, typ, 8, false)
				sorted = append(sorted, pks...)
				if dim := inputs[i].FieldsData[5].GetVectors().GetDim(); dim > sparseDim {
					sparseDim = dim
				}
			}
			sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
			for _, page := range [][2]int64{{1, 0}, {4, 2}, {2, 6}, {1, 10}} {
				out, err := NewSortAndCheckPKPageOperator(reduce.IReduceNoOrder, schema, page[0], page[1]).Run(context.Background(), nil, clonePageInputs(inputs))
				require.NoError(t, err)
				got := out[0].(*internalpb.RetrieveResults)
				want, _ := pageFixture(sorted[page[1]:page[1]+page[0]], true, typ, 8, false)
				if typ == schemapb.DataType_SparseFloatVector {
					// The old merge carries the full source dimension even after slicing.
					want.FieldsData[5].GetVectors().Dim = sparseDim
					want.FieldsData[5].GetVectors().GetSparseFloatVector().Dim = sparseDim
				}
				// Merged output carries row validity in both FieldData.valid_data
				// and the field-specific location.
				for _, field := range want.GetFieldsData() {
					if validData := typeutil.GetFieldDataValidData(field); len(validData) > 0 {
						typeutil.SetFieldDataValidData(field, validData)
					}
				}
				require.True(t, proto.Equal(want.GetIds(), got.GetIds()))
				require.Len(t, got.GetFieldsData(), len(want.GetFieldsData()))
				for i, field := range want.GetFieldsData() {
					require.True(t, proto.Equal(field, got.FieldsData[i]), "field=%s page=%v want=%v got=%v", field.GetFieldName(), page, field, got.FieldsData[i])
				}
			}
		})
	}
}

func TestPageMaterializationElementParity(t *testing.T) {
	for _, emptyIndices := range []bool{false, true} {
		for _, single := range []bool{false, true} {
			r1, schema := pageFixture([]int64{1, 3, 5}, true, schemapb.DataType_FloatVector, 4, false)
			r2, _ := pageFixture([]int64{2, 4, 6}, true, schemapb.DataType_FloatVector, 4, false)
			for _, r := range []*internalpb.RetrieveResults{r1, r2} {
				r.ElementLevel = true
				for i := range r.GetIds().GetStrId().GetData() {
					indices := []int32{0, 2, 4, 6}[:i+2]
					if emptyIndices && i == 1 {
						indices = nil
					}
					r.ElementIndices = append(r.ElementIndices, &internalpb.ElementIndices{Indices: append([]int32(nil), indices...)})
				}
			}
			inputs := []*internalpb.RetrieveResults{r1, r2}
			if single {
				inputs = inputs[:1]
			}
			for _, page := range [][2]int64{{1, 1}, {4, 1}, {2, 3}, {7, 2}, {1, 12}, {2, 100}, {-1, 2}, {0, 0}} {
				t.Run(fmt.Sprintf("empty=%v/single=%v/limit=%d/offset=%d", emptyIndices, single, page[0], page[1]), func(t *testing.T) {
					requirePageParity(t, inputs, schema, reduce.IReduceInOrderForBest, page[0], page[1])
				})
			}
		}
	}
}

func TestPageMaterializationLegacySelectionBoundaries(t *testing.T) {
	t.Run("ForBest_does_not_check_duplicates_after_drain", func(t *testing.T) {
		r1, schema := pageFixture([]int64{1}, false, schemapb.DataType_None, 0, false)
		r2, _ := pageFixture([]int64{2, 4}, false, schemapb.DataType_None, 0, false)
		r1.HasMoreResult = true
		r2.Ids.GetIntId().Data[1] = 2
		inputs := []*internalpb.RetrieveResults{r1, r2}
		_, err := legacyPageOracle(context.Background(), clonePageInputs(inputs), schema, reduce.IReduceInOrderForBest, 1, 0)
		require.NoError(t, err)
		requirePageParity(t, inputs, schema, reduce.IReduceInOrderForBest, 1, 0)
	})
	t.Run("single_input_retains_duplicate_semantics", func(t *testing.T) {
		r, schema := pageFixture([]int64{1, 1, 2}, false, schemapb.DataType_None, 0, false)
		requirePageParity(t, []*internalpb.RetrieveResults{r}, schema, reduce.IReduceNoOrder, 1, 1)
	})
	t.Run("single_element_result_without_indices", func(t *testing.T) {
		r, schema := pageFixture([]int64{1, 2, 3}, false, schemapb.DataType_None, 0, false)
		r.ElementLevel = true
		requirePageParity(t, []*internalpb.RetrieveResults{r}, schema, reduce.IReduceNoOrder, 1, 1)
	})
	t.Run("empty_shard_flags_are_not_a_drain_source", func(t *testing.T) {
		r1, schema := pageFixture([]int64{1, 3, 5}, false, schemapb.DataType_None, 0, false)
		r2, _ := pageFixture([]int64{2, 4, 6}, false, schemapb.DataType_None, 0, false)
		empty := &internalpb.RetrieveResults{Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{0}}}}, HasMoreResult: true}
		requirePageParity(t, []*internalpb.RetrieveResults{empty, r1, r2}, schema, reduce.IReduceInOrderForBest, 3, 1)
	})
	t.Run("template_metadata_comes_from_full_selection", func(t *testing.T) {
		r1, schema := pageFixture([]int64{1, 3, 5}, false, schemapb.DataType_None, 0, false)
		r2, _ := pageFixture([]int64{2, 4, 6}, false, schemapb.DataType_None, 0, false)
		r2.FieldsData[1].FieldName = "different-shard-metadata"
		r2.FieldsData[1].IsDynamic = false
		requirePageParity(t, []*internalpb.RetrieveResults{r1, r2}, schema, reduce.IReduceNoOrder, 1, 1)
	})
}

func TestPageMaterializationErrorsOutsidePage(t *testing.T) {
	for _, kind := range []string{"duplicate_after_page", "duplicate_before_offset", "field_count", "truncated_vector", "short_validity", "element_metadata"} {
		t.Run(kind, func(t *testing.T) {
			r1, schema := pageFixture([]int64{1, 3, 5}, false, schemapb.DataType_FloatVector, 4, false)
			r2, _ := pageFixture([]int64{2, 4, 6}, false, schemapb.DataType_FloatVector, 4, false)
			limit, offset := int64(1), int64(0)
			switch kind {
			case "duplicate_after_page":
				r2.Ids.GetIntId().Data[2] = 5
			case "duplicate_before_offset":
				r2.Ids.GetIntId().Data[0] = 1
				offset = 4
			case "field_count":
				r2.FieldsData = r2.FieldsData[:len(r2.FieldsData)-1]
			case "truncated_vector":
				v := r2.FieldsData[5].GetVectors().GetFloatVector()
				v.Data = v.Data[:len(v.Data)-1]
			case "short_validity":
				r2.FieldsData[5].GetVectors().ValidData = []bool{true}
			case "element_metadata":
				r2.ElementLevel = true
			}
			inputs := []*internalpb.RetrieveResults{r1, r2}
			_, err := legacyPageOracle(context.Background(), clonePageInputs(inputs), schema, reduce.IReduceNoOrder, limit, offset)
			require.Error(t, err, "fixture must exercise an error in the independent legacy oracle")
			requirePageParity(t, inputs, schema, reduce.IReduceNoOrder, limit, offset)
		})
	}
}

func TestPageMaterializationRandomizedOracle(t *testing.T) {
	for seed := int64(0); seed < 48; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			shards := 2 + rng.Intn(4)
			keys := make([][]int64, shards)
			for pk := int64(1); pk <= 9+int64(rng.Intn(35)); pk++ {
				s := rng.Intn(shards)
				keys[s] = append(keys[s], pk)
			}
			inputs := make([]*internalpb.RetrieveResults, shards)
			var schema *schemapb.CollectionSchema
			for s, pks := range keys {
				inputs[s], schema = pageFixture(pks, seed%2 == 0, schemapb.DataType_FloatVector, 4, seed%7 == 0)
				inputs[s].HasMoreResult = rng.Intn(3) == 0
			}
			mode := []reduce.IReduceType{reduce.IReduceNoOrder, reduce.IReduceInOrder, reduce.IReduceInOrderForBest}[seed%3]
			if seed%5 == 0 {
				schema = nil
			}
			for _, page := range [][2]int64{{1, 0}, {3, 2}, {8, 4}, {-1, 1}, {3, 50}} {
				requirePageParity(t, inputs, schema, mode, page[0], page[1])
			}
		})
	}
}

var pageBenchmarkSink *internalpb.RetrieveResults

// Benchmark the retained production operator sequence, including both Run
// instrumentation scopes. The frozen slicing oracle above is for semantic
// independence, not a fair measurement of the old operator graph's overhead.
func measuredLegacyPage(ctx context.Context, inputs []*internalpb.RetrieveResults, schema *schemapb.CollectionSchema, limit, offset int64) (*internalpb.RetrieveResults, error) {
	merged, err := NewSortAndCheckPKOperator(reduce.IReduceNoOrder, schema).Run(ctx, nil, inputs)
	if err != nil {
		return nil, err
	}
	page, err := NewSliceOperator(limit, offset).Run(ctx, nil, merged[0])
	if err != nil {
		return nil, err
	}
	return page[0].(*internalpb.RetrieveResults), nil
}

func BenchmarkPageMaterialization1536(b *testing.B) {
	const shards, rowsPerShard, dim = 4, 1024, 1536
	inputs := make([]*internalpb.RetrieveResults, shards)
	var schema *schemapb.CollectionSchema
	for s := 0; s < shards; s++ {
		keys := make([]int64, rowsPerShard)
		for i := range keys {
			keys[i] = int64((i*shards + s + 1) * 4) // non-null fixture vectors
		}
		inputs[s], schema = pageFixture(keys, true, schemapb.DataType_FloatVector, dim, false)
	}
	cases := []struct {
		name          string
		inputs        []*internalpb.RetrieveResults
		limit, offset int64
	}{
		{"multi_small_page", inputs, 100, 2000},
		{"multi_full_page", inputs, shards * rowsPerShard, 0},
		{"single_small_page", inputs[:1], 100, 500},
		{"single_full_page", inputs[:1], rowsPerShard, 0},
	}
	ctx := context.Background()
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			want, err := legacyPageOracle(ctx, tc.inputs, schema, reduce.IReduceNoOrder, tc.limit, tc.offset)
			if err != nil {
				b.Fatal(err)
			}
			got, err := NewSortAndCheckPKPageOperator(reduce.IReduceNoOrder, schema, tc.limit, tc.offset).Run(ctx, nil, tc.inputs)
			if err != nil {
				b.Fatal(err)
			}
			if !proto.Equal(want, got[0].(*internalpb.RetrieveResults)) {
				b.Fatal("benchmark paths returned different results")
			}
			for _, candidate := range []bool{false, true} {
				name := "legacy_reduce_then_slice"
				if candidate {
					name = "select_page_then_materialize"
				}
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if candidate {
							out, err := NewSortAndCheckPKPageOperator(reduce.IReduceNoOrder, schema, tc.limit, tc.offset).Run(ctx, nil, tc.inputs)
							if err != nil {
								b.Fatal(err)
							}
							pageBenchmarkSink = out[0].(*internalpb.RetrieveResults)
						} else {
							pageBenchmarkSink, err = measuredLegacyPage(ctx, tc.inputs, schema, tc.limit, tc.offset)
							if err != nil {
								b.Fatal(err)
							}
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(len(tc.inputs)*rowsPerShard), "input_rows/op")
					b.ReportMetric(float64(tc.limit), "output_rows/op")
				})
			}
		})
	}
}

// Frozen from d25e044f7 SliceOperator. Production old/new operators now share
// selectDocWindow/selectElementWindow; keeping the original paging algorithm
// here makes the differential oracle independent of that refactoring.
type legacyPageSliceOperator struct{ limit, offset int64 }

// applySlice applies doc-level offset and limit to the result.
func (op *legacyPageSliceOperator) applySlice(result *internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	size := getRowCount(result)
	if size == 0 {
		return result, nil
	}

	start := int(op.offset)
	if start >= size {
		return &internalpb.RetrieveResults{}, nil
	}

	end := size
	if op.limit > 0 && start+int(op.limit) < end {
		end = start + int(op.limit)
	}

	if start == 0 && end == size {
		return result, nil
	}

	return rangeSliceRetrieveResults(result, start, end)
}

// applyElementLevelSlice applies element-level offset and limit.
// Offset and limit count individual array elements, not documents.
// When a boundary falls mid-document, the document's ElementIndices are trimmed.
func (op *legacyPageSliceOperator) applyElementLevelSlice(result *internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	docCount := getRowCount(result)
	elemIndices := result.GetElementIndices()
	if docCount == 0 || len(elemIndices) == 0 {
		return result, nil
	}

	// Phase 1: Apply offset by skipping elements
	startDoc := 0
	var startTrimCount int // number of elements to trim from the start of startDoc (0 = no trim)
	if op.offset > 0 {
		var skipped int64
		for startDoc < docCount && skipped < op.offset {
			if startDoc >= len(elemIndices) {
				break
			}
			docElemCount := int64(len(elemIndices[startDoc].GetIndices()))
			if skipped+docElemCount > op.offset {
				// Offset lands mid-document: record how many elements to trim
				startTrimCount = int(op.offset - skipped)
				break
			}
			skipped += docElemCount
			startDoc++
		}
		if startDoc >= docCount {
			return &internalpb.RetrieveResults{ElementLevel: true}, nil
		}
	}

	// Phase 2: Apply limit by counting elements
	endDoc := docCount   // default: take all remaining
	var endTrimCount int // number of elements to keep in endDoc-1 (0 = no trim, keep all)
	if op.limit > 0 {
		var collected int64
		for i := startDoc; i < docCount; i++ {
			if i >= len(elemIndices) {
				endDoc = i
				break
			}
			docElemCount := int64(len(elemIndices[i].GetIndices()))
			if i == startDoc && startTrimCount > 0 {
				docElemCount -= int64(startTrimCount)
			}
			if collected+docElemCount >= op.limit {
				// Limit reached at or within this document
				remaining := op.limit - collected
				fullCount := int64(len(elemIndices[i].GetIndices()))
				if i == startDoc && startTrimCount > 0 {
					// Both trim from start and end of same doc
					endTrimCount = startTrimCount + int(remaining)
				} else if remaining < fullCount {
					endTrimCount = int(remaining)
				}
				endDoc = i + 1
				break
			}
			collected += docElemCount
		}
	}

	if startDoc >= endDoc {
		return &internalpb.RetrieveResults{ElementLevel: true}, nil
	}

	// Phase 3: Slice field data at document granularity [startDoc, endDoc)
	sliced, err := rangeSliceRetrieveResults(result, startDoc, endDoc)
	if err != nil {
		return nil, err
	}

	// Phase 4: Apply element-level trims to ElementIndices
	if startTrimCount > 0 && endTrimCount > 0 && startDoc+1 == endDoc {
		// Both trims apply to the same (single) document
		original := sliced.GetElementIndices()[0].GetIndices()
		sliced.ElementIndices[0] = &internalpb.ElementIndices{
			Indices: original[startTrimCount:endTrimCount],
		}
	} else {
		if startTrimCount > 0 && len(sliced.GetElementIndices()) > 0 {
			original := sliced.GetElementIndices()[0].GetIndices()
			sliced.ElementIndices[0] = &internalpb.ElementIndices{
				Indices: original[startTrimCount:],
			}
		}
		if endTrimCount > 0 && len(sliced.GetElementIndices()) > 0 {
			lastIdx := len(sliced.GetElementIndices()) - 1
			original := sliced.GetElementIndices()[lastIdx].GetIndices()
			sliced.ElementIndices[lastIdx] = &internalpb.ElementIndices{
				Indices: original[:endTrimCount],
			}
		}
	}

	return sliced, nil
}
