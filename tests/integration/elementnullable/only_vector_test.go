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

package elementnullable

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

// The only vector field of this collection is an element-nullable vector
// sub-field of a struct; there is no top-level vector and no other vector
// sub-field. Rows reuse vsRow / vsElem: only pk, null, elems[].nullable and
// elems[].ints are used.
var (
	// Every element vector of this batch is null: a row of null elements, a
	// row with one null element, an empty struct array and a null struct.
	ovAllNullRows = []vsRow{
		{pk: 1, elems: []vsElem{el(nil, 0, i64(1)), el(nil, 0, nil), el(nil, 0, i64(3))}},
		{pk: 2, elems: []vsElem{el(nil, 0, i64(5))}},
		{pk: 3},
		{pk: 4, null: true},
	}
	// Some null element vectors, a row whose element vectors are all null, an
	// empty struct array and a null struct.
	ovSealedRows = []vsRow{
		{pk: 11, elems: []vsElem{el(f32(0.90), 0, i64(1)), el(nil, 0, nil), el(f32(0.50), 0, i64(3))}},
		{pk: 12, elems: []vsElem{el(nil, 0, i64(4)), el(nil, 0, nil)}},
		{pk: 13},
		{pk: 14, null: true},
		{pk: 15, elems: []vsElem{el(nil, 0, i64(6)), el(f32(0.95), 0, nil), el(f32(0.40), 0, i64(7)), el(nil, 0, i64(8))}},
		{pk: 16, elems: []vsElem{el(f32(0.85), 0, i64(9))}},
	}
	ovGrowingRows = []vsRow{
		{pk: 21, elems: []vsElem{el(nil, 0, i64(2)), el(f32(0.97), 0, nil)}},
		{pk: 22, elems: []vsElem{el(nil, 0, nil)}},
		{pk: 23},
		{pk: 24, null: true},
		{pk: 25, elems: []vsElem{el(f32(0.70), 0, i64(5)), el(nil, 0, i64(10)), el(f32(0.30), 0, nil)}},
	}
)

func ovSchema(name string) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: name,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 101, Name: structName, Nullable: true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID: 102, Name: nullVec, DataType: schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector, Nullable: true, ElementNullable: true,
					TypeParams: []*commonpb.KeyValuePair{kv(common.DimKey, strconv.Itoa(dim)), kv(common.MaxCapacityKey, "8")},
				},
				{
					FieldID: 103, Name: intsName, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
					Nullable: true, ElementNullable: true, TypeParams: []*commonpb.KeyValuePair{kv(common.MaxCapacityKey, "8")},
				},
			},
		}},
	}
}

// ovFields builds a compact insert payload: a null struct row has no child
// entry, and element-nullable children carry only their valid values.
func ovFields(rows []vsRow) []*schemapb.FieldData {
	pks := make([]int64, 0, len(rows))
	structValid := make([]bool, 0, len(rows))
	var vecRows []*schemapb.VectorField
	var intRows []*schemapb.ScalarField
	for _, row := range rows {
		pks = append(pks, row.pk)
		structValid = append(structValid, !row.null)
		if row.null {
			continue
		}
		var vecData []float32
		var vecValid, intValid []bool
		var ints []int64
		for j, e := range row.elems {
			if e.nullable != nil {
				vecData = append(vecData, vsVector(*e.nullable, row.pk, j)...)
			}
			vecValid = append(vecValid, e.nullable != nil)
			if e.ints != nil {
				ints = append(ints, *e.ints)
			}
			intValid = append(intValid, e.ints != nil)
		}
		vecRows = append(vecRows, vectorRow(vecData, vecValid...))
		intRows = append(intRows, longRow(ints, intValid...))
	}
	children := []*schemapb.FieldData{vectorField(nullVec, 102, vecRows...), scalarField(intsName, 103, intRows...)}
	for _, child := range children {
		typeutil.SetFieldDataValidData(child, structValid)
	}
	return []*schemapb.FieldData{
		{
			FieldName: "pk", FieldId: 100, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
			}},
		},
		{
			FieldName: structName, FieldId: 101, Type: schemapb.DataType_ArrayOfStruct,
			Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: children}},
		},
	}
}

func (s *ElementNullableSuite) ovInsert(ctx context.Context, name string, model *vsModel, rows []vsRow) {
	result, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: name, FieldsData: ovFields(rows), NumRows: uint32(len(rows)),
		HashKeys: integration.GenerateHashKeys(len(rows)),
	})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	model.rows = append(model.rows, rows...)
}

// TestOnlyElementNullableVector runs a collection whose only vector is an
// element-nullable vector sub-field end to end: create, index, load, element
// level ANN (also without anns_field), filters and query output, on a sealed
// segment whose vectors are all null, a mixed sealed segment, growing data,
// and again after release and reload.
func (s *ElementNullableSuite) TestOnlyElementNullableVector() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	// Index the small sealed segments for real instead of skipping the build.
	revert := s.Cluster.MustModifyMilvusConfig(map[string]string{"indexCoord.segment.minSegmentNumRowsToEnableIndex": "1"})
	defer revert()

	name := "w10i_onlyvec_" + funcutil.RandomString(8)
	s.Require().NoError(merr.Error(s.create(ctx, ovSchema(name))))
	field := vsAnns(nullVec)

	describe, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(describe.GetStatus(), err))
	s.Require().Empty(typeutil.GetDenseVectorFieldSchemas(describe.GetSchema()), "no top-level vector field")
	vectors := typeutil.GetVectorFieldSchemas(describe.GetSchema())
	s.Require().Len(vectors, 1)
	// DescribeCollection returns the raw sub-field name.
	s.Require().Equal(nullVec, vectors[0].GetName())
	s.Require().True(vectors[0].GetElementNullable())

	// Load is refused until the only vector field has an index.
	status, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(err)
	s.Require().ErrorIs(merr.Error(status), merr.ErrIndexNotFound)

	s.vsCreateIndex(ctx, name, field)
	model := &vsModel{}
	s.ovInsert(ctx, name, model, ovAllNullRows)
	s.vsFlush(ctx, name)
	s.vsWaitIndexed(ctx, name, field, int64(len(ovAllNullRows)))

	// A loaded collection whose only sealed segment has no valid vector.
	s.vsLoad(ctx, name)
	s.Run("all_null_sealed", func() {
		s.Require().Empty(model.hits(nullVec, nil, nil))
		s.ovRunChecks(ctx, name, model)
	})
	status, err = s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))

	s.ovInsert(ctx, name, model, ovSealedRows)
	s.vsFlush(ctx, name)
	s.vsWaitIndexed(ctx, name, field, int64(len(ovAllNullRows)+len(ovSealedRows)))
	s.vsLoad(ctx, name)
	s.Run("sealed", func() { s.ovRunChecks(ctx, name, model) })

	s.ovInsert(ctx, name, model, ovGrowingRows)
	s.Run("sealed_and_growing", func() { s.ovRunChecks(ctx, name, model) })

	status, err = s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.vsLoad(ctx, name)
	s.Run("after_reload", func() { s.ovRunChecks(ctx, name, model) })
}

func (s *ElementNullableSuite) ovRunChecks(ctx context.Context, name string, model *vsModel) {
	s.Run("search", func() { s.ovCheckSearch(ctx, name, model) })
	s.Run("default_anns_field", func() { s.ovCheckDefaultAnnsField(ctx, name, model) })
	s.Run("filters", func() { s.ovCheckFilters(ctx, name, model) })
	s.Run("group_by_pk", func() { s.ovCheckGroupBy(ctx, name, model) })
	s.Run("query", func() { s.ovCheckQuery(ctx, name, model) })
}

// ovCheckSearch: null elements never hit, offsets are logical, and the
// returned struct holds the hit vector at the returned offset.
func (s *ElementNullableSuite) ovCheckSearch(ctx context.Context, name string, model *vsModel) {
	want := model.hits(nullVec, nil, nil)
	for _, topk := range []int{vsLimit, 3} {
		msg := fmt.Sprintf("topk %d", topk)
		request := vsSearchRequest(name, vsAnns(nullVec), "", true, topk, nil)
		request.OutputFields = []string{"pk", structName}
		result := s.vsSearch(ctx, request)
		got := vsHits(result.GetResults())
		s.vsAssertHits(want[:min(topk, len(want))], got, msg)
		s.ovCheckFields(result.GetResults().GetFieldsData(), ovPKs(got), model, msg)
		s.ovCheckHitVectors(result.GetResults().GetFieldsData(), got, model, msg)
	}
}

// ovCheckDefaultAnnsField: without anns_field the proxy picks the only
// vector field, the element-nullable sub-field.
func (s *ElementNullableSuite) ovCheckDefaultAnnsField(ctx context.Context, name string, model *vsModel) {
	request := vsSearchRequest(name, vsAnns(nullVec), "", true, vsLimit, nil)
	params := request.GetSearchParams()[:0]
	for _, param := range request.GetSearchParams() {
		if param.GetKey() != integration.AnnsFieldKey {
			params = append(params, param)
		}
	}
	request.SearchParams = params
	s.vsAssertHits(model.hits(nullVec, nil, nil), vsHits(s.vsSearch(ctx, request).GetResults()), "no anns_field")
}

func (s *ElementNullableSuite) ovCheckFilters(ctx context.Context, name string, model *vsModel) {
	for _, tc := range []vsFilterCase{
		{nullVec, "element_filter(items, $[ints] > 3)", nil, vsIntsIs(func(v int64) bool { return v > 3 })},
		{nullVec, "element_filter(items, $[ints] IS NULL)", nil, func(e vsElem) bool { return e.ints == nil }},
		{nullVec, "element_filter(items, $[nullable_vectors] IS NULL)", nil, func(e vsElem) bool { return e.nullable == nil }},
		{
			nullVec, "element_filter(items, $[nullable_vectors] IS NOT NULL && $[ints] IS NOT NULL)", nil,
			func(e vsElem) bool { return e.ints != nil },
		},
		{
			nullVec, "pk > 12 && element_filter(items, $[ints] < 8 || $[ints] IS NULL)", func(r vsRow) bool { return r.pk > 12 },
			func(e vsElem) bool { return e.ints == nil || *e.ints < 8 },
		},
		{nullVec, "MATCH_ANY(items, $[nullable_vectors] IS NULL)", func(r vsRow) bool {
			for _, e := range r.elems {
				if e.nullable == nil {
					return true
				}
			}
			return false
		}, nil},
	} {
		want := model.hits(tc.field, tc.row, tc.elem)
		result := s.vsSearch(ctx, vsSearchRequest(name, vsAnns(tc.field), tc.expr, true, vsLimit, nil))
		s.vsAssertHits(want, vsHits(result.GetResults()), tc.expr)
	}
}

func (s *ElementNullableSuite) ovCheckGroupBy(ctx context.Context, name string, model *vsModel) {
	const groupSize = 2
	want := vsGroups(model.hits(nullVec, nil, nil), groupSize, 3)
	result := s.vsSearch(ctx, vsSearchRequest(name, vsAnns(nullVec), "", true, 3, nil,
		kv(proxy.GroupByFieldKey, "pk"), kv(proxy.GroupSizeKey, strconv.Itoa(groupSize)), kv(proxy.StrictGroupSize, "true")))
	s.vsAssertHits(want, vsHits(result.GetResults()), "group by pk")
}

// ovCheckQuery reads every row back with the whole struct.
func (s *ElementNullableSuite) ovCheckQuery(ctx context.Context, name string, model *vsModel) {
	resp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: name, Expr: "pk >= 0", OutputFields: []string{"pk", structName},
	})
	s.Require().NoError(merr.CheckRPCCall(resp, err))
	var pks []int64
	for _, field := range resp.GetFieldsData() {
		if field.GetFieldName() == "pk" {
			pks = field.GetScalars().GetLongData().GetData()
		}
	}
	var want []int64
	for _, row := range model.rows {
		want = append(want, row.pk)
	}
	s.Require().ElementsMatch(want, pks)
	s.ovCheckFields(resp.GetFieldsData(), pks, model, "query")
}

func ovPKs(hits []vsHit) []int64 {
	pks := make([]int64, len(hits))
	for i, hit := range hits {
		pks[i] = hit.pk
	}
	return pks
}

func (m *vsModel) row(pk int64) vsRow {
	for _, row := range m.rows {
		if row.pk == pk {
			return row
		}
	}
	panic(fmt.Sprintf("pk %d is not in the model", pk))
}

// ovCheckFields checks the pk column and both struct children row by row:
// one dense entry per result row, struct validity in valid_data, compact
// valid vectors with a logical element mask, and dense ints with 0 at nulls.
func (s *ElementNullableSuite) ovCheckFields(fields []*schemapb.FieldData, pks []int64, model *vsModel, msg string) {
	var pkField, structField *schemapb.FieldData
	for _, field := range fields {
		switch field.GetFieldName() {
		case "pk":
			pkField = field
		case structName:
			structField = field
		}
	}
	if len(pks) == 0 {
		s.Require().Empty(pkField.GetScalars().GetLongData().GetData(), "%s: pk column", msg)
		return
	}
	s.Require().NotNil(pkField, msg)
	s.Require().Equal(pks, pkField.GetScalars().GetLongData().GetData(), "%s: pk column", msg)
	s.Require().NotNil(structField, msg)
	children := make(map[string]*schemapb.FieldData)
	for _, child := range structField.GetStructArrays().GetFields() {
		children[child.GetFieldName()] = child
	}
	s.Require().Len(children, 2, msg)
	rowValid := make([]bool, len(pks))
	for i, pk := range pks {
		rowValid[i] = !model.row(pk).null
	}

	vectors := children[nullVec]
	s.Require().NotNil(vectors, msg)
	s.Require().Equal(rowValid, vectors.GetValidData(), "%s: vector FieldData.valid_data", msg)
	s.Require().Equal(rowValid, vectors.GetVectors().GetValidData(), "%s: VectorField.valid_data", msg)
	vecRows := vectors.GetVectors().GetVectorArray().GetData()
	s.Require().Len(vecRows, len(pks), "%s: vector row count", msg)

	ints := children[intsName]
	s.Require().NotNil(ints, msg)
	s.Require().Equal(rowValid, ints.GetValidData(), "%s: ints FieldData.valid_data", msg)
	s.Require().Equal(rowValid, ints.GetScalars().GetValidData(), "%s: ScalarField.valid_data", msg)
	intRows := ints.GetScalars().GetArrayData().GetData()
	s.Require().Len(intRows, len(pks), "%s: ints row count", msg)

	for i, pk := range pks {
		row := model.row(pk)
		rmsg := fmt.Sprintf("%s: row %d (pk %d)", msg, i, pk)
		if row.null {
			s.Require().Empty(vecRows[i].GetValidData(), rmsg)
			s.Require().Empty(vecRows[i].GetFloatVector().GetData(), rmsg)
			s.Require().Empty(intRows[i].GetValidData(), rmsg)
			s.Require().Empty(intRows[i].GetLongData().GetData(), rmsg)
			continue
		}
		vecValid := make([]bool, len(row.elems))
		intValid := make([]bool, len(row.elems))
		intValues := make([]int64, len(row.elems))
		var vecData []float32
		for j, e := range row.elems {
			if e.nullable != nil {
				vecValid[j] = true
				vecData = append(vecData, vsVector(*e.nullable, pk, j)...)
			}
			if e.ints != nil {
				intValid[j], intValues[j] = true, *e.ints
			}
		}
		_, ok := vecRows[i].GetData().(*schemapb.VectorField_FloatVector)
		s.Require().True(ok, "%s: not float_vector: %v", rmsg, vecRows[i])
		emptyOrEqual(s, vecValid, vecRows[i].GetValidData(), rmsg+" vector valid_data")
		emptyOrEqual(s, vecData, vecRows[i].GetFloatVector().GetData(), rmsg+" compact vectors")
		_, ok = intRows[i].GetData().(*schemapb.ScalarField_LongData)
		s.Require().True(ok, "%s: not long_data: %v", rmsg, intRows[i])
		emptyOrEqual(s, intValid, intRows[i].GetValidData(), rmsg+" ints valid_data")
		emptyOrEqual(s, intValues, intRows[i].GetLongData().GetData(), rmsg+" ints values")
	}
}

// ovCheckHitVectors: at each hit, the element at the returned logical offset
// is valid and is the vector that scored.
func (s *ElementNullableSuite) ovCheckHitVectors(fields []*schemapb.FieldData, hits []vsHit, model *vsModel, msg string) {
	if len(hits) == 0 {
		return
	}
	var rows []*schemapb.VectorField
	for _, field := range fields {
		if field.GetFieldName() != structName {
			continue
		}
		for _, child := range field.GetStructArrays().GetFields() {
			if child.GetFieldName() == nullVec {
				rows = child.GetVectors().GetVectorArray().GetData()
			}
		}
	}
	s.Require().Len(rows, len(hits), msg)
	for i, hit := range hits {
		valid := rows[i].GetValidData()
		s.Require().Less(int(hit.offset), len(valid), "%s: hit %d", msg, i)
		s.Require().True(valid[hit.offset], "%s: hit %d is at a null element", msg, i)
		// Compact data: the physical index is the number of valid elements
		// before the logical offset.
		physical := 0
		for _, v := range valid[:hit.offset] {
			if v {
				physical++
			}
		}
		elem := model.row(hit.pk).elems[hit.offset]
		s.Require().NotNil(elem.nullable, "%s: hit %d", msg, i)
		data := rows[i].GetFloatVector().GetData()
		s.Require().GreaterOrEqual(len(data), (physical+1)*dim, "%s: hit %d", msg, i)
		s.Require().Equal(vsVector(*elem.nullable, hit.pk, int(hit.offset)), data[physical*dim:(physical+1)*dim], "%s: hit %d", msg, i)
	}
}
