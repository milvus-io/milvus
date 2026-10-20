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
	"sort"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

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

// The output contract of struct sub-fields, checked on the raw proto:
//   - every sub-field has one row per output row, in the order of the pk / ids;
//   - row validity is in FieldData.valid_data and in ScalarField / VectorField
//     valid_data; a NULL struct row has no values;
//   - element-nullable scalars are dense (zero value at a null element) with
//     one element valid_data entry per element;
//   - vectors are compact (only valid vectors) with logical-length valid_data;
//   - Array<Array<T>> rows hold one leaf-typed ScalarField per element; a NULL
//     inner array is an empty leaf-typed ScalarField marked invalid one level up.
const opStruct = "clips"

const (
	opInt = iota
	opStr
	opTags
	opVec
)

type opChild struct {
	name  string
	kind  int
	added bool // added with AlterCollectionSchema after data was written
}

var opChildren = []opChild{
	{"b", opInt, false},
	{"name", opStr, false},
	{"tags", opTags, false},
	{"vec", opVec, false},
	{"lb", opInt, true},
	{"lname", opStr, true},
	{"ltags", opTags, true},
	{"lvec", opVec, true},
}

// opTagList is one Array<Int64> element of an Array<Array<Int64>> sub-field:
// nil is a NULL inner array, a nil leaf is a NULL leaf.
type opTagList = *[]*int64

func tg(leaves ...*int64) opTagList { return &leaves }

func str(v string) *string { return &v }

// opElem is one struct element; nil is a null element value. vec / lvec hold
// the inner product of the element vector with the query e0.
type opElem struct {
	b, lb       *int64
	name, lname *string
	tags, ltags opTagList
	vec, lvec   *float32
}

func oe(b *int64, name *string, tags opTagList, vec *float32) opElem {
	return opElem{b: b, name: name, tags: tags, vec: vec}
}

func oa(b *int64, name *string, tags opTagList, vec *float32, lb *int64, lname *string, ltags opTagList, lvec *float32) opElem {
	return opElem{b: b, name: name, tags: tags, vec: vec, lb: lb, lname: lname, ltags: ltags, lvec: lvec}
}

func (e opElem) intOf(child string) *int64 {
	if child == "b" {
		return e.b
	}
	return e.lb
}

func (e opElem) strOf(child string) *string {
	if child == "name" {
		return e.name
	}
	return e.lname
}

func (e opElem) tagsOf(child string) opTagList {
	if child == "tags" {
		return e.tags
	}
	return e.ltags
}

func (e opElem) vecOf(child string) *float32 {
	if child == "vec" {
		return e.vec
	}
	return e.lvec
}

type opRow struct {
	pk    int64
	emb   float32
	null  bool // the whole struct is null
	elems []opElem
}

// Every batch mixes partly null elements, all-null elements, an empty struct
// array and NULL struct rows; tags hold NULL inner arrays, [] and [NULL].
var (
	opSealedA = []opRow{
		{pk: 1, emb: 0.11, elems: []opElem{
			oe(i64(1), str("a1"), tg(i64(1), i64(2)), f32(0.90)), oe(nil, nil, nil, nil), oe(i64(3), str("a3"), tg(), f32(0.50)),
		}},
		{pk: 2, emb: 0.12, elems: []opElem{oe(nil, nil, tg(nil), nil), oe(nil, nil, nil, nil)}},
		{pk: 3, emb: 0.13},
		{pk: 4, emb: 0.14, null: true},
		{pk: 5, emb: 0.15, elems: []opElem{oe(i64(5), str("a5"), tg(nil, i64(7)), f32(0.60)), oe(nil, str("a6"), tg(i64(8)), f32(0.95))}},
		{pk: 6, emb: 0.16, null: true},
	}
	opSealedB = []opRow{
		{pk: 7, emb: 0.17, null: true},
		{pk: 8, emb: 0.18, elems: []opElem{oe(i64(9), nil, tg(), f32(0.85)), oe(i64(10), str("b10"), nil, nil)}},
		{pk: 9, emb: 0.19},
		{pk: 10, emb: 0.20, elems: []opElem{oe(nil, str("b11"), tg(i64(11), nil), f32(0.40))}},
	}
	// Growing when the sub-fields are added, flushed later.
	opGrowingC = []opRow{
		{pk: 11, emb: 0.21, null: true},
		{pk: 12, emb: 0.22, elems: []opElem{oe(i64(12), str("c12"), tg(i64(12)), f32(0.97)), oe(nil, nil, nil, f32(0.30))}},
		{pk: 13, emb: 0.23},
		{pk: 14, emb: 0.24, elems: []opElem{oe(nil, nil, tg(nil), nil)}},
		{pk: 15, emb: 0.25, elems: []opElem{
			oe(i64(15), nil, nil, f32(0.70)), oe(nil, str("c16"), tg(), nil), oe(i64(17), str("c17"), tg(i64(17), i64(18)), f32(0.20)),
		}},
	}
	// Written after the add, with values in the added sub-fields.
	opAddedD = []opRow{
		{pk: 21, emb: 0.31, elems: []opElem{
			oa(i64(21), str("d21"), tg(i64(21)), f32(0.88), i64(210), str("x21"), tg(i64(1), nil), f32(0.91)),
			oa(nil, nil, nil, nil, nil, nil, nil, nil),
			oa(i64(22), nil, tg(), f32(0.35), nil, str("x22"), nil, f32(0.45)),
		}},
		{pk: 22, emb: 0.32, null: true},
		{pk: 23, emb: 0.33},
		{pk: 24, emb: 0.34, elems: []opElem{oa(nil, str("d24"), tg(nil), f32(0.65), i64(240), nil, tg(), nil)}},
		{pk: 25, emb: 0.35, elems: []opElem{oa(i64(25), str("d25"), tg(i64(25)), f32(0.15), nil, nil, nil, nil)}},
	}
	opGrowingE = []opRow{
		{pk: 31, emb: 0.41, null: true},
		{pk: 32, emb: 0.42, elems: []opElem{
			oa(i64(32), nil, nil, nil, i64(320), str("y32"), tg(nil), f32(0.83)),
			oa(nil, str("e33"), tg(i64(33)), f32(0.55), i64(330), nil, tg(i64(3), i64(4)), f32(0.25)),
		}},
		{pk: 33, emb: 0.43},
		{pk: 34, emb: 0.44, elems: []opElem{oa(i64(34), str("e34"), tg(), f32(0.78), nil, str("y34"), nil, nil)}},
	}
)

func opArrayParams() []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{kv(common.MaxCapacityKey, "8")}
}

func opChildSchema(c opChild) *schemapb.FieldSchema {
	field := &schemapb.FieldSchema{Name: c.name, ElementNullable: true, TypeParams: opArrayParams()}
	switch c.kind {
	case opInt:
		field.DataType, field.ElementType = schemapb.DataType_Array, schemapb.DataType_Int64
	case opStr:
		field.DataType, field.ElementType = schemapb.DataType_Array, schemapb.DataType_VarChar
		field.TypeParams = append(field.TypeParams, kv(common.MaxLengthKey, "32"))
	case opTags:
		field.DataType, field.ElementType = schemapb.DataType_Array, schemapb.DataType_Array
		field.TypeSchema = nestedType(schemapb.DataType_Int64, true, true, true)
	case opVec:
		field.DataType, field.ElementType = schemapb.DataType_ArrayOfVector, schemapb.DataType_FloatVector
		field.TypeParams = append(field.TypeParams, kv(common.DimKey, strconv.Itoa(dim)))
	}
	return field
}

func opSchema(name string) *schemapb.CollectionSchema {
	var children []*schemapb.FieldSchema
	for i, c := range opChildren {
		if c.added {
			continue
		}
		field := opChildSchema(c)
		field.FieldID, field.Nullable = int64(103+i), true
		children = append(children, field)
	}
	return &schemapb.CollectionSchema{
		Name: name,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: "embedding", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{kv(common.DimKey, strconv.Itoa(dim))},
			},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 102, Name: opStruct, Nullable: true, Fields: children}},
	}
}

// opChildData builds the compact insert payload of one sub-field: a NULL
// struct row has no entry and element-nullable values carry only valid ones.
func opChildData(c opChild, rows []opRow) *schemapb.FieldData {
	var scalars []*schemapb.ScalarField
	var vectors []*schemapb.VectorField
	for _, row := range rows {
		if row.null {
			continue
		}
		var valid []bool
		switch c.kind {
		case opInt:
			var values []int64
			for _, e := range row.elems {
				v := e.intOf(c.name)
				valid = append(valid, v != nil)
				if v != nil {
					values = append(values, *v)
				}
			}
			scalars = append(scalars, longRow(values, valid...))
		case opStr:
			var values []string
			for _, e := range row.elems {
				v := e.strOf(c.name)
				valid = append(valid, v != nil)
				if v != nil {
					values = append(values, *v)
				}
			}
			scalars = append(scalars, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}, ValidData: valid,
			})
		case opTags:
			var inner []*schemapb.ScalarField
			for _, e := range row.elems {
				t := e.tagsOf(c.name)
				valid = append(valid, t != nil)
				if t == nil {
					continue
				}
				var leaves []int64
				var leafValid []bool
				for _, leaf := range *t {
					leafValid = append(leafValid, leaf != nil)
					if leaf != nil {
						leaves = append(leaves, *leaf)
					}
				}
				inner = append(inner, longRow(leaves, leafValid...))
			}
			scalars = append(scalars, arrayRow(schemapb.DataType_Int64, valid, inner...))
		case opVec:
			var data []float32
			for j, e := range row.elems {
				v := e.vecOf(c.name)
				valid = append(valid, v != nil)
				if v != nil {
					data = append(data, vsVector(*v, row.pk, j)...)
				}
			}
			vectors = append(vectors, vectorRow(data, valid...))
		}
	}
	if c.kind == opVec {
		return vectorField(c.name, 0, vectors...)
	}
	return scalarField(c.name, 0, scalars...)
}

func opFields(rows []opRow, withAdded bool) []*schemapb.FieldData {
	pks := make([]int64, 0, len(rows))
	embeddings := make([]float32, 0, len(rows)*dim)
	structValid := make([]bool, 0, len(rows))
	for _, row := range rows {
		pks = append(pks, row.pk)
		embeddings = append(embeddings, vsVector(row.emb, row.pk, 0)...)
		structValid = append(structValid, !row.null)
	}
	var children []*schemapb.FieldData
	for _, c := range opChildren {
		if c.added && !withAdded {
			continue
		}
		child := opChildData(c, rows)
		typeutil.SetFieldDataValidData(child, structValid)
		children = append(children, child)
	}
	return []*schemapb.FieldData{
		{
			FieldName: "pk", FieldId: 100, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
			}},
		},
		{
			FieldName: "embedding", FieldId: 101, Type: schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  dim,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: embeddings}},
			}},
		},
		{
			FieldName: opStruct, FieldId: 102, Type: schemapb.DataType_ArrayOfStruct,
			Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: children}},
		},
	}
}

type opModel struct {
	rows  []opRow
	added bool // the schema has the added sub-fields
}

func (m *opModel) row(pk int64) opRow {
	for _, row := range m.rows {
		if row.pk == pk {
			return row
		}
	}
	panic(fmt.Sprintf("pk %d is not in the model", pk))
}

func (m *opModel) children() []opChild {
	var out []opChild
	for _, c := range opChildren {
		if !c.added || m.added {
			out = append(out, c)
		}
	}
	return out
}

func (m *opModel) byPK() []opRow {
	rows := append([]opRow(nil), m.rows...)
	sort.Slice(rows, func(i, j int) bool { return rows[i].pk < rows[j].pk })
	return rows
}

// elementHits is the exact element-level ANN answer on vector sub-field child.
func (m *opModel) elementHits(child string, row func(opRow) bool, elem func(opElem) bool) []vsHit {
	var hits []vsHit
	for _, r := range m.rows {
		if r.null || (row != nil && !row(r)) {
			continue
		}
		for j, e := range r.elems {
			if v := e.vecOf(child); v != nil && (elem == nil || elem(e)) {
				hits = append(hits, vsHit{pk: r.pk, offset: int64(j), score: *v})
			}
		}
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].score > hits[j].score })
	return hits
}

// rowHits is the exact row-level ANN answer on the top-level embedding.
func (m *opModel) rowHits(row func(opRow) bool) []vsHit {
	var hits []vsHit
	for _, r := range m.rows {
		if row == nil || row(r) {
			hits = append(hits, vsHit{pk: r.pk, offset: -1, score: r.emb})
		}
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].score > hits[j].score })
	return hits
}

func opAny(pred func(opElem) bool) func(opRow) bool {
	return func(r opRow) bool {
		for _, e := range r.elems {
			if pred(e) {
				return true
			}
		}
		return false
	}
}

func intIs(pred func(int64) bool, child string) func(opElem) bool {
	return func(e opElem) bool { v := e.intOf(child); return v != nil && pred(*v) }
}

// opOutputs are the output_fields sets checked in a phase.
func (m *opModel) outputs() [][]string {
	sets := [][]string{{"b", "tags"}, {"name", "vec"}}
	if m.added {
		sets = append(sets, []string{"lb", "ltags"}, []string{"lname", "lvec", "b"})
	}
	all := make([]string, 0, len(opChildren))
	for _, c := range m.children() {
		all = append(all, c.name)
	}
	return append([][]string{all}, sets...)
}

// opOutputFields is ["pk", "clips"] for the full set, otherwise the
// sub-fields by their "clips[x]" names.
func (m *opModel) opOutputFields(children []string) []string {
	if len(children) == len(m.children()) {
		return []string{"pk", opStruct}
	}
	fields := []string{"pk"}
	for _, c := range children {
		fields = append(fields, typeutil.ConcatStructFieldName(opStruct, c))
	}
	return fields
}

func opChildByName(name string) opChild {
	for _, c := range opChildren {
		if c.name == name {
			return c
		}
	}
	panic("unknown sub-field " + name)
}

func emptyOrEqual[T any](s *ElementNullableSuite, want, got []T, msg string) {
	if len(want) == 0 {
		s.Require().Empty(got, msg)
		return
	}
	s.Require().Equal(want, got, msg)
}

// opCheckFields checks the pk column and every requested sub-field of the
// struct column against the model, row by row in result order.
func (s *ElementNullableSuite) opCheckFields(fields []*schemapb.FieldData, pks []int64, children []string, m *opModel, msg string) {
	var structField, pkField *schemapb.FieldData
	for _, field := range fields {
		switch field.GetFieldName() {
		case opStruct:
			s.Require().Nil(structField, "%s: struct field returned twice", msg)
			structField = field
		case "pk":
			pkField = field
		}
	}
	s.Require().NotNil(pkField, msg)
	s.Require().Equal(pks, pkField.GetScalars().GetLongData().GetData(), "%s: pk column", msg)
	if len(pks) == 0 {
		return
	}
	s.Require().NotNil(structField, "%s: struct field missing", msg)
	s.Require().Equal(schemapb.DataType_ArrayOfStruct, structField.GetType(), msg)
	got := make(map[string]*schemapb.FieldData)
	var gotNames []string
	for _, child := range structField.GetStructArrays().GetFields() {
		got[child.GetFieldName()] = child
		gotNames = append(gotNames, child.GetFieldName())
	}
	s.Require().ElementsMatch(children, gotNames, "%s: struct sub-fields", msg)

	rowValid := make([]bool, len(pks))
	for i, pk := range pks {
		rowValid[i] = !m.row(pk).null
	}
	for _, name := range children {
		c := opChildByName(name)
		child := got[name]
		cmsg := fmt.Sprintf("%s: %s", msg, name)
		s.Require().Equal(rowValid, child.GetValidData(), "%s FieldData.valid_data", cmsg)
		if c.kind == opVec {
			s.opCheckVectorChild(child, c, pks, rowValid, m, cmsg)
			continue
		}
		scalars := child.GetScalars()
		s.Require().NotNil(scalars, cmsg)
		s.Require().Equal(rowValid, scalars.GetValidData(), "%s ScalarField.valid_data", cmsg)
		rows := scalars.GetArrayData().GetData()
		s.Require().Len(rows, len(pks), "%s: row count", cmsg)
		for i, pk := range pks {
			row := m.row(pk)
			rmsg := fmt.Sprintf("%s row %d (pk %d)", cmsg, i, pk)
			got := rows[i]
			if row.null {
				s.Require().Empty(got.GetValidData(), rmsg)
				s.Require().Empty(got.GetLongData().GetData(), rmsg)
				s.Require().Empty(got.GetStringData().GetData(), rmsg)
				s.Require().Empty(got.GetArrayData().GetData(), rmsg)
				continue
			}
			s.opCheckScalarRow(got, c, row, rmsg)
		}
	}
}

func (s *ElementNullableSuite) opCheckScalarRow(got *schemapb.ScalarField, c opChild, row opRow, msg string) {
	valid := make([]bool, len(row.elems))
	switch c.kind {
	case opInt:
		_, ok := got.GetData().(*schemapb.ScalarField_LongData)
		s.Require().True(ok, "%s: not long_data: %v", msg, got)
		values := make([]int64, len(row.elems))
		for j, e := range row.elems {
			if v := e.intOf(c.name); v != nil {
				valid[j], values[j] = true, *v
			}
		}
		emptyOrEqual(s, values, got.GetLongData().GetData(), msg+" values")
	case opStr:
		_, ok := got.GetData().(*schemapb.ScalarField_StringData)
		s.Require().True(ok, "%s: not string_data: %v", msg, got)
		values := make([]string, len(row.elems))
		for j, e := range row.elems {
			if v := e.strOf(c.name); v != nil {
				valid[j], values[j] = true, *v
			}
		}
		emptyOrEqual(s, values, got.GetStringData().GetData(), msg+" values")
	case opTags:
		array := got.GetArrayData()
		s.Require().NotNil(array, "%s: not array_data: %v", msg, got)
		s.Require().Equal(schemapb.DataType_Int64, array.GetElementType(), msg)
		s.Require().Len(array.GetData(), len(row.elems), "%s: inner array count", msg)
		for j, e := range row.elems {
			inner := array.GetData()[j]
			imsg := fmt.Sprintf("%s element %d", msg, j)
			_, ok := inner.GetData().(*schemapb.ScalarField_LongData)
			s.Require().True(ok, "%s: inner array is not a long_data ScalarField: %v", imsg, inner)
			t := e.tagsOf(c.name)
			if t == nil {
				s.Require().Empty(inner.GetLongData().GetData(), imsg)
				s.Require().Empty(inner.GetValidData(), imsg)
				continue
			}
			valid[j] = true
			leaves := make([]int64, len(*t))
			leafValid := make([]bool, len(*t))
			for k, leaf := range *t {
				if leaf != nil {
					leaves[k], leafValid[k] = *leaf, true
				}
			}
			emptyOrEqual(s, leaves, inner.GetLongData().GetData(), imsg+" leaves")
			emptyOrEqual(s, leafValid, inner.GetValidData(), imsg+" leaf valid_data")
		}
	}
	emptyOrEqual(s, valid, got.GetValidData(), msg+" element valid_data")
}

func (s *ElementNullableSuite) opCheckVectorChild(child *schemapb.FieldData, c opChild, pks []int64, rowValid []bool, m *opModel, msg string) {
	vectors := child.GetVectors()
	s.Require().NotNil(vectors, msg)
	s.Require().Equal(rowValid, vectors.GetValidData(), "%s VectorField.valid_data", msg)
	rows := vectors.GetVectorArray().GetData()
	s.Require().Len(rows, len(pks), "%s: row count", msg)
	for i, pk := range pks {
		row := m.row(pk)
		rmsg := fmt.Sprintf("%s row %d (pk %d)", msg, i, pk)
		got := rows[i]
		if row.null {
			s.Require().Empty(got.GetValidData(), rmsg)
			s.Require().Empty(got.GetFloatVector().GetData(), rmsg)
			continue
		}
		_, ok := got.GetData().(*schemapb.VectorField_FloatVector)
		s.Require().True(ok, "%s: not float_vector: %v", rmsg, got)
		s.Require().EqualValues(dim, got.GetDim(), rmsg)
		valid := make([]bool, len(row.elems))
		var data []float32
		for j, e := range row.elems {
			if v := e.vecOf(c.name); v != nil {
				valid[j] = true
				data = append(data, vsVector(*v, pk, j)...)
			}
		}
		emptyOrEqual(s, valid, got.GetValidData(), rmsg+" element valid_data")
		emptyOrEqual(s, data, got.GetFloatVector().GetData(), rmsg+" compact vectors")
	}
}

// opCheckHitElements: at every element-level hit, the hit vector is the
// element at the returned logical offset of the returned row.
func (s *ElementNullableSuite) opCheckHitElements(fields []*schemapb.FieldData, hits []vsHit, child string, msg string) {
	for _, field := range fields {
		if field.GetFieldName() != opStruct {
			continue
		}
		for _, sub := range field.GetStructArrays().GetFields() {
			if sub.GetFieldName() != child {
				continue
			}
			rows := sub.GetVectors().GetVectorArray().GetData()
			for i, hit := range hits {
				row := rows[i]
				s.Require().Less(int(hit.offset), len(row.GetValidData()), "%s: hit %d offset out of range", msg, i)
				s.Require().True(row.GetValidData()[hit.offset], "%s: hit %d is a null element", msg, i)
				physical := 0
				for _, v := range row.GetValidData()[:hit.offset] {
					if v {
						physical++
					}
				}
				vector := row.GetFloatVector().GetData()[physical*dim : (physical+1)*dim]
				s.Require().InDelta(hit.score, vector[0], 1e-6, "%s: hit %d", msg, i)
				s.Require().InDelta(float32(hit.offset)/10, vector[2], 1e-6, "%s: hit %d", msg, i)
			}
			return
		}
	}
}

func page[T any](items []T, limit, offset int) []T {
	if offset >= len(items) {
		return nil
	}
	end := len(items)
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	return items[offset:end]
}

func (s *ElementNullableSuite) opSearch(ctx context.Context, name string, m *opModel, anns, expr string, elementLevel bool,
	want []vsHit, limit, offset int, children []string,
) {
	msg := fmt.Sprintf("search %s expr=%q element=%v limit=%d offset=%d outputs=%v", anns, expr, elementLevel, limit, offset, children)
	var extra []*commonpb.KeyValuePair
	if offset > 0 {
		extra = append(extra, kv(proxy.OffsetKey, strconv.Itoa(offset)))
	}
	request := vsSearchRequest(name, anns, expr, elementLevel, limit, nil, extra...)
	request.OutputFields = m.opOutputFields(children)
	data := s.vsSearch(ctx, request).GetResults()
	want = page(want, limit, offset)
	s.Require().NotEmpty(want, msg)
	got := vsHits(data)
	s.vsAssertHits(want, got, msg)
	if !elementLevel {
		s.Require().Empty(data.GetElementIndices().GetData(), msg)
	}
	pks := make([]int64, len(got))
	for i, hit := range got {
		pks[i] = hit.pk
	}
	s.opCheckFields(data.GetFieldsData(), pks, children, m, msg)
	if elementLevel {
		for _, c := range children {
			if anns == typeutil.ConcatStructFieldName(opStruct, c) {
				s.opCheckHitElements(data.GetFieldsData(), got, c, msg)
			}
		}
	}
}

// opQuery checks a query. elem == nil is a row-level query over rows passing
// row; otherwise it is an element_filter query whose expected element_indices
// are the elements passing elem. limit / offset count rows for a row-level
// query and elements for an element-level one, both in pk order.
func (s *ElementNullableSuite) opQuery(ctx context.Context, name string, m *opModel, expr string,
	row func(opRow) bool, elem func(opElem) bool, limit, offset int, children []string,
) {
	msg := fmt.Sprintf("query expr=%q limit=%d offset=%d outputs=%v", expr, limit, offset, children)
	request := &milvuspb.QueryRequest{CollectionName: name, Expr: expr, OutputFields: m.opOutputFields(children)}
	if limit > 0 {
		request.QueryParams = []*commonpb.KeyValuePair{
			kv(proxy.LimitKey, strconv.Itoa(limit)), kv(proxy.OffsetKey, strconv.Itoa(offset)),
		}
	}
	resp, err := s.Cluster.MilvusClient.Query(ctx, request)
	s.Require().NoError(merr.CheckRPCCall(resp, err), msg)

	var wantPKs []int64
	var wantIndices [][]int64
	if elem == nil {
		for _, r := range m.byPK() {
			if row == nil || row(r) {
				wantPKs = append(wantPKs, r.pk)
			}
		}
		wantPKs = page(wantPKs, limit, offset)
	} else {
		var flat []vsHit
		for _, r := range m.byPK() {
			for j, e := range r.elems {
				if !r.null && elem(e) {
					flat = append(flat, vsHit{pk: r.pk, offset: int64(j)})
				}
			}
		}
		for _, hit := range page(flat, limit, offset) {
			if n := len(wantPKs); n == 0 || wantPKs[n-1] != hit.pk {
				wantPKs = append(wantPKs, hit.pk)
				wantIndices = append(wantIndices, nil)
			}
			wantIndices[len(wantIndices)-1] = append(wantIndices[len(wantIndices)-1], hit.offset)
		}
	}
	s.Require().NotEmpty(wantPKs, msg)

	var gotPKs []int64
	for _, field := range resp.GetFieldsData() {
		if field.GetFieldName() == "pk" {
			gotPKs = field.GetScalars().GetLongData().GetData()
		}
	}
	var gotIndices [][]int64
	for _, indices := range resp.GetElementIndices() {
		gotIndices = append(gotIndices, indices.GetIndices().GetData())
	}
	if elem == nil {
		s.Require().Empty(gotIndices, msg)
	} else {
		s.Require().Len(gotIndices, len(gotPKs), "%s: element_indices per row", msg)
	}
	if limit > 0 {
		// Paging is in pk order.
		s.Require().Equal(wantPKs, gotPKs, msg)
		if elem != nil {
			s.Require().Equal(wantIndices, gotIndices, msg)
		}
	} else {
		s.Require().ElementsMatch(wantPKs, gotPKs, msg)
		if elem != nil {
			want := make(map[int64][]int64)
			for i, pk := range wantPKs {
				want[pk] = wantIndices[i]
			}
			for i, pk := range gotPKs {
				s.Require().Equal(want[pk], gotIndices[i], "%s: element_indices of pk %d", msg, pk)
			}
		}
	}
	s.opCheckFields(resp.GetFieldsData(), gotPKs, children, m, msg)
}

func (s *ElementNullableSuite) opRunChecks(ctx context.Context, name string, m *opModel, vectors []string) {
	outputs := m.outputs()
	all := outputs[0]
	s.Run("query", func() {
		for _, children := range outputs {
			s.opQuery(ctx, name, m, "pk >= 0", nil, nil, 0, 0, children)
		}
		s.opQuery(ctx, name, m, "pk >= 0", nil, nil, 4, 3, all)
		s.opQuery(ctx, name, m, "pk >= 0", nil, nil, 5, 0, outputs[1])
		s.opQuery(ctx, name, m, "MATCH_ANY(clips, $[b] IS NULL)", opAny(func(e opElem) bool { return e.b == nil }), nil, 0, 0, all)
		s.opQuery(ctx, name, m, "array_contains(clips[name], \"a6\") || pk in [4, 9]",
			func(r opRow) bool { return r.pk == 5 || r.pk == 4 || r.pk == 9 }, nil, 0, 0, all)
	})

	type elemCase struct {
		expr string
		elem func(opElem) bool
	}
	elemCases := []elemCase{
		{"element_filter(clips, $[b] > 4)", intIs(func(v int64) bool { return v > 4 }, "b")},
		{"element_filter(clips, $[b] IS NULL)", func(e opElem) bool { return e.b == nil }},
		{"element_filter(clips, $[name] IS NOT NULL && $[b] IS NULL)", func(e opElem) bool { return e.name != nil && e.b == nil }},
	}
	if m.added {
		elemCases = append(elemCases,
			elemCase{"element_filter(clips, $[lb] IS NULL)", func(e opElem) bool { return e.lb == nil }},
			elemCase{"element_filter(clips, $[lb] > 0 || $[lname] IS NOT NULL)", func(e opElem) bool { return e.lb != nil || e.lname != nil }},
		)
	}
	s.Run("element_filter_query", func() {
		for _, tc := range elemCases {
			s.opQuery(ctx, name, m, tc.expr, nil, tc.elem, 0, 0, all)
			s.opQuery(ctx, name, m, tc.expr, nil, tc.elem, 3, 2, all)
		}
		for _, children := range outputs[1:] {
			s.opQuery(ctx, name, m, elemCases[1].expr, nil, elemCases[1].elem, 0, 0, children)
		}
	})

	s.Run("row_search", func() {
		for _, children := range outputs {
			s.opSearch(ctx, name, m, "embedding", "", false, m.rowHits(nil), vsLimit, 0, children)
		}
		s.opSearch(ctx, name, m, "embedding", "", false, m.rowHits(nil), 4, 3, all)
		anyNull := opAny(func(e opElem) bool { return e.b == nil })
		s.opSearch(ctx, name, m, "embedding", "MATCH_ANY(clips, $[b] IS NULL)", false, m.rowHits(anyNull), vsLimit, 0, all)
	})

	for _, vector := range vectors {
		anns := typeutil.ConcatStructFieldName(opStruct, vector)
		s.Run("element_search/"+vector, func() {
			hits := m.elementHits(vector, nil, nil)
			for _, children := range outputs {
				s.opSearch(ctx, name, m, anns, "", true, hits, vsLimit, 0, children)
			}
			s.opSearch(ctx, name, m, anns, "", true, hits, 3, 2, outputs[0])
			for _, tc := range elemCases {
				want := m.elementHits(vector, nil, tc.elem)
				if len(want) == 0 {
					continue
				}
				s.opSearch(ctx, name, m, anns, tc.expr, true, want, vsLimit, 0, outputs[0])
			}
			bigB := opAny(intIs(func(v int64) bool { return v > 4 }, "b"))
			s.opSearch(ctx, name, m, anns, "MATCH_ANY(clips, $[b] > 4)", true, m.elementHits(vector, bigB, nil), vsLimit, 0, outputs[0])
		})
	}
}

func (s *ElementNullableSuite) opInsert(ctx context.Context, name string, m *opModel, rows []opRow) {
	result, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: name, FieldsData: opFields(rows, m.added), NumRows: uint32(len(rows)),
		HashKeys: integration.GenerateHashKeys(len(rows)),
	})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	m.rows = append(m.rows, rows...)
}

func (s *ElementNullableSuite) opAddChild(ctx context.Context, name string, c opChild) {
	describe := func() int32 {
		resp, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
		if merr.CheckRPCCall(resp.GetStatus(), err) != nil {
			return -1
		}
		return resp.GetSchema().GetVersion()
	}
	before := describe()
	s.Require().GreaterOrEqual(before, int32(0))
	resp, err := s.Cluster.MilvusClient.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		CollectionName: name,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
			AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
				FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{{StructPath: opStruct, FieldSchema: opChildSchema(c)}},
			},
		}},
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetAlterStatus(), err), c.name)
	s.Require().Eventually(func() bool { return describe() > before }, 30*time.Second, 100*time.Millisecond)
}

func (s *ElementNullableSuite) opLogSegments(ctx context.Context, name, phase string) []int64 {
	resp, err := s.Cluster.MilvusClient.GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(resp.GetStatus(), err))
	var sealed []int64
	for _, info := range resp.GetInfos() {
		s.T().Logf("%s: segment %d state %s rows %d nodes %v", phase, info.GetSegmentID(), info.GetState(), info.GetNumRows(), info.GetNodeIds())
		if info.GetState() == commonpb.SegmentState_Sealed || info.GetState() == commonpb.SegmentState_Flushed {
			sealed = append(sealed, info.GetSegmentID())
		}
	}
	return sealed
}

// TestStructSubFieldOutput checks the outputs of element-nullable struct
// sub-fields (Int64, VarChar, Array<Array<Int64>> and vector) end to end, on
// the raw proto: query with output_fields, row-level and element-level search
// with output_fields, element_filter query, limit / offset, and the reduce
// across segments, two shards and two query nodes. Four sub-fields are added
// after data was written; old sealed segments, old growing data and new data
// are read together before and after a reload, and after a compaction.
func (s *ElementNullableSuite) TestStructSubFieldOutput() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	revert := s.Cluster.MustModifyMilvusConfig(map[string]string{"indexCoord.segment.minSegmentNumRowsToEnableIndex": "1"})
	defer revert()
	s.Cluster.AddQueryNode() // removed by the cluster reset after the test

	name := "w9i_output_" + funcutil.RandomString(8)
	data, err := proto.Marshal(opSchema(name))
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name, Schema: data, ShardsNum: 2, ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	defer func() {
		status, err := s.Cluster.MilvusClient.DropCollection(context.Background(), &milvuspb.DropCollectionRequest{CollectionName: name})
		s.NoError(merr.CheckRPCCall(status, err))
	}()

	vec := typeutil.ConcatStructFieldName(opStruct, "vec")
	lvec := typeutil.ConcatStructFieldName(opStruct, "lvec")
	indexed := []string{"embedding", vec}
	for _, field := range indexed {
		s.vsCreateIndex(ctx, name, field)
	}
	m := &opModel{}
	s.opInsert(ctx, name, m, opSealedA)
	s.vsFlush(ctx, name)
	s.opInsert(ctx, name, m, opSealedB)
	s.vsFlush(ctx, name)
	for _, field := range indexed {
		s.vsWaitIndexed(ctx, name, field, int64(len(m.rows)))
	}
	s.vsLoad(ctx, name)
	s.opInsert(ctx, name, m, opGrowingC)
	s.opLogSegments(ctx, name, "before_add")
	s.Run("before_add", func() { s.opRunChecks(ctx, name, m, []string{"vec"}) })

	// Old sealed segments (with NULL struct rows) and growing rows written
	// under the old schema stay loaded while the sub-fields are added.
	for _, c := range opChildren {
		if c.added {
			s.opAddChild(ctx, name, c)
		}
	}
	m.added = true
	s.opInsert(ctx, name, m, opAddedD)
	s.opLogSegments(ctx, name, "after_add")
	s.Run("after_add", func() { s.opRunChecks(ctx, name, m, []string{"vec"}) })

	indexed = append(indexed, lvec)
	s.vsCreateIndex(ctx, name, lvec)
	s.vsFlush(ctx, name)
	for _, field := range indexed {
		s.vsWaitIndexed(ctx, name, field, int64(len(m.rows)))
	}
	status, err = s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.vsLoad(ctx, name)
	s.opInsert(ctx, name, m, opGrowingE)
	before := s.opLogSegments(ctx, name, "after_reload")
	s.Run("after_reload", func() { s.opRunChecks(ctx, name, m, []string{"vec", "lvec"}) })

	// Mix compaction merges segments written before and after the add.
	describe, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(describe.GetStatus(), err))
	compact, err := s.Cluster.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{CollectionID: describe.GetCollectionID()})
	s.Require().NoError(merr.CheckRPCCall(compact.GetStatus(), err))
	s.Require().Greater(compact.GetCompactionPlanCount(), int32(0))
	s.Require().Eventually(func() bool {
		state, err := s.Cluster.MilvusClient.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{CompactionID: compact.GetCompactionID()})
		return err == nil && merr.Ok(state.GetStatus()) && state.GetState() == commonpb.CompactionState_Completed
	}, 5*time.Minute, time.Second)
	plans, err := s.Cluster.MilvusClient.GetCompactionStateWithPlans(ctx, &milvuspb.GetCompactionPlansRequest{CompactionID: compact.GetCompactionID()})
	s.Require().NoError(merr.CheckRPCCall(plans.GetStatus(), err))
	sources := typeutil.NewSet[int64]()
	targets := typeutil.NewSet[int64]()
	for _, plan := range plans.GetMergeInfos() {
		sources.Insert(plan.GetSources()...)
		targets.Insert(plan.GetTargets()...)
	}
	s.T().Logf("compaction %d: sources %v targets %v, loaded before %v", compact.GetCompactionID(), sources.Collect(), targets.Collect(), before)
	s.Require().NotZero(sources.Len())
	// Wait until the query nodes serve the compacted segments only.
	s.Require().Eventually(func() bool {
		resp, err := s.Cluster.MilvusClient.GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{CollectionName: name})
		if merr.CheckRPCCall(resp.GetStatus(), err) != nil {
			return false
		}
		loaded := typeutil.NewSet[int64]()
		for _, info := range resp.GetInfos() {
			loaded.Insert(info.GetSegmentID())
		}
		for _, id := range sources.Collect() {
			if loaded.Contain(id) {
				return false
			}
		}
		return true
	}, 5*time.Minute, time.Second, "compacted segments are not loaded")
	s.opLogSegments(ctx, name, "after_compaction")
	s.Run("after_compaction", func() { s.opRunChecks(ctx, name, m, []string{"vec", "lvec"}) })
}
