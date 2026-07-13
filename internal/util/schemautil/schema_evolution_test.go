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

package schemautil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

// base is a realistic collection: a pk, a float vector, a bm25 input (txt) + output (emb),
// a partition key, a clustering key, and a plain droppable field, plus one bm25 function.
func base() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}},
			{FieldID: 102, Name: "txt", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "100"}}},
			{FieldID: 103, Name: "emb", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			{FieldID: 104, Name: "pkey", DataType: schemapb.DataType_VarChar, IsPartitionKey: true, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}}},
			{FieldID: 105, Name: "ckey", DataType: schemapb.DataType_Int64, IsClusteringKey: true},
			{FieldID: 106, Name: "tag", DataType: schemapb.DataType_Int64, Nullable: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{Id: 1, Name: "bm25", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{102}, OutputFieldIds: []int64{103}},
		},
	}
}

func field(s *schemapb.CollectionSchema, id int64) *schemapb.FieldSchema {
	for _, f := range s.Fields {
		if f.FieldID == id {
			return f
		}
	}
	return nil
}

func dropField(s *schemapb.CollectionSchema, id int64) *schemapb.CollectionSchema {
	kept := s.Fields[:0:0]
	for _, f := range s.Fields {
		if f.FieldID != id {
			kept = append(kept, f)
		}
	}
	s.Fields = kept
	return s
}

func TestValidateSchemaEvolution_Allowed(t *testing.T) {
	// creation / no-op
	assert.NoError(t, ValidateSchemaEvolution(nil, base()))
	assert.NoError(t, ValidateSchemaEvolution(base(), base()))

	// add a nullable field
	add := base()
	add.Fields = append(add.Fields, &schemapb.FieldSchema{FieldID: 200, Name: "n", DataType: schemapb.DataType_Double, Nullable: true})
	assert.NoError(t, ValidateSchemaEvolution(base(), add))

	// add a non-nullable field that carries a default (backfillable)
	def := base()
	def.Fields = append(def.Fields, &schemapb.FieldSchema{FieldID: 201, Name: "d", DataType: schemapb.DataType_Int64, DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 7}}})
	assert.NoError(t, ValidateSchemaEvolution(base(), def))

	// add a function whose input AND output fields are both brand new (existing rows carry
	// neither, so nothing is silently left un-computed)
	addFn := base()
	addFn.Fields = append(addFn.Fields,
		&schemapb.FieldSchema{FieldID: 204, Name: "txt2", DataType: schemapb.DataType_VarChar, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "100"}}},
		&schemapb.FieldSchema{FieldID: 202, Name: "emb2", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true})
	addFn.Functions = append(addFn.Functions, &schemapb.FunctionSchema{Id: 2, Name: "bm25b", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{204}, OutputFieldIds: []int64{202}})
	assert.NoError(t, ValidateSchemaEvolution(base(), addFn))

	// drop a plain field
	assert.NoError(t, ValidateSchemaEvolution(base(), dropField(base(), 106)))

	// drop a function together with its output field
	dropFn := dropField(base(), 103)
	dropFn.Functions = nil
	assert.NoError(t, ValidateSchemaEvolution(base(), dropFn))

	// grow max_length
	grow := base()
	field(grow, 102).TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "200"}}
	assert.NoError(t, ValidateSchemaEvolution(base(), grow))

	// enable the dynamic field (adds $meta)
	enableDyn := base()
	enableDyn.EnableDynamicField = true
	enableDyn.Fields = append(enableDyn.Fields, &schemapb.FieldSchema{FieldID: 203, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true})
	assert.NoError(t, ValidateSchemaEvolution(base(), enableDyn))

	// disable the dynamic field (safe drop of $meta)
	withDyn := base()
	withDyn.EnableDynamicField = true
	withDyn.Fields = append(withDyn.Fields, &schemapb.FieldSchema{FieldID: 203, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true})
	assert.NoError(t, ValidateSchemaEvolution(withDyn, base()))

	// add a struct-array field whose sub-field is NOT individually nullable: allowed, because
	// the sub-field is backfilled through its nullable container, not on its own.
	addStruct := base()
	addStruct.StructArrayFields = append(addStruct.StructArrayFields, &schemapb.StructArrayFieldSchema{
		FieldID: 210, Name: "s", Fields: []*schemapb.FieldSchema{
			{FieldID: 211, Name: "s_a", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
		},
	})
	assert.NoError(t, ValidateSchemaEvolution(base(), addStruct))
}

func TestValidateSchemaEvolution_RejectsInPlace(t *testing.T) {
	// change data type
	retype := base()
	field(retype, 102).DataType = schemapb.DataType_Int64
	assert.Error(t, ValidateSchemaEvolution(base(), retype))

	// flip nullability
	nullable := base()
	field(nullable, 106).Nullable = false
	assert.Error(t, ValidateSchemaEvolution(base(), nullable))

	// shrink max_length
	shrink := base()
	field(shrink, 102).TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "10"}}
	assert.Error(t, ValidateSchemaEvolution(base(), shrink))

	// remove max_length entirely (fail closed)
	dropLen := base()
	field(dropLen, 102).TypeParams = nil
	assert.Error(t, ValidateSchemaEvolution(base(), dropLen))

	// change dim
	newDim := base()
	field(newDim, 101).TypeParams = []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "256"}}
	assert.Error(t, ValidateSchemaEvolution(base(), newDim))

	// remove dim entirely (fail closed)
	dropDim := base()
	field(dropDim, 101).TypeParams = nil
	assert.Error(t, ValidateSchemaEvolution(base(), dropDim))

	// repurpose an existing plain field as a function output (false->true): rejected.
	repurpose := base()
	field(repurpose, 101).IsFunctionOutput = true
	assert.Error(t, ValidateSchemaEvolution(base(), repurpose))

	// detach a function output field (true->false): dropping the function while keeping its
	// output field as plain data is allowed -- this is how DropCollectionFunction works.
	detach := base()
	detach.Functions = nil
	field(detach, 103).IsFunctionOutput = false
	assert.NoError(t, ValidateSchemaEvolution(base(), detach))

	// modify a surviving function in place
	changeFn := base()
	changeFn.Functions = []*schemapb.FunctionSchema{{Id: 1, Name: "bm25", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{104}, OutputFieldIds: []int64{103}}}
	assert.Error(t, ValidateSchemaEvolution(base(), changeFn))

	// add a function onto an EXISTING input field (txt id=102): rejected -- the field's existing
	// rows cannot be given the function output online, so its output would be silently incomplete.
	// The output field is brand new, so the rejection is specifically due to the pre-existing input.
	addFnOldInput := base()
	addFnOldInput.Fields = append(addFnOldInput.Fields, &schemapb.FieldSchema{FieldID: 205, Name: "emb3", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true})
	addFnOldInput.Functions = append(addFnOldInput.Functions, &schemapb.FunctionSchema{Id: 3, Name: "bm25c", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{102}, OutputFieldIds: []int64{205}})
	assert.Error(t, ValidateSchemaEvolution(base(), addFnOldInput))
}

func TestValidateSchemaEvolution_RejectsUnbackfillableAdd(t *testing.T) {
	// non-nullable, no default, not a function output
	add := base()
	add.Fields = append(add.Fields, &schemapb.FieldSchema{FieldID: 300, Name: "req", DataType: schemapb.DataType_Int64})
	assert.Error(t, ValidateSchemaEvolution(base(), add))

	// cannot add a primary key online
	addPK := base()
	addPK.Fields = append(addPK.Fields, &schemapb.FieldSchema{FieldID: 301, Name: "pk2", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, Nullable: true})
	assert.Error(t, ValidateSchemaEvolution(base(), addPK))
}

func TestValidateSchemaEvolution_RejectsGraphBreakingDrop(t *testing.T) {
	assert.Error(t, ValidateSchemaEvolution(base(), dropField(base(), 100)), "drop primary key")
	assert.Error(t, ValidateSchemaEvolution(base(), dropField(base(), 104)), "drop partition key")
	assert.Error(t, ValidateSchemaEvolution(base(), dropField(base(), 105)), "drop clustering key")
	assert.Error(t, ValidateSchemaEvolution(base(), dropField(base(), 102)), "drop a field a surviving function still inputs")
	assert.Error(t, ValidateSchemaEvolution(base(), dropField(base(), 103)), "drop a field a surviving function still outputs")

	// dropping the last vector field
	single := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}}},
	}}
	assert.Error(t, ValidateSchemaEvolution(single, dropField(cloneSchema(single), 101)), "drop the last vector field")
}

func TestValidateSchemaEvolution_RejectsInconsistentGraph(t *testing.T) {
	// orphan function-output field: flagged IsFunctionOutput but no function produces it (the
	// user cannot write it and nothing fills it -> permanently empty). This is the concrete
	// AddCollectionField hole.
	orphan := base()
	orphan.Fields = append(orphan.Fields, &schemapb.FieldSchema{FieldID: 300, Name: "orphan", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true})
	assert.Error(t, ValidateSchemaEvolution(base(), orphan), "orphan IsFunctionOutput field")

	// a dynamic field while EnableDynamicField is off.
	bogusDyn := base()
	bogusDyn.Fields = append(bogusDyn.Fields, &schemapb.FieldSchema{FieldID: 301, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true})
	assert.Error(t, ValidateSchemaEvolution(base(), bogusDyn), "dynamic field with dynamic disabled")

	// a dynamic field with the wrong name.
	badName := base()
	badName.EnableDynamicField = true
	badName.Fields = append(badName.Fields, &schemapb.FieldSchema{FieldID: 302, Name: "notmeta", DataType: schemapb.DataType_JSON, IsDynamic: true})
	assert.Error(t, ValidateSchemaEvolution(base(), badName), "dynamic field not named $meta")

	// a dynamic field with the wrong type.
	badType := base()
	badType.EnableDynamicField = true
	badType.Fields = append(badType.Fields, &schemapb.FieldSchema{FieldID: 303, Name: common.MetaFieldName, DataType: schemapb.DataType_Int64, IsDynamic: true})
	assert.Error(t, ValidateSchemaEvolution(base(), badType), "dynamic field not JSON")

	// EnableDynamicField set but no $meta field present.
	enabledNoMeta := base()
	enabledNoMeta.EnableDynamicField = true
	assert.Error(t, ValidateSchemaEvolution(base(), enabledNoMeta), "dynamic enabled without $meta")

	// two dynamic fields.
	twoDyn := base()
	twoDyn.EnableDynamicField = true
	twoDyn.Fields = append(twoDyn.Fields,
		&schemapb.FieldSchema{FieldID: 306, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true},
		&schemapb.FieldSchema{FieldID: 307, Name: "meta2", DataType: schemapb.DataType_JSON, IsDynamic: true})
	assert.Error(t, ValidateSchemaEvolution(base(), twoDyn), "two dynamic fields")

	// a newly-added function referencing a non-existent input field.
	badInput := base()
	badInput.Fields = append(badInput.Fields, &schemapb.FieldSchema{FieldID: 304, Name: "o2", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true})
	badInput.Functions = append(badInput.Functions, &schemapb.FunctionSchema{Id: 9, Name: "bad", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{9999}, OutputFieldIds: []int64{304}})
	assert.Error(t, ValidateSchemaEvolution(base(), badInput), "function input field does not exist")

	// two functions producing the same output field (input is brand new so it clears the
	// input-must-be-new rule and the failure is specifically the shared output).
	dupOut := base()
	dupOut.Fields = append(dupOut.Fields, &schemapb.FieldSchema{FieldID: 305, Name: "in2", DataType: schemapb.DataType_VarChar, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "50"}}})
	dupOut.Functions = append(dupOut.Functions, &schemapb.FunctionSchema{Id: 9, Name: "bad", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{305}, OutputFieldIds: []int64{103}})
	assert.Error(t, ValidateSchemaEvolution(base(), dupOut), "output field shared by two functions")
}

// cloneSchema returns a shallow structural copy sufficient for these tests (fresh field slice).
func cloneSchema(s *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	out := &schemapb.CollectionSchema{EnableDynamicField: s.EnableDynamicField}
	out.Fields = append(out.Fields, s.Fields...)
	out.Functions = append(out.Functions, s.Functions...)
	return out
}
