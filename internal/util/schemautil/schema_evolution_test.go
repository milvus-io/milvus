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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestValidateSchemaEvolutionAllowed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*schemapb.CollectionSchema)
	}{
		{name: "unchanged", mutate: func(*schemapb.CollectionSchema) {}},
		{name: "add nullable field", mutate: func(schema *schemapb.CollectionSchema) {
			schema.Fields = append(schema.Fields, evolutionField(106, "nullable", schemapb.DataType_Int64, true))
			setMaxFieldID(schema, 106)
		}},
		{name: "add field with default", mutate: func(schema *schemapb.CollectionSchema) {
			field := evolutionField(106, "with_default", schemapb.DataType_Int64, false)
			field.DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 7}}
			schema.Fields = append(schema.Fields, field)
			setMaxFieldID(schema, 106)
		}},
		{name: "add nullable struct container", mutate: func(schema *schemapb.CollectionSchema) {
			child := evolutionField(107, "profile[values]", schemapb.DataType_Array, true)
			child.ElementType = schemapb.DataType_Int64
			child.TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "32"}}
			schema.StructArrayFields = append(schema.StructArrayFields, &schemapb.StructArrayFieldSchema{
				FieldID:  106,
				Name:     "profile",
				Nullable: true,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxCapacityKey, Value: "32"},
				},
				Fields: []*schemapb.FieldSchema{child},
			})
			setMaxFieldID(schema, 107)
		}},
		{name: "drop ordinary field", mutate: func(schema *schemapb.CollectionSchema) {
			schema.Fields = removeEvolutionField(schema.Fields, 105)
		}},
		{name: "grow max length", mutate: func(schema *schemapb.CollectionSchema) {
			setEvolutionTypeParam(evolutionFieldByID(schema, 104), common.MaxLengthKey, "512")
		}},
		{name: "grow max capacity", mutate: func(schema *schemapb.CollectionSchema) {
			setEvolutionTypeParam(evolutionFieldByID(schema, 105), common.MaxCapacityKey, "128")
		}},
		{name: "enable dynamic field", mutate: func(schema *schemapb.CollectionSchema) {
			schema.EnableDynamicField = true
			field := evolutionField(106, common.MetaFieldName, schemapb.DataType_JSON, true)
			field.IsDynamic = true
			field.DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_BytesData{BytesData: []byte("{}")}}
			schema.Fields = append(schema.Fields, field)
			setMaxFieldID(schema, 106)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldSchema := evolutionBaseSchema()
			newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
			test.mutate(newSchema)
			require.NoError(t, ValidateSchemaEvolution(oldSchema, newSchema))
		})
	}

	t.Run("disable dynamic field", func(t *testing.T) {
		oldSchema := evolutionBaseSchema()
		oldSchema.EnableDynamicField = true
		field := evolutionField(106, common.MetaFieldName, schemapb.DataType_JSON, true)
		field.IsDynamic = true
		oldSchema.Fields = append(oldSchema.Fields, field)
		setMaxFieldID(oldSchema, 106)
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		newSchema.EnableDynamicField = false
		newSchema.Fields = removeEvolutionField(newSchema.Fields, 106)
		require.NoError(t, ValidateSchemaEvolution(oldSchema, newSchema))
	})

	t.Run("drop function and clear former output role", func(t *testing.T) {
		oldSchema := evolutionSchemaWithFunction()
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		newSchema.Functions = nil
		evolutionFieldByID(newSchema, 106).IsFunctionOutput = false
		require.NoError(t, ValidateSchemaEvolution(oldSchema, newSchema))
	})

	t.Run("drop whole struct container", func(t *testing.T) {
		oldSchema := evolutionSchemaWithStruct()
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		newSchema.StructArrayFields = nil
		require.NoError(t, ValidateSchemaEvolution(oldSchema, newSchema))
	})

	for _, test := range []struct {
		name     string
		oldValue string
	}{
		{name: "repair missing max field id", oldValue: ""},
		{name: "repair malformed max field id", oldValue: "not-a-number"},
		{name: "repair stale low max field id", oldValue: "102"},
	} {
		t.Run(test.name, func(t *testing.T) {
			oldSchema := evolutionBaseSchema()
			if test.oldValue == "" {
				removeEvolutionProperty(oldSchema, common.MaxFieldIDKey)
			} else {
				setEvolutionProperty(oldSchema, common.MaxFieldIDKey, test.oldValue)
			}
			newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
			setMaxFieldID(newSchema, 105)
			require.NoError(t, ValidateSchemaEvolution(oldSchema, newSchema))
		})
	}

	t.Run("repair stale low max field id using struct child floor", func(t *testing.T) {
		oldSchema := evolutionSchemaWithStruct()
		setMaxFieldID(oldSchema, 102)
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		setMaxFieldID(newSchema, 107)
		require.NoError(t, ValidateSchemaEvolution(oldSchema, newSchema))
	})

	t.Run("preserve stale high max field id", func(t *testing.T) {
		oldSchema := evolutionBaseSchema()
		setMaxFieldID(oldSchema, 120)
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		newSchema.Fields = removeEvolutionField(newSchema.Fields, 105)
		require.NoError(t, ValidateSchemaEvolution(oldSchema, newSchema))
	})

	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "unchanged malformed max field id", value: "not-a-number"},
		{name: "unchanged stale low max field id", value: "102"},
	} {
		t.Run(test.name, func(t *testing.T) {
			oldSchema := evolutionBaseSchema()
			setEvolutionProperty(oldSchema, common.MaxFieldIDKey, test.value)
			newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
			require.NoError(t, ValidateSchemaEvolution(oldSchema, newSchema))
		})
	}
}

func TestValidateSchemaEvolutionRejectsKeptFieldReinterpretation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*schemapb.FieldSchema)
	}{
		{name: "data type", mutate: func(field *schemapb.FieldSchema) { field.DataType = schemapb.DataType_Int32 }},
		{name: "element type", mutate: func(field *schemapb.FieldSchema) { field.ElementType = schemapb.DataType_Int32 }},
		{name: "nullability", mutate: func(field *schemapb.FieldSchema) { field.Nullable = false }},
		{name: "dimension", mutate: func(field *schemapb.FieldSchema) { setEvolutionTypeParam(field, common.DimKey, "256") }},
		{name: "function output role", mutate: func(field *schemapb.FieldSchema) { field.IsFunctionOutput = true }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldSchema := evolutionBaseSchema()
			newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
			var field *schemapb.FieldSchema
			switch test.name {
			case "element type", "nullability":
				field = evolutionFieldByID(newSchema, 105)
			case "dimension":
				field = evolutionFieldByID(newSchema, 103)
			default:
				field = evolutionFieldByID(newSchema, 102)
			}
			test.mutate(field)
			require.Error(t, ValidateSchemaEvolution(oldSchema, newSchema))
		})
	}
}

func TestValidateSchemaEvolutionRejectsNonMonotonicBounds(t *testing.T) {
	tests := []struct {
		name   string
		field  int64
		key    string
		value  string
		remove bool
	}{
		{name: "shrink max length", field: 104, key: common.MaxLengthKey, value: "64"},
		{name: "remove max length", field: 104, key: common.MaxLengthKey, remove: true},
		{name: "shrink max capacity", field: 105, key: common.MaxCapacityKey, value: "16"},
		{name: "remove max capacity", field: 105, key: common.MaxCapacityKey, remove: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldSchema := evolutionBaseSchema()
			newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
			field := evolutionFieldByID(newSchema, test.field)
			if test.remove {
				removeEvolutionTypeParam(field, test.key)
			} else {
				setEvolutionTypeParam(field, test.key, test.value)
			}
			require.Error(t, ValidateSchemaEvolution(oldSchema, newSchema))
		})
	}

	t.Run("shrink max field id", func(t *testing.T) {
		oldSchema := evolutionBaseSchema()
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		setMaxFieldID(newSchema, 104)
		require.Error(t, ValidateSchemaEvolution(oldSchema, newSchema))
	})

	t.Run("remove max field id", func(t *testing.T) {
		oldSchema := evolutionBaseSchema()
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		removeEvolutionProperty(newSchema, common.MaxFieldIDKey)
		require.Error(t, ValidateSchemaEvolution(oldSchema, newSchema))
	})

	t.Run("alter max field id without allocating a field", func(t *testing.T) {
		oldSchema := evolutionBaseSchema()
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		setMaxFieldID(newSchema, 110)
		require.Error(t, ValidateSchemaEvolution(oldSchema, newSchema))
	})
}

func TestValidateSchemaEvolutionRejectsUnsafeAddedFields(t *testing.T) {
	tests := []struct {
		name  string
		field *schemapb.FieldSchema
	}{
		{name: "non-nullable field without default", field: evolutionField(106, "required", schemapb.DataType_Int64, false)},
		{name: "primary key", field: func() *schemapb.FieldSchema {
			field := evolutionField(106, "new_pk", schemapb.DataType_Int64, true)
			field.IsPrimaryKey = true
			return field
		}()},
		{name: "auto id", field: func() *schemapb.FieldSchema {
			field := evolutionField(106, "auto_id", schemapb.DataType_Int64, true)
			field.AutoID = true
			return field
		}()},
		{name: "partition key", field: func() *schemapb.FieldSchema {
			field := evolutionField(106, "partition_key", schemapb.DataType_Int64, true)
			field.IsPartitionKey = true
			return field
		}()},
		{name: "system field id", field: evolutionField(int64(common.RowIDField), "fake_system", schemapb.DataType_Int64, true)},
		{name: "system field name", field: evolutionField(106, common.TimeStampFieldName, schemapb.DataType_Int64, true)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldSchema := evolutionBaseSchema()
			newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
			newSchema.Fields = append(newSchema.Fields, test.field)
			setMaxFieldID(newSchema, 106)
			require.Error(t, ValidateSchemaEvolution(oldSchema, newSchema))
		})
	}

	t.Run("reused dropped field id", func(t *testing.T) {
		oldSchema := evolutionBaseSchema()
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		newSchema.Fields = removeEvolutionField(newSchema.Fields, 105)
		newSchema.Fields = append(newSchema.Fields, evolutionField(105, "replacement", schemapb.DataType_Int64, true))
		require.Error(t, ValidateSchemaEvolution(oldSchema, newSchema))
	})

	t.Run("legacy schema must establish max field id before adding", func(t *testing.T) {
		oldSchema := evolutionBaseSchema()
		removeEvolutionProperty(oldSchema, common.MaxFieldIDKey)
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		newSchema.Fields = append(newSchema.Fields, evolutionField(106, "nullable", schemapb.DataType_Int64, true))
		require.Error(t, ValidateSchemaEvolution(oldSchema, newSchema))
	})
}

func TestValidateSchemaEvolutionRejectsStructSubFieldReparenting(t *testing.T) {
	oldSchema := evolutionBaseSchema()
	oldSchema.StructArrayFields = []*schemapb.StructArrayFieldSchema{
		{
			FieldID:  106,
			Name:     "left",
			Nullable: true,
			Fields: []*schemapb.FieldSchema{
				evolutionField(107, "left[value]", schemapb.DataType_Int64, true),
			},
		},
		{FieldID: 108, Name: "right", Nullable: true},
	}
	setMaxFieldID(oldSchema, 108)
	newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
	moved := newSchema.StructArrayFields[0].Fields[0]
	newSchema.StructArrayFields[0].Fields = nil
	newSchema.StructArrayFields[1].Fields = []*schemapb.FieldSchema{moved}

	require.Error(t, ValidateSchemaEvolution(oldSchema, newSchema))
}

func TestValidateSchemaEvolutionRejectsIndependentStructSubFieldMutation(t *testing.T) {
	t.Run("add child to kept container", func(t *testing.T) {
		oldSchema := evolutionSchemaWithStruct()
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		child := evolutionField(108, "profile[extra]", schemapb.DataType_Array, true)
		child.ElementType = schemapb.DataType_Int64
		child.TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "16"}}
		newSchema.StructArrayFields[0].Fields = append(newSchema.StructArrayFields[0].Fields, child)
		setMaxFieldID(newSchema, 108)
		require.ErrorContains(t, ValidateSchemaEvolution(oldSchema, newSchema), "cannot add sub-field")
	})

	t.Run("drop child from kept container", func(t *testing.T) {
		oldSchema := evolutionSchemaWithStruct()
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		newSchema.StructArrayFields[0].Fields = nil
		require.ErrorContains(t, ValidateSchemaEvolution(oldSchema, newSchema), "cannot drop sub-field")
	})

	t.Run("add whole container with non-nullable child", func(t *testing.T) {
		oldSchema := evolutionBaseSchema()
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		child := evolutionField(107, "profile[values]", schemapb.DataType_Array, false)
		child.ElementType = schemapb.DataType_Int64
		child.TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "16"}}
		newSchema.StructArrayFields = []*schemapb.StructArrayFieldSchema{
			{FieldID: 106, Name: "profile", Nullable: true, Fields: []*schemapb.FieldSchema{child}},
		}
		setMaxFieldID(newSchema, 107)
		require.ErrorContains(t, ValidateSchemaEvolution(oldSchema, newSchema), "non-nullable sub-field")
	})

	t.Run("add empty whole container", func(t *testing.T) {
		oldSchema := evolutionBaseSchema()
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		newSchema.StructArrayFields = []*schemapb.StructArrayFieldSchema{
			{FieldID: 106, Name: "profile", Nullable: true},
		}
		setMaxFieldID(newSchema, 106)
		require.ErrorContains(t, ValidateSchemaEvolution(oldSchema, newSchema), "must contain at least one sub-field")
	})
}

func TestValidateSchemaEvolutionRejectsProtectedDrops(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*schemapb.CollectionSchema)
	}{
		{name: "primary key", mutate: func(schema *schemapb.CollectionSchema) { schema.Fields = removeEvolutionField(schema.Fields, 100) }},
		{name: "system field", mutate: func(schema *schemapb.CollectionSchema) {
			schema.Fields = removeEvolutionField(schema.Fields, int64(common.RowIDField))
		}},
		{name: "last vector field", mutate: func(schema *schemapb.CollectionSchema) { schema.Fields = removeEvolutionField(schema.Fields, 103) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldSchema := evolutionBaseSchema()
			newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
			test.mutate(newSchema)
			require.Error(t, ValidateSchemaEvolution(oldSchema, newSchema))
		})
	}

	for _, test := range []struct {
		name string
		mark func(*schemapb.FieldSchema)
	}{
		{name: "partition key", mark: func(field *schemapb.FieldSchema) { field.IsPartitionKey = true }},
		{name: "clustering key", mark: func(field *schemapb.FieldSchema) { field.IsClusteringKey = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			oldSchema := evolutionBaseSchema()
			test.mark(evolutionFieldByID(oldSchema, 104))
			newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
			newSchema.Fields = removeEvolutionField(newSchema.Fields, 104)
			require.Error(t, ValidateSchemaEvolution(oldSchema, newSchema))
		})
	}

	t.Run("surviving function input", func(t *testing.T) {
		oldSchema := evolutionSchemaWithFunction()
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		newSchema.Fields = removeEvolutionField(newSchema.Fields, 102)
		require.Error(t, ValidateSchemaEvolution(oldSchema, newSchema))
	})

	t.Run("surviving function output", func(t *testing.T) {
		oldSchema := evolutionSchemaWithFunction()
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		newSchema.Fields = removeEvolutionField(newSchema.Fields, 106)
		require.Error(t, ValidateSchemaEvolution(oldSchema, newSchema))
	})
}

func TestValidateSchemaEvolutionRejectsMalformedDynamicGraphs(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*schemapb.CollectionSchema)
	}{
		{name: "enabled without dynamic field", mutate: func(schema *schemapb.CollectionSchema) { schema.EnableDynamicField = true }},
		{name: "disabled with dynamic field", mutate: func(schema *schemapb.CollectionSchema) {
			field := evolutionField(106, common.MetaFieldName, schemapb.DataType_JSON, true)
			field.IsDynamic = true
			schema.Fields = append(schema.Fields, field)
			setMaxFieldID(schema, 106)
		}},
		{name: "multiple dynamic fields", mutate: func(schema *schemapb.CollectionSchema) {
			schema.EnableDynamicField = true
			for id := int64(106); id <= 107; id++ {
				field := evolutionField(id, common.MetaFieldName+strconv.FormatInt(id, 10), schemapb.DataType_JSON, true)
				field.IsDynamic = true
				schema.Fields = append(schema.Fields, field)
			}
			setMaxFieldID(schema, 107)
		}},
		{name: "dynamic field has wrong type", mutate: func(schema *schemapb.CollectionSchema) {
			schema.EnableDynamicField = true
			field := evolutionField(106, common.MetaFieldName, schemapb.DataType_Int64, true)
			field.IsDynamic = true
			schema.Fields = append(schema.Fields, field)
			setMaxFieldID(schema, 106)
		}},
		{name: "dynamic field has wrong name", mutate: func(schema *schemapb.CollectionSchema) {
			schema.EnableDynamicField = true
			field := evolutionField(106, "dynamic", schemapb.DataType_JSON, true)
			field.IsDynamic = true
			schema.Fields = append(schema.Fields, field)
			setMaxFieldID(schema, 106)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldSchema := evolutionBaseSchema()
			newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
			test.mutate(newSchema)
			require.Error(t, ValidateSchemaEvolution(oldSchema, newSchema))
		})
	}
}

func TestValidateSchemaEvolutionClusteringKey(t *testing.T) {
	longDefault := func() *schemapb.ValueField {
		return &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 0}}
	}

	t.Run("add clustering key allowed", func(t *testing.T) {
		oldSchema := evolutionBaseSchema()
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		ck := evolutionField(106, "ck", schemapb.DataType_Int64, false)
		ck.IsClusteringKey = true
		ck.DefaultValue = longDefault()
		newSchema.Fields = append(newSchema.Fields, ck)
		setMaxFieldID(newSchema, 106)
		require.NoError(t, ValidateSchemaEvolution(oldSchema, newSchema))
	})

	t.Run("second clustering key rejected", func(t *testing.T) {
		oldSchema := evolutionBaseSchema()
		evolutionFieldByID(oldSchema, 104).IsClusteringKey = true
		newSchema := proto.Clone(oldSchema).(*schemapb.CollectionSchema)
		ck := evolutionField(106, "ck2", schemapb.DataType_Int64, false)
		ck.IsClusteringKey = true
		ck.DefaultValue = longDefault()
		newSchema.Fields = append(newSchema.Fields, ck)
		setMaxFieldID(newSchema, 106)
		require.ErrorContains(t, ValidateSchemaEvolution(oldSchema, newSchema), "one clustering key")
	})
}

func evolutionBaseSchema() *schemapb.CollectionSchema {
	rowID := evolutionField(int64(common.RowIDField), common.RowIDFieldName, schemapb.DataType_Int64, false)
	rowID.AutoID = true
	timestamp := evolutionField(int64(common.TimeStampField), common.TimeStampFieldName, schemapb.DataType_Int64, false)
	pk := evolutionField(100, "pk", schemapb.DataType_Int64, false)
	pk.IsPrimaryKey = true
	body := evolutionField(102, "body", schemapb.DataType_Text, true)
	body.TypeParams = []*commonpb.KeyValuePair{{Key: common.EnableAnalyzerKey, Value: "true"}}
	vector := evolutionField(103, "embedding", schemapb.DataType_FloatVector, false)
	vector.TypeParams = []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}
	title := evolutionField(104, "title", schemapb.DataType_VarChar, true)
	title.TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "128"}}
	tags := evolutionField(105, "tags", schemapb.DataType_Array, true)
	tags.ElementType = schemapb.DataType_VarChar
	tags.TypeParams = []*commonpb.KeyValuePair{
		{Key: common.MaxCapacityKey, Value: "64"},
		{Key: common.MaxLengthKey, Value: "32"},
	}
	return &schemapb.CollectionSchema{
		Name:       "evolution",
		Fields:     []*schemapb.FieldSchema{rowID, timestamp, pk, body, vector, title, tags},
		Properties: []*commonpb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "105"}},
	}
}

func evolutionSchemaWithFunction() *schemapb.CollectionSchema {
	schema := evolutionBaseSchema()
	output := evolutionField(106, "sparse", schemapb.DataType_SparseFloatVector, false)
	output.IsFunctionOutput = true
	schema.Fields = append(schema.Fields, output)
	schema.Functions = []*schemapb.FunctionSchema{evolutionFunction(200, 102, 106)}
	setMaxFieldID(schema, 106)
	return schema
}

func evolutionSchemaWithStruct() *schemapb.CollectionSchema {
	schema := evolutionBaseSchema()
	child := evolutionField(107, "profile[values]", schemapb.DataType_Array, true)
	child.ElementType = schemapb.DataType_Int64
	child.TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "32"}}
	schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{
		{
			FieldID:  106,
			Name:     "profile",
			Nullable: true,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.MaxCapacityKey, Value: "64"},
			},
			Fields: []*schemapb.FieldSchema{child},
		},
	}
	setMaxFieldID(schema, 107)
	return schema
}

func evolutionFunction(id, inputID, outputID int64) *schemapb.FunctionSchema {
	return &schemapb.FunctionSchema{
		Id:               id,
		Name:             "bm25",
		Type:             schemapb.FunctionType_BM25,
		InputFieldIds:    []int64{inputID},
		InputFieldNames:  []string{"body"},
		OutputFieldIds:   []int64{outputID},
		OutputFieldNames: []string{"sparse"},
	}
}

func evolutionField(id int64, name string, dataType schemapb.DataType, nullable bool) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{FieldID: id, Name: name, DataType: dataType, Nullable: nullable}
}

func evolutionFieldByID(schema *schemapb.CollectionSchema, fieldID int64) *schemapb.FieldSchema {
	for _, field := range schema.GetFields() {
		if field.GetFieldID() == fieldID {
			return field
		}
	}
	return nil
}

func removeEvolutionField(fields []*schemapb.FieldSchema, fieldID int64) []*schemapb.FieldSchema {
	result := make([]*schemapb.FieldSchema, 0, len(fields))
	for _, field := range fields {
		if field.GetFieldID() != fieldID {
			result = append(result, field)
		}
	}
	return result
}

func setEvolutionTypeParam(field *schemapb.FieldSchema, key, value string) {
	for _, param := range field.GetTypeParams() {
		if param.GetKey() == key {
			param.Value = value
			return
		}
	}
	field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{Key: key, Value: value})
}

func removeEvolutionTypeParam(field *schemapb.FieldSchema, key string) {
	params := make([]*commonpb.KeyValuePair, 0, len(field.GetTypeParams()))
	for _, param := range field.GetTypeParams() {
		if param.GetKey() != key {
			params = append(params, param)
		}
	}
	field.TypeParams = params
}

func setMaxFieldID(schema *schemapb.CollectionSchema, value int64) {
	for _, property := range schema.GetProperties() {
		if property.GetKey() == common.MaxFieldIDKey {
			property.Value = strconv.FormatInt(value, 10)
			return
		}
	}
	schema.Properties = append(schema.Properties, &commonpb.KeyValuePair{
		Key:   common.MaxFieldIDKey,
		Value: strconv.FormatInt(value, 10),
	})
}

func removeEvolutionProperty(schema *schemapb.CollectionSchema, key string) {
	properties := make([]*commonpb.KeyValuePair, 0, len(schema.GetProperties()))
	for _, property := range schema.GetProperties() {
		if property.GetKey() != key {
			properties = append(properties, property)
		}
	}
	schema.Properties = properties
}

func setEvolutionProperty(schema *schemapb.CollectionSchema, key, value string) {
	for _, property := range schema.GetProperties() {
		if property.GetKey() == key {
			property.Value = value
			return
		}
	}
	schema.Properties = append(schema.Properties, &commonpb.KeyValuePair{Key: key, Value: value})
}
