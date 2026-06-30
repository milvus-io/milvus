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

package validator

import (
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func ValidateFunction(coll *schemapb.CollectionSchema, needValidateFunctionName string, disableRuntimeCheck bool) error {
	nameMap := lo.SliceToMap(coll.GetFields(), func(field *schemapb.FieldSchema) (string, *schemapb.FieldSchema) {
		return field.GetName(), field
	})
	usedOutputField := typeutil.NewSet[string]()
	usedFunctionName := typeutil.NewSet[string]()

	for _, function := range coll.GetFunctions() {
		if err := CheckFunctionBasicParams(function); err != nil {
			return err
		}

		if usedFunctionName.Contain(function.GetName()) {
			return merr.WrapErrParameterInvalidMsg("duplicate function name: %s", function.GetName())
		}

		usedFunctionName.Insert(function.GetName())
		inputFields := []*schemapb.FieldSchema{}
		for _, name := range function.GetInputFieldNames() {
			inputField, ok := nameMap[name]
			if !ok {
				return merr.WrapErrParameterInvalidMsg("function input field not found: %s", name)
			}
			inputFields = append(inputFields, inputField)
		}

		if typeutil.IsExternalCollection(coll) &&
			function.GetType() == schemapb.FunctionType_TextEmbedding &&
			len(inputFields) == 1 &&
			inputFields[0].GetNullable() &&
			inputFields[0].GetExternalField() != "" {
			inputField := proto.Clone(inputFields[0]).(*schemapb.FieldSchema)
			inputField.Nullable = false
			inputFields = []*schemapb.FieldSchema{inputField}
		}

		if err := CheckFunctionInputField(function, inputFields); err != nil {
			return err
		}

		outputFields := make([]*schemapb.FieldSchema, len(function.GetOutputFieldNames()))
		for i, name := range function.GetOutputFieldNames() {
			outputField, ok := nameMap[name]
			if !ok {
				return merr.WrapErrParameterInvalidMsg("function output field not found: %s", name)
			}

			if outputField.GetIsPrimaryKey() {
				return merr.WrapErrParameterInvalidMsg("function output field cannot be primary key: function %s, field %s", function.GetName(), outputField.GetName())
			}

			if outputField.GetIsPartitionKey() || outputField.GetIsClusteringKey() {
				return merr.WrapErrParameterInvalidMsg("function output field cannot be partition key or clustering key: function %s, field %s", function.GetName(), outputField.GetName())
			}

			if outputField.GetNullable() {
				return merr.WrapErrParameterInvalidMsg("function output field cannot be nullable: function %s, field %s", function.GetName(), outputField.GetName())
			}

			outputFields[i] = outputField
			if usedOutputField.Contain(name) {
				return merr.WrapErrParameterInvalidMsg("duplicate function output field: function %s, field %s", function.GetName(), name)
			}
			usedOutputField.Insert(name)
		}

		if err := CheckFunctionOutputField(function, outputFields); err != nil {
			return err
		}
	}
	if !disableRuntimeCheck {
		if err := embedding.ValidateFunctions(coll, needValidateFunctionName, &models.ModelExtraInfo{ClusterID: paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), DBName: coll.DbName}); err != nil {
			return err
		}
	}
	return nil
}

func NormalizeFunctionOutputFields(coll *schemapb.CollectionSchema) error {
	if coll == nil {
		return nil
	}
	fieldsByName := lo.SliceToMap(coll.GetFields(), func(field *schemapb.FieldSchema) (string, *schemapb.FieldSchema) {
		return field.GetName(), field
	})
	fieldsByID := lo.SliceToMap(coll.GetFields(), func(field *schemapb.FieldSchema) (int64, *schemapb.FieldSchema) {
		return field.GetFieldID(), field
	})

	for _, field := range coll.GetFields() {
		field.IsFunctionOutput = false
	}

	for _, function := range coll.GetFunctions() {
		for _, fieldID := range function.GetOutputFieldIds() {
			if fieldID == 0 {
				continue
			}
			field, ok := fieldsByID[fieldID]
			if !ok {
				return merr.WrapErrParameterInvalidMsg("function output field id %d not found in schema", fieldID)
			}
			field.IsFunctionOutput = true
		}
		for _, name := range function.GetOutputFieldNames() {
			if name == "" {
				continue
			}
			field, ok := fieldsByName[name]
			if !ok {
				return merr.WrapErrParameterInvalidMsg("function output field not found: %s", name)
			}
			field.IsFunctionOutput = true
		}
	}
	return nil
}

func CheckFunctionOutputField(fSchema *schemapb.FunctionSchema, fields []*schemapb.FieldSchema) error {
	switch fSchema.GetType() {
	case schemapb.FunctionType_BM25:
		if len(fields) != 1 {
			return merr.WrapErrParameterInvalidMsg("BM25 function only need 1 output field, but got %d", len(fields))
		}

		if !typeutil.IsSparseFloatVectorType(fields[0].GetDataType()) {
			return merr.WrapErrParameterInvalidMsg("BM25 function output field must be a SparseFloatVector field, but got %s", fields[0].DataType.String())
		}
	case schemapb.FunctionType_TextEmbedding:
		if err := embedding.TextEmbeddingOutputsCheck(fields); err != nil {
			return err
		}
	case schemapb.FunctionType_MinHash:
		if len(fields) != 1 {
			return merr.WrapErrParameterInvalidMsg("MinHash function only need 1 output field, but got %d", len(fields))
		}
		if fields[0].GetDataType() != schemapb.DataType_BinaryVector {
			return merr.WrapErrParameterInvalidMsg("MinHash function output field must be a BinaryVector field, but got %s", fields[0].DataType.String())
		}
	default:
		return merr.WrapErrParameterInvalidMsg("check output field for unknown function type")
	}
	return nil
}

func CheckFunctionInputField(function *schemapb.FunctionSchema, fields []*schemapb.FieldSchema) error {
	switch function.GetType() {
	case schemapb.FunctionType_BM25:
		if len(fields) != 1 || (fields[0].DataType != schemapb.DataType_VarChar && fields[0].DataType != schemapb.DataType_Text) {
			return merr.WrapErrParameterInvalidMsg("BM25 function input field must be a VARCHAR/TEXT field, got %d field with type %s",
				len(fields), fields[0].DataType.String())
		}
		h := typeutil.CreateFieldSchemaHelper(fields[0])
		if !h.EnableAnalyzer() {
			return merr.WrapErrParameterInvalidMsg("BM25 function input field must set enable_analyzer to true")
		}
	case schemapb.FunctionType_TextEmbedding:
		if err := embedding.TextEmbeddingInputsCheck(function.GetName(), fields); err != nil {
			return err
		}
	case schemapb.FunctionType_MinHash:
		if len(fields) != 1 || (fields[0].DataType != schemapb.DataType_VarChar && fields[0].DataType != schemapb.DataType_Text) {
			return merr.WrapErrParameterInvalidMsg("MinHash function input field must be a VARCHAR/TEXT field, got %d field with type %s",
				len(fields), fields[0].DataType.String())
		}
	default:
		return merr.WrapErrParameterInvalidMsg("check input field with unknown function type")
	}
	return nil
}

func CheckFunctionBasicParams(function *schemapb.FunctionSchema) error {
	if function.GetName() == "" {
		return merr.WrapErrParameterMissingMsg("function name cannot be empty")
	}
	if len(function.GetInputFieldNames()) == 0 {
		return merr.WrapErrParameterMissingMsg("function input field names cannot be empty, function: %s", function.GetName())
	}
	if len(function.GetOutputFieldNames()) == 0 {
		return merr.WrapErrParameterMissingMsg("function output field names cannot be empty, function: %s", function.GetName())
	}
	for _, input := range function.GetInputFieldNames() {
		if input == "" {
			return merr.WrapErrParameterMissingMsg("function input field name cannot be empty string, function: %s", function.GetName())
		}
		// if input occurs more than once, error
		if lo.Count(function.GetInputFieldNames(), input) > 1 {
			return merr.WrapErrParameterInvalidMsg("each function input field should be used exactly once in the same function, function: %s, input field: %s", function.GetName(), input)
		}
	}
	for _, output := range function.GetOutputFieldNames() {
		if output == "" {
			return merr.WrapErrParameterMissingMsg("function output field name cannot be empty string, function: %s", function.GetName())
		}
		if lo.Count(function.GetInputFieldNames(), output) > 0 {
			return merr.WrapErrParameterInvalidMsg("a single field cannot be both input and output in the same function, function: %s, field: %s", function.GetName(), output)
		}
		if lo.Count(function.GetOutputFieldNames(), output) > 1 {
			return merr.WrapErrParameterInvalidMsg("each function output field should be used exactly once in the same function, function: %s, output field: %s", function.GetName(), output)
		}
	}
	switch function.GetType() {
	case schemapb.FunctionType_BM25:
		if len(function.GetParams()) != 0 {
			return merr.WrapErrParameterInvalidMsg("BM25 function accepts no params")
		}
	case schemapb.FunctionType_TextEmbedding:
		if len(function.GetParams()) == 0 {
			return merr.WrapErrParameterInvalidMsg("TextEmbedding function accepts no params")
		}
	case schemapb.FunctionType_MinHash:
		// MinHash function can accept optional params
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg("check function params with unknown function type")
	}
	return nil
}
