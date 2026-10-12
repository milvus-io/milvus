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

package chain

import (
	"fmt"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// functionInputSchema binds the actual fields selected for this invocation.
// Repeated argument references remain positional; adapters may deduplicate them.
func functionInputSchema(input *DataFrame, names []string) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(names))
	for i, name := range names {
		field, err := input.resolveArrowField(name)
		if err != nil {
			return nil, err
		}
		if dataType, ok := input.FieldType(name); ok {
			metadata := field.Metadata.ToMap()
			if raw, exists := metadata["milvus.data_type"]; exists && raw != strconv.Itoa(int(dataType)) {
				return nil, merr.WrapErrServiceInternalMsg("column %q has conflicting Milvus type metadata", name)
			}
			metadata["milvus.data_type"] = strconv.Itoa(int(dataType))
			if _, declared := field.Metadata.GetValue(types.MetadataKeyMilvusDataType); !declared {
				// Inferred arrays and FloatVector need their element type/dimension,
				// not just the enum. Build this from the self-describing Arrow field.
				if logical, err := types.MilvusSchemaFromArrowField(field); err == nil && logical.GetDataType() == dataType {
					encoded, err := protojson.Marshal(logical)
					if err != nil {
						return nil, merr.WrapErrServiceInternalErr(err, "encode inferred logical schema")
					}
					metadata[types.MetadataKeyMilvusFieldSchema] = string(encoded)
					if logical.GetElementType() != schemapb.DataType_None {
						metadata["element_type"] = strconv.Itoa(int(logical.GetElementType()))
					}
					for _, param := range logical.GetTypeParams() {
						metadata[param.GetKey()] = param.GetValue()
					}
				}
			}
			field.Metadata = arrow.MetadataFrom(metadata)
		}
		field, err = normalizeRuntimeArrowField(field, name, field.Nullable, false)
		if err != nil {
			return nil, err
		}
		fields[i] = field
	}
	var metadata arrow.Metadata
	if input.schema != nil {
		metadata = input.schema.Metadata()
	}
	return arrow.NewSchema(fields, &metadata), nil
}

func validateOperatorInputSchema(op Operator, input *DataFrame) error {
	schema, err := functionInputSchema(input, op.Inputs())
	if err != nil {
		return err
	}
	if typed, ok := op.(interface{ InputDataTypes() []schemapb.DataType }); ok {
		hints := typed.InputDataTypes()
		if len(hints) != 0 && len(hints) != schema.NumFields() {
			return merr.WrapErrServiceInternal("operator input hints are inconsistent")
		}
		for i, expected := range hints {
			if expected == schemapb.DataType_None {
				continue
			}
			field := schema.Field(i)
			if logical, err := types.MilvusSchemaFromArrowField(field); err == nil && compatibleScalarHint(logical.GetDataType(), expected) {
				// Legacy hints describe compatible scalar arguments; they do not
				// erase the actual logical type passed to a schema-aware function.
				field.Metadata = arrow.Metadata{}
			}
			if err := types.ValidateArrowFieldMilvusType(field, expected); err != nil {
				// The request explicitly chose this hint. A mismatch is a bad
				// binding, distinct from malformed data returned by a function.
				return merr.WrapErrParameterInvalidErr(err, "input %q type hint %s", op.Inputs()[i], expected)
			}
		}
	}
	return nil
}

func compatibleScalarHint(actual, expected schemapb.DataType) bool {
	text := func(t schemapb.DataType) bool {
		return t == schemapb.DataType_String || t == schemapb.DataType_VarChar || t == schemapb.DataType_Text
	}
	return text(actual) && text(expected) || actual == schemapb.DataType_Timestamptz && expected == schemapb.DataType_Int64 ||
		actual == schemapb.DataType_Date && expected == schemapb.DataType_Int32 || actual == schemapb.DataType_Time && expected == schemapb.DataType_Int64
}

// executeFunction preserves runtime Arrow schema when the adapter provides it.
// A conventional FunctionExpr remains valid without a separate schema method.
func executeFunction(ctx *types.FuncContext, function types.FunctionExpr, inputSchema *arrow.Schema, inputs []*arrow.Chunked) (schema *arrow.Schema, outputs []*arrow.Chunked, err error) {
	if aware, ok := function.(types.SchemaAwareFunctionExpr); ok {
		schema, outputs, err = aware.ExecuteWithSchema(ctx, inputSchema, inputs)
	} else {
		outputs, err = function.Execute(ctx, inputs)
	}
	if err != nil {
		releaseColumns(outputs)
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			releaseColumns(outputs)
			outputs = nil
		}
	}()
	declared := function.OutputDataTypes()
	if declared != nil && len(declared) != len(outputs) {
		return nil, outputs, merr.WrapErrFunctionFailedMsg("function %q returned %d outputs, declared %d", function.Name(), len(outputs), len(declared))
	}
	fields := make([]arrow.Field, len(outputs))
	for i, output := range outputs {
		if output == nil || output.DataType() == nil {
			return nil, outputs, merr.WrapErrFunctionFailedMsg("function output %d is nil or has no type", i)
		}
		if declared != nil && (declared[i] == nil || !arrow.TypeEqual(declared[i], output.DataType())) {
			return nil, outputs, merr.WrapErrFunctionFailedMsg("function output %d type %s differs from declared type %v", i, output.DataType(), declared[i])
		}
		fields[i] = arrow.Field{Name: fmt.Sprintf("c%d", i), Type: output.DataType(), Nullable: output.NullN() > 0}
	}
	if schema == nil {
		schema = arrow.NewSchema(fields, nil)
	}
	if schema.NumFields() != len(outputs) {
		return nil, outputs, merr.WrapErrFunctionFailedMsg("function output schema/column count differs")
	}
	if err = types.ValidateRuntimeArrowSchema(schema); err != nil {
		return nil, outputs, merr.Wrap(err, "invalid function output schema")
	}
	for i, output := range outputs {
		if !arrow.TypeEqual(schema.Field(i).Type, output.DataType()) {
			return nil, outputs, merr.WrapErrFunctionFailedMsg("function output %d differs from its Arrow schema", i)
		}
		for group, chunk := range output.Chunks() {
			if err = types.ValidateArrowArrayAgainstField(chunk, schema.Field(i)); err != nil {
				return nil, outputs, merr.Wrapf(err, "invalid function output column %d group %d", i, group)
			}
		}
	}
	return schema, outputs, nil
}

// Returned fields describe generated payloads, never persisted source identity.
func setFunctionOutputField(builder *DataFrameBuilder, name string, field arrow.Field) error {
	field, err := normalizeRuntimeArrowField(field, name, field.Nullable, true)
	if err != nil {
		return err
	}
	metadata := field.Metadata.ToMap()
	delete(metadata, "milvus.field_id")
	builder.SetArrowFieldMetadata(name, arrow.MetadataFrom(metadata)).SetFieldNullable(name, field.Nullable)
	builder.SetFieldID(name, 0)
	if raw, exists := metadata["milvus.data_type"]; exists {
		dataType, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return merr.WrapErrFunctionFailed(err, "invalid output logical type metadata")
		}
		builder.SetFieldType(name, schemapb.DataType(dataType))
	} else if dataType, err := ToMilvusType(field.Type); err == nil {
		builder.SetFieldType(name, dataType)
	}
	return nil
}

// A projection/aggregate may make a field nullable through its parent mask.
// Rebind field identity/nullability while preserving its logical type parameters.
func normalizeRuntimeArrowField(field arrow.Field, name string, nullable, generated bool) (arrow.Field, error) {
	metadata := field.Metadata.ToMap()
	field.Name, field.Nullable = name, nullable
	if generated {
		delete(metadata, "milvus.field_id")
	}
	if raw, exists := metadata[types.MetadataKeyMilvusFieldSchema]; exists {
		logical := &schemapb.FieldSchema{}
		if err := protojson.Unmarshal([]byte(raw), logical); err != nil {
			return arrow.Field{}, merr.WrapErrFunctionFailed(err, "decode runtime logical schema")
		}
		logical.Name, logical.Nullable = name, nullable
		if generated {
			logical.FieldID = 0
		}
		if logical.GetTypeSchema() != nil {
			logical.TypeSchema.Nullable = nullable
		}
		encoded, err := protojson.Marshal(logical)
		if err != nil {
			return arrow.Field{}, merr.WrapErrFunctionFailed(err, "encode runtime logical schema")
		}
		metadata[types.MetadataKeyMilvusFieldSchema] = string(encoded)
	}
	field.Metadata = arrow.MetadataFrom(metadata)
	return field, nil
}

// RebindArrowField keeps logical metadata consistent with an adapter's source
// name/nullability while retaining source identity and type parameters.
func RebindArrowField(field arrow.Field, name string, nullable bool) (arrow.Field, error) {
	return normalizeRuntimeArrowField(field, name, nullable, false)
}
