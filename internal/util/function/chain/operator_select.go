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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type SelectOp struct{ BaseOp }

func init() {
	MustRegisterOperator(types.OpTypeSelect, statelessOperatorFactory(NewSelectOpFromRepr))
}

func NewSelectOp(inputs, outputs []string) (*SelectOp, error) {
	op, err := transform.Select(inputs, outputs)
	if err != nil {
		return nil, err
	}
	return &SelectOp{BaseOp{inputs: op.Inputs, outputs: op.Outputs}}, nil
}

func (o *SelectOp) Name() string   { return "Select" }
func (o *SelectOp) String() string { return fmt.Sprintf("Select(%v -> %v)", o.inputs, o.outputs) }

func (o *SelectOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	builder := NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes(input.chunkSizes).CopyAllMetadata(input)
	kept := make(map[string]struct{}, len(o.outputs))
	for i, name := range o.inputs {
		column, err := input.ResolveColumn(name, ctx.Pool())
		if err != nil {
			return nil, merr.Wrapf(err, "select input %q", name)
		}
		output := o.outputs[i]
		if err := builder.AddColumns([]string{output}, []*arrow.Chunked{column}); err != nil {
			return nil, err
		}
		if input.HasColumn(name) {
			if dataType, ok := input.FieldType(name); ok {
				builder.SetFieldType(output, dataType)
			}
			if fieldID, ok := input.FieldID(name); ok && name == output {
				builder.SetFieldID(output, fieldID)
			}
			builder.SetFieldNullable(output, input.fieldNullables[name])
			if indexes := input.schema.FieldIndices(name); len(indexes) == 1 {
				metadata := input.schema.Field(indexes[0]).Metadata.ToMap()
				if name != output {
					delete(metadata, "milvus.field_id")
				}
				builder.SetArrowFieldMetadata(output, arrow.MetadataFrom(metadata))
			}
		} else {
			if dataType, err := ToMilvusType(column.DataType()); err == nil {
				builder.SetFieldType(output, dataType)
			}
			builder.SetFieldNullable(output, true)
			field, err := input.resolveArrowField(name)
			if err != nil {
				return nil, err
			}
			metadata := field.Metadata.ToMap()
			delete(metadata, "milvus.field_id")
			builder.SetArrowFieldMetadata(output, arrow.MetadataFrom(metadata))
			if raw, ok := metadata["milvus.data_type"]; ok {
				value, err := strconv.ParseInt(raw, 10, 32)
				if err != nil {
					return nil, merr.WrapErrServiceInternalErr(err, "invalid projected Arrow type metadata")
				}
				if _, ok := schemapb.DataType_name[int32(value)]; !ok {
					return nil, merr.WrapErrServiceInternal("unknown projected Arrow type metadata")
				}
				builder.SetFieldType(output, schemapb.DataType(value))
			}
		}
		if name != output || !input.HasColumn(name) {
			field, err := input.resolveArrowField(name)
			if err != nil {
				return nil, err
			}
			if err := setFunctionOutputField(builder, output, field); err != nil {
				return nil, err
			}
		}
		kept[output] = struct{}{}
	}
	// Search identity and internal provenance survive projection. Schema/dynamic
	// $meta is deliberately not a system column under this predicate.
	for _, name := range input.ColumnNames() {
		if _, exists := kept[name]; exists || ctx.TableResult() || !IsFunctionChainSystemName(name) {
			continue
		}
		if err := builder.AddColumnFrom(input, name); err != nil {
			return nil, err
		}
	}
	return builder.Build(), nil
}

func NewSelectOpFromRepr(repr *OperatorRepr) (Operator, error) {
	if repr.Function != nil || len(repr.Params) != 0 {
		return nil, merr.WrapErrParameterInvalidMsg("select does not accept expression or params")
	}
	return NewSelectOp(repr.Inputs, repr.Outputs)
}

// Select keeps the named non-system columns; system identity stays protected.
func (fc *FuncChain) Select(columns ...string) *FuncChain {
	op, err := NewSelectOp(columns, nil)
	return fc.addWithError(op, err)
}

func (fc *FuncChain) SelectAs(inputs, outputs []string) *FuncChain {
	op, err := NewSelectOp(inputs, outputs)
	return fc.addWithError(op, err)
}
