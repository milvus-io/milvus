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
	"context"
	"reflect"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/transformio"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type transformExecutor struct {
	allocator memory.Allocator
	buildCtx  types.FunctionBuildContext
}

// NewTransformExecutor executes native Function Chain operators over logical
// Arrow table groups. Expressions use the normal function registry/build context.
func NewTransformExecutor(allocator memory.Allocator, buildCtx types.FunctionBuildContext) transform.Executor {
	if allocator == nil {
		allocator = memory.DefaultAllocator
	}
	return &transformExecutor{allocator: allocator, buildCtx: buildCtx}
}

func (e *transformExecutor) Execute(ctx context.Context, plan *schemapb.FunctionChain, input transform.Batch) (transform.Result, error) {
	if ctx == nil {
		return transform.Result{}, merr.WrapErrServiceInternal("transform execution context is nil")
	}
	if err := transform.ValidatePlan(plan); err != nil {
		return transform.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return transform.Result{}, err
	}
	repr, err := ProtoChainToRepr(plan)
	if err != nil {
		return transform.Result{}, err
	}
	fc, err := FuncChainFromReprWithContext(repr, e.allocator, e.buildCtx)
	if err != nil {
		return transform.Result{}, err
	}
	if len(input.Groups) == 0 {
		if transform.IsTablePlan(plan) {
			return transform.Result{}, merr.WrapErrServiceInternal("POST TABLE requires a typed input group, including for empty results")
		}
		return transform.Result{}, nil
	}
	frame, err := FromArrowBatch(input, e.allocator)
	if err != nil {
		return transform.Result{}, err
	}
	defer frame.Release()
	opts := ExecuteOptions{QueryContext: input.QueryContext, OutputRowLimit: input.OutputRowLimit}
	if len(fc.operators) > 0 {
		if selection, ok := fc.operators[len(fc.operators)-1].(*SelectOp); ok {
			// A terminal select explicitly defines every ordinary result column.
			// Without one, callers keep the complete parent payload by default.
			opts.EnableColumnPruning = true
			opts.Downstream.RequiredColumns = selection.Outputs()
		}
	}
	transformed, err := fc.ExecuteWithOptions(ctx, opts, frame)
	if err != nil {
		return transform.Result{}, err
	}
	if transformed != frame {
		defer transformed.Release()
	}
	output, err := ToArrowBatch(transformed)
	if err != nil {
		return transform.Result{}, err
	}
	result := transform.Result{Data: output, Lineage: transformed.Lineage()}
	if err := transform.ValidateResultForPlan(plan, input, result); err != nil {
		ReleaseArrowBatch(output)
		return transform.Result{}, err
	}
	return result, nil
}

// FromArrowBatch borrows tables and returns an owned DataFrame with identity
// lineage. Physical Arrow chunks are combined within each logical group.
func FromArrowBatch(input transform.Batch, allocator memory.Allocator) (*DataFrame, error) {
	if allocator == nil {
		allocator = memory.DefaultAllocator
	}
	builder := NewDataFrameBuilder()
	defer builder.Release()
	if len(input.Groups) == 0 {
		builder.SetChunkSizes(nil).SetIdentityLineage()
		return builder.Build(), nil
	}
	var schema *arrow.Schema
	sizes := make([]int64, len(input.Groups))
	for group, table := range input.Groups {
		if nilArrowTable(table) || table.Schema() == nil || table.NumRows() < 0 {
			return nil, merr.WrapErrServiceInternalMsg("invalid transform Arrow table in group %d", group)
		}
		if schema != nil && !schema.Equal(table.Schema()) {
			return nil, merr.WrapErrServiceInternal("transform Arrow group schemas differ")
		}
		schema = table.Schema()
		sizes[group] = table.NumRows()
		if table.NumCols() != int64(len(schema.Fields())) {
			return nil, merr.WrapErrServiceInternal("transform Arrow column count differs from schema")
		}
		if err := types.ValidateRuntimeArrowSchema(schema); err != nil {
			return nil, merr.Wrapf(err, "invalid transform Arrow schema in group %d", group)
		}
	}
	builder.SetChunkSizes(sizes).SetIdentityLineage()
	builder.SetArrowSchemaMetadata(schema.Metadata())
	for column, field := range schema.Fields() {
		chunks := make([]arrow.Array, len(input.Groups))
		for group, table := range input.Groups {
			values := table.Column(column)
			if values == nil || values.Data() == nil || int64(values.Len()) != table.NumRows() {
				for _, chunk := range chunks {
					if chunk != nil {
						chunk.Release()
					}
				}
				return nil, merr.WrapErrServiceInternal("transform Arrow column row count differs from table")
			}
			var err error
			for _, chunk := range values.Data().Chunks() {
				if err := types.ValidateArrowArrayAgainstField(chunk, field); err != nil {
					for _, owned := range chunks {
						if owned != nil {
							owned.Release()
						}
					}
					return nil, merr.Wrapf(err, "invalid transform Arrow column %q group %d", field.Name, group)
				}
			}
			if len(values.Data().Chunks()) == 0 {
				chunks[group] = array.MakeArrayOfNull(allocator, field.Type, 0)
			} else {
				chunks[group], err = array.Concatenate(values.Data().Chunks(), allocator)
			}
			if err != nil {
				for _, chunk := range chunks {
					if chunk != nil {
						chunk.Release()
					}
				}
				return nil, merr.WrapErrServiceInternalErr(err, "combine transform Arrow chunks")
			}
		}
		if err := builder.AddColumnFromChunks(field.Name, chunks); err != nil {
			return nil, err
		}
		builder.SetFieldNullable(field.Name, field.Nullable).SetArrowFieldMetadata(field.Name, field.Metadata)
		if value, ok := field.Metadata.GetValue(transformio.DataTypeMetadataKey); ok {
			dataType, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return nil, merr.WrapErrServiceInternalErr(err, "invalid Arrow Milvus type metadata")
			}
			if _, ok := schemapb.DataType_name[int32(dataType)]; !ok {
				return nil, merr.WrapErrServiceInternal("unknown Arrow Milvus type metadata")
			}
			builder.SetFieldType(field.Name, schemapb.DataType(dataType))
		}
		if value, ok := field.Metadata.GetValue("milvus.field_id"); ok {
			id, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, merr.WrapErrServiceInternalErr(err, "invalid Arrow field ID metadata")
			}
			builder.SetFieldID(field.Name, id)
		}
	}
	return builder.Build(), nil
}

// ToArrowBatch returns independently owned tables, retaining the DataFrame's
// arrays. Source type/field metadata survives conversion and projection.
func ToArrowBatch(frame *DataFrame) (transform.Batch, error) {
	if err := validateFrameShape(frame); err != nil {
		return transform.Batch{}, err
	}
	runtimeSchema, err := functionInputSchema(frame, frame.ColumnNames())
	if err != nil {
		return transform.Batch{}, err
	}
	fields := runtimeSchema.Fields()
	for column, field := range fields {
		metadata := field.Metadata.ToMap()
		if dataType, ok := frame.FieldType(field.Name); ok {
			metadata[transformio.DataTypeMetadataKey] = strconv.Itoa(int(dataType))
		}
		if id, ok := frame.FieldID(field.Name); ok {
			metadata["milvus.field_id"] = strconv.FormatInt(id, 10)
		}
		field.Metadata = arrow.MetadataFrom(metadata)
		fields[column] = field
	}
	metadata := runtimeSchema.Metadata()
	schema := arrow.NewSchema(fields, &metadata)
	result := transform.Batch{Groups: make([]arrow.Table, frame.NumChunks())}
	for group, size := range frame.chunkSizes {
		columns := make([]arrow.Array, frame.NumColumns())
		for column, name := range frame.ColumnNames() {
			columns[column] = frame.Column(name).Chunk(group)
		}
		record := array.NewRecord(schema, columns, size)
		result.Groups[group] = array.NewTableFromRecords(schema, []arrow.Record{record})
		record.Release()
	}
	return result, nil
}

func ReleaseArrowBatch(batch transform.Batch) {
	for _, table := range batch.Groups {
		if !nilArrowTable(table) {
			table.Release()
		}
	}
}

func validateFrameShape(frame *DataFrame) error {
	if frame == nil {
		return merr.WrapErrServiceInternal("DataFrame is nil")
	}
	if frame.schema == nil && frame.NumColumns() != 0 || frame.schema != nil && len(frame.schema.Fields()) != frame.NumColumns() {
		return merr.WrapErrServiceInternal("DataFrame schema/column count is inconsistent")
	}
	if frame.schema != nil {
		if err := types.ValidateRuntimeArrowSchema(frame.schema); err != nil {
			return merr.Wrap(err, "invalid DataFrame schema")
		}
	}
	for _, size := range frame.chunkSizes {
		if size < 0 {
			return merr.WrapErrServiceInternal("DataFrame has negative chunk size")
		}
	}
	for _, name := range frame.ColumnNames() {
		column := frame.Column(name)
		if column == nil || len(column.Chunks()) != frame.NumChunks() {
			return merr.WrapErrServiceInternalMsg("DataFrame column %q has inconsistent chunk count", name)
		}
		schema, err := functionInputSchema(frame, []string{name})
		if err != nil {
			return err
		}
		field := schema.Field(0)
		if !arrow.TypeEqual(field.Type, column.DataType()) {
			return merr.WrapErrServiceInternalMsg("DataFrame column %q differs from schema type", name)
		}
		for group, chunk := range column.Chunks() {
			if chunk == nil || int64(chunk.Len()) != frame.chunkSizes[group] {
				return merr.WrapErrServiceInternalMsg("DataFrame column %q group %d has inconsistent row count", name, group)
			}
			if err := types.ValidateArrowArrayAgainstField(chunk, field); err != nil {
				return merr.Wrapf(err, "invalid DataFrame column %q group %d", name, group)
			}
		}
	}
	if frame.lineage != nil {
		if len(frame.lineage) != frame.NumChunks() {
			return merr.WrapErrServiceInternal("DataFrame lineage group count differs")
		}
		for group, rows := range frame.lineage {
			if int64(len(rows)) != frame.chunkSizes[group] {
				return merr.WrapErrServiceInternalMsg("DataFrame lineage row count differs in group %d", group)
			}
		}
	}
	return nil
}

func nilArrowTable(table arrow.Table) bool {
	return table == nil || reflect.ValueOf(table).Kind() == reflect.Ptr && reflect.ValueOf(table).IsNil()
}
