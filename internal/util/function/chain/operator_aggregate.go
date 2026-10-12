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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// AggregateSpec describes one native aggregate. Count without Input is COUNT(*).
// Aliases are held by AggregateOp.Outputs, after its key aliases.
type AggregateSpec struct{ Op, Input string }

// AggregateOp constructs a new schema, independently within each logical group.
// Concat explicitly removes the previous query/ANN group boundaries.
type AggregateOp struct {
	BaseOp
	keys  []string
	specs []AggregateSpec
}

func init() {
	MustRegisterOperator(types.OpTypeAggregate, statelessOperatorFactory(NewAggregateOpFromRepr))
}

func NewAggregateOp(keys, outputs []string, specs []AggregateSpec) (*AggregateOp, error) {
	keys = append([]string(nil), keys...)
	outputs = append([]string(nil), outputs...)
	for index := range keys {
		keys[index] = strings.TrimSpace(keys[index])
	}
	for index := range outputs {
		outputs[index] = strings.TrimSpace(outputs[index])
	}
	if len(specs) == 0 || len(outputs) != len(keys)+len(specs) {
		return nil, merr.WrapErrParameterInvalidMsg("aggregate requires specs and one output for each key/spec")
	}
	if err := validateAggregateNames(keys, outputs); err != nil {
		return nil, err
	}
	inputs := append([]string(nil), keys...)
	normalized := append([]AggregateSpec(nil), specs...)
	for index := range normalized {
		spec := &normalized[index]
		spec.Op = strings.ToLower(strings.TrimSpace(spec.Op))
		spec.Input = strings.TrimSpace(spec.Input)
		switch spec.Op {
		case "sum", "avg", "min", "max", "first", "last", "collect_list":
			if spec.Input == "" {
				return nil, merr.WrapErrParameterMissingMsg("aggregate %s requires input", spec.Op)
			}
		case "count":
		default:
			return nil, merr.WrapErrParameterInvalidMsg("unsupported aggregate %q", spec.Op)
		}
		if spec.Input != "" {
			inputs = append(inputs, spec.Input)
		}
	}
	return &AggregateOp{BaseOp: BaseOp{inputs: inputs, outputs: append([]string(nil), outputs...)}, keys: append([]string(nil), keys...), specs: normalized}, nil
}

func validateAggregateNames(keys, outputs []string) error {
	seenKeys, seenOutputs := map[string]struct{}{}, map[string]struct{}{}
	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			return merr.WrapErrParameterInvalidMsg("aggregate key is empty")
		}
		if _, ok := seenKeys[key]; ok {
			return merr.WrapErrParameterInvalidMsg("duplicate aggregate key %q", key)
		}
		seenKeys[key] = struct{}{}
	}
	for index, output := range outputs {
		if strings.TrimSpace(output) == "" {
			return merr.WrapErrParameterInvalidMsg("aggregate output is empty")
		}
		if _, ok := seenOutputs[output]; ok {
			return merr.WrapErrParameterInvalidMsg("duplicate aggregate output %q", output)
		}
		seenOutputs[output] = struct{}{}
		if strings.HasPrefix(output, "$") && (index >= len(keys) || output != keys[index]) {
			return merr.WrapErrParameterInvalidMsg("aggregate cannot rewrite system output %q", output)
		}
	}
	return nil
}

func (o *AggregateOp) Name() string { return "Aggregate" }
func (o *AggregateOp) String() string {
	return fmt.Sprintf("Aggregate(keys=%v, specs=%v)", o.keys, o.specs)
}
func (o *AggregateOp) ResetsColumnScope() bool { return true }

func NewAggregateOpFromRepr(repr *OperatorRepr) (Operator, error) {
	if repr == nil || repr.Function != nil {
		return nil, merr.WrapErrParameterInvalidMsg("native aggregate does not accept expression")
	}
	for key := range repr.Params {
		if key != "keys" && key != "aggregates" {
			return nil, merr.WrapErrParameterInvalidMsg("aggregate: unsupported parameter %q", key)
		}
	}
	reader := types.NewParamReader("aggregate", repr.Params)
	keys, err := reader.StringSlice("keys", true)
	if err != nil {
		return nil, err
	}
	value := repr.Params["aggregates"]
	if value == nil || value.GetArrayValue() == nil {
		return nil, merr.WrapErrParameterInvalidMsg("aggregate: aggregates must be an array")
	}
	specs := make([]AggregateSpec, 0, len(value.GetArrayValue().GetValues()))
	for _, value := range value.GetArrayValue().GetValues() {
		if value == nil || value.GetObjectValue() == nil {
			return nil, merr.WrapErrParameterInvalidMsg("aggregate spec must be an object")
		}
		fields := value.GetObjectValue().GetFields()
		for name := range fields {
			if name != "op" && name != "input" {
				return nil, merr.WrapErrParameterInvalidMsg("aggregate spec: unsupported field %q", name)
			}
		}
		params := types.NewParamReader("aggregate spec", fields)
		name, err := params.String("op", true)
		if err != nil {
			return nil, err
		}
		input, err := params.String("input", false)
		if err != nil {
			return nil, err
		}
		if _, exists := fields["input"]; exists && strings.TrimSpace(input) == "" {
			return nil, merr.WrapErrParameterInvalidMsg("aggregate input must not be empty when specified")
		}
		specs = append(specs, AggregateSpec{Op: name, Input: input})
	}
	op, err := NewAggregateOp(keys, repr.Outputs, specs)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(op.Inputs(), repr.Inputs) {
		return nil, merr.WrapErrParameterInvalidMsg("aggregate inputs must match key/spec occurrences")
	}
	return op, nil
}

func (fc *FuncChain) Aggregate(keys, outputs []string, specs ...AggregateSpec) *FuncChain {
	op, err := NewAggregateOp(keys, outputs, specs)
	return fc.addWithError(op, err)
}

// postTableRowGroup is shared by native aggregates and PyUDF reduce.
// FirstRow is -1 only for an empty, unkeyed aggregate group.
type postTableRowGroup struct {
	Chunk    int
	Rows     []int
	FirstRow int
}

func groupPostTable(ctx *types.FuncContext, input *DataFrame, keys []string) ([][]postTableRowGroup, error) {
	if err := requirePostTable(ctx, "grouping"); err != nil {
		return nil, err
	}
	if err := validateFrameShape(input); err != nil {
		return nil, err
	}
	columns := make([]*arrow.Chunked, len(keys))
	for index, key := range keys {
		column, err := input.ResolveColumn(key, ctx.Pool())
		if err != nil {
			return nil, err
		}
		columns[index] = column
		defer column.Release()
		if err := types.ValidateRuntimeArrowDataType(column.DataType()); err != nil {
			return nil, merr.Wrapf(err, "aggregate key %q", key)
		}
		if !postTableKeyType(column.DataType().ID()) {
			return nil, merr.WrapErrParameterInvalidMsg("aggregate key %q has unsupported type %s", key, column.DataType())
		}
		if column.DataType().ID() != arrow.NULL {
			if err := validateComparableColumn(input, key, "aggregate key"); err != nil {
				return nil, err
			}
		}
	}
	groups := make([][]postTableRowGroup, input.NumChunks())
	for chunk, size := range input.chunkSizes {
		if len(keys) == 0 {
			rows := make([]int, int(size))
			for index := range rows {
				if index%1024 == 0 {
					if err := ctx.Context().Err(); err != nil {
						return nil, err
					}
				}
				rows[index] = index
			}
			first := -1
			if size > 0 {
				first = 0
			}
			groups[chunk] = []postTableRowGroup{{Chunk: chunk, Rows: rows, FirstRow: first}}
			continue
		}
		seen := make(map[string]int)
		var key bytes.Buffer
		for row := 0; row < int(size); row++ {
			if row%1024 == 0 {
				if err := ctx.Context().Err(); err != nil {
					return nil, err
				}
			}
			key.Reset()
			for _, column := range columns {
				encodePostTableKey(&key, column.Chunk(chunk), row)
			}
			encoded := key.String()
			if index, ok := seen[encoded]; ok {
				groups[chunk][index].Rows = append(groups[chunk][index].Rows, row)
			} else {
				seen[encoded] = len(groups[chunk])
				groups[chunk] = append(groups[chunk], postTableRowGroup{Chunk: chunk, Rows: []int{row}, FirstRow: row})
			}
		}
	}
	return groups, nil
}

func postTableKeyType(id arrow.Type) bool {
	switch id {
	case arrow.NULL, arrow.BOOL, arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64, arrow.FLOAT32, arrow.FLOAT64,
		arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY,
		arrow.TIMESTAMP, arrow.DATE32, arrow.DATE64, arrow.TIME32, arrow.TIME64, arrow.DURATION:
		return true
	default:
		return false
	}
}

// Keys are length-delimited and typed, so tuples and null values never alias.
// Float signed zero and NaN payloads are canonicalized for grouping only.
func encodePostTableKey(key *bytes.Buffer, data arrow.Array, row int) {
	var buffer [8]byte
	binary.LittleEndian.PutUint16(buffer[:2], uint16(data.DataType().ID()))
	key.Write(buffer[:2])
	if data.IsNull(row) {
		key.WriteByte(0)
		return
	}
	key.WriteByte(1)
	var bits uint64
	var variable []byte
	variableType := false
	switch value := data.(type) {
	case *array.Boolean:
		if value.Value(row) {
			bits = 1
		}
	case *array.Int8:
		bits = uint64(value.Value(row))
	case *array.Int16:
		bits = uint64(value.Value(row))
	case *array.Int32:
		bits = uint64(value.Value(row))
	case *array.Int64:
		bits = uint64(value.Value(row))
	case *array.Uint8:
		bits = uint64(value.Value(row))
	case *array.Uint16:
		bits = uint64(value.Value(row))
	case *array.Uint32:
		bits = uint64(value.Value(row))
	case *array.Uint64:
		bits = value.Value(row)
	case *array.Timestamp:
		bits = uint64(value.Value(row))
	case *array.Date32:
		bits = uint64(value.Value(row))
	case *array.Date64:
		bits = uint64(value.Value(row))
	case *array.Time32:
		bits = uint64(value.Value(row))
	case *array.Time64:
		bits = uint64(value.Value(row))
	case *array.Duration:
		bits = uint64(value.Value(row))
	case *array.Float32:
		v := float64(value.Value(row))
		if math.IsNaN(v) {
			bits = 0x7ff8000000000000
		} else if v != 0 {
			bits = math.Float64bits(v)
		}
	case *array.Float64:
		v := value.Value(row)
		if math.IsNaN(v) {
			bits = 0x7ff8000000000000
		} else if v != 0 {
			bits = math.Float64bits(v)
		}
	case *array.String:
		variable, variableType = []byte(value.Value(row)), true
	case *array.LargeString:
		variable, variableType = []byte(value.Value(row)), true
	case *array.Binary:
		variable, variableType = value.Value(row), true
	case *array.LargeBinary:
		variable, variableType = value.Value(row), true
	case *array.FixedSizeBinary:
		variable, variableType = value.Value(row), true
	}
	if variableType {
		bits = uint64(len(variable))
	}
	binary.LittleEndian.PutUint64(buffer[:], bits)
	key.Write(buffer[:])
	if variableType {
		key.Write(variable)
	}
}

func postTableGroupLineage(input *DataFrame, group postTableRowGroup, contexts ...*types.FuncContext) (transform.RowLineage, error) {
	if input.lineage == nil {
		return nil, nil
	}
	if group.Chunk < 0 || group.Chunk >= len(input.lineage) {
		return nil, merr.WrapErrServiceInternal("aggregate lineage chunk is out of bounds")
	}
	seen := make(map[transform.SourceRow]struct{})
	var sources transform.RowLineage
	visitedSources := 0
	for index, row := range group.Rows {
		if index%1024 == 0 && len(contexts) > 0 {
			if err := contexts[0].Context().Err(); err != nil {
				return nil, err
			}
		}
		if row < 0 || row >= len(input.lineage[group.Chunk]) {
			return nil, merr.WrapErrServiceInternal("aggregate lineage row is out of bounds")
		}
		for _, source := range input.lineage[group.Chunk][row] {
			if visitedSources%1024 == 0 && len(contexts) > 0 {
				if err := contexts[0].Context().Err(); err != nil {
					return nil, err
				}
			}
			visitedSources++
			if _, exists := seen[source]; !exists {
				seen[source] = struct{}{}
				sources = append(sources, source)
			}
		}
	}
	return sources, nil
}

func (o *AggregateOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	if err := requirePostTable(ctx, o.Name()); err != nil {
		return nil, err
	}
	groups, err := groupPostTable(ctx, input, o.keys)
	if err != nil {
		return nil, err
	}
	builder := NewDataFrameBuilder()
	defer builder.Release()
	sizes := make([]int64, len(groups))
	lineage := make([][]transform.RowLineage, len(groups))
	for chunk, chunkGroups := range groups {
		sizes[chunk] = int64(len(chunkGroups))
		lineage[chunk] = make([]transform.RowLineage, len(chunkGroups))
		for row, group := range chunkGroups {
			lineage[chunk][row], err = postTableGroupLineage(input, group, ctx)
			if err != nil {
				return nil, err
			}
		}
	}
	builder.SetChunkSizes(sizes).SetLineage(lineage)
	for index, key := range o.keys {
		column, err := input.ResolveColumn(key, ctx.Pool())
		if err != nil {
			return nil, err
		}
		defer column.Release()
		chunks := make([]arrow.Array, len(groups))
		for chunk, chunkGroups := range groups {
			indices := make([]int, len(chunkGroups))
			for row, group := range chunkGroups {
				indices[row] = group.FirstRow
			}
			chunks[chunk], err = takeArrayWithNulls(ctx.Pool(), column.Chunk(chunk), indices)
			if err != nil {
				releaseAggregateArrays(chunks)
				return nil, err
			}
		}
		if err := builder.AddColumnFromChunks(o.outputs[index], chunks); err != nil {
			return nil, err
		}
		field, err := postTableSourceField(input, key)
		if err != nil {
			return nil, err
		}
		field.Nullable = true
		if err := setFunctionOutputField(builder, o.outputs[index], field); err != nil {
			return nil, err
		}
	}
	for index, spec := range o.specs {
		var column *arrow.Chunked
		var sourceField arrow.Field
		if spec.Input != "" {
			column, err = input.ResolveColumn(spec.Input, ctx.Pool())
			if err != nil {
				return nil, err
			}
			defer column.Release()
			sourceField, err = postTableSourceField(input, spec.Input)
			if err != nil {
				return nil, err
			}
			if spec.Op == "min" || spec.Op == "max" {
				if err := validateComparableArrowField(sourceField, "aggregate "+spec.Op); err != nil {
					return nil, err
				}
			}
			if spec.Op == "sum" || spec.Op == "avg" {
				if err := validateNumericAggregateField(sourceField, spec.Op); err != nil {
					return nil, err
				}
			}
		}
		dt, accumulator, err := aggregateOutputType(spec, column)
		if err != nil {
			return nil, err
		}
		if spec.Op == "collect_list" {
			// Element metadata distinguishes encoded vectors/JSON from plain
			// binary, while the new outer list owns its independent schema.
			sourceField, err = normalizeRuntimeArrowField(sourceField, "item", true, true)
			if err != nil {
				return nil, err
			}
			dt = arrow.ListOfField(sourceField)
		}
		chunks := make([]arrow.Array, len(groups))
		for chunk, chunkGroups := range groups {
			if err := ctx.Context().Err(); err != nil {
				releaseAggregateArrays(chunks)
				return nil, err
			}
			chunks[chunk], err = executeAggregateChunk(ctx, spec, column, dt, accumulator, chunk, chunkGroups)
			if err != nil {
				releaseAggregateArrays(chunks)
				return nil, err
			}
		}
		name := o.outputs[len(o.keys)+index]
		if err := builder.AddColumnFromChunks(name, chunks); err != nil {
			return nil, err
		}
		builder.SetFieldNullable(name, spec.Op != "count" && spec.Op != "collect_list")
		if spec.Op == "first" || spec.Op == "last" || spec.Op == "min" || spec.Op == "max" {
			sourceField.Nullable = true
			if err := setFunctionOutputField(builder, name, sourceField); err != nil {
				return nil, err
			}
		}
	}
	return builder.Build(), nil
}

func postTableSourceField(input *DataFrame, name string) (arrow.Field, error) {
	schema, err := functionInputSchema(input, []string{name})
	if err != nil {
		return arrow.Field{}, err
	}
	return schema.Field(0), nil
}

func validateNumericAggregateField(field arrow.Field, operation string) error {
	switch field.Type.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.FLOAT32, arrow.FLOAT64:
	default:
		return merr.WrapErrParameterInvalidMsg("aggregate %s requires a signed integer or float column, got %s", operation, field.Type)
	}
	_, hasType := field.Metadata.GetValue(types.MetadataKeyMilvusDataType)
	_, hasSchema := field.Metadata.GetValue(types.MetadataKeyMilvusFieldSchema)
	if !hasType && !hasSchema {
		return nil
	}
	schema, err := types.MilvusSchemaFromArrowField(field)
	if err != nil {
		return merr.Wrapf(err, "aggregate %s input %q", operation, field.Name)
	}
	switch schema.GetDataType() {
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32,
		schemapb.DataType_Int64, schemapb.DataType_Float, schemapb.DataType_Double:
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg("aggregate %s does not support logical type %s", operation, schema.GetDataType())
	}
}

func releaseAggregateArrays(values []arrow.Array) {
	for _, value := range values {
		if value != nil {
			value.Release()
		}
	}
}

func aggregateOutputType(spec AggregateSpec, input *arrow.Chunked) (arrow.DataType, agg.AggregateBase, error) {
	if input != nil {
		if err := types.ValidateRuntimeArrowDataType(input.DataType()); err != nil {
			return nil, nil, merr.Wrapf(err, "aggregate %s", spec.Op)
		}
	}
	if spec.Op == "first" || spec.Op == "last" {
		return input.DataType(), nil, nil
	}
	if spec.Op == "collect_list" {
		return arrow.ListOf(input.DataType()), nil, nil
	}
	if spec.Op == "count" {
		return arrow.PrimitiveTypes.Int64, &agg.CountAggregate{}, nil
	}
	if spec.Op == "min" || spec.Op == "max" {
		if !isComparableType(input.DataType()) {
			return nil, nil, merr.WrapErrParameterInvalidMsg("aggregate %s does not support type %s", spec.Op, input.DataType())
		}
		return input.DataType(), nil, nil
	}
	switch input.DataType().ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.FLOAT32, arrow.FLOAT64:
	default:
		return nil, nil, merr.WrapErrParameterInvalidMsg("aggregate %s requires a signed integer or float column, got %s", spec.Op, input.DataType())
	}
	var accumulator agg.AggregateBase
	var result arrow.DataType = input.DataType()
	switch spec.Op {
	case "sum":
		accumulator = &agg.SumAggregate{}
		if input.DataType().ID() == arrow.FLOAT32 || input.DataType().ID() == arrow.FLOAT64 {
			result = arrow.PrimitiveTypes.Float64
		} else {
			result = arrow.PrimitiveTypes.Int64
		}
	case "avg":
		accumulator, result = agg.NewAvgAggregate(0, ""), arrow.PrimitiveTypes.Float64
	}
	return result, accumulator, nil
}

func executeAggregateChunk(ctx *types.FuncContext, spec AggregateSpec, input *arrow.Chunked, dt arrow.DataType, accumulator agg.AggregateBase, chunk int, groups []postTableRowGroup) (arrow.Array, error) {
	if spec.Op == "min" || spec.Op == "max" {
		data := input.Chunk(chunk)
		indices := make([]int, len(groups))
		for index, group := range groups {
			best := -1
			for position, row := range group.Rows {
				if position%1024 == 0 {
					if err := ctx.Context().Err(); err != nil {
						return nil, err
					}
				}
				if data.IsNull(row) {
					continue
				}
				if best == -1 {
					best = row
					continue
				}
				comparison := compareAggregateValues(data, row, best)
				if spec.Op == "min" && comparison < 0 || spec.Op == "max" && comparison > 0 {
					best = row
				}
			}
			indices[index] = best
		}
		return takeArrayWithNulls(ctx.Pool(), data, indices)
	}
	if spec.Op == "first" || spec.Op == "last" {
		indices := make([]int, len(groups))
		for row, group := range groups {
			indices[row] = group.FirstRow
			if spec.Op == "last" && len(group.Rows) > 0 {
				indices[row] = group.Rows[len(group.Rows)-1]
			}
		}
		return takeArrayWithNulls(ctx.Pool(), input.Chunk(chunk), indices)
	}
	if spec.Op == "collect_list" {
		indices := make([]int, 0)
		offsets := array.NewInt32Builder(ctx.Pool())
		defer offsets.Release()
		offsets.Append(0)
		for _, group := range groups {
			indices = append(indices, group.Rows...)
			if len(indices) > math.MaxInt32 {
				return nil, merr.WrapErrParameterTooLarge("collect_list", "exceeds Arrow List offsets")
			}
			offsets.Append(int32(len(indices)))
		}
		values, err := takeArrayWithNulls(ctx.Pool(), input.Chunk(chunk), indices)
		if err != nil {
			return nil, err
		}
		defer values.Release()
		offsetArray := offsets.NewArray()
		defer offsetArray.Release()
		data := array.NewData(dt, len(groups), []*memory.Buffer{nil, offsetArray.Data().Buffers()[1]}, []arrow.ArrayData{values.Data()}, 0, 0)
		defer data.Release()
		return array.MakeFromData(data), nil
	}
	builder := array.NewBuilder(ctx.Pool(), dt)
	defer builder.Release()
	for _, group := range groups {
		if err := ctx.Context().Err(); err != nil {
			return nil, err
		}
		slots := accumulator.NewState()
		for index, row := range group.Rows {
			if index%1024 == 0 {
				if err := ctx.Context().Err(); err != nil {
					return nil, err
				}
			}
			value := agg.NewFieldValue(int64(1))
			if input != nil {
				data := input.Chunk(chunk)
				if data.IsNull(row) {
					value = agg.NewNullFieldValue()
				} else if spec.Op != "count" {
					scalar, err := aggregateScalarValue(data, row)
					if err != nil {
						return nil, err
					}
					value = agg.NewFieldValue(scalar)
					// AVG's result is Double. Float64 accumulation avoids overflow of
					// an Int64 intermediate; ordinary IEEE rounding still applies.
					if spec.Op == "avg" {
						if integer, ok := scalar.(int64); ok {
							value = agg.NewFieldValue(float64(integer))
						}
					}
				}
			}
			if spec.Op == "sum" && !value.IsNull() && !slots[0].IsNull() {
				if integer, ok := value.Value().(int64); ok {
					current := slots[0].Value().(int64)
					if integer > 0 && current > math.MaxInt64-integer || integer < 0 && current < math.MinInt64-integer {
						return nil, merr.WrapErrFunctionFailedMsg("aggregate SUM exceeds Int64 range")
					}
				}
			}
			if err := accumulator.UpdateState(slots, value); err != nil {
				return nil, merr.Wrap(err, "aggregate update")
			}
		}
		value, err := accumulator.Terminate(slots)
		if err != nil {
			return nil, merr.Wrap(err, "aggregate finalize")
		}
		if value == nil && spec.Op == "count" {
			value = int64(0)
		}
		if err := appendAggregateScalar(builder, value); err != nil {
			return nil, err
		}
	}
	return builder.NewArray(), nil
}

func aggregateScalarValue(data arrow.Array, row int) (any, error) {
	switch value := data.(type) {
	case *array.Int8:
		return int64(value.Value(row)), nil
	case *array.Int16:
		return int64(value.Value(row)), nil
	case *array.Int32:
		return int64(value.Value(row)), nil
	case *array.Int64:
		return value.Value(row), nil
	case *array.Float32:
		return float64(value.Value(row)), nil
	case *array.Float64:
		return value.Value(row), nil
	case *array.String:
		return value.Value(row), nil
	case *array.LargeString:
		return value.Value(row), nil
	case *array.Timestamp:
		return int64(value.Value(row)), nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("numeric/ordered aggregate does not support type %s", data.DataType())
	}
}

// Aggregate min/max previously used direct floating-point comparisons rather
// than sort's total NaN ordering. Preserve that behavior until the public NaN
// policy changes: a NaN never replaces a selected value, and an initial NaN
// remains selected. Other comparable types use the shared Arrow comparator.
func compareAggregateValues(data arrow.Array, left, right int) int {
	switch values := data.(type) {
	case *array.Float32:
		if math.IsNaN(float64(values.Value(left))) || math.IsNaN(float64(values.Value(right))) {
			return 0
		}
	case *array.Float64:
		if math.IsNaN(values.Value(left)) || math.IsNaN(values.Value(right)) {
			return 0
		}
	}
	return compareArrayValues(data, left, right)
}

func appendAggregateScalar(builder array.Builder, value any) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}
	switch value := value.(type) {
	case int64:
		switch builder := builder.(type) {
		case *array.Int8Builder:
			builder.Append(int8(value))
			return nil
		case *array.Int16Builder:
			builder.Append(int16(value))
			return nil
		case *array.Int32Builder:
			builder.Append(int32(value))
			return nil
		case *array.Int64Builder:
			builder.Append(value)
			return nil
		case *array.TimestampBuilder:
			builder.Append(arrow.Timestamp(value))
			return nil
		}
	case float64:
		switch builder := builder.(type) {
		case *array.Float32Builder:
			builder.Append(float32(value))
			return nil
		case *array.Float64Builder:
			builder.Append(value)
			return nil
		}
	case string:
		switch builder := builder.(type) {
		case *array.StringBuilder:
			builder.Append(value)
			return nil
		case *array.LargeStringBuilder:
			builder.Append(value)
			return nil
		}
	}
	return merr.WrapErrServiceInternalMsg("aggregate output %T is incompatible with builder %T", value, builder)
}
