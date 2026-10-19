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

package segments

import (
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/internal/util/arrowconv"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func queryScanColumn(record arrow.Record, name string) (arrow.Array, error) {
	if record == nil {
		return nil, merr.WrapErrServiceInternalMsg("missing QueryScan Arrow record")
	}
	indices := record.Schema().FieldIndices(name)
	if len(indices) != 1 {
		return nil, merr.WrapErrServiceInternalMsg("QueryScan requires exactly one %s column", name)
	}
	col := record.Column(indices[0])
	if col.NullN() != 0 || int64(col.Len()) != record.NumRows() {
		return nil, merr.WrapErrServiceInternalMsg("invalid QueryScan %s column length or nulls", name)
	}
	return col, nil
}

// queryFrameRows is a reducer-local borrowed view. The DataFrame owns all arrays.
type queryFrameRows struct {
	pks        arrow.Array
	timestamps *array.Int64
	offsets    *array.Int64
	rows       int
}

func queryFrameColumn(frame *chain.DataFrame, name string) (arrow.Array, error) {
	if frame == nil {
		return nil, merr.WrapErrServiceInternalMsg("missing query DataFrame")
	}
	col := frame.Column(name)
	if col == nil || len(col.Chunks()) != 1 || int64(col.Len()) != frame.NumRows() || col.Chunk(0).NullN() != 0 {
		return nil, merr.WrapErrServiceInternalMsg("invalid query column %s layout", name)
	}
	return col.Chunk(0), nil
}
func queryFrameInt64Column(frame *chain.DataFrame, name string) (*array.Int64, error) {
	col, err := queryFrameColumn(frame, name)
	if err != nil {
		return nil, err
	}
	value, ok := col.(*array.Int64)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("query column %s must be Int64", name)
	}
	return value, nil
}

func prepareQueryFrame(frame *chain.DataFrame) (*queryFrameRows, error) {
	r := &queryFrameRows{}
	if frame == nil {
		return nil, merr.WrapErrServiceInternalMsg("missing query DataFrame")
	}
	if frame.NumRows() == 0 {
		return r, nil
	}
	var err error
	r.offsets, err = queryFrameInt64Column(frame, types.SegOffsetFieldName)
	if err != nil {
		return nil, err
	}
	r.pks, err = queryFrameColumn(frame, types.IDFieldName)
	if err != nil {
		return nil, err
	}
	switch r.pks.(type) {
	case *array.Int64, *array.String:
	default:
		return nil, merr.WrapErrServiceInternalMsg("query identity PK must be Int64 or String")
	}
	r.timestamps, err = queryFrameInt64Column(frame, types.TimestampFieldName)
	if err != nil {
		return nil, err
	}
	r.rows = int(frame.NumRows())
	return r, nil
}

func compareQueryScanPK(a *queryFrameRows, ai int, b *queryFrameRows, bi int) int {
	if ids, ok := a.pks.(*array.Int64); ok {
		x, y := ids.Value(ai), b.pks.(*array.Int64).Value(bi)
		if x < y {
			return -1
		}
		if x > y {
			return 1
		}
		return 0
	}
	return strings.Compare(a.pks.(*array.String).Value(ai), b.pks.(*array.String).Value(bi))
}

func (r *queryFrameRows) pk(row int) any {
	if ids, ok := r.pks.(*array.Int64); ok {
		return ids.Value(row)
	}
	return r.pks.(*array.String).Value(row)
}

// queryScanMergeResult follows the L0 reduce layout: Arrow data plus source
// locations for late materialization. The caller owns Frame.
type queryScanMergeResult struct {
	Frame      *chain.DataFrame
	Selections []OffsetSelection
}

// mergeQueryScanResults preserves the existing PK/timestamp cursor and limit
// rules. No result payload is converted to protobuf here.
func mergeQueryScanResults(frames []*chain.DataFrame, limit int64, reduceType reduce.IReduceType) (*queryScanMergeResult, error) {
	merged := &queryScanMergeResult{}
	var detailRows []int
	results := make([]*queryFrameRows, len(frames))
	var pkType arrow.DataType
	for i, frame := range frames {
		var err error
		results[i], err = prepareQueryFrame(frame)
		if err != nil {
			return nil, err
		}
		if results[i].rows == 0 {
			continue
		}
		if pkType == nil {
			pkType = results[i].pks.DataType()
		} else if !arrow.TypeEqual(pkType, results[i].pks.DataType()) {
			return nil, merr.WrapErrServiceInternalMsg("query identity PK types differ across segments")
		}
	}
	cursors := make([]int, len(results))
	type entry struct {
		index     int
		timestamp int64
	}
	winners := make(map[any]entry)
	for limit == typeutil.Unlimited || !reduce.ShouldUseInputLimit(reduceType) || int64(len(merged.Selections)) < limit {
		best, row := -1, 0
		for i, r := range results {
			if cursors[i] == r.rows {
				continue
			}
			candidate := cursors[i]
			if best < 0 {
				best, row = i, candidate
				continue
			}
			cmp := compareQueryScanPK(r, candidate, results[best], row)
			if cmp < 0 || (cmp == 0 && r.timestamps.Value(candidate) > results[best].timestamps.Value(row)) {
				best, row = i, candidate
			}
		}
		if best < 0 {
			break
		}
		r := results[best]
		pk, timestamp := r.pk(row), r.timestamps.Value(row)
		selection := OffsetSelection{SegmentIndex: best, Offset: r.offsets.Value(row)}
		if previous, found := winners[pk]; !found {
			winners[pk] = entry{len(merged.Selections), timestamp}
			merged.Selections = append(merged.Selections, selection)
			detailRows = append(detailRows, row)
		} else if timestamp != 0 && timestamp > previous.timestamp {
			winners[pk] = entry{previous.index, timestamp}
			merged.Selections[previous.index] = selection
			detailRows[previous.index] = row
		}
		cursors[best]++
	}
	var err error
	merged.Frame, err = gatherQueryFrames(frames, merged.Selections, detailRows)
	if err != nil {
		return nil, err
	}
	return merged, nil
}

// gatherQueryFrames coalesces consecutive source rows before concatenation.
// Only winning rows are copied; one contiguous run can share its Arrow buffers.
func gatherQueryFrames(frames []*chain.DataFrame, selections []OffsetSelection, rows []int) (*chain.DataFrame, error) {
	builder := chain.NewDataFrameBuilder().SetChunkSizes([]int64{int64(len(rows))})
	defer builder.Release()
	if len(frames) == 0 {
		return builder.Build(), nil
	}
	aligned, err := alignQueryFrameColumns(frames)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, frame := range aligned {
			frame.Release()
		}
	}()
	frames = aligned
	schema := frames[0].Schema()
	if err := chain.ValidateMergeColumnMetadata(frames); err != nil {
		return nil, err
	}
	for _, field := range schema.Fields() {
		var runs []arrow.Array
		for i := 0; i < len(rows); {
			j := i + 1
			for j < len(rows) && selections[j].SegmentIndex == selections[i].SegmentIndex && rows[j] == rows[j-1]+1 {
				j++
			}
			source := frames[selections[i].SegmentIndex].Column(field.Name).Chunk(0)
			runs = append(runs, array.NewSlice(source, int64(rows[i]), int64(rows[j-1]+1)))
			i = j
		}
		var column arrow.Array
		if len(runs) == 0 {
			column = array.NewSlice(frames[0].Column(field.Name).Chunk(0), 0, 0)
		} else if len(runs) == 1 {
			column = runs[0]
		} else {
			var err error
			column, err = array.Concatenate(runs, memory.DefaultAllocator)
			for _, run := range runs {
				run.Release()
			}
			if err != nil {
				return nil, merr.WrapErrServiceInternalErr(err, "cannot gather query column %s", field.Name)
			}
		}
		if err := builder.AddColumnFromChunks(field.Name, []arrow.Array{column}); err != nil {
			return nil, err
		}
		builder.CopyFieldMetadata(frames[0], field.Name)
	}
	builder.CopyAllMetadata(frames[0])
	return builder.Build(), nil
}

// alignQueryFrameColumns makes computed column order local to each producer.
// It reorders Arrow headers by name before type checks/concatenation; collection
// declarations are copied unchanged and still checked by field ID in metadata.
func alignQueryFrameColumns(frames []*chain.DataFrame) (aligned []*chain.DataFrame, err error) {
	defer func() {
		if err != nil {
			for _, frame := range aligned {
				frame.Release()
			}
		}
	}()
	target := frames[0].Schema()
	for _, frame := range frames {
		output, err := alignQueryFrame(frame, frames[0], target)
		if err != nil {
			return aligned, err
		}
		aligned = append(aligned, output)
	}
	return aligned, nil
}

func alignQueryFrame(frame, template *chain.DataFrame, target *arrow.Schema) (*chain.DataFrame, error) {
	if frame.Schema().NumFields() != target.NumFields() {
		return nil, merr.WrapErrServiceInternalMsg("query frame schemas differ across segments")
	}
	builder := chain.NewDataFrameBuilder().SetChunkSizes(frame.ChunkSizes()).CopyAllMetadata(frame)
	defer builder.Release()
	for _, field := range target.Fields() {
		indices := frame.Schema().FieldIndices(field.Name)
		if len(indices) != 1 {
			return nil, merr.WrapErrServiceInternalMsg("query frame column %s is missing or duplicated", field.Name)
		}
		actual := frame.Schema().Field(indices[0])
		computedStruct := frame.IsComputedColumn(field.Name) && template.IsComputedColumn(field.Name) && field.Type.ID() == arrow.STRUCT
		if !computedStruct {
			if !field.Equal(actual) {
				return nil, merr.WrapErrServiceInternalMsg("query frame schemas differ across segments")
			}
			if err := builder.AddColumnFrom(frame, field.Name); err != nil {
				return nil, err
			}
			continue
		}
		// Only child ordering may differ. Other field attributes must match.
		actual.Type = field.Type
		if !field.Equal(actual) {
			return nil, merr.WrapErrServiceInternalMsg("query frame schemas differ across segments")
		}
		var chunks []arrow.Array
		for _, chunk := range frame.Column(field.Name).Chunks() {
			value, err := arrowconv.AlignComputedStructArray(chunk, field.Type.(*arrow.StructType))
			if err != nil {
				for _, owned := range chunks {
					owned.Release()
				}
				return nil, err
			}
			chunks = append(chunks, value)
		}
		if err := builder.AddColumnFromChunks(field.Name, chunks); err != nil {
			return nil, err
		}
		builder.CopyFieldMetadata(frame, field.Name)
	}
	return builder.Build(), nil
}
