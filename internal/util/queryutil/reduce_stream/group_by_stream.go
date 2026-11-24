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

package reducestream

import (
	"io"
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type orderedGroups struct {
	reducer   *agg.GroupAggReducer    // Validates partial columns and materializes typed empty results.
	order     []*orderby.OrderByField // Requested order followed by all remaining group keys.
	positions []int                   // Key positions in the positional partial aggregate layout.
	ops       []agg.AggregateBase     // Partial aggregate operations; AVG is already split into SUM and COUNT.
	produced  int64                   // Complete groups, including those skipped by the client offset.
	limit     int64                   // Internal offset + limit; nonpositive means unbounded.
	maxGroups int64                   // Enforced for groups visited, without scanning an unread tail.
	lastGroup *internalpb.RetrieveResults
}

func newOrderedGroups(req *internalpb.RetrieveRequest, schema *schemapb.CollectionSchema) (*orderedGroups, error) {
	keys := req.GetGroupByFieldIds()
	explicit, err := orderby.ConvertFromPlanOrderByFields(req.GetOrderByFields(), schema)
	if err != nil {
		return nil, err
	}
	// Like Batch, GROUP BY without ORDER BY compares the group keys in declaration
	// order (ascending, NULLs last). The k-way merge must match the workers' top K.
	order, err := orderby.GroupKeyOrder(explicit, keys, schema)
	if err != nil {
		return nil, err
	}
	g := &orderedGroups{
		reducer: agg.NewGroupAggReducer(keys, req.GetAggregates(), typeutil.Unlimited, schema),
		order:   order, limit: req.GetLimit(),
		maxGroups: paramtable.Get().CommonCfg.GroupByMaxGroups.GetAsInt64(),
	}
	for _, f := range g.order {
		pos := slices.Index(keys, f.FieldID)
		if pos < 0 {
			return nil, merr.WrapErrParameterInvalidMsg("ordered GROUP BY requires a grouping key, got %d", f.FieldID)
		}
		g.positions = append(g.positions, pos)
	}
	for _, def := range req.GetAggregates() {
		op, err := agg.FromPB(def)
		if err != nil {
			return nil, err
		}
		g.ops = append(g.ops, op)
	}
	return g, nil
}

func (g *orderedGroups) compare(a, b queryReduceUnit) int {
	for i, pos := range g.positions {
		left, right := a.result.FieldsData[pos], b.result.FieldsData[pos]
		order := g.order[i]
		if c := queryutil.CompareFieldDataRows(left, int(a.rowIndex), right, int(b.rowIndex), left.GetType(), order.Ascending, order.NullsFirst); c != 0 {
			return c
		}
	}
	return 0
}

func (g *orderedGroups) accept(b *orderedChildBuffer, chunk *internalpb.RetrieveResults) error {
	partial, err := g.reducer.DecodePartial(chunk.GetFieldsData(), 0)
	if err != nil || partial.IsEmpty() {
		return err
	}
	columns := make([]agg.FieldAccessor, len(chunk.FieldsData))
	for i, fd := range chunk.FieldsData {
		columns[i], err = agg.NewFieldAccessor(fd.GetType())
		if err != nil {
			return err
		}
		columns[i].SetVals(fd)
	}
	*b = orderedChildBuffer{chunk: chunk, rows: columns[0].RowCount(), groupColumns: columns}
	return nil
}

// Each child has already merged its own equal keys. Once all live heads
// are known, all contributions to the least key are at those heads, even
// when the next Chunk is unavailable. No lookahead past the last group is needed.
func (g *orderedGroups) appendNext(ready []*orderedChildBuffer, out *internalpb.RetrieveResults) error {
	first := ready[0].front()
	for _, b := range ready[1:] {
		if candidate := b.front(); g.compare(candidate, first) < 0 {
			first = candidate
		}
	}
	if g.lastGroup != nil && g.compare(first, queryReduceUnit{result: g.lastGroup, rowIndex: 0}) <= 0 {
		return merr.WrapErrServiceInternalMsg("GROUP BY child streams are not strictly ordered by group key")
	}
	var merged *agg.Row
	for _, b := range ready {
		if g.compare(b.front(), first) != 0 {
			continue
		}
		value := func(column int) *agg.FieldValue {
			a := b.groupColumns[column]
			if a.IsNullAt(int(b.cursor)) {
				return agg.NewNullFieldValue()
			}
			return agg.NewFieldValue(a.ValAt(int(b.cursor)))
		}
		if merged == nil {
			values := make([]*agg.FieldValue, len(b.groupColumns))
			for i := range values {
				values[i] = value(i)
			}
			merged = agg.NewRow(values)
		} else {
			for i, op := range g.ops {
				col := len(b.groupColumns) - len(g.ops) + i
				op.Update(merged.FieldValueAt(col), value(col))
			}
		}
		b.pop()
	}
	if out.FieldsData == nil {
		out.FieldsData = typeutil.PrepareResultFieldData(first.result.FieldsData, 0)
	}
	if err := agg.AssembleSingleRow(len(out.FieldsData), merged, out.FieldsData); err != nil {
		return err
	}
	lastGroup := &internalpb.RetrieveResults{
		FieldsData: typeutil.PrepareResultFieldData(out.FieldsData, 1),
	}
	if err := agg.AssembleSingleRow(len(lastGroup.FieldsData), merged, lastGroup.FieldsData); err != nil {
		return err
	}
	g.lastGroup = lastGroup
	return nil
}

func (s *OrderedReduceStream) recvGroups() (*internalpb.RetrieveResults, error) {
	g := s.groups
	out := &internalpb.RetrieveResults{Status: merr.Success()}
	var size int64
	if len(s.outputChunks) > 0 { // Drain split chunks one at a time before pulling more groups.
		out, s.outputChunks = s.outputChunks[0], s.outputChunks[1:]
		if len(s.outputChunks) == 0 {
			s.outputChunks = nil
		}
		size = int64(s.chunkBytes)
	}
	var sizeRow func(int64) int64
	exhausted := false
	for size < int64(s.chunkBytes) && (g.limit <= 0 || g.produced < g.limit) {
		ready, err := s.getReadyBuffers()
		if err != nil {
			return nil, s.fail(err)
		}
		if len(ready) == 0 {
			exhausted = true
			break
		}
		g.produced++
		if g.produced > g.maxGroups {
			return nil, s.fail(merr.WrapErrParameterInvalidMsg("GROUP BY produced too many groups (%d). Add filters or increase common.groupBy.maxGroups (current: %d)", g.produced, g.maxGroups))
		}
		if err := g.appendNext(ready, out); err != nil {
			return nil, s.fail(err)
		}
		if sizeRow == nil {
			sizeRow = queryutil.NewOrderedRowSizeCalculator(out.FieldsData, nil, nil).RowSize
		}
		n := sizeRow(int64(queryutil.GetRowCount(out) - 1))
		if n > int64(s.chunkBytes) && size > 0 {
			chunks, err := SplitRetrieveResult(out, s.chunkBytes)
			if err != nil {
				return nil, s.fail(err)
			}
			out, s.outputChunks = chunks[0], chunks[1:]
			break
		}
		size += n
	}
	if g.produced == 0 {
		empty, err := g.reducer.EncodePartial(&agg.PartialAggregationResult{})
		if err != nil {
			return nil, s.fail(err)
		}
		out.FieldsData = empty.FieldsData
	}
	if out.FieldsData == nil {
		s.finished = true
		if s.metadata != nil && !s.metadataEmitted {
			s.attachMetadata(out)
			if err := s.closeChildren(); err != nil {
				return nil, err
			}
			return out, nil
		}
		if err := s.closeChildren(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	s.attachMetadata(out)
	if exhausted || (g.limit > 0 && g.produced == g.limit && len(s.outputChunks) == 0) {
		s.finished = true
		if err := s.closeChildren(); err != nil {
			return nil, err
		}
	}
	return out, nil
}
