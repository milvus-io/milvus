/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GroupScorer defines how to compute the group score from individual scores.
type GroupScorer string

const (
	// GroupScorerMax uses the maximum score in the group.
	GroupScorerMax GroupScorer = "max"
	// GroupScorerSum uses the sum of scores in the group.
	GroupScorerSum GroupScorer = "sum"
	// GroupScorerAvg uses the average of scores in the group.
	GroupScorerAvg GroupScorer = "avg"
)

// GroupByOp groups rows by a field, keeps top N rows per group, and limits the number of groups.
// This operator is designed for grouping search scenarios.
//
// Parameters:
//   - groupByField: the field to group by
//   - groupSize: maximum rows per group (sorted by $score DESC)
//   - limit: maximum number of groups to return
//   - offset: number of groups to skip
//   - groupScorer: how to compute group score ("max", "sum", "avg")
//
// The operator also adds a $group_score column containing the computed group score.
//
// Workflow example (groupByField="category", groupSize=2, limit=2, offset=0, scorer=max):
//
// Input:
//
//	| row | $id | $score | category |
//	|-----|-----|--------|----------|
//	|  0  | a1  |  0.9   | cat      |
//	|  1  | a2  |  0.7   | dog      |
//	|  2  | a3  |  0.8   | cat      |
//	|  3  | a4  |  0.6   | cat      |
//	|  4  | a5  |  0.85  | dog      |
//	|  5  | a6  |  0.5   | bird     |
//
// Step 1 - buildGroups: group rows by category
//
//	cat:  rowIndices=[0,2,3] scores=[0.9,0.8,0.6]
//	dog:  rowIndices=[1,4]   scores=[0.7,0.85]
//	bird: rowIndices=[5]     scores=[0.5]
//
// Step 2 - sortAndLimitGroup: sort each group by score DESC, keep top groupSize=2
//
//	cat:  rowIndices=[0,2] scores=[0.9,0.8]  (row 3 removed)
//	dog:  rowIndices=[4,1] scores=[0.85,0.7] (reordered)
//	bird: rowIndices=[5]   scores=[0.5]
//
// Step 3 - computeGroupScore: scorer=max, take the highest score per group
//
//	cat:  groupScore=0.9
//	dog:  groupScore=0.85
//	bird: groupScore=0.5
//
// Step 4 - sort groups by groupScore DESC: cat(0.9) > dog(0.85) > bird(0.5)
//
// Step 5 - apply offset=0, limit=2: select cat and dog, bird is dropped
//
// Step 6 - expand selected groups into output with $group_score column:
//
//	| $id | $score | category | $group_score |
//	|-----|--------|----------|--------------|
//	| a1  |  0.9   | cat      | 0.9          |
//	| a3  |  0.8   | cat      | 0.9          |
//	| a5  |  0.85  | dog      | 0.85         |
//	| a2  |  0.7   | dog      | 0.85         |
type GroupByOp struct {
	BaseOp
	groupByField   string
	groupSize      int64
	limit          int64
	offset         int64
	groupScorer    GroupScorer
	sortDescending bool // true (default) means larger score = better match
}

// NewGroupByOp creates a new GroupByOp with default max scorer.
func NewGroupByOp(groupByField string, groupSize, limit, offset int64) *GroupByOp {
	return NewGroupByOpWithScorer(groupByField, groupSize, limit, offset, GroupScorerMax)
}

// SetSortDescending configures the sort direction for both within-group and
// cross-group ordering. true (the default) treats larger scores as better
// matches; false treats smaller scores as better matches — required for
// distance metrics (L2, HAMMING, JACCARD, ...) when scores are not normalized
// (e.g., weighted reranker on raw L2 distances).
//
// When sortDescending=false:
//   - within-group trim keeps the smallest groupSize rows (best matches)
//   - the Max scorer still picks the "best representative" (which is now the
//     smallest score after ASC sort)
//   - groups are ordered ASC by group score
//
// Returns the receiver to support builder-style chaining.
func (o *GroupByOp) SetSortDescending(sortDescending bool) *GroupByOp {
	o.sortDescending = sortDescending
	return o
}

// ValidateGroupScorer checks if the scorer is valid.
func ValidateGroupScorer(scorer GroupScorer) error {
	switch scorer {
	case GroupScorerMax, GroupScorerSum, GroupScorerAvg:
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg("invalid group scorer %q, must be max/sum/avg", scorer)
	}
}

// NewGroupByOpWithScorer creates a new GroupByOp with specified scorer.
// Defaults to descending sort direction (larger score = better match);
// callers that need ASC ordering should chain SetSortDescending(false).
func NewGroupByOpWithScorer(groupByField string, groupSize, limit, offset int64, scorer GroupScorer) *GroupByOp {
	return &GroupByOp{
		BaseOp: BaseOp{
			inputs:  []string{groupByField, types.ScoreFieldName, types.IDFieldName},
			outputs: []string{GroupScoreFieldName},
		},
		groupByField:   groupByField,
		groupSize:      groupSize,
		limit:          limit,
		offset:         offset,
		groupScorer:    scorer,
		sortDescending: true,
	}
}

// GroupScoreFieldName is the name of the group score column added by GroupByOp.
const GroupScoreFieldName = "$group_score"

func (o *GroupByOp) Name() string { return "GroupBy" }

func (o *GroupByOp) String() string {
	return fmt.Sprintf("GroupBy(%s, groupSize=%d, limit=%d, offset=%d, scorer=%s)",
		o.groupByField, o.groupSize, o.limit, o.offset, o.groupScorer)
}

func (o *GroupByOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	// Validate columns exist
	groupCol := input.Column(o.groupByField)
	if groupCol == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("group_by_op: column %q not found", o.groupByField))
	}
	scoreCol := input.Column(types.ScoreFieldName)
	if scoreCol == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("group_by_op: column %q not found", types.ScoreFieldName))
	}

	numChunks := input.NumChunks()
	colNames := input.ColumnNames()

	// Prepare collectors
	collector := NewChunkCollector(colNames, numChunks)
	defer collector.Release()

	// Prepare group score builder
	groupScoreChunks := make([]arrow.Array, numChunks)
	newChunkSizes := make([]int64, numChunks)

	// Release groupScoreChunks on error
	success := false
	defer func() {
		if !success {
			for _, chunk := range groupScoreChunks {
				if chunk != nil {
					chunk.Release()
				}
			}
		}
	}()

	// Process each chunk independently
	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		result, err := o.processChunk(ctx, input, chunkIdx)
		if err != nil {
			return nil, err
		}

		newChunkSizes[chunkIdx] = int64(len(result.indices))
		groupScoreChunks[chunkIdx] = result.groupScores

		// Reorder existing columns by indices
		for _, colName := range colNames {
			col := input.Column(colName)
			dataChunk := col.Chunk(chunkIdx)
			reordered, err := dispatchPickByIndices(ctx.Pool(), dataChunk, result.indices)
			if err != nil {
				return nil, merr.WrapErrServiceInternal(fmt.Sprintf("group_by_op: reorder column %s: %v", colName, err))
			}
			collector.Set(colName, chunkIdx, reordered)
		}
	}

	// Build output DataFrame
	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes(newChunkSizes)

	// Add existing columns
	for _, colName := range colNames {
		if err := builder.AddColumnFromChunks(colName, collector.Consume(colName)); err != nil {
			return nil, err
		}
		builder.CopyFieldMetadata(input, colName)
	}

	// Add group score column
	if err := builder.AddColumnFromChunks(GroupScoreFieldName, groupScoreChunks); err != nil {
		return nil, err
	}

	success = true
	return builder.Build(), nil
}

// chunkResult holds the result of processing a single chunk.
type chunkResult struct {
	indices     []int       // Row indices in output order
	groupScores arrow.Array // Group score for each output row
}

// processChunk processes a single chunk and returns the result.
func (o *GroupByOp) processChunk(ctx *types.FuncContext, input *DataFrame, chunkIdx int) (*chunkResult, error) {
	groupCol := input.Column(o.groupByField)
	scoreCol := input.Column(types.ScoreFieldName)
	idCol := input.Column(types.IDFieldName)
	if idCol == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("group_by_op: column %q not found", types.IDFieldName))
	}

	groupChunk := groupCol.Chunk(chunkIdx)
	scoreChunk, ok := scoreCol.Chunk(chunkIdx).(*array.Float32)
	if !ok {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("group_by_op: score column chunk %d is not Float32", chunkIdx))
	}
	idChunk := idCol.Chunk(chunkIdx)
	chunkLen := groupChunk.Len()

	// Step 1: Build groups
	groups := o.buildGroups(groupChunk, scoreChunk, idChunk, chunkLen)

	// Step 2: Sort rows within each group by score DESC, keep top groupSize
	for _, g := range groups {
		o.sortAndLimitGroup(g)
	}

	// Step 3: Compute group scores based on scorer mode
	for _, g := range groups {
		o.computeGroupScore(g)
	}

	// Step 4: Sort groups by group score (DESC if sortDescending, ASC otherwise),
	// with tiebreaking:
	// - same groupScore: larger group first
	// - same groupScore and size: smaller first id first
	sort.SliceStable(groups, func(i, j int) bool {
		if groups[i].groupScore != groups[j].groupScore {
			if o.sortDescending {
				return groups[i].groupScore > groups[j].groupScore
			}
			return groups[i].groupScore < groups[j].groupScore
		}
		if len(groups[i].rowIndices) != len(groups[j].rowIndices) {
			return len(groups[i].rowIndices) > len(groups[j].rowIndices)
		}
		return compareValues(groups[i].ids[0], groups[j].ids[0]) < 0
	})

	// Step 5: Apply offset and limit on groups
	startGroup := int(o.offset)
	endGroup := int(o.offset + o.limit)
	if startGroup > len(groups) {
		startGroup = len(groups)
	}
	if endGroup > len(groups) {
		endGroup = len(groups)
	}
	selectedGroups := groups[startGroup:endGroup]

	// Step 6: Build output indices and group scores
	indices := make([]int, 0)
	groupScores := make([]float32, 0)

	for _, g := range selectedGroups {
		for _, idx := range g.rowIndices {
			indices = append(indices, idx)
			groupScores = append(groupScores, g.groupScore)
		}
	}

	// Build group score array
	groupScoreBuilder := array.NewFloat32Builder(ctx.Pool())
	defer groupScoreBuilder.Release()
	groupScoreBuilder.AppendValues(groupScores, nil)

	return &chunkResult{
		indices:     indices,
		groupScores: groupScoreBuilder.NewArray(),
	}, nil
}

// group represents a group of rows.
type group struct {
	key        any       // Group key value
	rowIndices []int     // Row indices belonging to this group
	groupScore float32   // Computed group score
	scores     []float32 // Individual scores for sum/avg computation
	ids        []any     // ID values for tiebreaking
}

// buildGroups builds groups from the chunk.
func (o *GroupByOp) buildGroups(groupChunk arrow.Array, scoreChunk *array.Float32, idChunk arrow.Array, chunkLen int) []*group {
	groupMap := make(map[any]*group)
	groupOrder := make([]any, 0) // Maintain appearance order

	for i := 0; i < chunkLen; i++ {
		key := getArrayValue(groupChunk, i)
		score := scoreChunk.Value(i)
		id := getArrayValue(idChunk, i)

		if g, exists := groupMap[key]; exists {
			g.rowIndices = append(g.rowIndices, i)
			g.scores = append(g.scores, score)
			g.ids = append(g.ids, id)
		} else {
			g := &group{
				key:        key,
				rowIndices: []int{i},
				scores:     []float32{score},
				ids:        []any{id},
			}
			groupMap[key] = g
			groupOrder = append(groupOrder, key)
		}
	}

	// Return groups in appearance order
	result := make([]*group, 0, len(groupOrder))
	for _, key := range groupOrder {
		result = append(result, groupMap[key])
	}
	return result
}

// sortAndLimitGroup sorts rows within a group by score in the configured
// direction (DESC by default, ASC when sortDescending=false), with id ASC
// tiebreaking, then keeps top groupSize.
func (o *GroupByOp) sortAndLimitGroup(g *group) {
	n := len(g.rowIndices)
	indices := make([]int, n)
	for i := 0; i < n; i++ {
		indices[i] = i
	}

	sort.SliceStable(indices, func(i, j int) bool {
		si, sj := g.scores[indices[i]], g.scores[indices[j]]
		if si != sj {
			if o.sortDescending {
				return si > sj
			}
			return si < sj
		}
		return compareValues(g.ids[indices[i]], g.ids[indices[j]]) < 0
	})

	// Reorder rowIndices, scores and ids
	newRowIndices := make([]int, n)
	newScores := make([]float32, n)
	newIDs := make([]any, n)
	for i, idx := range indices {
		newRowIndices[i] = g.rowIndices[idx]
		newScores[i] = g.scores[idx]
		newIDs[i] = g.ids[idx]
	}
	g.rowIndices = newRowIndices
	g.scores = newScores
	g.ids = newIDs

	// Keep top groupSize
	if int64(len(g.rowIndices)) > o.groupSize {
		g.rowIndices = g.rowIndices[:o.groupSize]
		g.scores = g.scores[:o.groupSize]
		g.ids = g.ids[:o.groupSize]
	}
}

// computeGroupScore computes the group score based on the scorer mode.
func (o *GroupByOp) computeGroupScore(g *group) {
	if len(g.scores) == 0 {
		g.groupScore = 0
		return
	}

	switch o.groupScorer {
	case GroupScorerSum:
		var sum float32
		for _, s := range g.scores {
			sum += s
		}
		g.groupScore = sum

	case GroupScorerAvg:
		var sum float32
		for _, s := range g.scores {
			sum += s
		}
		g.groupScore = sum / float32(len(g.scores))

	case GroupScorerMax:
		// scores[0] is the best representative under the current sort
		// direction: largest score in DESC mode (default), smallest score in
		// ASC mode. sortAndLimitGroup is responsible for ordering the slice
		// in the right direction before this runs.
		g.groupScore = g.scores[0]

	default:
		// Should not reach here if scorer is validated upfront
		g.groupScore = g.scores[0]
	}
}

// NewGroupByOpFromRepr creates a GroupByOp from an OperatorRepr.
func NewGroupByOpFromRepr(repr *OperatorRepr) (Operator, error) {
	field, ok := repr.Params["field"].(string)
	if !ok || field == "" {
		return nil, merr.WrapErrParameterInvalidMsg("group_by_op: field is required")
	}

	groupSize, err := getInt64Param(repr.Params, "group_size")
	if err != nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("group_by_op: %v", err))
	}
	if groupSize <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("group_by_op: group_size must be positive")
	}

	limit, err := getInt64Param(repr.Params, "limit")
	if err != nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("group_by_op: %v", err))
	}
	if limit <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("group_by_op: limit must be positive")
	}

	offset := int64(0)
	if _, ok := repr.Params["offset"]; ok {
		offset, err = getInt64Param(repr.Params, "offset")
		if err != nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("group_by_op: %v", err))
		}
		if offset < 0 {
			return nil, merr.WrapErrParameterInvalidMsg("group_by_op: offset must be non-negative")
		}
	}

	scorer := GroupScorerMax
	if scorerStr, ok := repr.Params["scorer"].(string); ok {
		scorer = GroupScorer(scorerStr)
		if err := ValidateGroupScorer(scorer); err != nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("group_by_op: %v", err))
		}
	}

	return NewGroupByOpWithScorer(field, groupSize, limit, offset, scorer), nil
}

// getInt64Param extracts an int64 parameter from a map.
func getInt64Param(params map[string]interface{}, key string) (int64, error) {
	val, ok := params[key]
	if !ok {
		return 0, merr.WrapErrParameterInvalidMsg("%s is required", key)
	}
	switch v := val.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg("%s must be a number", key)
	}
}

// compareValues compares two values for tiebreaking.
// Supports int64 and string (the two possible PK types in Milvus).
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func compareValues(a, b any) int {
	switch va := a.(type) {
	case int64:
		vb, ok := b.(int64)
		if !ok {
			return 0
		}
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case string:
		vb, ok := b.(string)
		if !ok {
			return 0
		}
		return strings.Compare(va, vb)
	default:
		return 0
	}
}

func init() {
	MustRegisterOperator(types.OpTypeGroupBy, NewGroupByOpFromRepr)
}
