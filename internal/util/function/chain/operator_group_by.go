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

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
type GroupByOp struct {
	BaseOp
	groupByField string
	groupSize    int64
	limit        int64
	offset       int64
	groupScorer  GroupScorer
}

// NewGroupByOp creates a new GroupByOp with default max scorer.
func NewGroupByOp(groupByField string, groupSize, limit, offset int64) *GroupByOp {
	return NewGroupByOpWithScorer(groupByField, groupSize, limit, offset, GroupScorerMax)
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
func NewGroupByOpWithScorer(groupByField string, groupSize, limit, offset int64, scorer GroupScorer) *GroupByOp {
	return &GroupByOp{
		BaseOp: BaseOp{
			inputs:  []string{groupByField, types.ScoreFieldName},
			outputs: []string{GroupScoreFieldName},
		},
		groupByField: groupByField,
		groupSize:    groupSize,
		limit:        limit,
		offset:       offset,
		groupScorer:  scorer,
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

	// Process each chunk independently
	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		result, err := o.processChunk(ctx, input, chunkIdx)
		if err != nil {
			// Release any already built chunks
			for i := 0; i < chunkIdx; i++ {
				if groupScoreChunks[i] != nil {
					groupScoreChunks[i].Release()
				}
			}
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
				result.groupScores.Release()
				for i := 0; i < chunkIdx; i++ {
					if groupScoreChunks[i] != nil {
						groupScoreChunks[i].Release()
					}
				}
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
			for _, chunk := range groupScoreChunks {
				if chunk != nil {
					chunk.Release()
				}
			}
			return nil, err
		}
		builder.CopyFieldMetadata(input, colName)
	}

	// Add group score column
	if err := builder.AddColumnFromChunks(GroupScoreFieldName, groupScoreChunks); err != nil {
		return nil, err
	}

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

	groupChunk := groupCol.Chunk(chunkIdx)
	scoreChunk, ok := scoreCol.Chunk(chunkIdx).(*array.Float32)
	if !ok {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("group_by_op: score column chunk %d is not Float32", chunkIdx))
	}
	chunkLen := groupChunk.Len()

	// Step 1: Build groups
	groups := o.buildGroups(groupChunk, scoreChunk, chunkLen)

	// Step 2: Sort rows within each group by score DESC, keep top groupSize
	for _, g := range groups {
		o.sortAndLimitGroup(g)
	}

	// Step 3: Compute group scores based on scorer mode
	for _, g := range groups {
		o.computeGroupScore(g)
	}

	// Step 4: Sort groups by group score DESC (stable to preserve insertion order for equal scores)
	sort.SliceStable(groups, func(i, j int) bool {
		return groups[i].groupScore > groups[j].groupScore
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
}

// buildGroups builds groups from the chunk.
func (o *GroupByOp) buildGroups(groupChunk arrow.Array, scoreChunk *array.Float32, chunkLen int) []*group {
	groupMap := make(map[any]*group)
	groupOrder := make([]any, 0) // Maintain appearance order

	for i := 0; i < chunkLen; i++ {
		key := getArrayValue(groupChunk, i)
		score := scoreChunk.Value(i)

		if g, exists := groupMap[key]; exists {
			g.rowIndices = append(g.rowIndices, i)
			g.scores = append(g.scores, score)
		} else {
			g := &group{
				key:        key,
				rowIndices: []int{i},
				scores:     []float32{score},
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

// sortAndLimitGroup sorts rows within a group by score DESC and keeps top groupSize.
func (o *GroupByOp) sortAndLimitGroup(g *group) {
	// Sort indices and scores together by score DESC
	n := len(g.rowIndices)
	indices := make([]int, n)
	for i := 0; i < n; i++ {
		indices[i] = i
	}

	sort.SliceStable(indices, func(i, j int) bool {
		return g.scores[indices[i]] > g.scores[indices[j]]
	})

	// Reorder rowIndices and scores
	newRowIndices := make([]int, n)
	newScores := make([]float32, n)
	for i, idx := range indices {
		newRowIndices[i] = g.rowIndices[idx]
		newScores[i] = g.scores[idx]
	}
	g.rowIndices = newRowIndices
	g.scores = newScores

	// Keep top groupSize
	if int64(len(g.rowIndices)) > o.groupSize {
		g.rowIndices = g.rowIndices[:o.groupSize]
		g.scores = g.scores[:o.groupSize]
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
		// Max is the first score after sorting (scores are sorted DESC)
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

func init() {
	MustRegisterOperator(types.OpTypeGroupBy, NewGroupByOpFromRepr)
}
