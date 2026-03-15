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
	"math"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

// =============================================================================
// MergeStrategy
// =============================================================================

// MergeStrategy defines how to merge multiple DataFrames.
type MergeStrategy string

const (
	MergeStrategyRRF      MergeStrategy = "rrf"
	MergeStrategyWeighted MergeStrategy = "weighted"
	MergeStrategyMax      MergeStrategy = "max"
	MergeStrategySum      MergeStrategy = "sum"
	MergeStrategyAvg      MergeStrategy = "avg"
)

// =============================================================================
// MergeOp
// =============================================================================

// MergeOp merges multiple DataFrames into one with optional normalization.
// This operator is typically used as the first operator in a rerank chain.
type MergeOp struct {
	BaseOp
	strategy    MergeStrategy
	weights     []float64 // for weighted strategy
	rrfK        float64   // for rrf strategy, default 60
	metricTypes []string  // metric type for each input (IP/L2/COSINE/BM25)
	normalize   bool      // whether to normalize scores
}

// MergeOption is a functional option for MergeOp.
type MergeOption func(*MergeOp)

// WithWeights sets the weights for weighted merge strategy.
func WithWeights(weights []float64) MergeOption {
	return func(op *MergeOp) {
		op.weights = weights
	}
}

// WithRRFK sets the k parameter for RRF merge strategy.
func WithRRFK(k float64) MergeOption {
	return func(op *MergeOp) {
		op.rrfK = k
	}
}

// WithMetricTypes sets the metric types for each input.
func WithMetricTypes(metricTypes []string) MergeOption {
	return func(op *MergeOp) {
		op.metricTypes = metricTypes
	}
}

// WithNormalize sets whether to normalize scores.
func WithNormalize(normalize bool) MergeOption {
	return func(op *MergeOp) {
		op.normalize = normalize
	}
}

// NewMergeOp creates a new MergeOp with the given strategy and options.
func NewMergeOp(strategy MergeStrategy, opts ...MergeOption) *MergeOp {
	op := &MergeOp{
		BaseOp: BaseOp{
			inputs:  []string{},
			outputs: []string{},
		},
		strategy:  strategy,
		rrfK:      60, // default RRF k
		normalize: false,
	}

	for _, opt := range opts {
		opt(op)
	}

	return op
}

func (op *MergeOp) Name() string { return "Merge" }

// SortDescending returns the correct sort direction for results produced by this MergeOp.
// Returns true if results should be sorted descending (larger score = better match).
func (op *MergeOp) SortDescending() bool {
	if op.normalize {
		return true
	}
	if len(op.metricTypes) == 0 {
		return true
	}
	_, sortDescending := classifyMetricsOrder(op.metricTypes)
	return sortDescending
}

func (op *MergeOp) String() string {
	return fmt.Sprintf("Merge(%s)", op.strategy)
}

// Execute delegates to ExecuteMulti with a single input.
func (op *MergeOp) Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	return op.ExecuteMulti(ctx, []*DataFrame{input})
}

// ExecuteMulti merges multiple DataFrames into one.
func (op *MergeOp) ExecuteMulti(ctx *types.FuncContext, inputs []*DataFrame) (*DataFrame, error) {
	if len(inputs) == 0 {
		return nil, merr.WrapErrServiceInternal("merge_op: no inputs provided")
	}

	// Validate inputs have same number of chunks (NQ)
	numChunks := inputs[0].NumChunks()
	for i, df := range inputs {
		if df.NumChunks() != numChunks {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: input[%d] has %d chunks, expected %d", i, df.NumChunks(), numChunks))
		}
	}

	// Validate metric types count matches inputs count
	if len(op.metricTypes) > 0 && len(op.metricTypes) != len(inputs) {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: metric types count %d != inputs count %d", len(op.metricTypes), len(inputs)))
	}

	// Validate weights for weighted strategy
	if op.strategy == MergeStrategyWeighted {
		if len(op.weights) != len(inputs) {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: weights count %d != inputs count %d", len(op.weights), len(inputs)))
		}
	}

	// Single input: just normalize if needed and return
	if len(inputs) == 1 {
		return op.processSingleInput(ctx, inputs[0])
	}

	// Multiple inputs: merge based on strategy
	switch op.strategy {
	case MergeStrategyRRF:
		return op.mergeRRF(ctx, inputs)
	case MergeStrategyWeighted:
		return op.mergeWeighted(ctx, inputs)
	case MergeStrategyMax:
		return op.mergeScoreCombine(ctx, inputs, maxMergeFunc)
	case MergeStrategySum:
		return op.mergeScoreCombine(ctx, inputs, sumMergeFunc)
	case MergeStrategyAvg:
		return op.mergeScoreCombine(ctx, inputs, avgMergeFunc)
	default:
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: unsupported strategy %s", op.strategy))
	}
}

// processSingleInput handles single input case: normalize and pass through.
func (op *MergeOp) processSingleInput(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error) {
	if !op.normalize || len(op.metricTypes) == 0 {
		// No normalization needed, retain and return same DataFrame
		// Note: We need to create a new DataFrame with retained columns
		builder := NewDataFrameBuilder()
		defer builder.Release()

		builder.SetChunkSizes(input.ChunkSizes())

		for _, colName := range input.ColumnNames() {
			if err := builder.AddColumnFrom(input, colName); err != nil {
				return nil, err
			}
		}

		return builder.Build(), nil
	}

	// Normalize scores
	normFunc := getNormalizeFunc(op.metricTypes[0])

	builder := NewDataFrameBuilder()
	defer builder.Release()

	builder.SetChunkSizes(input.ChunkSizes())

	// Copy all columns except score, normalize score
	for _, colName := range input.ColumnNames() {
		if colName == types.ScoreFieldName {
			scoreCol := input.Column(types.ScoreFieldName)
			normalizedChunks := make([]arrow.Array, input.NumChunks())
			for i := 0; i < input.NumChunks(); i++ {
				chunk, ok := scoreCol.Chunk(i).(*array.Float32)
				if !ok {
					for j := 0; j < i; j++ {
						normalizedChunks[j].Release()
					}
					return nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: score column chunk %d is not Float32", i))
				}
				normalized, err := normalizeScoreChunk(ctx, chunk, normFunc)
				if err != nil {
					for j := 0; j < i; j++ {
						normalizedChunks[j].Release()
					}
					return nil, err
				}
				normalizedChunks[i] = normalized
			}
			if err := builder.AddColumnFromChunks(types.ScoreFieldName, normalizedChunks); err != nil {
				return nil, err
			}
			builder.CopyFieldMetadata(input, types.ScoreFieldName)
		} else {
			if err := builder.AddColumnFrom(input, colName); err != nil {
				return nil, err
			}
		}
	}

	return builder.Build(), nil
}

// =============================================================================
// Merge Strategies
// =============================================================================

// mergeRRF implements Reciprocal Rank Fusion.
// scoreCollectFunc collects scores for a single chunk, returning per-ID scores and locations.
type scoreCollectFunc func(inputs []*DataFrame, chunkIdx int) (map[any]float32, map[any]idLocation, error)

// mergeWithScoreCollector is the common merge skeleton shared by all strategies.
// The only varying part — how scores are collected per chunk — is injected via collectFn.
func (op *MergeOp) mergeWithScoreCollector(ctx *types.FuncContext, inputs []*DataFrame, collectFn scoreCollectFunc) (*DataFrame, error) {
	numChunks := inputs[0].NumChunks()

	builder := NewDataFrameBuilder()
	defer builder.Release()

	newChunkSizes := make([]int64, numChunks)
	idChunks := make([]arrow.Array, numChunks)
	scoreChunks := make([]arrow.Array, numChunks)

	fieldCollectors := make(map[string]*ChunkCollector)

	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		idScores, idLocs, err := collectFn(inputs, chunkIdx)
		if err != nil {
			op.releaseChunks(idChunks, scoreChunks, fieldCollectors)
			return nil, err
		}

		ids, scores, locs := sortAndExtractResults(idScores, idLocs, op.SortDescending())
		newChunkSizes[chunkIdx] = int64(len(ids))

		idArr, scoreArr, err := op.buildResultArrays(ctx, ids, scores)
		if err != nil {
			op.releaseChunks(idChunks, scoreChunks, fieldCollectors)
			return nil, err
		}
		idChunks[chunkIdx] = idArr
		scoreChunks[chunkIdx] = scoreArr

		if err := op.collectFieldData(ctx, fieldCollectors, locs, inputs, chunkIdx); err != nil {
			op.releaseChunks(idChunks, scoreChunks, fieldCollectors)
			return nil, err
		}
	}

	builder.SetChunkSizes(newChunkSizes)

	if err := builder.AddColumnFromChunks(types.IDFieldName, idChunks); err != nil {
		op.releaseChunks(nil, scoreChunks, fieldCollectors)
		return nil, err
	}
	if err := builder.AddColumnFromChunks(types.ScoreFieldName, scoreChunks); err != nil {
		op.releaseChunks(nil, nil, fieldCollectors)
		return nil, err
	}

	for _, colName := range collectOrderedFieldNames(inputs) {
		collector, exists := fieldCollectors[colName]
		if !exists {
			continue
		}
		if err := builder.AddColumnFromChunks(colName, collector.Consume(colName)); err != nil {
			return nil, err
		}
		for _, input := range inputs {
			if input.HasColumn(colName) {
				builder.CopyFieldMetadata(input, colName)
				break
			}
		}
	}

	return builder.Build(), nil
}

// mergeRRF implements Reciprocal Rank Fusion.
func (op *MergeOp) mergeRRF(ctx *types.FuncContext, inputs []*DataFrame) (*DataFrame, error) {
	return op.mergeWithScoreCollector(ctx, inputs, op.collectRRFScores)
}

// collectRRFScores collects RRF scores for a single chunk.
func (op *MergeOp) collectRRFScores(inputs []*DataFrame, chunkIdx int) (map[any]float32, map[any]idLocation, error) {
	idScores := make(map[any]float32)
	idLocs := make(map[any]idLocation)

	for inputIdx, df := range inputs {
		idCol := df.Column(types.IDFieldName)
		if idCol == nil {
			return nil, nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: input[%d] missing %s column", inputIdx, types.IDFieldName))
		}

		idChunk := idCol.Chunk(chunkIdx)
		for rowIdx := 0; rowIdx < idChunk.Len(); rowIdx++ {
			id := getIDValue(idChunk, rowIdx)
			if id == nil {
				continue
			}

			// RRF score: 1 / (k + rank), rank is 1-based
			rrfScore := float32(1.0 / (op.rrfK + float64(rowIdx+1)))

			if existingScore, exists := idScores[id]; exists {
				idScores[id] = existingScore + rrfScore
			} else {
				idScores[id] = rrfScore
				idLocs[id] = idLocation{inputIdx: inputIdx, rowIdx: rowIdx}
			}
		}
	}

	return idScores, idLocs, nil
}

// mergeWeighted implements weighted score merge.
func (op *MergeOp) mergeWeighted(ctx *types.FuncContext, inputs []*DataFrame) (*DataFrame, error) {
	return op.mergeWithScoreCollector(ctx, inputs, op.collectWeightedScores)
}

// collectWeightedScores collects weighted scores for a single chunk.
func (op *MergeOp) collectWeightedScores(inputs []*DataFrame, chunkIdx int) (map[any]float32, map[any]idLocation, error) {
	idScores := make(map[any]float32)
	idLocs := make(map[any]idLocation)

	// When normalize=false but metrics are mixed, apply direction conversion
	isMixed := false
	if !op.normalize && len(op.metricTypes) > 0 {
		isMixed, _ = classifyMetricsOrder(op.metricTypes)
	}

	for inputIdx, df := range inputs {
		idCol := df.Column(types.IDFieldName)
		scoreCol := df.Column(types.ScoreFieldName)
		if idCol == nil || scoreCol == nil {
			return nil, nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: input[%d] missing ID or score column", inputIdx))
		}

		idChunk := idCol.Chunk(chunkIdx)
		scoreChunk, ok := scoreCol.Chunk(chunkIdx).(*array.Float32)
		if !ok {
			return nil, nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: input[%d] score column chunk %d is not Float32", inputIdx, chunkIdx))
		}

		weight := float32(op.weights[inputIdx])

		// Get normalization function for this input
		var normFunc normalizeFunc
		if op.normalize && len(op.metricTypes) > inputIdx {
			normFunc = getNormalizeFunc(op.metricTypes[inputIdx])
		} else if isMixed && len(op.metricTypes) > inputIdx {
			normFunc = getDirectionConvertFunc(op.metricTypes[inputIdx])
		}

		for rowIdx := 0; rowIdx < idChunk.Len(); rowIdx++ {
			id := getIDValue(idChunk, rowIdx)
			if id == nil {
				continue
			}

			score := scoreChunk.Value(rowIdx)
			if normFunc != nil {
				score = normFunc(score)
			}
			weightedScore := weight * score

			if existingScore, exists := idScores[id]; exists {
				idScores[id] = existingScore + weightedScore
			} else {
				idScores[id] = weightedScore
				idLocs[id] = idLocation{inputIdx: inputIdx, rowIdx: rowIdx}
			}
		}
	}

	return idScores, idLocs, nil
}

// scoreMergeFunc defines how to merge scores for the same ID.
type scoreMergeFunc func(existing float32, new float32, count int) (float32, int)

func maxMergeFunc(existing, new float32, count int) (float32, int) {
	if new > existing {
		return new, count + 1
	}
	return existing, count + 1
}

func sumMergeFunc(existing, new float32, count int) (float32, int) {
	return existing + new, count + 1
}

func avgMergeFunc(existing, new float32, count int) (float32, int) {
	// For avg, we accumulate sum and count, then compute average at the end
	return existing + new, count + 1
}

// mergeScoreCombine implements max/sum/avg score merge.
func (op *MergeOp) mergeScoreCombine(ctx *types.FuncContext, inputs []*DataFrame, mergeFunc scoreMergeFunc) (*DataFrame, error) {
	return op.mergeWithScoreCollector(ctx, inputs, func(inputs []*DataFrame, chunkIdx int) (map[any]float32, map[any]idLocation, error) {
		idScores, idCounts, idLocs, err := op.collectCombinedScores(inputs, chunkIdx, mergeFunc)
		if err != nil {
			return nil, nil, err
		}

		// For avg strategy, compute final average
		if op.strategy == MergeStrategyAvg {
			for id, score := range idScores {
				if count, exists := idCounts[id]; exists && count > 0 {
					idScores[id] = score / float32(count)
				}
			}
		}

		return idScores, idLocs, nil
	})
}

// collectCombinedScores collects combined scores for max/sum/avg strategies.
func (op *MergeOp) collectCombinedScores(inputs []*DataFrame, chunkIdx int, mergeFunc scoreMergeFunc) (map[any]float32, map[any]int, map[any]idLocation, error) {
	idScores := make(map[any]float32)
	idCounts := make(map[any]int)
	idLocs := make(map[any]idLocation)

	// When normalize=false but metrics are mixed, apply direction conversion
	isMixed := false
	if !op.normalize && len(op.metricTypes) > 0 {
		isMixed, _ = classifyMetricsOrder(op.metricTypes)
	}

	for inputIdx, df := range inputs {
		idCol := df.Column(types.IDFieldName)
		scoreCol := df.Column(types.ScoreFieldName)
		if idCol == nil || scoreCol == nil {
			return nil, nil, nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: input[%d] missing ID or score column", inputIdx))
		}

		idChunk := idCol.Chunk(chunkIdx)
		scoreChunk, ok := scoreCol.Chunk(chunkIdx).(*array.Float32)
		if !ok {
			return nil, nil, nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: input[%d] score column chunk %d is not Float32", inputIdx, chunkIdx))
		}

		var normFunc normalizeFunc
		if op.normalize && len(op.metricTypes) > inputIdx {
			normFunc = getNormalizeFunc(op.metricTypes[inputIdx])
		} else if isMixed && len(op.metricTypes) > inputIdx {
			normFunc = getDirectionConvertFunc(op.metricTypes[inputIdx])
		}

		for rowIdx := 0; rowIdx < idChunk.Len(); rowIdx++ {
			id := getIDValue(idChunk, rowIdx)
			if id == nil {
				continue
			}

			score := scoreChunk.Value(rowIdx)
			if normFunc != nil {
				score = normFunc(score)
			}

			if existingScore, exists := idScores[id]; exists {
				newScore, newCount := mergeFunc(existingScore, score, idCounts[id])
				idScores[id] = newScore
				idCounts[id] = newCount
			} else {
				idScores[id] = score
				idCounts[id] = 1
				idLocs[id] = idLocation{inputIdx: inputIdx, rowIdx: rowIdx}
			}
		}
	}

	return idScores, idCounts, idLocs, nil
}

// =============================================================================
// MergeOp Helper Types and Functions
// =============================================================================

// idLocation tracks where an ID was first seen.
type idLocation struct {
	inputIdx int
	rowIdx   int
}

// normalizeFunc normalizes a score based on metric type.
type normalizeFunc func(float32) float32

// classifyMetricsOrder inspects the given metrics and determines
// whether they contain mixed types and what the sorting order should be.
func classifyMetricsOrder(metricTypes []string) (mixed bool, sortDescending bool) {
	countLargerIsBetter := 0
	countSmallerIsBetter := 0
	for _, m := range metricTypes {
		switch strings.ToUpper(m) {
		case metric.COSINE, metric.IP, metric.BM25:
			countLargerIsBetter++
		default:
			countSmallerIsBetter++
		}
	}
	if countLargerIsBetter > 0 && countSmallerIsBetter > 0 {
		return true, true
	}
	return false, countSmallerIsBetter == 0
}

// getDirectionConvertFunc returns a function that converts smaller-is-better
// metrics (like L2) to larger-is-better direction without full range normalization.
// Returns nil for metrics that are already larger-is-better.
func getDirectionConvertFunc(metricType string) normalizeFunc {
	switch strings.ToUpper(metricType) {
	case metric.COSINE, metric.IP, metric.BM25:
		return nil
	default:
		return func(distance float32) float32 {
			return 1.0 - 2*float32(math.Atan(float64(distance)))/math.Pi
		}
	}
}

// getNormalizeFunc returns the normalization function for a metric type.
func getNormalizeFunc(metricType string) normalizeFunc {
	switch strings.ToUpper(metricType) {
	case metric.COSINE:
		return func(score float32) float32 {
			return (1 + score) * 0.5
		}
	case metric.IP:
		return func(score float32) float32 {
			return 0.5 + float32(math.Atan(float64(score)))/math.Pi
		}
	case metric.BM25:
		return func(score float32) float32 {
			return 2 * float32(math.Atan(float64(score))) / math.Pi
		}
	default:
		// L2 and other distance metrics: smaller is better, need to invert
		return func(distance float32) float32 {
			return 1.0 - 2*float32(math.Atan(float64(distance)))/math.Pi
		}
	}
}

// normalizeScoreChunk normalizes a score chunk.
func normalizeScoreChunk(ctx *types.FuncContext, chunk *array.Float32, normFunc normalizeFunc) (arrow.Array, error) {
	builder := array.NewFloat32Builder(ctx.Pool())
	defer builder.Release()

	for i := 0; i < chunk.Len(); i++ {
		if chunk.IsNull(i) {
			builder.AppendNull()
		} else {
			builder.Append(normFunc(chunk.Value(i)))
		}
	}

	return builder.NewArray(), nil
}

// getIDValue extracts ID value from an array at given index.
func getIDValue(arr arrow.Array, idx int) any {
	if arr.IsNull(idx) {
		return nil
	}

	switch a := arr.(type) {
	case *array.Int64:
		return a.Value(idx)
	case *array.String:
		return a.Value(idx)
	default:
		return nil
	}
}

// collectOrderedFieldNames returns field names (excluding $id and $score)
// in deterministic order, preserving first-seen order from inputs.
func collectOrderedFieldNames(inputs []*DataFrame) []string {
	seen := make(map[string]bool)
	var names []string
	for _, df := range inputs {
		for _, colName := range df.ColumnNames() {
			if colName == types.IDFieldName || colName == types.ScoreFieldName {
				continue
			}
			if !seen[colName] {
				seen[colName] = true
				names = append(names, colName)
			}
		}
	}
	return names
}

// getArrayValue extracts value from an array at given index.
func getArrayValue(arr arrow.Array, idx int) any {
	if arr.IsNull(idx) {
		return nil
	}

	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(idx)
	case *array.Int8:
		return a.Value(idx)
	case *array.Int16:
		return a.Value(idx)
	case *array.Int32:
		return a.Value(idx)
	case *array.Int64:
		return a.Value(idx)
	case *array.Uint8:
		return a.Value(idx)
	case *array.Uint16:
		return a.Value(idx)
	case *array.Uint32:
		return a.Value(idx)
	case *array.Uint64:
		return a.Value(idx)
	case *array.Float32:
		return a.Value(idx)
	case *array.Float64:
		return a.Value(idx)
	case *array.String:
		return a.Value(idx)
	default:
		return nil
	}
}

// sortAndExtractResults sorts IDs by score and extracts results.
// When descending is true, larger scores sort first (higher = better match).
// When descending is false, smaller scores sort first (lower = better match, e.g. L2).
func sortAndExtractResults(idScores map[any]float32, idLocs map[any]idLocation, descending bool) ([]any, []float32, []idLocation) {
	ids := make([]any, 0, len(idScores))
	for id := range idScores {
		ids = append(ids, id)
	}

	sortIDs(ids, idScores, descending)

	scores := make([]float32, len(ids))
	locs := make([]idLocation, len(ids))
	for i, id := range ids {
		scores[i] = idScores[id]
		locs[i] = idLocs[id]
	}

	return ids, scores, locs
}

// sortIDs sorts IDs by score with stable tie-breaking by ID.
func sortIDs(ids []any, idScores map[any]float32, descending bool) {
	sort.SliceStable(ids, func(i, j int) bool {
		scoreI := idScores[ids[i]]
		scoreJ := idScores[ids[j]]
		if scoreI != scoreJ {
			if descending {
				return scoreI > scoreJ
			}
			return scoreI < scoreJ
		}
		return compareIDs(ids[i], ids[j]) < 0
	})
}

// compareIDs compares two IDs for stable sorting.
func compareIDs(a, b any) int {
	switch va := a.(type) {
	case int64:
		vb, ok := b.(int64)
		if !ok {
			return 0
		}
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	case string:
		vb, ok := b.(string)
		if !ok {
			return 0
		}
		if va < vb {
			return -1
		} else if va > vb {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// buildResultArrays builds ID and score arrays from results.
func (op *MergeOp) buildResultArrays(ctx *types.FuncContext, ids []any, scores []float32) (arrow.Array, arrow.Array, error) {
	if len(ids) == 0 {
		// Empty result
		idBuilder := array.NewInt64Builder(ctx.Pool())
		scoreBuilder := array.NewFloat32Builder(ctx.Pool())
		defer idBuilder.Release()
		defer scoreBuilder.Release()
		return idBuilder.NewArray(), scoreBuilder.NewArray(), nil
	}

	// Determine ID type from first ID
	switch ids[0].(type) {
	case int64:
		return op.buildInt64Results(ctx, ids, scores)
	case string:
		return op.buildStringResults(ctx, ids, scores)
	default:
		return nil, nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: unsupported ID type %T", ids[0]))
	}
}

func (op *MergeOp) buildInt64Results(ctx *types.FuncContext, ids []any, scores []float32) (arrow.Array, arrow.Array, error) {
	idBuilder := array.NewInt64Builder(ctx.Pool())
	scoreBuilder := array.NewFloat32Builder(ctx.Pool())
	defer idBuilder.Release()
	defer scoreBuilder.Release()

	for i, id := range ids {
		v, ok := id.(int64)
		if !ok {
			return nil, nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: expected int64 ID at index %d, got %T", i, id))
		}
		idBuilder.Append(v)
		scoreBuilder.Append(scores[i])
	}

	return idBuilder.NewArray(), scoreBuilder.NewArray(), nil
}

func (op *MergeOp) buildStringResults(ctx *types.FuncContext, ids []any, scores []float32) (arrow.Array, arrow.Array, error) {
	idBuilder := array.NewStringBuilder(ctx.Pool())
	scoreBuilder := array.NewFloat32Builder(ctx.Pool())
	defer idBuilder.Release()
	defer scoreBuilder.Release()

	for i, id := range ids {
		v, ok := id.(string)
		if !ok {
			return nil, nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: expected string ID at index %d, got %T", i, id))
		}
		idBuilder.Append(v)
		scoreBuilder.Append(scores[i])
	}

	return idBuilder.NewArray(), scoreBuilder.NewArray(), nil
}

// collectFieldData collects field data for merged results.
// When locs is empty, empty arrays are created to avoid nil chunks in collectors.
func (op *MergeOp) collectFieldData(ctx *types.FuncContext, collectors map[string]*ChunkCollector, locs []idLocation, inputs []*DataFrame, chunkIdx int) error {
	// Get all field names from all inputs
	fieldNames := make(map[string]bool)
	for _, df := range inputs {
		for _, colName := range df.ColumnNames() {
			if colName == types.IDFieldName || colName == types.ScoreFieldName {
				continue
			}
			fieldNames[colName] = true
		}
	}

	if len(fieldNames) == 0 {
		return nil
	}

	numChunks := inputs[0].NumChunks()

	// Initialize collectors for new fields
	for colName := range fieldNames {
		if _, exists := collectors[colName]; !exists {
			collectors[colName] = NewChunkCollector([]string{colName}, numChunks)
		}
	}

	// Build field arrays for this chunk (buildFieldArray handles empty locs by
	// creating empty arrays of the appropriate type)
	for colName := range fieldNames {
		arr, err := op.buildFieldArray(ctx, colName, locs, inputs, chunkIdx)
		if err != nil {
			return err
		}
		collectors[colName].Set(colName, chunkIdx, arr)
	}

	return nil
}

// buildFieldArray builds a field array from merged locations.
func (op *MergeOp) buildFieldArray(ctx *types.FuncContext, colName string, locs []idLocation, inputs []*DataFrame, chunkIdx int) (arrow.Array, error) {
	if len(locs) == 0 {
		// Return empty array of appropriate type
		// Find type from first input that has this column
		for _, df := range inputs {
			if col := df.Column(colName); col != nil {
				return buildEmptyArray(ctx.Pool(), col.DataType())
			}
		}
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: cannot determine type for column %s", colName))
	}

	// Find the data type from first input that has this column
	var dataType arrow.DataType
	for _, df := range inputs {
		if col := df.Column(colName); col != nil {
			dataType = col.DataType()
			break
		}
	}

	if dataType == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: column %s not found in any input", colName))
	}

	return buildArrayFromLocations(ctx.Pool(), colName, locs, inputs, dataType, chunkIdx)
}

// buildEmptyArray creates an empty array of the given type.
func buildEmptyArray(pool memory.Allocator, dt arrow.DataType) (arrow.Array, error) {
	switch dt.ID() {
	case arrow.BOOL:
		b := array.NewBooleanBuilder(pool)
		defer b.Release()
		return b.NewArray(), nil
	case arrow.INT8:
		b := array.NewInt8Builder(pool)
		defer b.Release()
		return b.NewArray(), nil
	case arrow.INT16:
		b := array.NewInt16Builder(pool)
		defer b.Release()
		return b.NewArray(), nil
	case arrow.INT32:
		b := array.NewInt32Builder(pool)
		defer b.Release()
		return b.NewArray(), nil
	case arrow.INT64:
		b := array.NewInt64Builder(pool)
		defer b.Release()
		return b.NewArray(), nil
	case arrow.FLOAT32:
		b := array.NewFloat32Builder(pool)
		defer b.Release()
		return b.NewArray(), nil
	case arrow.FLOAT64:
		b := array.NewFloat64Builder(pool)
		defer b.Release()
		return b.NewArray(), nil
	case arrow.STRING:
		b := array.NewStringBuilder(pool)
		defer b.Release()
		return b.NewArray(), nil
	default:
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("unsupported type: %s", dt.Name()))
	}
}

// buildArrayFromLocations builds an array from locations.
func buildArrayFromLocations(pool memory.Allocator, colName string, locs []idLocation, inputs []*DataFrame, dt arrow.DataType, chunkIdx int) (arrow.Array, error) {
	switch dt.ID() {
	case arrow.BOOL:
		return buildTypedArrayFromLocations[bool](pool, colName, locs, inputs, array.NewBooleanBuilder(pool), chunkIdx)
	case arrow.INT8:
		return buildTypedArrayFromLocations[int8](pool, colName, locs, inputs, array.NewInt8Builder(pool), chunkIdx)
	case arrow.INT16:
		return buildTypedArrayFromLocations[int16](pool, colName, locs, inputs, array.NewInt16Builder(pool), chunkIdx)
	case arrow.INT32:
		return buildTypedArrayFromLocations[int32](pool, colName, locs, inputs, array.NewInt32Builder(pool), chunkIdx)
	case arrow.INT64:
		return buildTypedArrayFromLocations[int64](pool, colName, locs, inputs, array.NewInt64Builder(pool), chunkIdx)
	case arrow.FLOAT32:
		return buildTypedArrayFromLocations[float32](pool, colName, locs, inputs, array.NewFloat32Builder(pool), chunkIdx)
	case arrow.FLOAT64:
		return buildTypedArrayFromLocations[float64](pool, colName, locs, inputs, array.NewFloat64Builder(pool), chunkIdx)
	case arrow.STRING:
		return buildTypedArrayFromLocations[string](pool, colName, locs, inputs, array.NewStringBuilder(pool), chunkIdx)
	default:
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("unsupported type: %s", dt.Name()))
	}
}

// typedArrayBuilder is a generic builder interface for MergeOp.
type typedArrayBuilder[T any] interface {
	Append(T)
	AppendNull()
	NewArray() arrow.Array
	Release()
}

// buildTypedArrayFromLocations builds a typed array from locations.
func buildTypedArrayFromLocations[T any, B typedArrayBuilder[T]](pool memory.Allocator, colName string, locs []idLocation, inputs []*DataFrame, builder B, chunkIdx int) (arrow.Array, error) {
	defer builder.Release()

	for _, loc := range locs {
		df := inputs[loc.inputIdx]
		col := df.Column(colName)
		if col == nil {
			builder.AppendNull()
			continue
		}

		chunk := col.Chunk(chunkIdx)
		if chunk.IsNull(loc.rowIdx) {
			builder.AppendNull()
			continue
		}

		val := getTypedValue[T](chunk, loc.rowIdx)
		builder.Append(val)
	}

	return builder.NewArray(), nil
}

// getTypedValue extracts a typed value from an array.
// The caller (buildArrayFromLocations) dispatches by Arrow type and instantiates T
// to match the concrete array type, so the type assertion is guaranteed to succeed.
func getTypedValue[T any](arr arrow.Array, idx int) T {
	var zero T
	switch a := arr.(type) {
	case *array.Boolean:
		if v, ok := any(a.Value(idx)).(T); ok {
			return v
		}
	case *array.Int8:
		if v, ok := any(a.Value(idx)).(T); ok {
			return v
		}
	case *array.Int16:
		if v, ok := any(a.Value(idx)).(T); ok {
			return v
		}
	case *array.Int32:
		if v, ok := any(a.Value(idx)).(T); ok {
			return v
		}
	case *array.Int64:
		if v, ok := any(a.Value(idx)).(T); ok {
			return v
		}
	case *array.Float32:
		if v, ok := any(a.Value(idx)).(T); ok {
			return v
		}
	case *array.Float64:
		if v, ok := any(a.Value(idx)).(T); ok {
			return v
		}
	case *array.String:
		if v, ok := any(a.Value(idx)).(T); ok {
			return v
		}
	}
	return zero
}

// releaseChunks releases chunks and collectors on error.
func (op *MergeOp) releaseChunks(idChunks, scoreChunks []arrow.Array, collectors map[string]*ChunkCollector) {
	for _, chunk := range idChunks {
		if chunk != nil {
			chunk.Release()
		}
	}
	for _, chunk := range scoreChunks {
		if chunk != nil {
			chunk.Release()
		}
	}
	for _, collector := range collectors {
		collector.Release()
	}
}
