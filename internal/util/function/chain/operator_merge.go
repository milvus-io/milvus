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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
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

// NOTE: MergeOp does NOT register itself via init() / MustRegisterOperator like other operators.
// Reasons:
//   1. MergeOp requires runtime context (metricTypes, weights, rrfK, normalize) that comes from
//      the search request and collection schema, which cannot be recovered from a static OperatorRepr alone.
//   2. MergeOp can only appear at position 0 in a chain (enforced by FuncChain.validate) and is always
//      constructed programmatically by builder functions (e.g. BuildRerankChain) via NewMergeOp().
//   3. There is no NewMergeOpFromRepr factory — the functional-options pattern (WithWeights, WithMetricTypes, etc.)
//      does not map cleanly to the generic OperatorRepr dictionary.
//
// TODO: refactor MergeOp to support OperatorRepr-based construction and register it in the operator registry,
// so that merge chains can be fully described and deserialized from a declarative representation.

// MergeOp merges multiple DataFrames into one with optional normalization.
// This operator is typically used as the first operator in a rerank chain.
//
// Behavioral fields (sortDescending, scoreNormFuncs) are pre-computed at construction
// time from the mergeConfig, so the execution path has no metric-type branching.
type MergeOp struct {
	BaseOp
	strategy       MergeStrategy
	weights        []float64       // for weighted strategy
	rrfK           float64         // for rrf strategy, default 60
	sortDescending bool            // pre-computed: true means larger score = better match
	scoreNormFuncs []normalizeFunc // pre-computed per-input normalization; nil entry = no-op
}

// mergeConfig collects construction-time parameters from functional options.
// These fields are consumed once by NewMergeOp to derive the behavioral fields
// on MergeOp, then discarded.
type mergeConfig struct {
	weights         []float64
	rrfK            float64
	metricTypes     []string
	normalize       bool
	forceDescending bool
}

// MergeOption is a functional option for MergeOp.
type MergeOption func(*mergeConfig)

// WithWeights sets the weights for weighted merge strategy.
func WithWeights(weights []float64) MergeOption {
	return func(cfg *mergeConfig) {
		cfg.weights = weights
	}
}

// WithRRFK sets the k parameter for RRF merge strategy.
func WithRRFK(k float64) MergeOption {
	return func(cfg *mergeConfig) {
		cfg.rrfK = k
	}
}

// WithMetricTypes sets the metric types for each input.
func WithMetricTypes(metricTypes []string) MergeOption {
	return func(cfg *mergeConfig) {
		cfg.metricTypes = metricTypes
	}
}

// WithNormalize sets whether to normalize scores.
func WithNormalize(normalize bool) MergeOption {
	return func(cfg *mergeConfig) {
		cfg.normalize = normalize
	}
}

// WithForceDescending forces the merged $score column to be sorted by
// "larger = better match". For metrics that are smaller-is-better
// (e.g., L2, HAMMING, JACCARD), each input score is converted via
// 1 - 2·atan(d)/π so the resulting score is descending-sortable; metrics
// that are already larger-is-better (COSINE, IP, BM25, etc.) pass through
// unchanged. When WithNormalize(true) is also set, full normalization
// already implies descending direction and this option has no extra effect.
//
// Used by the decay reranker, which multiplies $score by a decay factor in
// [0, 1] and assumes "larger = better" semantics — see buildDecayChain.
func WithForceDescending(force bool) MergeOption {
	return func(cfg *mergeConfig) {
		cfg.forceDescending = force
	}
}

// NewMergeOp creates a new MergeOp with the given strategy and options.
// Behavioral fields (sortDescending, scoreNormFuncs) are resolved eagerly
// so that the execution path is free of metric-type branching.
func NewMergeOp(strategy MergeStrategy, opts ...MergeOption) *MergeOp {
	cfg := &mergeConfig{rrfK: 60}
	for _, opt := range opts {
		opt(cfg)
	}

	// No metricTypes → pure dedup, no score processing (e.g. model rerank).
	sortDesc := true
	var normFuncs []normalizeFunc
	if len(cfg.metricTypes) > 0 {
		sortDesc, normFuncs = resolveMergeBehavior(cfg.normalize, cfg.forceDescending, cfg.metricTypes)
	}

	return &MergeOp{
		BaseOp: BaseOp{
			inputs:  []string{},
			outputs: []string{},
		},
		strategy:       strategy,
		weights:        cfg.weights,
		rrfK:           cfg.rrfK,
		sortDescending: sortDesc,
		scoreNormFuncs: normFuncs,
	}
}

func (op *MergeOp) Name() string { return "Merge" }

// SortDescending returns the pre-computed sort direction for results produced by this MergeOp.
// Returns true if results should be sorted descending (larger score = better match).
func (op *MergeOp) SortDescending() bool {
	return op.sortDescending
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

	// Validate scoreNormFuncs count matches inputs count (when present)
	if len(op.scoreNormFuncs) > 0 && len(op.scoreNormFuncs) != len(inputs) {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: scoreNormFuncs count %d != inputs count %d", len(op.scoreNormFuncs), len(inputs)))
	}

	// Validate weights for weighted strategy
	if op.strategy == MergeStrategyWeighted {
		if len(op.weights) != len(inputs) {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("merge_op: weights count %d != inputs count %d", len(op.weights), len(inputs)))
		}
	}

	// Merge based on strategy (works for both single and multiple inputs)
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

	// On error, release all un-consumed chunks and collectors in one place.
	success := false
	defer func() {
		if !success {
			op.releaseChunks(idChunks, scoreChunks, fieldCollectors)
		}
	}()

	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		idScores, idLocs, err := collectFn(inputs, chunkIdx)
		if err != nil {
			return nil, err
		}

		ids, scores, locs := sortAndExtractResults(idScores, idLocs, op.SortDescending())
		newChunkSizes[chunkIdx] = int64(len(ids))

		idArr, scoreArr, err := op.buildResultArrays(ctx, ids, scores)
		if err != nil {
			return nil, err
		}
		idChunks[chunkIdx] = idArr
		scoreChunks[chunkIdx] = scoreArr

		if err := op.collectFieldData(ctx, fieldCollectors, locs, inputs, chunkIdx); err != nil {
			return nil, err
		}
	}

	builder.SetChunkSizes(newChunkSizes)

	// AddColumnFromChunks takes ownership: it retains via NewChunked then releases
	// the individual arrays. Nil out the slice so the deferred cleanup won't
	// double-release them.
	if err := builder.AddColumnFromChunks(types.IDFieldName, idChunks); err != nil {
		return nil, err
	}
	idChunks = nil

	if err := builder.AddColumnFromChunks(types.ScoreFieldName, scoreChunks); err != nil {
		return nil, err
	}
	scoreChunks = nil

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

	success = true
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
		normFunc := op.scoreNormFunc(inputIdx)

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

		normFunc := op.scoreNormFunc(inputIdx)

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

// scoreNormFunc returns the pre-computed normalization function for the given input index.
// Returns nil (no-op) when scoreNormFuncs is empty or the index is out of range.
func (op *MergeOp) scoreNormFunc(inputIdx int) normalizeFunc {
	if inputIdx < len(op.scoreNormFuncs) {
		return op.scoreNormFuncs[inputIdx]
	}
	return nil
}

// resolveMergeBehavior pre-computes the sort direction and per-input normalization
// functions from the construction-time config. This is called once in NewMergeOp
// so that the execution path has no metric-type branching.
//
// Precondition: metricTypes is non-empty (caller guards the empty case).
//
// The returned normFuncs always has len == len(metricTypes) so that ExecuteMulti
// can validate input count. Entries may be nil (no-op for that input).
//
// Decision matrix:
//   - normalize=true: full range normalization per metric → DESC sort.
//   - normalize=false, mixed metrics OR forceDescending=true: direction-only
//     conversion (atan for distance metrics, identity for similarity metrics)
//     → DESC sort.
//   - normalize=false, single direction: no conversion, sort by metric's
//     natural order.
func resolveMergeBehavior(normalize, forceDescending bool, metricTypes []string) (bool, []normalizeFunc) {
	normFuncs := make([]normalizeFunc, len(metricTypes))

	if normalize {
		for i, m := range metricTypes {
			normFuncs[i] = getNormalizeFunc(m)
		}
		return true, normFuncs
	}

	mixed, sortDescending := classifyMetricsOrder(metricTypes)
	if mixed || forceDescending {
		for i, m := range metricTypes {
			normFuncs[i] = getDirectionConvertFunc(m)
		}
		return true, normFuncs
	}

	// Non-mixed: all normFuncs stay nil (no-op), sort by metric's natural order.
	return sortDescending, normFuncs
}

// classifyMetricsOrder inspects the given metrics and determines
// whether they contain mixed types and what the sorting order should be.
func classifyMetricsOrder(metricTypes []string) (mixed bool, sortDescending bool) {
	countLargerIsBetter := 0
	countSmallerIsBetter := 0
	for _, m := range metricTypes {
		if metric.PositivelyRelated(m) {
			countLargerIsBetter++
		} else {
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
	if metric.PositivelyRelated(metricType) {
		return nil
	}
	return func(distance float32) float32 {
		return 1.0 - 2*float32(math.Atan(float64(distance)))/math.Pi
	}
}

// getNormalizeFunc returns the normalization function for a metric type.
// For positively-related metrics (larger = more similar), scores are mapped to [0, 1].
// For distance metrics (smaller = more similar), distances are inverted so larger = better.
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
		if metric.PositivelyRelated(metricType) {
			// Other positively-related metrics (MHJACCARD, MaxSim, MaxSimIP, MaxSimCosine):
			// scores are already "larger = better", apply atan-based normalization to [0, 1].
			return func(score float32) float32 {
				return 0.5 + float32(math.Atan(float64(score)))/math.Pi
			}
		}
		// Distance metrics (L2, HAMMING, JACCARD, etc.): smaller is better, need to invert.
		return func(distance float32) float32 {
			return 1.0 - 2*float32(math.Atan(float64(distance)))/math.Pi
		}
	}
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
