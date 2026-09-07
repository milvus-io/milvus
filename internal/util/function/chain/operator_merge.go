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
	"slices"
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

const (
	MergeParamStrategy  = "strategy"
	MergeParamK         = "k"
	MergeParamWeights   = "weights"
	MergeParamNormScore = "norm_score"
)

func init() {
	MustRegisterOperator(types.OpTypeMerge, NewMergeOpFromReprWithContext)
}

type mergeSpec struct {
	strategy   MergeStrategy
	rrfK       float64
	weights    []float64
	weightsSet bool
	normalize  bool
}

// NewMergeOpFromReprWithContext creates a MergeOp from its declarative
// representation and runtime-only search input information.
func NewMergeOpFromReprWithContext(repr *OperatorRepr, buildCtx types.FunctionBuildContext) (Operator, error) {
	spec, err := parseMergeSpec(repr)
	if err != nil {
		return nil, err
	}
	if buildCtx.Search == nil {
		return nil, merr.WrapErrServiceInternal("merge_op: search runtime info is required")
	}

	metricTypes := append([]string(nil), buildCtx.Search.MetricTypes...)
	if len(metricTypes) == 0 {
		return nil, merr.WrapErrServiceInternal("merge_op: search runtime metric types are empty")
	}

	opts := []MergeOption{withExpectedInputs(len(metricTypes))}
	switch spec.strategy {
	case MergeStrategyRRF:
		opts = append(opts, WithRRFK(spec.rrfK))
		if spec.weightsSet {
			if len(spec.weights) != len(metricTypes) {
				return nil, merr.WrapErrParameterInvalidMsg(
					"merge_op: weights count %d does not match search input count %d",
					len(spec.weights), len(metricTypes))
			}
			opts = append(opts, WithWeights(spec.weights))
		}
	case MergeStrategyWeighted:
		if len(spec.weights) != len(metricTypes) {
			return nil, merr.WrapErrParameterInvalidMsg(
				"merge_op: weights count %d does not match search input count %d",
				len(spec.weights), len(metricTypes))
		}
		opts = append(opts,
			WithWeights(spec.weights),
			WithNormalize(spec.normalize),
			WithMetricTypes(metricTypes))
	case MergeStrategyMax, MergeStrategySum, MergeStrategyAvg:
		opts = append(opts,
			WithNormalize(spec.normalize),
			WithMetricTypes(metricTypes))
	default:
		return nil, merr.WrapErrParameterInvalidMsg("merge_op: unsupported strategy %q", spec.strategy)
	}

	return NewMergeOp(spec.strategy, opts...), nil
}

func parseMergeSpec(repr *OperatorRepr) (*mergeSpec, error) {
	if repr == nil {
		return nil, merr.WrapErrParameterInvalidMsg("merge_op: operator representation is nil")
	}
	if strings.TrimSpace(repr.Type) != types.OpTypeMerge {
		return nil, merr.WrapErrParameterInvalidMsg("merge_op: operator type must be %q", types.OpTypeMerge)
	}
	if repr.Function != nil {
		return nil, merr.WrapErrParameterInvalidMsg("merge_op: expr is not supported")
	}
	if len(repr.Inputs) != 0 {
		return nil, merr.WrapErrParameterInvalidMsg("merge_op: explicit inputs are not supported")
	}
	if len(repr.Outputs) != 0 {
		return nil, merr.WrapErrParameterInvalidMsg("merge_op: explicit outputs are not supported")
	}

	reader := types.NewParamReader("merge_op", repr.Params)
	strategyName, err := reader.String(MergeParamStrategy, true)
	if err != nil {
		return nil, err
	}
	strategy := MergeStrategy(strings.ToLower(strings.TrimSpace(strategyName)))

	allowed := map[string]struct{}{MergeParamStrategy: {}}
	switch strategy {
	case MergeStrategyRRF:
		allowed[MergeParamK] = struct{}{}
		allowed[MergeParamWeights] = struct{}{}
	case MergeStrategyWeighted:
		allowed[MergeParamWeights] = struct{}{}
		allowed[MergeParamNormScore] = struct{}{}
	case MergeStrategyMax, MergeStrategySum, MergeStrategyAvg:
		allowed[MergeParamNormScore] = struct{}{}
	default:
		return nil, merr.WrapErrParameterInvalidMsg("merge_op: unsupported strategy %q", strategyName)
	}

	for key := range repr.Params {
		if _, ok := allowed[key]; !ok {
			return nil, merr.WrapErrParameterInvalidMsg("merge_op: parameter %q is not supported for strategy %q", key, strategy)
		}
	}

	spec := &mergeSpec{strategy: strategy, rrfK: 60}
	switch strategy {
	case MergeStrategyRRF:
		spec.rrfK, err = reader.Float64(MergeParamK, false, 60)
		if err != nil {
			return nil, err
		}
		if !isFiniteFloat64(spec.rrfK) || spec.rrfK <= 0 || spec.rrfK >= 16384 {
			return nil, merr.WrapErrParameterInvalidMsg("merge_op: k must be finite and in range (0, 16384)")
		}
		_, spec.weightsSet = repr.Params[MergeParamWeights]
		if spec.weightsSet {
			spec.weights, err = parseMergeWeights(reader)
			if err != nil {
				return nil, err
			}
		}
	case MergeStrategyWeighted:
		spec.weights, err = parseMergeWeights(reader)
		if err != nil {
			return nil, err
		}
		spec.normalize, err = reader.Bool(MergeParamNormScore, false, false)
		if err != nil {
			return nil, err
		}
	case MergeStrategyMax, MergeStrategySum, MergeStrategyAvg:
		spec.normalize, err = reader.Bool(MergeParamNormScore, false, false)
		if err != nil {
			return nil, err
		}
	}
	return spec, nil
}

func parseMergeWeights(reader types.ParamReader) ([]float64, error) {
	weights, err := reader.Float64Slice(MergeParamWeights, true)
	if err != nil {
		return nil, err
	}
	if len(weights) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("merge_op: weights must not be empty")
	}
	for i, weight := range weights {
		if !isFiniteFloat64(weight) || weight < 0 || weight > 1 {
			return nil, merr.WrapErrParameterInvalidMsg("merge_op: weights[%d] must be finite and in range [0, 1]", i)
		}
	}
	return weights, nil
}

func isFiniteFloat64(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

// =============================================================================
// MergeOp
// =============================================================================

// MergeOp's context-aware representation factory is registered in the common
// operator registry. Search metrics are supplied through FunctionBuildContext
// when a declarative chain is constructed.

// MergeOp merges multiple DataFrames into one with optional normalization.
// This operator is typically used as the first operator in a rerank chain.
//
// Behavioral fields (sortDescending, scoreNormFuncs) are pre-computed at construction
// time from the mergeConfig, so the execution path has no metric-type branching.
type MergeOp struct {
	BaseOp
	strategy       MergeStrategy
	weights        []float64       // for weighted strategy and optional RRF path weights
	weightsSet     bool            // distinguishes omitted RRF weights from an explicit empty value
	rrfK           float64         // for rrf strategy, default 60
	sortDescending bool            // pre-computed: true means larger score = better match
	scoreNormFuncs []normalizeFunc // pre-computed per-input normalization; nil entry = no-op
	expectedInputs int             // derived runtime input count; zero skips the check for legacy builders
}

// mergeConfig collects construction-time parameters from functional options.
// These fields are consumed once by NewMergeOp to derive the behavioral fields
// on MergeOp, then discarded.
type mergeConfig struct {
	weights         []float64
	weightsSet      bool
	rrfK            float64
	metricTypes     []string
	normalize       bool
	forceDescending bool
	expectedInputs  int
}

// MergeOption is a functional option for MergeOp.
type MergeOption func(*mergeConfig)

// WithWeights sets the per-input weights for weighted or RRF merge strategy.
func WithWeights(weights []float64) MergeOption {
	return func(cfg *mergeConfig) {
		cfg.weights = append([]float64(nil), weights...)
		cfg.weightsSet = true
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
		cfg.metricTypes = append([]string(nil), metricTypes...)
	}
}

// withExpectedInputs records an input count derived from runtime search input
// metadata. It is intentionally not exported as public Merge configuration.
func withExpectedInputs(count int) MergeOption {
	return func(cfg *mergeConfig) {
		cfg.expectedInputs = count
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
		weights:        append([]float64(nil), cfg.weights...),
		weightsSet:     cfg.weightsSet,
		rrfK:           cfg.rrfK,
		sortDescending: sortDesc,
		scoreNormFuncs: normFuncs,
		expectedInputs: cfg.expectedInputs,
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
	layout, err := op.validateInputs(ctx, inputs)
	if err != nil {
		return nil, err
	}

	// Merge based on strategy (works for both single and multiple inputs)
	switch op.strategy {
	case MergeStrategyRRF:
		return op.mergeRRF(ctx, inputs, layout)
	case MergeStrategyWeighted:
		return op.mergeWeighted(ctx, inputs, layout)
	case MergeStrategyMax:
		return op.mergeNumCombine(ctx, inputs, layout, maxMergeFunc)
	case MergeStrategySum:
		return op.mergeNumCombine(ctx, inputs, layout, sumMergeFunc)
	case MergeStrategyAvg:
		return op.mergeNumCombine(ctx, inputs, layout, avgMergeFunc)
	default:
		return nil, merr.WrapErrServiceInternalMsg("merge_op: unsupported strategy %s", op.strategy)
	}
}

// mergeInputLayout is resolved and validated once before any strategy reads
// Arrow values. Candidate identity is determined only by fixed system columns.
type mergeInputLayout struct {
	numChunks  int
	idType     arrow.DataType
	hasElement bool
}

func (op *MergeOp) validateInputs(ctx *types.FuncContext, inputs []*DataFrame) (*mergeInputLayout, error) {
	if ctx == nil {
		return nil, merr.WrapErrServiceInternal("merge_op: function context is nil")
	}
	if len(inputs) == 0 {
		return nil, merr.WrapErrServiceInternal("merge_op: no inputs provided")
	}
	if op.expectedInputs > 0 && len(inputs) != op.expectedInputs {
		return nil, merr.WrapErrServiceInternalMsg("merge_op: input count %d != expected count %d", len(inputs), op.expectedInputs)
	}
	if op.strategy != MergeStrategyRRF && op.strategy != MergeStrategyWeighted &&
		op.strategy != MergeStrategyMax && op.strategy != MergeStrategySum && op.strategy != MergeStrategyAvg {
		return nil, merr.WrapErrServiceInternalMsg("merge_op: unsupported strategy %s", op.strategy)
	}

	if inputs[0] == nil {
		return nil, merr.WrapErrFunctionFailedMsg("merge_op: input[0] is nil")
	}
	numChunks := inputs[0].NumChunks()
	if numChunks == 0 {
		return nil, merr.WrapErrFunctionFailedMsg("merge_op: inputs have no query chunks")
	}
	for i, df := range inputs {
		if df == nil {
			return nil, merr.WrapErrFunctionFailedMsg("merge_op: input[%d] is nil", i)
		}
		if df.NumChunks() != numChunks {
			return nil, merr.WrapErrFunctionFailedMsg("merge_op: input[%d] has %d chunks, expected %d", i, df.NumChunks(), numChunks)
		}
	}

	// Validate scoreNormFuncs count matches inputs count (when present)
	if len(op.scoreNormFuncs) > 0 && len(op.scoreNormFuncs) != len(inputs) {
		return nil, merr.WrapErrServiceInternalMsg("merge_op: scoreNormFuncs count %d != inputs count %d", len(op.scoreNormFuncs), len(inputs))
	}

	// Weighted score fusion always requires weights. RRF validates them only
	// when the optional weights setting was explicitly supplied.
	if op.strategy == MergeStrategyWeighted || (op.strategy == MergeStrategyRRF && op.weightsSet) {
		if len(op.weights) != len(inputs) {
			return nil, merr.WrapErrServiceInternalMsg("merge_op: weights count %d != inputs count %d", len(op.weights), len(inputs))
		}
		for index, weight := range op.weights {
			if math.IsNaN(weight) || math.IsInf(weight, 0) || weight < 0 || weight > 1 {
				return nil, merr.WrapErrServiceInternalMsg("merge_op: weight[%d] must be finite and in range [0, 1]", index)
			}
		}
	}

	hasElement := inputs[0].HasColumn(types.ElementIndicesFieldName)
	var idType arrow.DataType
	for inputIdx, df := range inputs {
		if df.HasColumn(types.ElementIndicesFieldName) != hasElement {
			return nil, merr.WrapErrFunctionFailedMsg(
				"merge_op: input[%d] has inconsistent %s presence", inputIdx, types.ElementIndicesFieldName)
		}

		idCol := df.Column(types.IDFieldName)
		if idCol == nil {
			return nil, merr.WrapErrFunctionFailedMsg("merge_op: input[%d] missing %s column", inputIdx, types.IDFieldName)
		}
		if len(idCol.Chunks()) != numChunks {
			return nil, merr.WrapErrFunctionFailedMsg(
				"merge_op: input[%d] column %s has %d chunks, expected %d",
				inputIdx, types.IDFieldName, len(idCol.Chunks()), numChunks)
		}
		if idCol.Len() > 0 {
			if idCol.DataType().ID() != arrow.INT64 && idCol.DataType().ID() != arrow.STRING {
				return nil, merr.WrapErrFunctionFailedMsg(
					"merge_op: input[%d] column %s has unsupported type %s",
					inputIdx, types.IDFieldName, idCol.DataType())
			}
			if idType == nil {
				idType = idCol.DataType()
			} else if idType.ID() != idCol.DataType().ID() {
				return nil, merr.WrapErrFunctionFailedMsg(
					"merge_op: input[%d] column %s type %s does not match %s",
					inputIdx, types.IDFieldName, idCol.DataType(), idType)
			}
		}

		var scoreCol *arrow.Chunked
		if op.strategy != MergeStrategyRRF {
			scoreCol = df.Column(types.ScoreFieldName)
			if scoreCol == nil {
				return nil, merr.WrapErrFunctionFailedMsg("merge_op: input[%d] missing %s column", inputIdx, types.ScoreFieldName)
			}
			if scoreCol.DataType().ID() != arrow.FLOAT32 {
				return nil, merr.WrapErrFunctionFailedMsg(
					"merge_op: input[%d] column %s is not Float32", inputIdx, types.ScoreFieldName)
			}
			if len(scoreCol.Chunks()) != numChunks {
				return nil, merr.WrapErrFunctionFailedMsg(
					"merge_op: input[%d] column %s has %d chunks, expected %d",
					inputIdx, types.ScoreFieldName, len(scoreCol.Chunks()), numChunks)
			}
		}

		var elementCol *arrow.Chunked
		if hasElement {
			elementCol = df.Column(types.ElementIndicesFieldName)
			if elementCol.DataType().ID() != arrow.INT32 {
				return nil, merr.WrapErrFunctionFailedMsg(
					"merge_op: input[%d] column %s is not Int32", inputIdx, types.ElementIndicesFieldName)
			}
			if len(elementCol.Chunks()) != numChunks {
				return nil, merr.WrapErrFunctionFailedMsg(
					"merge_op: input[%d] column %s has %d chunks, expected %d",
					inputIdx, types.ElementIndicesFieldName, len(elementCol.Chunks()), numChunks)
			}
		}

		for chunkIdx, expectedRows := range df.chunkSizes {
			idChunk := idCol.Chunk(chunkIdx)
			if idChunk.Len() != int(expectedRows) {
				return nil, merr.WrapErrFunctionFailedMsg(
					"merge_op: input[%d] chunk[%d] column %s has %d rows, expected %d",
					inputIdx, chunkIdx, types.IDFieldName, idChunk.Len(), expectedRows)
			}
			if idChunk.DataType().ID() != arrow.INT64 && idChunk.DataType().ID() != arrow.STRING {
				return nil, merr.WrapErrFunctionFailedMsg(
					"merge_op: input[%d] chunk[%d] column %s has unsupported type %s",
					inputIdx, chunkIdx, types.IDFieldName, idChunk.DataType())
			}
			for rowIdx := 0; rowIdx < idChunk.Len(); rowIdx++ {
				if idChunk.IsNull(rowIdx) {
					return nil, merr.WrapErrFunctionFailedMsg(
						"merge_op: input[%d] chunk[%d] column %s has null at row %d",
						inputIdx, chunkIdx, types.IDFieldName, rowIdx)
				}
			}

			if scoreCol != nil {
				scoreChunk := scoreCol.Chunk(chunkIdx)
				if scoreChunk.Len() != idChunk.Len() {
					return nil, merr.WrapErrFunctionFailedMsg(
						"merge_op: input[%d] chunk[%d] column %s has %d rows, expected %d",
						inputIdx, chunkIdx, types.ScoreFieldName, scoreChunk.Len(), idChunk.Len())
				}
				for rowIdx := 0; rowIdx < scoreChunk.Len(); rowIdx++ {
					if scoreChunk.IsNull(rowIdx) {
						return nil, merr.WrapErrFunctionFailedMsg(
							"merge_op: input[%d] chunk[%d] column %s has null at row %d",
							inputIdx, chunkIdx, types.ScoreFieldName, rowIdx)
					}
				}
			}

			if elementCol != nil {
				elementChunk := elementCol.Chunk(chunkIdx)
				if elementChunk.Len() != idChunk.Len() {
					return nil, merr.WrapErrFunctionFailedMsg(
						"merge_op: input[%d] chunk[%d] column %s has %d rows, expected %d",
						inputIdx, chunkIdx, types.ElementIndicesFieldName, elementChunk.Len(), idChunk.Len())
				}
				for rowIdx := 0; rowIdx < elementChunk.Len(); rowIdx++ {
					if elementChunk.IsNull(rowIdx) {
						return nil, merr.WrapErrFunctionFailedMsg(
							"merge_op: input[%d] chunk[%d] column %s has null at row %d",
							inputIdx, chunkIdx, types.ElementIndicesFieldName, rowIdx)
					}
				}
			}
		}
	}

	if idType == nil {
		idType = arrow.PrimitiveTypes.Int64
	}
	return &mergeInputLayout{numChunks: numChunks, idType: idType, hasElement: hasElement}, nil
}

// =============================================================================
// Merge Strategies
// =============================================================================

// scoreCollectFunc collects scores for a single chunk, returning per-candidate
// scores and representative source locations.
type scoreCollectFunc func(inputs []*DataFrame, chunkIdx int, layout *mergeInputLayout) (map[candidateKey]float32, map[candidateKey]idLocation, error)

// mergeWithScoreCollector is the common merge skeleton shared by all strategies.
// The only varying part — how scores are collected per chunk — is injected via collectFn.
func (op *MergeOp) mergeWithScoreCollector(ctx *types.FuncContext, inputs []*DataFrame, layout *mergeInputLayout, collectFn scoreCollectFunc) (*DataFrame, error) {
	numChunks := layout.numChunks

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
		candidateScores, candidateLocs, err := collectFn(inputs, chunkIdx, layout)
		if err != nil {
			return nil, err
		}

		scores, locs := sortAndExtractResults(candidateScores, candidateLocs, op.SortDescending())
		newChunkSizes[chunkIdx] = int64(len(scores))

		idArr, err := op.buildFieldArrayOfType(ctx, types.IDFieldName, locs, inputs, chunkIdx, layout.idType)
		if err != nil {
			return nil, err
		}
		scoreArr := buildScoreArray(ctx.Pool(), scores)
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
	for _, input := range inputs {
		if col := input.Column(types.IDFieldName); col != nil && col.DataType().ID() == layout.idType.ID() {
			builder.CopyFieldMetadata(input, types.IDFieldName)
			break
		}
	}

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
func (op *MergeOp) mergeRRF(ctx *types.FuncContext, inputs []*DataFrame, layout *mergeInputLayout) (*DataFrame, error) {
	return op.mergeWithScoreCollector(ctx, inputs, layout, op.collectRRFScores)
}

// collectRRFScores collects RRF scores for a single chunk.
func (op *MergeOp) collectRRFScores(inputs []*DataFrame, chunkIdx int, layout *mergeInputLayout) (map[candidateKey]float32, map[candidateKey]idLocation, error) {
	candidateScores := make(map[candidateKey]float32)
	candidateLocs := make(map[candidateKey]idLocation)

	for inputIdx, df := range inputs {
		idCol := df.Column(types.IDFieldName)
		pathWeight := 1.0
		if op.weightsSet {
			pathWeight = op.weights[inputIdx]
		}

		idChunk := idCol.Chunk(chunkIdx)
		var elementChunk arrow.Array
		if layout.hasElement {
			elementChunk = df.Column(types.ElementIndicesFieldName).Chunk(chunkIdx)
		}
		for rowIdx := 0; rowIdx < idChunk.Len(); rowIdx++ {
			key := readCandidateKey(idChunk, elementChunk, rowIdx)

			// Weighted RRF score: pathWeight / (k + rank), rank is 1-based.
			// pathWeight defaults to 1 to preserve classic RRF scores.
			rrfScore := float32(pathWeight / (op.rrfK + float64(rowIdx+1)))

			if existingScore, exists := candidateScores[key]; exists {
				candidateScores[key] = existingScore + rrfScore
			} else {
				candidateScores[key] = rrfScore
				candidateLocs[key] = idLocation{inputIdx: inputIdx, rowIdx: rowIdx}
			}
		}
	}

	return candidateScores, candidateLocs, nil
}

// mergeWeighted implements weighted score merge.
func (op *MergeOp) mergeWeighted(ctx *types.FuncContext, inputs []*DataFrame, layout *mergeInputLayout) (*DataFrame, error) {
	return op.mergeWithScoreCollector(ctx, inputs, layout, op.collectWeightedScores)
}

// collectWeightedScores collects weighted scores for a single chunk.
func (op *MergeOp) collectWeightedScores(inputs []*DataFrame, chunkIdx int, layout *mergeInputLayout) (map[candidateKey]float32, map[candidateKey]idLocation, error) {
	candidateScores := make(map[candidateKey]float32)
	candidateLocs := make(map[candidateKey]idLocation)

	for inputIdx, df := range inputs {
		idCol := df.Column(types.IDFieldName)
		scoreCol := df.Column(types.ScoreFieldName)
		idChunk := idCol.Chunk(chunkIdx)
		scoreChunk := scoreCol.Chunk(chunkIdx).(*array.Float32)
		var elementChunk arrow.Array
		if layout.hasElement {
			elementChunk = df.Column(types.ElementIndicesFieldName).Chunk(chunkIdx)
		}

		weight := float32(op.weights[inputIdx])
		normFunc := op.scoreNormFunc(inputIdx)

		for rowIdx := 0; rowIdx < idChunk.Len(); rowIdx++ {
			key := readCandidateKey(idChunk, elementChunk, rowIdx)

			score := scoreChunk.Value(rowIdx)
			if normFunc != nil {
				score = normFunc(score)
			}
			weightedScore := weight * score

			if existingScore, exists := candidateScores[key]; exists {
				candidateScores[key] = existingScore + weightedScore
			} else {
				candidateScores[key] = weightedScore
				candidateLocs[key] = idLocation{inputIdx: inputIdx, rowIdx: rowIdx}
			}
		}
	}

	return candidateScores, candidateLocs, nil
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

// mergeNumCombine implements max/sum/avg score merge.
func (op *MergeOp) mergeNumCombine(ctx *types.FuncContext, inputs []*DataFrame, layout *mergeInputLayout, mergeFunc scoreMergeFunc) (*DataFrame, error) {
	return op.mergeWithScoreCollector(ctx, inputs, layout, func(inputs []*DataFrame, chunkIdx int, layout *mergeInputLayout) (map[candidateKey]float32, map[candidateKey]idLocation, error) {
		candidateScores, candidateCounts, candidateLocs, err := op.collectCombinedScores(inputs, chunkIdx, layout, mergeFunc)
		if err != nil {
			return nil, nil, err
		}

		// For avg strategy, compute final average
		if op.strategy == MergeStrategyAvg {
			for key, score := range candidateScores {
				if count, exists := candidateCounts[key]; exists && count > 0 {
					candidateScores[key] = score / float32(count)
				}
			}
		}

		return candidateScores, candidateLocs, nil
	})
}

// collectCombinedScores collects combined scores for max/sum/avg strategies.
func (op *MergeOp) collectCombinedScores(inputs []*DataFrame, chunkIdx int, layout *mergeInputLayout, mergeFunc scoreMergeFunc) (map[candidateKey]float32, map[candidateKey]int, map[candidateKey]idLocation, error) {
	candidateScores := make(map[candidateKey]float32)
	candidateCounts := make(map[candidateKey]int)
	candidateLocs := make(map[candidateKey]idLocation)

	for inputIdx, df := range inputs {
		idCol := df.Column(types.IDFieldName)
		scoreCol := df.Column(types.ScoreFieldName)
		idChunk := idCol.Chunk(chunkIdx)
		scoreChunk := scoreCol.Chunk(chunkIdx).(*array.Float32)
		var elementChunk arrow.Array
		if layout.hasElement {
			elementChunk = df.Column(types.ElementIndicesFieldName).Chunk(chunkIdx)
		}

		normFunc := op.scoreNormFunc(inputIdx)

		for rowIdx := 0; rowIdx < idChunk.Len(); rowIdx++ {
			key := readCandidateKey(idChunk, elementChunk, rowIdx)

			score := scoreChunk.Value(rowIdx)
			if normFunc != nil {
				score = normFunc(score)
			}

			if existingScore, exists := candidateScores[key]; exists {
				newScore, newCount := mergeFunc(existingScore, score, candidateCounts[key])
				candidateScores[key] = newScore
				candidateCounts[key] = newCount
			} else {
				candidateScores[key] = score
				candidateCounts[key] = 1
				candidateLocs[key] = idLocation{inputIdx: inputIdx, rowIdx: rowIdx}
			}
		}
	}

	return candidateScores, candidateCounts, candidateLocs, nil
}

// =============================================================================
// MergeOp Helper Types and Functions
// =============================================================================

// idLocation tracks where an ID was first seen.
type idLocation struct {
	inputIdx int
	rowIdx   int
}

type candidateIDKind uint8

const (
	candidateIDInt64 candidateIDKind = iota
	candidateIDString
)

// candidateKey is the internal deduplication identity. The public primary key
// stays in the source DataFrame and is gathered from the representative row.
type candidateKey struct {
	kind         candidateIDKind
	intID        int64
	stringID     string
	elementIndex int32
	hasElement   bool
}

// readCandidateKey operates on input that has already passed validateInputs.
func readCandidateKey(idChunk arrow.Array, elementChunk arrow.Array, rowIdx int) candidateKey {
	key := candidateKey{hasElement: elementChunk != nil}
	switch ids := idChunk.(type) {
	case *array.Int64:
		key.kind = candidateIDInt64
		key.intID = ids.Value(rowIdx)
	case *array.String:
		key.kind = candidateIDString
		key.stringID = ids.Value(rowIdx)
	}
	if elementChunk != nil {
		key.elementIndex = elementChunk.(*array.Int32).Value(rowIdx)
	}
	return key
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

// sortAndExtractResults sorts candidates by score and extracts results.
// When descending is true, larger scores sort first (higher = better match).
// When descending is false, smaller scores sort first (lower = better match, e.g. L2).
// scoredID carries the score and location alongside the key so that sorting does
// not have to look them up. Sorting a slice of these with slices.SortStableFunc
// also avoids sort.SliceStable's reflect-based swapper.
type scoredID struct {
	key   candidateKey
	score float32
	loc   idLocation
}

func sortAndExtractResults(candidateScores map[candidateKey]float32, candidateLocs map[candidateKey]idLocation, descending bool) ([]float32, []idLocation) {
	entries := make([]scoredID, 0, len(candidateScores))
	for key, score := range candidateScores {
		entries = append(entries, scoredID{key: key, score: score, loc: candidateLocs[key]})
	}

	sortScoredIDs(entries, descending)

	scores := make([]float32, len(entries))
	locs := make([]idLocation, len(entries))
	for i, e := range entries {
		scores[i] = e.score
		locs[i] = e.loc
	}

	return scores, locs
}

// lessScoredID is the ordering sortIDs used to express directly, kept as a
// predicate so the three-way comparator below is equivalent to the previous
// sort.SliceStable call by construction -- including for scores that compare
// unequal in both directions, such as NaN.
func lessScoredID(a, b scoredID, descending bool) bool {
	if a.score != b.score {
		if descending {
			return a.score > b.score
		}
		return a.score < b.score
	}
	return compareCandidateKeys(a.key, b.key) < 0
}

func compareCandidateKeys(a, b candidateKey) int {
	if a.kind != b.kind {
		if a.kind < b.kind {
			return -1
		}
		return 1
	}

	var cmp int
	switch a.kind {
	case candidateIDInt64:
		cmp = compareIDs(a.intID, b.intID)
	case candidateIDString:
		cmp = compareIDs(a.stringID, b.stringID)
	}
	if cmp != 0 {
		return cmp
	}
	if a.hasElement != b.hasElement {
		if !a.hasElement {
			return -1
		}
		return 1
	}
	if a.hasElement {
		if a.elementIndex < b.elementIndex {
			return -1
		}
		if a.elementIndex > b.elementIndex {
			return 1
		}
	}
	return 0
}

// sortScoredIDs sorts by score with stable tie-breaking by ID.
func sortScoredIDs(entries []scoredID, descending bool) {
	slices.SortStableFunc(entries, func(a, b scoredID) int {
		if lessScoredID(a, b, descending) {
			return -1
		}
		if lessScoredID(b, a, descending) {
			return 1
		}
		return 0
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

func buildScoreArray(pool memory.Allocator, scores []float32) arrow.Array {
	builder := array.NewFloat32Builder(pool)
	defer builder.Release()
	builder.AppendValues(scores, nil)
	return builder.NewArray()
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
	// Find the data type from first input that has this column
	var dataType arrow.DataType
	for _, df := range inputs {
		if col := df.Column(colName); col != nil {
			dataType = col.DataType()
			break
		}
	}

	if dataType == nil {
		return nil, merr.WrapErrServiceInternalMsg("merge_op: column %s not found in any input", colName)
	}

	return op.buildFieldArrayOfType(ctx, colName, locs, inputs, chunkIdx, dataType)
}

func (op *MergeOp) buildFieldArrayOfType(ctx *types.FuncContext, colName string, locs []idLocation, inputs []*DataFrame, chunkIdx int, dataType arrow.DataType) (arrow.Array, error) {
	if len(locs) == 0 {
		return buildEmptyArray(ctx.Pool(), dataType)
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
		return nil, merr.WrapErrServiceInternalMsg("unsupported type: %s", dt.Name())
	}
}

// buildArrayFromLocations builds an array from locations.
func buildArrayFromLocations(pool memory.Allocator, colName string, locs []idLocation, inputs []*DataFrame, dt arrow.DataType, chunkIdx int) (arrow.Array, error) {
	switch dt.ID() {
	case arrow.BOOL:
		return buildTypedArrayFromLocations[bool](colName, locs, inputs, array.NewBooleanBuilder(pool), chunkIdx, dt.ID())
	case arrow.INT8:
		return buildTypedArrayFromLocations[int8](colName, locs, inputs, array.NewInt8Builder(pool), chunkIdx, dt.ID())
	case arrow.INT16:
		return buildTypedArrayFromLocations[int16](colName, locs, inputs, array.NewInt16Builder(pool), chunkIdx, dt.ID())
	case arrow.INT32:
		return buildTypedArrayFromLocations[int32](colName, locs, inputs, array.NewInt32Builder(pool), chunkIdx, dt.ID())
	case arrow.INT64:
		return buildTypedArrayFromLocations[int64](colName, locs, inputs, array.NewInt64Builder(pool), chunkIdx, dt.ID())
	case arrow.FLOAT32:
		return buildTypedArrayFromLocations[float32](colName, locs, inputs, array.NewFloat32Builder(pool), chunkIdx, dt.ID())
	case arrow.FLOAT64:
		return buildTypedArrayFromLocations[float64](colName, locs, inputs, array.NewFloat64Builder(pool), chunkIdx, dt.ID())
	case arrow.STRING:
		return buildTypedArrayFromLocations[string](colName, locs, inputs, array.NewStringBuilder(pool), chunkIdx, dt.ID())
	default:
		return nil, merr.WrapErrServiceInternalMsg("unsupported type: %s", dt.Name())
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
func buildTypedArrayFromLocations[T any, B typedArrayBuilder[T]](colName string, locs []idLocation, inputs []*DataFrame, builder B, chunkIdx int, expectedType arrow.Type) (arrow.Array, error) {
	defer builder.Release()

	for _, loc := range locs {
		df := inputs[loc.inputIdx]
		col := df.Column(colName)
		if col == nil {
			builder.AppendNull()
			continue
		}
		if col.DataType().ID() != expectedType {
			return nil, merr.WrapErrFunctionFailedMsg(
				"merge_op: input[%d] column %s type %s does not match output type",
				loc.inputIdx, colName, col.DataType())
		}
		if chunkIdx >= len(col.Chunks()) {
			return nil, merr.WrapErrFunctionFailedMsg(
				"merge_op: input[%d] column %s missing chunk %d", loc.inputIdx, colName, chunkIdx)
		}

		chunk := col.Chunk(chunkIdx)
		if loc.rowIdx >= chunk.Len() {
			return nil, merr.WrapErrFunctionFailedMsg(
				"merge_op: input[%d] chunk[%d] column %s has no row %d",
				loc.inputIdx, chunkIdx, colName, loc.rowIdx)
		}
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
