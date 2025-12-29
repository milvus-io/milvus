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

package types

// =============================================================================
// Operator Type Constants
// =============================================================================

const (
	OpTypeMap    = "map"
	OpTypeFilter = "filter"
	OpTypeSelect = "select"
	OpTypeSort   = "sort"
	OpTypeLimit  = "limit"
)

// =============================================================================
// Decay Function Constants
// =============================================================================

const (
	// Decay function types
	DecayFuncGauss  = "gauss"
	DecayFuncLinear = "linear"
	DecayFuncExp    = "exp"

	// Decay parameter keys
	DecayParamFunction = "function"
	DecayParamOrigin   = "origin"
	DecayParamScale    = "scale"
	DecayParamOffset   = "offset"
	DecayParamDecay    = "decay"
)

// =============================================================================
// Score Combine Constants
// =============================================================================

const (
	// Score combine parameter keys
	ScoreCombineParamMode       = "mode"
	ScoreCombineParamWeights    = "weights"
	ScoreCombineParamInputCount = "input_count"

	// Score combine mode values
	ScoreCombineModeMultiply = "multiply"
	ScoreCombineModeSum      = "sum"
	ScoreCombineModeMax      = "max"
	ScoreCombineModeMin      = "min"
	ScoreCombineModeAvg      = "avg"
	ScoreCombineModeWeighted = "weighted"
)

// =============================================================================
// Special Field Names
// =============================================================================

const (
	IDFieldName    = "$id"
	ScoreFieldName = "$score"
)

// =============================================================================
// All Stages (for functions that support all stages)
// =============================================================================

// AllStages contains all supported execution stages.
// Use this when a function supports running in any stage.
var AllStages = []string{
	StageIngestion,
	StageL2Rerank,
	StageL1Rerank,
	StageL0Rerank,
	StagePreProcess,
	StagePostProcess,
}
