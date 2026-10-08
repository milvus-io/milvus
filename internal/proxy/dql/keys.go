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

package dql

import (
	"math"

	"github.com/milvus-io/milvus/pkg/v3/common"
)

const (
	SumScorer string = "sum"
	MaxScorer string = "max"
	AvgScorer string = "avg"

	IgnoreGrowingKey     = "ignore_growing"
	ReduceStopForBestKey = "reduce_stop_for_best"
	IteratorField        = "iterator"
	CollectionID         = "collection_id"
	GroupByFieldKey      = "group_by_field"
	GroupSizeKey         = "group_size"
	StrictGroupSize      = "strict_group_size"
	JSONPath             = "json_path"
	JSONType             = "json_type"
	StrictCastKey        = "strict_cast"
	RankGroupScorer      = "rank_group_scorer"
	AnnsFieldKey         = "anns_field"
	AnalyzerKey          = "analyzer_name"
	TopKKey              = "topk"
	NQKey                = "nq"
	MetricTypeKey        = common.MetricTypeKey
	ParamsKey            = common.ParamsKey
	ExprParamsKey        = "expr_params"
	RoundDecimalKey      = "round_decimal"
	OffsetKey            = "offset"
	LimitKey             = "limit"
	// key for timestamptz translation
	TimefieldsKey = "time_fields"

	SearchIterV2Key        = "search_iter_v2"
	SearchIterBatchSizeKey = "search_iter_batch_size"
	SearchIterLastBoundKey = "search_iter_last_bound"
	SearchIterIdKey        = "search_iter_id"
	QueryIterLastPKKey     = "query_iter_last_pk"
	QueryIterLastOffsetKey = "query_iter_last_element_offset"
	GroupByFieldsKey       = "group_by_fields"
	OrderByFieldsKey       = "order_by_fields"
	PipelineTraceKey       = "pipeline_trace"

	// minFloat32 minimum float.
	minFloat32 = -1 * float32(math.MaxFloat32)

	RankTypeKey      = "strategy"
	RRFParamsKey     = "k"
	WeightsParamsKey = "weights"
	NormScoreKey     = "norm_score"
)
