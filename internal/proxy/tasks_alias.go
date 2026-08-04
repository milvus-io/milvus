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

package proxy

import (
	"github.com/milvus-io/milvus/internal/proxy/dql"
)

// Aliases to the extracted dql package, kept so the composition root can keep
// using the original (unexported) type names. This is the only place in the
// root package allowed to reference the dql package.

type (
	searchTask                  = dql.SearchTask
	queryTask                   = dql.QueryTask
	getStatisticsTask           = dql.GetStatisticsTask
	getCollectionStatisticsTask = dql.GetCollectionStatisticsTask
	getPartitionStatisticsTask  = dql.GetPartitionStatisticsTask
)

// Constants re-exported from the dql package so external consumers of the root
// proxy package (e.g. the REST httpserver) keep compiling. They are true consts
// because dql declares them as consts; a var alias would break callers that use
// them in compile-time constant contexts.
const (
	AnnsFieldKey           = dql.AnnsFieldKey
	AnalyzerKey            = dql.AnalyzerKey
	ExprParamsKey          = dql.ExprParamsKey
	GroupByFieldKey        = dql.GroupByFieldKey
	GroupByFieldsKey       = dql.GroupByFieldsKey
	GroupSizeKey           = dql.GroupSizeKey
	IgnoreGrowingKey       = dql.IgnoreGrowingKey
	IteratorField          = dql.IteratorField
	LimitKey               = dql.LimitKey
	MetricTypeKey          = dql.MetricTypeKey
	NQKey                  = dql.NQKey
	NormScoreKey           = dql.NormScoreKey
	OffsetKey              = dql.OffsetKey
	OrderByFieldsKey       = dql.OrderByFieldsKey
	ParamsKey              = dql.ParamsKey
	PipelineTraceKey       = dql.PipelineTraceKey
	QueryIterLastOffsetKey = dql.QueryIterLastOffsetKey
	QueryIterLastPKKey     = dql.QueryIterLastPKKey
	RRFParamsKey           = dql.RRFParamsKey
	RankTypeKey            = dql.RankTypeKey
	ReduceStopForBestKey   = dql.ReduceStopForBestKey
	RoundDecimalKey        = dql.RoundDecimalKey
	SearchIterBatchSizeKey = dql.SearchIterBatchSizeKey
	SearchIterIdKey        = dql.SearchIterIdKey
	SearchIterLastBoundKey = dql.SearchIterLastBoundKey
	StrictCastKey          = dql.StrictCastKey
	StrictGroupSize        = dql.StrictGroupSize
	TimefieldsKey          = dql.TimefieldsKey
	TopKKey                = dql.TopKKey
	WeightsParamsKey       = dql.WeightsParamsKey
	CollectionID           = dql.CollectionID
)

var (
	NewSearchTask                            = dql.NewSearchTask
	NewQueryTask                             = dql.NewQueryTask
	NewGetStatisticsTask                     = dql.NewGetStatisticsTask
	NewGetCollectionStatisticsTask           = dql.NewGetCollectionStatisticsTask
	NewGetPartitionStatisticsTask            = dql.NewGetPartitionStatisticsTask
	ConvertHybridSearchToSearch              = dql.ConvertHybridSearchToSearch
	PickFieldData                            = dql.PickFieldData
	MatchCountRule                           = dql.MatchCountRule
	WrapPlanCreationError                    = dql.WrapPlanCreationError
	GetPartitionIDs                          = dql.GetPartitionIDs
	MarshalPlanWithMembershipFilterSizeLimit = dql.MarshalPlanWithMembershipFilterSizeLimit
	NormalizeFP32ToFP16BF16VectorFieldData   = dql.NormalizeFP32ToFP16BF16VectorFieldData
)
