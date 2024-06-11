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

package client

import (
	"encoding/json"
	"strconv"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

const (
	spAnnsField     = `anns_field`
	spTopK          = `topk`
	spOffset        = `offset`
	spParams        = `params`
	spMetricsType   = `metric_type`
	spRoundDecimal  = `round_decimal`
	spIgnoreGrowing = `ignore_growing`
	spGroupBy       = `group_by_field`
)

type SearchOption interface {
	Request() *milvuspb.SearchRequest
}

var _ SearchOption = (*searchOption)(nil)

type searchOption struct {
	collectionName             string
	partitionNames             []string
	topK                       int
	offset                     int
	outputFields               []string
	consistencyLevel           entity.ConsistencyLevel
	useDefaultConsistencyLevel bool
	ignoreGrowing              bool
	expr                       string

	// normal search request
	request *annRequest
	// TODO add sub request when support hybrid search
}

type annRequest struct {
	vectors []entity.Vector

	annField     string
	metricsType  entity.MetricType
	searchParam  map[string]string
	groupByField string
}

func (opt *searchOption) Request() *milvuspb.SearchRequest {
	// TODO check whether search is hybrid after logic merged
	return opt.prepareSearchRequest(opt.request)
}

func (opt *searchOption) prepareSearchRequest(annRequest *annRequest) *milvuspb.SearchRequest {
	request := &milvuspb.SearchRequest{
		CollectionName:   opt.collectionName,
		PartitionNames:   opt.partitionNames,
		Dsl:              opt.expr,
		DslType:          commonpb.DslType_BoolExprV1,
		ConsistencyLevel: commonpb.ConsistencyLevel(opt.consistencyLevel),
		OutputFields:     opt.outputFields,
	}
	if annRequest != nil {
		// nq
		request.Nq = int64(len(annRequest.vectors))

		// search param
		bs, _ := json.Marshal(annRequest.searchParam)
		params := map[string]string{
			spAnnsField:     annRequest.annField,
			spTopK:          strconv.Itoa(opt.topK),
			spOffset:        strconv.Itoa(opt.offset),
			spParams:        string(bs),
			spMetricsType:   string(annRequest.metricsType),
			spRoundDecimal:  "-1",
			spIgnoreGrowing: strconv.FormatBool(opt.ignoreGrowing),
		}
		if annRequest.groupByField != "" {
			params[spGroupBy] = annRequest.groupByField
		}
		request.SearchParams = entity.MapKvPairs(params)

		// placeholder group
		request.PlaceholderGroup = vector2PlaceholderGroupBytes(annRequest.vectors)
	}

	return request
}

func (opt *searchOption) WithFilter(expr string) *searchOption {
	opt.expr = expr
	return opt
}

func (opt *searchOption) WithOffset(offset int) *searchOption {
	opt.offset = offset
	return opt
}

func (opt *searchOption) WithOutputFields(fieldNames []string) *searchOption {
	opt.outputFields = fieldNames
	return opt
}

func (opt *searchOption) WithConsistencyLevel(consistencyLevel entity.ConsistencyLevel) *searchOption {
	opt.consistencyLevel = consistencyLevel
	opt.useDefaultConsistencyLevel = false
	return opt
}

func (opt *searchOption) WithANNSField(annsField string) *searchOption {
	opt.request.annField = annsField
	return opt
}

func (opt *searchOption) WithPartitions(partitionNames []string) *searchOption {
	opt.partitionNames = partitionNames
	return opt
}

func NewSearchOption(collectionName string, limit int, vectors []entity.Vector) *searchOption {
	return &searchOption{
		collectionName: collectionName,
		topK:           limit,
		request: &annRequest{
			vectors: vectors,
		},
		useDefaultConsistencyLevel: true,
		consistencyLevel:           entity.ClBounded,
	}
}

func vector2PlaceholderGroupBytes(vectors []entity.Vector) []byte {
	phg := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			vector2Placeholder(vectors),
		},
	}

	bs, _ := proto.Marshal(phg)
	return bs
}

func vector2Placeholder(vectors []entity.Vector) *commonpb.PlaceholderValue {
	var placeHolderType commonpb.PlaceholderType
	ph := &commonpb.PlaceholderValue{
		Tag:    "$0",
		Values: make([][]byte, 0, len(vectors)),
	}
	if len(vectors) == 0 {
		return ph
	}
	switch vectors[0].(type) {
	case entity.FloatVector:
		placeHolderType = commonpb.PlaceholderType_FloatVector
	case entity.BinaryVector:
		placeHolderType = commonpb.PlaceholderType_BinaryVector
	case entity.BFloat16Vector:
		placeHolderType = commonpb.PlaceholderType_BFloat16Vector
	case entity.Float16Vector:
		placeHolderType = commonpb.PlaceholderType_Float16Vector
	case entity.SparseEmbedding:
		placeHolderType = commonpb.PlaceholderType_SparseFloatVector
	}
	ph.Type = placeHolderType
	for _, vector := range vectors {
		ph.Values = append(ph.Values, vector.Serialize())
	}
	return ph
}

type QueryOption interface {
	Request() *milvuspb.QueryRequest
}

type queryOption struct {
	collectionName string
	partitionNames []string

	limit                      int
	offset                     int
	outputFields               []string
	consistencyLevel           entity.ConsistencyLevel
	useDefaultConsistencyLevel bool
	ignoreGrowing              bool
	expr                       string
}

func (opt *queryOption) Request() *milvuspb.QueryRequest {
	return &milvuspb.QueryRequest{
		CollectionName: opt.collectionName,
		PartitionNames: opt.partitionNames,
		OutputFields:   opt.outputFields,

		Expr:             opt.expr,
		ConsistencyLevel: opt.consistencyLevel.CommonConsistencyLevel(),
	}
}

func (opt *queryOption) WithFilter(expr string) *queryOption {
	opt.expr = expr
	return opt
}

func (opt *queryOption) WithOffset(offset int) *queryOption {
	opt.offset = offset
	return opt
}

func (opt *queryOption) WithOutputFields(fieldNames []string) *queryOption {
	opt.outputFields = fieldNames
	return opt
}

func (opt *queryOption) WithConsistencyLevel(consistencyLevel entity.ConsistencyLevel) *queryOption {
	opt.consistencyLevel = consistencyLevel
	opt.useDefaultConsistencyLevel = false
	return opt
}

func (opt *queryOption) WithPartitions(partitionNames []string) *queryOption {
	opt.partitionNames = partitionNames
	return opt
}

func NewQueryOption(collectionName string) *queryOption {
	return &queryOption{
		collectionName:             collectionName,
		useDefaultConsistencyLevel: true,
		consistencyLevel:           entity.ClBounded,
	}
}
