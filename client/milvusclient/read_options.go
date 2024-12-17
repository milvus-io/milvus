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

package milvusclient

import (
	"encoding/json"
	"strconv"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
)

const (
	spAnnsField     = `anns_field`
	spTopK          = `topk`
	spOffset        = `offset`
	spLimit         = `limit`
	spParams        = `params`
	spMetricsType   = `metric_type`
	spRoundDecimal  = `round_decimal`
	spIgnoreGrowing = `ignore_growing`
	spGroupBy       = `group_by_field`
)

type SearchOption interface {
	Request() (*milvuspb.SearchRequest, error)
}

var _ SearchOption = (*searchOption)(nil)

type searchOption struct {
	annRequest                 *annRequest
	collectionName             string
	partitionNames             []string
	outputFields               []string
	consistencyLevel           entity.ConsistencyLevel
	useDefaultConsistencyLevel bool
}

type annRequest struct {
	vectors []entity.Vector

	annField      string
	metricsType   entity.MetricType
	searchParam   map[string]string
	groupByField  string
	annParam      index.AnnParam
	ignoreGrowing bool
	expr          string
	topK          int
	offset        int
}

func NewAnnRequest(annField string, limit int, vectors ...entity.Vector) *annRequest {
	return &annRequest{
		annField: annField,
		vectors:  vectors,
		topK:     limit,
	}
}

func (r *annRequest) searchRequest() (*milvuspb.SearchRequest, error) {
	request := &milvuspb.SearchRequest{
		Nq:      int64(len(r.vectors)),
		Dsl:     r.expr,
		DslType: commonpb.DslType_BoolExprV1,
	}

	var err error
	// placeholder group
	request.PlaceholderGroup, err = vector2PlaceholderGroupBytes(r.vectors)
	if err != nil {
		return nil, err
	}

	params := map[string]string{
		spAnnsField:     r.annField,
		spTopK:          strconv.Itoa(r.topK),
		spOffset:        strconv.Itoa(r.offset),
		spMetricsType:   string(r.metricsType),
		spRoundDecimal:  "-1",
		spIgnoreGrowing: strconv.FormatBool(r.ignoreGrowing),
	}
	if r.groupByField != "" {
		params[spGroupBy] = r.groupByField
	}
	// ann param
	if r.annParam != nil {
		bs, _ := json.Marshal(r.annParam.Params())
		params[spParams] = string(bs)
	} else {
		params[spParams] = "{}"
	}
	// use custom search param to overwrite
	for k, v := range r.searchParam {
		params[k] = v
	}
	request.SearchParams = entity.MapKvPairs(params)

	return request, nil
}

func (r *annRequest) WithANNSField(annsField string) *annRequest {
	r.annField = annsField
	return r
}

func (r *annRequest) WithGroupByField(groupByField string) *annRequest {
	r.groupByField = groupByField
	return r
}

func (r *annRequest) WithSearchParam(key, value string) *annRequest {
	r.searchParam[key] = value
	return r
}

func (r *annRequest) WithAnnParam(ap index.AnnParam) *annRequest {
	r.annParam = ap
	return r
}

func (r *annRequest) WithFilter(expr string) *annRequest {
	r.expr = expr
	return r
}

func (r *annRequest) WithOffset(offset int) *annRequest {
	r.offset = offset
	return r
}

func (r *annRequest) WithIgnoreGrowing(ignoreGrowing bool) *annRequest {
	r.ignoreGrowing = ignoreGrowing
	return r
}

func (opt *searchOption) Request() (*milvuspb.SearchRequest, error) {
	request, err := opt.annRequest.searchRequest()
	if err != nil {
		return nil, err
	}

	request.CollectionName = opt.collectionName
	request.PartitionNames = opt.partitionNames
	request.ConsistencyLevel = commonpb.ConsistencyLevel(opt.consistencyLevel)
	request.UseDefaultConsistency = opt.useDefaultConsistencyLevel
	request.OutputFields = opt.outputFields

	return request, nil
}

func (opt *searchOption) WithPartitions(partitionNames ...string) *searchOption {
	opt.partitionNames = partitionNames
	return opt
}

func (opt *searchOption) WithFilter(expr string) *searchOption {
	opt.annRequest.WithFilter(expr)
	return opt
}

func (opt *searchOption) WithOffset(offset int) *searchOption {
	opt.annRequest.WithOffset(offset)
	return opt
}

func (opt *searchOption) WithOutputFields(fieldNames ...string) *searchOption {
	opt.outputFields = fieldNames
	return opt
}

func (opt *searchOption) WithConsistencyLevel(consistencyLevel entity.ConsistencyLevel) *searchOption {
	opt.consistencyLevel = consistencyLevel
	opt.useDefaultConsistencyLevel = false
	return opt
}

func (opt *searchOption) WithANNSField(annsField string) *searchOption {
	opt.annRequest.WithANNSField(annsField)
	return opt
}

func (opt *searchOption) WithGroupByField(groupByField string) *searchOption {
	opt.annRequest.WithGroupByField(groupByField)
	return opt
}

func (opt *searchOption) WithIgnoreGrowing(ignoreGrowing bool) *searchOption {
	opt.annRequest.WithIgnoreGrowing(ignoreGrowing)
	return opt
}

func (opt *searchOption) WithAnnParam(ap index.AnnParam) *searchOption {
	opt.annRequest.WithAnnParam(ap)
	return opt
}

func (opt *searchOption) WithSearchParam(key, value string) *searchOption {
	opt.annRequest.WithSearchParam(key, value)
	return opt
}

func NewSearchOption(collectionName string, limit int, vectors []entity.Vector) *searchOption {
	return &searchOption{
		annRequest: &annRequest{
			vectors:     vectors,
			searchParam: make(map[string]string),
			topK:        limit,
		},
		collectionName:             collectionName,
		useDefaultConsistencyLevel: true,
		consistencyLevel:           entity.ClBounded,
	}
}

func vector2PlaceholderGroupBytes(vectors []entity.Vector) ([]byte, error) {
	phv, err := vector2Placeholder(vectors)
	if err != nil {
		return nil, err
	}
	phg := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			phv,
		},
	}

	bs, err := proto.Marshal(phg)
	return bs, err
}

func vector2Placeholder(vectors []entity.Vector) (*commonpb.PlaceholderValue, error) {
	var placeHolderType commonpb.PlaceholderType
	ph := &commonpb.PlaceholderValue{
		Tag:    "$0",
		Values: make([][]byte, 0, len(vectors)),
	}
	if len(vectors) == 0 {
		return ph, nil
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
	case entity.Text:
		placeHolderType = commonpb.PlaceholderType_VarChar
	default:
		return nil, errors.Newf("unsupported search data type: %T", vectors[0])
	}
	ph.Type = placeHolderType
	for _, vector := range vectors {
		ph.Values = append(ph.Values, vector.Serialize())
	}
	return ph, nil
}

type HybridSearchOption interface {
	HybridRequest() (*milvuspb.HybridSearchRequest, error)
}

type hybridSearchOption struct {
	collectionName string
	partitionNames []string

	reqs []*annRequest

	outputFields          []string
	useDefaultConsistency bool
	consistencyLevel      entity.ConsistencyLevel
}

func (opt *hybridSearchOption) WithConsistencyLevel(cl entity.ConsistencyLevel) *hybridSearchOption {
	opt.consistencyLevel = cl
	opt.useDefaultConsistency = false
	return opt
}

func (opt *hybridSearchOption) WithPartitons(partitions ...string) *hybridSearchOption {
	opt.partitionNames = partitions
	return opt
}

func (opt *hybridSearchOption) WithOutputFields(outputFields ...string) *hybridSearchOption {
	opt.outputFields = outputFields
	return opt
}

func (opt *hybridSearchOption) HybridRequest() (*milvuspb.HybridSearchRequest, error) {
	requests := make([]*milvuspb.SearchRequest, 0, len(opt.reqs))
	for _, annRequest := range opt.reqs {
		req, err := annRequest.searchRequest()
		if err != nil {
			return nil, err
		}
		requests = append(requests, req)
	}

	return &milvuspb.HybridSearchRequest{
		CollectionName:        opt.collectionName,
		PartitionNames:        opt.partitionNames,
		Requests:              requests,
		UseDefaultConsistency: opt.useDefaultConsistency,
		ConsistencyLevel:      commonpb.ConsistencyLevel(opt.consistencyLevel),
		OutputFields:          opt.outputFields,
	}, nil
}

func NewHybridSearchOption(collectionName string, annRequests ...*annRequest) *hybridSearchOption {
	return &hybridSearchOption{
		collectionName: collectionName,

		reqs:                  annRequests,
		useDefaultConsistency: true,
	}
}

type QueryOption interface {
	Request() *milvuspb.QueryRequest
}

type queryOption struct {
	collectionName             string
	partitionNames             []string
	queryParams                map[string]string
	outputFields               []string
	consistencyLevel           entity.ConsistencyLevel
	useDefaultConsistencyLevel bool
	expr                       string
}

func (opt *queryOption) Request() *milvuspb.QueryRequest {
	return &milvuspb.QueryRequest{
		CollectionName: opt.collectionName,
		PartitionNames: opt.partitionNames,
		OutputFields:   opt.outputFields,

		Expr:             opt.expr,
		QueryParams:      entity.MapKvPairs(opt.queryParams),
		ConsistencyLevel: opt.consistencyLevel.CommonConsistencyLevel(),
	}
}

func (opt *queryOption) WithFilter(expr string) *queryOption {
	opt.expr = expr
	return opt
}

func (opt *queryOption) WithOffset(offset int) *queryOption {
	if opt.queryParams == nil {
		opt.queryParams = make(map[string]string)
	}
	opt.queryParams[spOffset] = strconv.Itoa(offset)
	return opt
}

func (opt *queryOption) WithLimit(limit int) *queryOption {
	if opt.queryParams == nil {
		opt.queryParams = make(map[string]string)
	}
	opt.queryParams[spLimit] = strconv.Itoa(limit)
	return opt
}

func (opt *queryOption) WithOutputFields(fieldNames ...string) *queryOption {
	opt.outputFields = fieldNames
	return opt
}

func (opt *queryOption) WithConsistencyLevel(consistencyLevel entity.ConsistencyLevel) *queryOption {
	opt.consistencyLevel = consistencyLevel
	opt.useDefaultConsistencyLevel = false
	return opt
}

func (opt *queryOption) WithPartitions(partitionNames ...string) *queryOption {
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
