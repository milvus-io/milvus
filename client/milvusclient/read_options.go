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
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
)

const (
	spAnnsField       = `anns_field`
	spTopK            = `topk`
	spOffset          = `offset`
	spLimit           = `limit`
	spParams          = `params`
	spMetricsType     = `metric_type`
	spRoundDecimal    = `round_decimal`
	spIgnoreGrowing   = `ignore_growing`
	spGroupBy         = `group_by_field`
	spGroupSize       = `group_size`
	spStrictGroupSize = `strict_group_size`
)

type SearchOption interface {
	Request() (*milvuspb.SearchRequest, error)
}

var _ SearchOption = (*searchOption)(nil)

type searchOption struct {
	annRequest                 *AnnRequest
	collectionName             string
	partitionNames             []string
	outputFields               []string
	consistencyLevel           entity.ConsistencyLevel
	useDefaultConsistencyLevel bool
}

type AnnRequest struct {
	vectors []entity.Vector

	annField        string
	metricsType     entity.MetricType
	searchParam     map[string]string
	groupByField    string
	groupSize       int
	strictGroupSize bool
	annParam        index.AnnParam
	ignoreGrowing   bool
	expr            string
	topK            int
	offset          int
	templateParams  map[string]any

	functionRerankers []*entity.Function
}

func NewAnnRequest(annField string, limit int, vectors ...entity.Vector) *AnnRequest {
	return &AnnRequest{
		annField:       annField,
		vectors:        vectors,
		topK:           limit,
		searchParam:    make(map[string]string),
		templateParams: make(map[string]any),
	}
}

func (r *AnnRequest) searchRequest() (*milvuspb.SearchRequest, error) {
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
	if r.groupSize != 0 {
		params[spGroupSize] = strconv.Itoa(r.groupSize)
	}
	if r.strictGroupSize {
		params[spStrictGroupSize] = "true"
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

	request.ExprTemplateValues = make(map[string]*schemapb.TemplateValue)
	for key, value := range r.templateParams {
		tmplVal, err := any2TmplValue(value)
		if err != nil {
			return nil, err
		}
		request.ExprTemplateValues[key] = tmplVal
	}

	if len(r.functionRerankers) > 0 {
		request.FunctionScore = &schemapb.FunctionScore{}
		for _, fr := range r.functionRerankers {
			request.FunctionScore.Functions = append(request.FunctionScore.Functions, fr.ProtoMessage())
		}
	}

	return request, nil
}

func any2TmplValue(val any) (*schemapb.TemplateValue, error) {
	result := &schemapb.TemplateValue{}
	switch v := val.(type) {
	case int, int8, int16, int32:
		result.Val = &schemapb.TemplateValue_Int64Val{Int64Val: reflect.ValueOf(v).Int()}
	case int64:
		result.Val = &schemapb.TemplateValue_Int64Val{Int64Val: v}
	case float32:
		result.Val = &schemapb.TemplateValue_FloatVal{FloatVal: float64(v)}
	case float64:
		result.Val = &schemapb.TemplateValue_FloatVal{FloatVal: v}
	case bool:
		result.Val = &schemapb.TemplateValue_BoolVal{BoolVal: v}
	case string:
		result.Val = &schemapb.TemplateValue_StringVal{StringVal: v}
	default:
		if reflect.TypeOf(val).Kind() == reflect.Slice {
			return slice2TmplValue(val)
		}
		return nil, fmt.Errorf("unsupported template value type: %T", val)
	}
	return result, nil
}

func slice2TmplValue(val any) (*schemapb.TemplateValue, error) {
	arrVal := &schemapb.TemplateValue_ArrayVal{
		ArrayVal: &schemapb.TemplateArrayValue{},
	}

	rv := reflect.ValueOf(val)
	switch t := reflect.TypeOf(val).Elem().Kind(); t {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		data := make([]int64, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			data = append(data, rv.Index(i).Int())
		}
		arrVal.ArrayVal.Data = &schemapb.TemplateArrayValue_LongData{
			LongData: &schemapb.LongArray{
				Data: data,
			},
		}
	case reflect.Bool:
		data := make([]bool, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			data = append(data, rv.Index(i).Bool())
		}
		arrVal.ArrayVal.Data = &schemapb.TemplateArrayValue_BoolData{
			BoolData: &schemapb.BoolArray{
				Data: data,
			},
		}
	case reflect.Float32, reflect.Float64:
		data := make([]float64, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			data = append(data, rv.Index(i).Float())
		}
		arrVal.ArrayVal.Data = &schemapb.TemplateArrayValue_DoubleData{
			DoubleData: &schemapb.DoubleArray{
				Data: data,
			},
		}
	case reflect.String:
		data := make([]string, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			data = append(data, rv.Index(i).String())
		}
		arrVal.ArrayVal.Data = &schemapb.TemplateArrayValue_StringData{
			StringData: &schemapb.StringArray{
				Data: data,
			},
		}
	default:
		return nil, fmt.Errorf("unsupported template type: slice of %v", t)
	}

	return &schemapb.TemplateValue{
		Val: arrVal,
	}, nil
}

func (r *AnnRequest) WithANNSField(annsField string) *AnnRequest {
	r.annField = annsField
	return r
}

func (r *AnnRequest) WithGroupByField(groupByField string) *AnnRequest {
	r.groupByField = groupByField
	return r
}

func (r *AnnRequest) WithGroupSize(groupSize int) *AnnRequest {
	r.groupSize = groupSize
	return r
}

func (r *AnnRequest) WithStrictGroupSize(strictGroupSize bool) *AnnRequest {
	r.strictGroupSize = strictGroupSize
	return r
}

func (r *AnnRequest) WithSearchParam(key, value string) *AnnRequest {
	r.searchParam[key] = value
	return r
}

func (r *AnnRequest) WithAnnParam(ap index.AnnParam) *AnnRequest {
	r.annParam = ap
	return r
}

func (r *AnnRequest) WithFilter(expr string) *AnnRequest {
	r.expr = expr
	return r
}

func (r *AnnRequest) WithTemplateParam(key string, val any) *AnnRequest {
	r.templateParams[key] = val
	return r
}

func (r *AnnRequest) WithOffset(offset int) *AnnRequest {
	r.offset = offset
	return r
}

func (r *AnnRequest) WithIgnoreGrowing(ignoreGrowing bool) *AnnRequest {
	r.ignoreGrowing = ignoreGrowing
	return r
}

func (r *AnnRequest) WithFunctionReranker(fr *entity.Function) *AnnRequest {
	r.functionRerankers = append(r.functionRerankers, fr)
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

func (opt *searchOption) WithTemplateParam(key string, val any) *searchOption {
	opt.annRequest.WithTemplateParam(key, val)
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

func (opt *searchOption) WithGroupSize(groupSize int) *searchOption {
	opt.annRequest.WithGroupSize(groupSize)
	return opt
}

func (opt *searchOption) WithStrictGroupSize(strictGroupSize bool) *searchOption {
	opt.annRequest.WithStrictGroupSize(strictGroupSize)
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

func (opt *searchOption) WithFunctionReranker(fr *entity.Function) *searchOption {
	opt.annRequest.WithFunctionReranker(fr)
	return opt
}

func NewSearchOption(collectionName string, limit int, vectors []entity.Vector) *searchOption {
	return &searchOption{
		annRequest:                 NewAnnRequest("", limit, vectors...),
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
	case entity.Int8Vector:
		placeHolderType = commonpb.PlaceholderType_Int8Vector
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

	reqs []*AnnRequest

	outputFields          []string
	useDefaultConsistency bool
	consistencyLevel      entity.ConsistencyLevel

	limit             int
	offset            int
	reranker          Reranker
	functionRerankers []*entity.Function
}

func (opt *hybridSearchOption) WithConsistencyLevel(cl entity.ConsistencyLevel) *hybridSearchOption {
	opt.consistencyLevel = cl
	opt.useDefaultConsistency = false
	return opt
}

// Deprecated: typo, use WithPartitions instead
func (opt *hybridSearchOption) WithPartitons(partitions ...string) *hybridSearchOption {
	return opt.WithPartitions(partitions...)
}

func (opt *hybridSearchOption) WithPartitions(partitions ...string) *hybridSearchOption {
	opt.partitionNames = partitions
	return opt
}

func (opt *hybridSearchOption) WithOutputFields(outputFields ...string) *hybridSearchOption {
	opt.outputFields = outputFields
	return opt
}

func (opt *hybridSearchOption) WithReranker(reranker Reranker) *hybridSearchOption {
	opt.reranker = reranker
	return opt
}

func (opt *hybridSearchOption) WithFunctionRerankers(functionReranker *entity.Function) *hybridSearchOption {
	opt.functionRerankers = append(opt.functionRerankers, functionReranker)
	return opt
}

func (opt *hybridSearchOption) WithOffset(offset int) *hybridSearchOption {
	opt.offset = offset
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

	var params []*commonpb.KeyValuePair
	if opt.reranker != nil {
		params = opt.reranker.GetParams()
	}
	params = append(params, &commonpb.KeyValuePair{Key: spLimit, Value: strconv.FormatInt(int64(opt.limit), 10)})
	if opt.offset > 0 {
		params = append(params, &commonpb.KeyValuePair{Key: spOffset, Value: strconv.FormatInt(int64(opt.offset), 10)})
	}

	r := &milvuspb.HybridSearchRequest{
		CollectionName:        opt.collectionName,
		PartitionNames:        opt.partitionNames,
		Requests:              requests,
		UseDefaultConsistency: opt.useDefaultConsistency,
		ConsistencyLevel:      commonpb.ConsistencyLevel(opt.consistencyLevel),
		OutputFields:          opt.outputFields,
		RankParams:            params,
	}

	if len(opt.functionRerankers) > 0 {
		r.FunctionScore = &schemapb.FunctionScore{}
		for _, fr := range opt.functionRerankers {
			r.FunctionScore.Functions = append(r.FunctionScore.Functions, fr.ProtoMessage())
		}
	}

	return r, nil
}

func NewHybridSearchOption(collectionName string, limit int, annRequests ...*AnnRequest) *hybridSearchOption {
	return &hybridSearchOption{
		collectionName:        collectionName,
		reqs:                  annRequests,
		useDefaultConsistency: true,
		limit:                 limit,
	}
}

type QueryOption interface {
	Request() (*milvuspb.QueryRequest, error)
}

type queryOption struct {
	collectionName             string
	partitionNames             []string
	queryParams                map[string]string
	outputFields               []string
	consistencyLevel           entity.ConsistencyLevel
	useDefaultConsistencyLevel bool
	expr                       string
	templateParams             map[string]any
}

func (opt *queryOption) Request() (*milvuspb.QueryRequest, error) {
	req := &milvuspb.QueryRequest{
		CollectionName: opt.collectionName,
		PartitionNames: opt.partitionNames,
		OutputFields:   opt.outputFields,

		Expr:                  opt.expr,
		QueryParams:           entity.MapKvPairs(opt.queryParams),
		ConsistencyLevel:      opt.consistencyLevel.CommonConsistencyLevel(),
		UseDefaultConsistency: opt.useDefaultConsistencyLevel,
	}

	req.ExprTemplateValues = make(map[string]*schemapb.TemplateValue)
	for key, value := range opt.templateParams {
		tmplVal, err := any2TmplValue(value)
		if err != nil {
			return nil, err
		}
		req.ExprTemplateValues[key] = tmplVal
	}

	return req, nil
}

func (opt *queryOption) WithFilter(expr string) *queryOption {
	opt.expr = expr
	return opt
}

func (opt *queryOption) WithTemplateParam(key string, val any) *queryOption {
	opt.templateParams[key] = val
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

func (opt *queryOption) WithIDs(ids column.Column) *queryOption {
	opt.expr = pks2Expr(ids)
	return opt
}

func pks2Expr(ids column.Column) string {
	var expr string
	pkName := ids.Name()
	switch ids.Type() {
	case entity.FieldTypeInt64:
		expr = fmt.Sprintf("%s in %s", pkName, strings.Join(strings.Fields(fmt.Sprint(ids.FieldData().GetScalars().GetLongData().GetData())), ","))
	case entity.FieldTypeVarChar:
		data := ids.FieldData().GetScalars().GetData().(*schemapb.ScalarField_StringData).StringData.GetData()
		for i := range data {
			data[i] = fmt.Sprintf("\"%s\"", data[i])
		}
		expr = fmt.Sprintf("%s in [%s]", pkName, strings.Join(data, ","))
	}
	return expr
}

func NewQueryOption(collectionName string) *queryOption {
	return &queryOption{
		collectionName:             collectionName,
		useDefaultConsistencyLevel: true,
		consistencyLevel:           entity.ClBounded,
		templateParams:             make(map[string]any),
	}
}

type RunAnalyzerOption interface {
	Request() (*milvuspb.RunAnalyzerRequest, error)
}

type runAnalyzerOption struct {
	text           []string
	collectionName string
	fieldName      string
	analyzerNames  []string
	analyzerParams string
	withDetail     bool
	withHash       bool
	err            error
}

func (opt *runAnalyzerOption) Request() (*milvuspb.RunAnalyzerRequest, error) {
	if opt.err != nil {
		return nil, opt.err
	}
	return &milvuspb.RunAnalyzerRequest{
		Placeholder:    lo.Map(opt.text, func(str string, _ int) []byte { return []byte(str) }),
		AnalyzerParams: opt.analyzerParams,
		CollectionName: opt.collectionName,
		FieldName:      opt.fieldName,
		AnalyzerNames:  opt.analyzerNames,
		WithDetail:     opt.withDetail,
		WithHash:       opt.withHash,
	}, nil
}

func (opt *runAnalyzerOption) WithAnalyzerParamsStr(params string) *runAnalyzerOption {
	opt.analyzerParams = params
	return opt
}

func (opt *runAnalyzerOption) WithAnalyzerParams(params map[string]any) *runAnalyzerOption {
	s, err := json.Marshal(params)
	if err != nil {
		opt.err = err
	}
	opt.analyzerParams = string(s)
	return opt
}

func (opt *runAnalyzerOption) WithDetail() *runAnalyzerOption {
	opt.withDetail = true
	return opt
}

func (opt *runAnalyzerOption) WithHash() *runAnalyzerOption {
	opt.withHash = true
	return opt
}

func (opt *runAnalyzerOption) WithField(collectionName, fieldName string) *runAnalyzerOption {
	opt.collectionName = collectionName
	opt.fieldName = fieldName
	return opt
}

func (opt *runAnalyzerOption) WithAnalyzerName(names ...string) *runAnalyzerOption {
	opt.analyzerNames = names
	return opt
}

func NewRunAnalyzerOption(text ...string) *runAnalyzerOption {
	return &runAnalyzerOption{
		text: text,
	}
}
