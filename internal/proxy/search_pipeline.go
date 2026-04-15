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
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/search_agg"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type opMsg map[string]any

type operator interface {
	run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error)
}

type nodeDef struct {
	name    string
	inputs  []string
	outputs []string
	params  map[string]any
	opName  string
}

type Node struct {
	name    string
	opName  string
	inputs  []string
	outputs []string

	op operator
}

func (n *Node) unpackInputs(msg opMsg) ([]any, error) {
	for _, input := range n.inputs {
		if _, ok := msg[input]; !ok {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("Node [%s]'s input %s not found", n.name, input))
		}
	}
	inputs := make([]any, len(n.inputs))
	for i, input := range n.inputs {
		inputs[i] = msg[input]
	}
	return inputs, nil
}

func (n *Node) packOutputs(outputs []any, srcMsg opMsg) (opMsg, error) {
	msg := srcMsg
	if len(outputs) != len(n.outputs) {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("Node [%s] output size not match operator output size", n.name))
	}
	for i, output := range n.outputs {
		msg[output] = outputs[i]
	}
	return msg, nil
}

func (n *Node) Run(ctx context.Context, span trace.Span, msg opMsg) (opMsg, error) {
	inputs, err := n.unpackInputs(msg)
	if err != nil {
		return nil, err
	}
	ret, err := n.op.run(ctx, span, inputs...)
	if err != nil {
		return nil, err
	}
	outputs, err := n.packOutputs(ret, msg)
	if err != nil {
		return nil, err
	}
	return outputs, nil
}

const aggOp = "search_agg"

const (
	searchReduceOp       = "search_reduce"
	hybridSearchReduceOp = "hybrid_search_reduce"
	rerankOp             = "rerank"
	requeryOp            = "requery"
	organizeOp           = "organize"
	hybridAssembleOp     = "hybrid_assemble"
	endOp                = "end"
	lambdaOp             = "lambda"
	highlightOp          = "highlight"
	orderByOp            = "order_by"
)

const (
	pipelineOutput      = "output"
	pipelineInput       = "input"
	pipelineStorageCost = "storage_cost"
)

var opFactory = map[string]func(t *searchTask, params map[string]any) (operator, error){
	searchReduceOp:       newSearchReduceOperator,
	hybridSearchReduceOp: newHybridSearchReduceOperator,
	aggOp:                newAggregateOperator,
	rerankOp:             newRerankOperator,
	organizeOp:           newOrganizeOperator,
	hybridAssembleOp:     newHybridAssembleOperator,
	requeryOp:            newRequeryOperator,
	lambdaOp:             newLambdaOperator,
	endOp:                newEndOperator,
	highlightOp:          newHighlightOperator,
	orderByOp:            newOrderByOperator,
}

func NewNode(info *nodeDef, t *searchTask) (*Node, error) {
	n := Node{
		name:    info.name,
		opName:  info.opName,
		inputs:  info.inputs,
		outputs: info.outputs,
	}
	op, err := opFactory[info.opName](t, info.params)
	if err != nil {
		return nil, err
	}
	n.op = op
	return &n, nil
}

type searchReduceOperator struct {
	traceCtx            context.Context
	primaryFieldSchema  *schemapb.FieldSchema
	nq                  int64
	topK                int64
	offset              int64
	collectionID        int64
	partitionIDs        []int64
	queryInfos          []*planpb.QueryInfo
	collSchema          *schemapb.CollectionSchema
	isSearchAggregation bool
}

func newSearchReduceOperator(t *searchTask, _ map[string]any) (operator, error) {
	pkField, err := t.schema.GetPkField()
	if err != nil {
		return nil, err
	}
	return &searchReduceOperator{
		traceCtx:            t.TraceCtx(),
		primaryFieldSchema:  pkField,
		nq:                  t.GetNq(),
		topK:                t.GetTopk(),
		offset:              t.GetOffset(),
		collectionID:        t.GetCollectionID(),
		partitionIDs:        t.GetPartitionIDs(),
		queryInfos:          t.queryInfos,
		collSchema:          t.schema.CollectionSchema,
		isSearchAggregation: t.aggCtx != nil,
	}, nil
}

func (op *searchReduceOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "searchReduceOperator")
	defer sp.End()
	toReduceResults := inputs[0].([]*internalpb.SearchResults)
	metricType := getMetricType(toReduceResults)
	result, err := reduceResults(
		op.traceCtx, toReduceResults, op.nq, op.topK, op.offset,
		metricType, op.primaryFieldSchema.GetDataType(), op.queryInfos[0], false, op.isSearchAggregation, op.collectionID, op.partitionIDs)
	if err != nil {
		return nil, err
	}
	fillFieldNames(op.collSchema, result.GetResults())
	return []any{[]*milvuspb.SearchResults{result}, []string{metricType}}, nil
}

type hybridSearchReduceOperator struct {
	traceCtx           context.Context
	subReqs            []*internalpb.SubSearchRequest
	primaryFieldSchema *schemapb.FieldSchema
	collectionID       int64
	partitionIDs       []int64
	queryInfos         []*planpb.QueryInfo
	collSchema         *schemapb.CollectionSchema
}

func newHybridSearchReduceOperator(t *searchTask, _ map[string]any) (operator, error) {
	pkField, err := t.schema.GetPkField()
	if err != nil {
		return nil, err
	}
	return &hybridSearchReduceOperator{
		traceCtx:           t.TraceCtx(),
		subReqs:            t.GetSubReqs(),
		primaryFieldSchema: pkField,
		collectionID:       t.GetCollectionID(),
		partitionIDs:       t.GetPartitionIDs(),
		queryInfos:         t.queryInfos,
		collSchema:         t.schema.CollectionSchema,
	}, nil
}

func (op *hybridSearchReduceOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "hybridSearchReduceOperator")
	defer sp.End()
	toReduceResults := inputs[0].([]*internalpb.SearchResults)
	// Collecting the results of a subsearch
	// [[shard1, shard2, ...],[shard1, shard2, ...]]
	multipleInternalResults := make([][]*internalpb.SearchResults, len(op.subReqs))
	for _, searchResult := range toReduceResults {
		// if get a non-advanced result, skip all
		if !searchResult.GetIsAdvanced() {
			continue
		}
		for _, subResult := range searchResult.GetSubResults() {
			// swallow copy
			internalResults := &internalpb.SearchResults{
				MetricType:     subResult.GetMetricType(),
				NumQueries:     subResult.GetNumQueries(),
				TopK:           subResult.GetTopK(),
				SlicedBlob:     subResult.GetSlicedBlob(),
				ResultData:     subResult.GetResultData(),
				SlicedNumCount: subResult.GetSlicedNumCount(),
				SlicedOffset:   subResult.GetSlicedOffset(),
				IsAdvanced:     false,
			}
			reqIndex := subResult.GetReqIndex()
			multipleInternalResults[reqIndex] = append(multipleInternalResults[reqIndex], internalResults)
		}
	}

	multipleMilvusResults := make([]*milvuspb.SearchResults, len(op.subReqs))
	searchMetrics := []string{}
	for index, internalResults := range multipleInternalResults {
		subReq := op.subReqs[index]
		// Since the metrictype in the request may be empty, it can only be obtained from the result
		subMetricType := getMetricType(internalResults)
		result, err := reduceResults(
			op.traceCtx, internalResults, subReq.GetNq(), subReq.GetTopk(), subReq.GetOffset(), subMetricType,
			op.primaryFieldSchema.GetDataType(), op.queryInfos[index], true, false, op.collectionID, op.partitionIDs)
		if err != nil {
			return nil, err
		}
		fillFieldNames(op.collSchema, result.GetResults())
		searchMetrics = append(searchMetrics, subMetricType)
		multipleMilvusResults[index] = result
	}
	return []any{multipleMilvusResults, searchMetrics}, nil
}

type aggregateOperator struct {
	aggCtx     *search_agg.SearchAggregationContext
	collSchema *schemapb.CollectionSchema
}

func newAggregateOperator(t *searchTask, _ map[string]any) (operator, error) {
	if t.aggCtx == nil {
		return nil, merr.WrapErrServiceInternal("aggregate operator requires non-nil aggCtx")
	}
	return &aggregateOperator{
		aggCtx:     t.aggCtx,
		collSchema: t.schema.CollectionSchema,
	}, nil
}

func (op *aggregateOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "aggregateOperator")
	defer sp.End()

	// Defensive guards for pipeline-wire invariants (len + type). Static
	// analysis of searchWithAggPipe makes these unreachable, but surfacing
	// them as service-internal errors is cheaper than a process-level panic
	// if a future refactor breaks the wire.
	if len(inputs) == 0 {
		return nil, merr.WrapErrServiceInternal("aggregateOperator: missing inputs (pipeline wire)")
	}
	reducedList, ok := inputs[0].([]*milvuspb.SearchResults)
	if !ok {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("aggregateOperator: expected []*milvuspb.SearchResults, got %T (pipeline wire)", inputs[0]))
	}
	// Upstream searchReduceOp has already done cross-shard composite-key reduce
	// and produced a single *milvuspb.SearchResults wrapping one SearchResultData.
	if len(reducedList) == 0 || reducedList[0] == nil || reducedList[0].GetResults() == nil {
		return nil, merr.WrapErrServiceInternal("aggregateOperator received empty reduced results")
	}
	computer := search_agg.NewSearchAggregationComputer(reducedList[0].GetResults(), op.aggCtx)
	nqAggResults, err := computer.Compute(ctx)
	if err != nil {
		return nil, err
	}

	fieldIDToName := make(map[int64]string, len(op.collSchema.GetFields()))
	for _, f := range op.collSchema.GetFields() {
		fieldIDToName[f.GetFieldID()] = f.GetName()
	}

	aggBuckets := make([]*schemapb.AggBucket, 0)
	aggTopks := make([]int64, 0, len(nqAggResults))
	for _, buckets := range nqAggResults {
		aggTopks = append(aggTopks, int64(len(buckets)))
		aggBuckets = append(aggBuckets, serializeAggBuckets(buckets, fieldIDToName, op.aggCtx.Levels, 0)...)
	}

	result := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries:     op.aggCtx.NQ,
			Topks:          make([]int64, op.aggCtx.NQ),
			AggBuckets:     aggBuckets,
			AggTopks:       aggTopks,
			AllSearchCount: aggregatedAllSearchCount(reducedList),
		},
	}
	return []any{result}, nil
}

func serializeAggBuckets(buckets []*search_agg.AggBucketResult, fieldIDToName map[int64]string, levels []search_agg.LevelContext, levelIdx int) []*schemapb.AggBucket {
	if len(buckets) == 0 {
		return nil
	}
	serialized := make([]*schemapb.AggBucket, 0, len(buckets))
	for _, bucket := range buckets {
		if bucket == nil {
			continue
		}
		serialized = append(serialized, serializeAggBucket(bucket, fieldIDToName, levels, levelIdx))
	}
	return serialized
}

func serializeAggBucket(bucket *search_agg.AggBucketResult, fieldIDToName map[int64]string, levels []search_agg.LevelContext, levelIdx int) *schemapb.AggBucket {
	if bucket == nil {
		return nil
	}

	var fieldOrder []int64
	if levelIdx < len(levels) {
		fieldOrder = levels[levelIdx].OwnFieldIDs
	}
	result := &schemapb.AggBucket{
		Key:       serializeBucketKey(bucket.Key, fieldIDToName, fieldOrder),
		Count:     bucket.Count,
		Metrics:   serializeAggMetrics(bucket.Metrics),
		Hits:      serializeAggHits(bucket.Hits, fieldIDToName),
		SubGroups: serializeAggBuckets(bucket.SubAggBuckets, fieldIDToName, levels, levelIdx+1),
	}
	return result
}

// serializeAggMetrics maps each metric alias value into the proto MetricValue
// oneof. Numeric widths collapse: all signed ints → int_val, all floats →
// double_val. nil (accumulator never received a non-null row) is dropped.
func serializeAggMetrics(metrics map[string]any) map[string]*schemapb.MetricValue {
	if len(metrics) == 0 {
		return nil
	}
	out := make(map[string]*schemapb.MetricValue, len(metrics))
	for alias, v := range metrics {
		if v == nil {
			continue
		}
		mv := &schemapb.MetricValue{}
		switch val := v.(type) {
		case int:
			mv.Value = &schemapb.MetricValue_IntVal{IntVal: int64(val)}
		case int8:
			mv.Value = &schemapb.MetricValue_IntVal{IntVal: int64(val)}
		case int16:
			mv.Value = &schemapb.MetricValue_IntVal{IntVal: int64(val)}
		case int32:
			mv.Value = &schemapb.MetricValue_IntVal{IntVal: int64(val)}
		case int64:
			mv.Value = &schemapb.MetricValue_IntVal{IntVal: val}
		case float32:
			mv.Value = &schemapb.MetricValue_DoubleVal{DoubleVal: float64(val)}
		case float64:
			mv.Value = &schemapb.MetricValue_DoubleVal{DoubleVal: val}
		case string:
			mv.Value = &schemapb.MetricValue_StringVal{StringVal: val}
		case bool:
			mv.Value = &schemapb.MetricValue_BoolVal{BoolVal: val}
		default:
			// Unknown scalar: fall back to string representation so the SDK
			// still sees some result rather than a dropped alias.
			mv.Value = &schemapb.MetricValue_StringVal{StringVal: fmt.Sprintf("%v", val)}
		}
		out[alias] = mv
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func serializeBucketKey(key map[int64]interface{}, fieldIDToName map[int64]string, fieldOrder []int64) []*schemapb.BucketKeyEntry {
	if len(key) == 0 {
		return nil
	}
	fieldIDs := make([]int64, 0, len(key))
	seen := make(map[int64]struct{}, len(key))
	for _, fieldID := range fieldOrder {
		if _, ok := key[fieldID]; ok {
			fieldIDs = append(fieldIDs, fieldID)
			seen[fieldID] = struct{}{}
		}
	}
	for fieldID := range key {
		if _, ok := seen[fieldID]; ok {
			continue
		}
		fieldIDs = append(fieldIDs, fieldID)
	}

	entries := make([]*schemapb.BucketKeyEntry, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		entry := &schemapb.BucketKeyEntry{FieldId: fieldID, FieldName: fieldIDToName[fieldID]}
		value := key[fieldID]
		if value == nil {
			entries = append(entries, entry)
			continue
		}
		switch value := value.(type) {
		case int:
			entry.Value = &schemapb.BucketKeyEntry_IntVal{IntVal: int64(value)}
		case int8:
			entry.Value = &schemapb.BucketKeyEntry_IntVal{IntVal: int64(value)}
		case int16:
			entry.Value = &schemapb.BucketKeyEntry_IntVal{IntVal: int64(value)}
		case int32:
			entry.Value = &schemapb.BucketKeyEntry_IntVal{IntVal: int64(value)}
		case int64:
			entry.Value = &schemapb.BucketKeyEntry_IntVal{IntVal: value}
		case uint:
			entry.Value = &schemapb.BucketKeyEntry_IntVal{IntVal: int64(value)}
		case uint8:
			entry.Value = &schemapb.BucketKeyEntry_IntVal{IntVal: int64(value)}
		case uint16:
			entry.Value = &schemapb.BucketKeyEntry_IntVal{IntVal: int64(value)}
		case uint32:
			entry.Value = &schemapb.BucketKeyEntry_IntVal{IntVal: int64(value)}
		case uint64:
			entry.Value = &schemapb.BucketKeyEntry_IntVal{IntVal: int64(value)}
		case string:
			entry.Value = &schemapb.BucketKeyEntry_StringVal{StringVal: value}
		case bool:
			entry.Value = &schemapb.BucketKeyEntry_BoolVal{BoolVal: value}
		default:
			entry.Value = &schemapb.BucketKeyEntry_StringVal{StringVal: fmt.Sprintf("%v", value)}
		}
		entries = append(entries, entry)
	}
	return entries
}

func serializeAggHits(hits []*search_agg.HitResult, fieldIDToName map[int64]string) []*schemapb.AggHit {
	if len(hits) == 0 {
		return nil
	}
	serialized := make([]*schemapb.AggHit, 0, len(hits))
	for _, hit := range hits {
		if hit == nil {
			continue
		}
		aggHit := &schemapb.AggHit{Score: hit.Score}
		switch pk := hit.PK.(type) {
		case int64:
			aggHit.Pk = &schemapb.AggHit_IntPk{IntPk: pk}
		case string:
			aggHit.Pk = &schemapb.AggHit_StrPk{StrPk: pk}
		}
		aggHit.Fields = serializeAggHitFields(hit.Fields, fieldIDToName)
		serialized = append(serialized, aggHit)
	}
	return serialized
}

func serializeAggHitFields(fields map[int64]interface{}, fieldIDToName map[int64]string) []*schemapb.AggHitField {
	if len(fields) == 0 {
		return nil
	}
	fieldIDs := make([]int64, 0, len(fields))
	for fieldID := range fields {
		fieldIDs = append(fieldIDs, fieldID)
	}
	sort.Slice(fieldIDs, func(i, j int) bool { return fieldIDs[i] < fieldIDs[j] })

	serialized := make([]*schemapb.AggHitField, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		field := &schemapb.AggHitField{FieldId: fieldID, FieldName: fieldIDToName[fieldID]}
		value := fields[fieldID]
		if value == nil {
			serialized = append(serialized, field)
			continue
		}
		switch value := value.(type) {
		case int:
			field.Value = &schemapb.AggHitField_IntVal{IntVal: int64(value)}
		case int8:
			field.Value = &schemapb.AggHitField_IntVal{IntVal: int64(value)}
		case int16:
			field.Value = &schemapb.AggHitField_IntVal{IntVal: int64(value)}
		case int32:
			field.Value = &schemapb.AggHitField_IntVal{IntVal: int64(value)}
		case int64:
			field.Value = &schemapb.AggHitField_IntVal{IntVal: value}
		case uint:
			field.Value = &schemapb.AggHitField_IntVal{IntVal: int64(value)}
		case uint8:
			field.Value = &schemapb.AggHitField_IntVal{IntVal: int64(value)}
		case uint16:
			field.Value = &schemapb.AggHitField_IntVal{IntVal: int64(value)}
		case uint32:
			field.Value = &schemapb.AggHitField_IntVal{IntVal: int64(value)}
		case uint64:
			field.Value = &schemapb.AggHitField_IntVal{IntVal: int64(value)}
		case bool:
			field.Value = &schemapb.AggHitField_BoolVal{BoolVal: value}
		case float32:
			field.Value = &schemapb.AggHitField_FloatVal{FloatVal: value}
		case float64:
			field.Value = &schemapb.AggHitField_DoubleVal{DoubleVal: value}
		case string:
			field.Value = &schemapb.AggHitField_StringVal{StringVal: value}
		case []byte:
			field.Value = &schemapb.AggHitField_BytesVal{BytesVal: value}
		default:
			field.Value = &schemapb.AggHitField_StringVal{StringVal: fmt.Sprintf("%v", value)}
		}
		serialized = append(serialized, field)
	}
	return serialized
}

type rerankOperator struct {
	nq               int64
	topK             int64
	offset           int64
	roundDecimal     int64
	groupByFieldName string
	groupSize        int64
	groupScorerStr   string

	collSchema *schemapb.CollectionSchema
	rerankMeta rerankMeta
	dbName     string
}

// getChainNeededFields returns the field names that the chain actually needs
// from FieldsData (rerank input fields). Returns nil if no filtering is needed.
// Note: the group-by field is imported separately via GroupByFieldValue, not FieldsData.
func (op *rerankOperator) getChainNeededFields() []string {
	if op.rerankMeta != nil {
		return op.rerankMeta.GetInputFieldNames()
	}
	return nil
}

func resolveFieldName(schema *schemapb.CollectionSchema, fieldID int64) string {
	for _, field := range schema.GetFields() {
		if field.GetFieldID() == fieldID {
			return field.GetName()
		}
	}
	return ""
}

// fillFieldNames populates missing FieldName in SearchResultData using the collection schema.
// Real search results from QueryNode only have FieldId set; FieldName is empty.
func fillFieldNames(schema *schemapb.CollectionSchema, resultData *schemapb.SearchResultData) {
	if schema == nil || resultData == nil {
		return
	}
	fieldIDToName := make(map[int64]string, len(schema.GetFields()))
	for _, field := range schema.GetFields() {
		fieldIDToName[field.GetFieldID()] = field.GetName()
	}
	for _, fd := range resultData.GetFieldsData() {
		if fd.GetFieldName() == "" {
			if name, ok := fieldIDToName[fd.GetFieldId()]; ok {
				fd.FieldName = name
			}
		}
	}
	for _, gbv := range resultData.GetGroupByFieldValues() {
		if gbv == nil || gbv.GetFieldName() != "" {
			continue
		}
		if name, ok := fieldIDToName[gbv.GetFieldId()]; ok {
			gbv.FieldName = name
		}
	}
}

func newRerankOperator(t *searchTask, params map[string]any) (operator, error) {
	if t.GetIsAdvanced() {
		return &rerankOperator{
			nq:               t.GetNq(),
			topK:             t.rankParams.limit,
			offset:           t.rankParams.offset,
			roundDecimal:     t.rankParams.roundDecimal,
			groupByFieldName: t.rankParams.GetGroupByFieldName(),
			groupSize:        t.rankParams.groupSize,
			groupScorerStr:   getGroupScorerStr(t.request.GetSearchParams()),
			collSchema:       t.schema.CollectionSchema,
			rerankMeta:       t.rerankMeta,
			dbName:           t.request.GetDbName(),
		}, nil
	}
	return &rerankOperator{
		nq:               t.GetNq(),
		topK:             t.GetTopk(),
		offset:           0, // Search performs Offset in the reduce phase
		roundDecimal:     t.queryInfos[0].RoundDecimal,
		groupByFieldName: resolveFieldName(t.schema.CollectionSchema, t.queryInfos[0].GroupByFieldId),
		groupSize:        t.queryInfos[0].GroupSize,
		groupScorerStr:   getGroupScorerStr(t.request.GetSearchParams()),
		collSchema:       t.schema.CollectionSchema,
		rerankMeta:       t.rerankMeta,
		dbName:           t.request.GetDbName(),
	}, nil
}

func buildChainFromMeta(
	meta rerankMeta,
	collSchema *schemapb.CollectionSchema,
	metrics []string,
	searchParams *chain.SearchParams,
	alloc memory.Allocator,
) (*chain.FuncChain, error) {
	switch m := meta.(type) {
	case *funcScoreRerankMeta:
		return chain.BuildRerankChain(collSchema, m.funcScore, metrics, searchParams, alloc)
	case *legacyRerankMeta:
		return chain.BuildRerankChainWithLegacy(collSchema, m.legacyParams, metrics, searchParams, alloc)
	default:
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("rerank operator: unsupported rerankMeta type %T", meta))
	}
}

func (op *rerankOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "rerankOperator")
	defer sp.End()

	reducedResults, ok := inputs[0].([]*milvuspb.SearchResults)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("rerank operator: inputs[0] must be []*SearchResults, got %T", inputs[0])
	}
	inputMetrics, ok := inputs[1].([]string)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("rerank operator: inputs[1] must be []string, got %T", inputs[1])
	}

	alloc := memory.DefaultAllocator

	// Only convert fields that the chain actually needs (rerank input fields + group-by field).
	// Other fields are not used by chain and will be re-fetched by organize/requery later.
	neededFields := op.getChainNeededFields()

	// Convert all inputs to DataFrames.
	// Note: reducedResults entries are never nil — the reduce operator always produces
	// valid SearchResults (returns error otherwise). We keep a simple nil guard but
	// do not need to filter metrics since no entries are skipped.
	dataframes := make([]*chain.DataFrame, 0, len(reducedResults))
	for _, result := range reducedResults {
		if result == nil || result.GetResults() == nil {
			continue
		}
		df, err := chain.FromSearchResultData(result.GetResults(), alloc, neededFields)
		if err != nil {
			for _, d := range dataframes {
				d.Release()
			}
			return nil, err
		}
		dataframes = append(dataframes, df)
	}

	// If no valid results, or all results are empty (zero rows), return empty result directly.
	// This avoids errors from chain operators that expect field columns (e.g., Decay reranker).
	allEmpty := len(dataframes) == 0
	if !allEmpty {
		allEmpty = true
		for _, df := range dataframes {
			if df.NumRows() > 0 {
				allEmpty = false
				break
			}
		}
	}
	if allEmpty {
		for _, df := range dataframes {
			df.Release()
		}
		return []any{&milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries:     op.nq,
				TopK:           op.topK,
				FieldsData:     make([]*schemapb.FieldData, 0),
				Scores:         []float32{},
				Ids:            &schemapb.IDs{},
				Topks:          make([]int64, op.nq),
				AllSearchCount: aggregatedAllSearchCount(reducedResults),
			},
		}}, nil
	}

	// Build search params
	var searchParams *chain.SearchParams
	if op.groupByFieldName != "" && op.groupSize > 0 {
		scorer := chain.GroupScorer(op.groupScorerStr)
		searchParams = chain.NewSearchParamsWithGroupingAndScorer(
			op.nq, op.topK, op.offset, op.roundDecimal,
			op.groupByFieldName, op.groupSize, scorer)
	} else {
		searchParams = chain.NewSearchParams(op.nq, op.topK, op.offset, op.roundDecimal)
	}
	// Build chain
	if op.rerankMeta == nil {
		for _, df := range dataframes {
			df.Release()
		}
		return nil, merr.WrapErrServiceInternal("rerank operator: rerankMeta is nil, cannot build rerank chain")
	}
	searchParams.ModelExtraInfo = &models.ModelExtraInfo{
		ClusterID: paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
		DBName:    op.dbName,
	}
	fc, err := buildChainFromMeta(op.rerankMeta, op.collSchema, inputMetrics, searchParams, alloc)
	if err != nil {
		for _, df := range dataframes {
			df.Release()
		}
		return nil, err
	}

	// Execute chain
	resultDF, err := fc.ExecuteWithContext(ctx, dataframes...)
	// Release input dataframes
	for _, df := range dataframes {
		df.Release()
	}
	if err != nil {
		return nil, err
	}

	// Convert back to SearchResultData
	var exportOpts *chain.ExportOptions
	if op.groupByFieldName != "" {
		exportOpts = &chain.ExportOptions{GroupByField: op.groupByFieldName}
	}
	resultData, err := chain.ToSearchResultDataWithOptions(resultDF, exportOpts)
	resultDF.Release()
	if err != nil {
		return nil, err
	}

	// Aggregate all search count
	allSearchCount := aggregatedAllSearchCount(reducedResults)

	resultData.AllSearchCount = allSearchCount
	return []any{&milvuspb.SearchResults{
		Status:  merr.Success(),
		Results: resultData,
	}}, nil
}

type requeryOperator struct {
	traceCtx         context.Context
	outputFieldNames []string

	timestamp          uint64
	dbName             string
	collectionName     string
	notReturnAllMeta   bool
	partitionNames     []string
	partitionIDs       []int64
	primaryFieldSchema *schemapb.FieldSchema
	queryChannelsTs    map[string]Timestamp
	consistencyLevel   commonpb.ConsistencyLevel
	guaranteeTimestamp uint64
	namespace          *string

	node types.ProxyComponent
}

func newRequeryOperator(t *searchTask, _ map[string]any) (operator, error) {
	pkField, err := t.schema.GetPkField()
	if err != nil {
		return nil, err
	}
	outputFieldNames := typeutil.NewSet(t.translatedOutputFields...)
	if t.GetIsAdvanced() && t.rerankMeta != nil {
		outputFieldNames.Insert(t.rerankMeta.GetInputFieldNames()...)
	}
	// Union order_by field names with output fields for requery
	// Use OutputFieldName which is the proper name for requery:
	// - For dynamic fields: the original key (e.g., "age") so QueryNode extracts only that subfield
	// - For regular fields: the field name
	// - For regular JSON fields: the base field name (whole JSON, extracted on proxy)
	for _, orderByField := range t.orderByFields {
		outputFieldNames.Insert(orderByField.OutputFieldName)
	}
	// Add highlight dynamic fields to requery output
	if t.highlighter != nil {
		highlightDynFields := t.highlighter.DynamicFieldNames()
		if len(highlightDynFields) > 0 {
			outputFieldNames.Insert(highlightDynFields...)
		}
	}
	return &requeryOperator{
		traceCtx:           t.TraceCtx(),
		outputFieldNames:   outputFieldNames.Collect(),
		timestamp:          t.BeginTs(),
		dbName:             t.request.GetDbName(),
		collectionName:     t.request.GetCollectionName(),
		primaryFieldSchema: pkField,
		queryChannelsTs:    t.queryChannelsTs,
		consistencyLevel:   t.GetConsistencyLevel(),
		guaranteeTimestamp: t.GetGuaranteeTimestamp(),
		notReturnAllMeta:   t.request.GetNotReturnAllMeta(),
		partitionNames:     t.request.GetPartitionNames(),
		partitionIDs:       t.GetPartitionIDs(),
		node:               t.node,
		namespace:          t.request.Namespace,
	}, nil
}

func (op *requeryOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	allIDs := inputs[0].(*schemapb.IDs)
	storageCostFromLastOp := inputs[1].(segcore.StorageCost)
	if typeutil.GetSizeOfIDs(allIDs) == 0 {
		return []any{[]*schemapb.FieldData{}, storageCostFromLastOp}, nil
	}

	queryResult, storageCost, err := op.requery(ctx, span, allIDs, op.outputFieldNames)
	if err != nil {
		return nil, err
	}
	storageCost.ScannedRemoteBytes += storageCostFromLastOp.ScannedRemoteBytes
	storageCost.ScannedTotalBytes += storageCostFromLastOp.ScannedTotalBytes
	return []any{queryResult.GetFieldsData(), storageCost}, nil
}

func (op *requeryOperator) requery(ctx context.Context, span trace.Span, ids *schemapb.IDs, outputFields []string) (*milvuspb.QueryResults, segcore.StorageCost, error) {
	queryReq := &milvuspb.QueryRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Retrieve,
			Timestamp: op.timestamp,
		},
		DbName:                op.dbName,
		CollectionName:        op.collectionName,
		ConsistencyLevel:      op.consistencyLevel,
		NotReturnAllMeta:      op.notReturnAllMeta,
		Expr:                  "",
		OutputFields:          outputFields,
		PartitionNames:        op.partitionNames,
		UseDefaultConsistency: false,
		GuaranteeTimestamp:    op.guaranteeTimestamp,
		Namespace:             op.namespace,
	}
	plan := planparserv2.CreateRequeryPlan(op.primaryFieldSchema, ids)
	plan.Namespace = op.namespace
	channelsMvcc := make(map[string]Timestamp)
	for k, v := range op.queryChannelsTs {
		channelsMvcc[k] = v
	}
	qt := &queryTask{
		ctx:       op.traceCtx,
		Condition: NewTaskCondition(op.traceCtx),
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Retrieve),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ReqID:            paramtable.GetNodeID(),
			PartitionIDs:     op.partitionIDs, // use search partitionIDs
			ConsistencyLevel: op.consistencyLevel,
			QueryLabel:       metrics.ReQueryLabel,
		},
		request:        queryReq,
		plan:           plan,
		mixCoord:       op.node.(*Proxy).mixCoord,
		lb:             op.node.(*Proxy).lbPolicy,
		shardclientMgr: op.node.(*Proxy).shardMgr,
		channelsMvcc:   channelsMvcc,
		fastSkip:       true,
		reQuery:        true,
	}
	queryResult, storageCost, err := op.node.(*Proxy).query(op.traceCtx, qt, span)
	if err != nil {
		return nil, segcore.StorageCost{}, err
	}

	if queryResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return nil, segcore.StorageCost{}, merr.Error(queryResult.GetStatus())
	}
	return queryResult, storageCost, nil
}

type organizeOperator struct {
	traceCtx           context.Context
	primaryFieldSchema *schemapb.FieldSchema
	collectionID       int64
}

func newOrganizeOperator(t *searchTask, _ map[string]any) (operator, error) {
	pkField, err := t.schema.GetPkField()
	if err != nil {
		return nil, err
	}
	return &organizeOperator{
		traceCtx:           t.TraceCtx(),
		primaryFieldSchema: pkField,
		collectionID:       t.GetCollectionID(),
	}, nil
}

func (op *organizeOperator) emptyFieldDataAccordingFieldSchema(fieldData *schemapb.FieldData) *schemapb.FieldData {
	ret := &schemapb.FieldData{
		Type:      fieldData.Type,
		FieldName: fieldData.FieldName,
		FieldId:   fieldData.FieldId,
		IsDynamic: fieldData.IsDynamic,
		ValidData: make([]bool, 0),
	}
	if fieldData.Type == schemapb.DataType_FloatVector ||
		fieldData.Type == schemapb.DataType_BinaryVector ||
		fieldData.Type == schemapb.DataType_BFloat16Vector ||
		fieldData.Type == schemapb.DataType_Float16Vector ||
		fieldData.Type == schemapb.DataType_Int8Vector {
		ret.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: fieldData.GetVectors().GetDim(),
			},
		}
	}
	return ret
}

func (op *organizeOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(op.traceCtx, "organizeOperator")
	defer sp.End()

	fields := inputs[0].([]*schemapb.FieldData)
	var idsList []*schemapb.IDs
	switch inputs[1].(type) {
	case *schemapb.IDs:
		idsList = []*schemapb.IDs{inputs[1].(*schemapb.IDs)}
	case []*schemapb.IDs:
		idsList = inputs[1].([]*schemapb.IDs)
	default:
		panic(fmt.Sprintf("invalid ids type: %T", inputs[1]))
	}
	if len(fields) == 0 {
		emptyFields := make([][]*schemapb.FieldData, len(idsList))
		return []any{emptyFields}, nil
	}
	pkFieldData, err := typeutil.GetPrimaryFieldData(fields, op.primaryFieldSchema)
	if err != nil {
		return nil, err
	}
	offsets := make(map[any]int)
	pkItr := typeutil.GetDataIterator(pkFieldData)
	for i := 0; i < typeutil.GetPKSize(pkFieldData); i++ {
		pk := pkItr(i)
		offsets[pk] = i
	}

	allFieldData := make([][]*schemapb.FieldData, len(idsList))
	for idx, ids := range idsList {
		if typeutil.GetSizeOfIDs(ids) == 0 {
			emptyFields := []*schemapb.FieldData{}
			for _, field := range fields {
				emptyFields = append(emptyFields, op.emptyFieldDataAccordingFieldSchema(field))
			}
			allFieldData[idx] = emptyFields
			continue
		}
		if fieldData, err := pickFieldData(ids, offsets, fields, op.collectionID); err != nil {
			return nil, err
		} else {
			allFieldData[idx] = fieldData
		}
	}
	return []any{allFieldData}, nil
}

func pickFieldData(ids *schemapb.IDs, pkOffset map[any]int, fields []*schemapb.FieldData, collectionID int64) ([]*schemapb.FieldData, error) {
	// Reorganize Results. The order of query result ids will be altered and differ from queried ids.
	// We should reorganize query results to keep the order of original queried ids. For example:
	// ===========================================
	//  3  2  5  4  1  (query ids)
	//       ||
	//       || (query)
	//       \/
	//  4  3  5  1  2  (result ids)
	// v4 v3 v5 v1 v2  (result vectors)
	//       ||
	//       || (reorganize)
	//       \/
	//  3  2  5  4  1  (result ids)
	// v3 v2 v5 v4 v1  (result vectors)
	// ===========================================
	fieldsData := make([]*schemapb.FieldData, len(fields))
	idxComputer := typeutil.NewFieldDataIdxComputer(fields)
	for i := 0; i < typeutil.GetSizeOfIDs(ids); i++ {
		id := typeutil.GetPK(ids, int64(i))
		if _, ok := pkOffset[id]; !ok {
			return nil, merr.WrapErrInconsistentRequery(fmt.Sprintf("incomplete query result, missing id %s, len(searchIDs) = %d, len(queryIDs) = %d, collection=%d",
				id, typeutil.GetSizeOfIDs(ids), len(pkOffset), collectionID))
		}
		rowIdx := int64(pkOffset[id])
		fieldIdxs := idxComputer.Compute(rowIdx)
		typeutil.AppendFieldData(fieldsData, fields, rowIdx, fieldIdxs...)
	}

	return fieldsData, nil
}

// hybridAssembleOperator picks field data for reranked IDs directly from multiple
// sub-search results using a PK index, avoiding the full data copy that
// merging all FieldsData would require.
type hybridAssembleOperator struct {
	collectionID int64
}

func newHybridAssembleOperator(t *searchTask, _ map[string]any) (operator, error) {
	return &hybridAssembleOperator{
		collectionID: t.GetCollectionID(),
	}, nil
}

func (op *hybridAssembleOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	reducedResults, ok := inputs[0].([]*milvuspb.SearchResults)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("hybrid assemble: inputs[0] must be []*SearchResults, got %T", inputs[0])
	}
	rankResult, ok := inputs[1].(*milvuspb.SearchResults)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("hybrid assemble: inputs[1] must be *SearchResults, got %T", inputs[1])
	}

	rerankedIDs := rankResult.GetResults().GetIds()
	numReranked := typeutil.GetSizeOfIDs(rerankedIDs)
	if numReranked == 0 {
		return []any{rankResult}, nil
	}

	// Build PK → (resultIdx, rowIdx) index across all sub-search results.
	type pkLoc struct{ resultIdx, rowIdx int }
	pkIndex := make(map[any]pkLoc)
	for rIdx, result := range reducedResults {
		ids := result.GetResults().GetIds()
		for i := 0; i < typeutil.GetSizeOfIDs(ids); i++ {
			pkIndex[typeutil.GetPK(ids, int64(i))] = pkLoc{rIdx, i}
		}
	}

	// Find the first non-empty FieldsData to determine the output field schema.
	var templateFields []*schemapb.FieldData
	for _, r := range reducedResults {
		if len(r.GetResults().GetFieldsData()) > 0 {
			templateFields = r.GetResults().GetFieldsData()
			break
		}
	}
	if templateFields == nil {
		return []any{rankResult}, nil
	}

	// Pre-compute field-index computers per sub-search result (one per distinct FieldsData layout).
	computers := make([]*typeutil.FieldDataIdxComputer, len(reducedResults))
	for i, r := range reducedResults {
		if len(r.GetResults().GetFieldsData()) > 0 {
			computers[i] = typeutil.NewFieldDataIdxComputer(r.GetResults().GetFieldsData())
		}
	}

	// Assemble only the rows referenced by the reranked IDs.
	//
	// Invariant: every sub-result that contributes a reranked id must carry
	// FieldsData with the same layout as templateFields. Proxy upstream
	// (task_search.go) sets identical plan.OutputFieldIds across all
	// sub-requests, so this should always hold. If we ever observe a nil
	// computer here, an upstream invariant has been broken — silently
	// dropping the row would corrupt the PK ↔ field mapping downstream
	// (rerankedIDs and FieldsData would have different lengths). Fail loud
	// instead so the bug is caught immediately at its source.
	fieldsData := make([]*schemapb.FieldData, len(templateFields))
	for i := 0; i < numReranked; i++ {
		pk := typeutil.GetPK(rerankedIDs, int64(i))
		loc, ok := pkIndex[pk]
		if !ok {
			return nil, merr.WrapErrInconsistentRequery(
				fmt.Sprintf("hybrid assemble: missing id %v, collection=%d", pk, op.collectionID))
		}
		if computers[loc.resultIdx] == nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf(
				"hybrid assemble: sub-result[%d] has empty FieldsData but contributed reranked id %v; "+
					"all sub-results that contribute ids must share the same FieldsData layout, "+
					"collection=%d", loc.resultIdx, pk, op.collectionID))
		}
		srcFields := reducedResults[loc.resultIdx].GetResults().GetFieldsData()
		fieldIdxs := computers[loc.resultIdx].Compute(int64(loc.rowIdx))
		typeutil.AppendFieldData(fieldsData, srcFields, int64(loc.rowIdx), fieldIdxs...)
	}

	rankResult.Results.FieldsData = fieldsData
	return []any{rankResult}, nil
}

const (
	lambdaParamKey = "lambda"
)

type lambdaOperator struct {
	f func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error)
}

func newLambdaOperator(_ *searchTask, params map[string]any) (operator, error) {
	return &lambdaOperator{
		f: params[lambdaParamKey].(func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error)),
	}, nil
}

func (op *lambdaOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	return op.f(ctx, span, inputs...)
}

type endOperator struct {
	outputFieldNames []string
	fieldSchemas     []*schemapb.FieldSchema
}

func newEndOperator(t *searchTask, _ map[string]any) (operator, error) {
	return &endOperator{
		outputFieldNames: t.translatedOutputFields,
		fieldSchemas:     typeutil.GetAllFieldSchemas(t.schema.CollectionSchema),
	}, nil
}

func (op *endOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	result := inputs[0].(*milvuspb.SearchResults)
	for _, retField := range result.Results.FieldsData {
		for _, fieldSchema := range op.fieldSchemas {
			if retField != nil && retField.FieldId == fieldSchema.FieldID {
				retField.FieldName = fieldSchema.Name
				retField.Type = fieldSchema.DataType
				retField.IsDynamic = fieldSchema.IsDynamic
			}
		}
	}
	result.Results.FieldsData = lo.Filter(result.Results.FieldsData, func(field *schemapb.FieldData, _ int) bool {
		return lo.Contains(op.outputFieldNames, field.FieldName)
	})
	allSearchCount := aggregatedAllSearchCount(inputs[1].([]*milvuspb.SearchResults))
	result.GetResults().AllSearchCount = allSearchCount
	return []any{result}, nil
}

func newHighlightOperator(t *searchTask, _ map[string]any) (operator, error) {
	return t.highlighter.AsSearchPipelineOperator(t)
}

type orderByOperator struct {
	orderByFields  []OrderByField
	groupByFieldId int64
	groupSize      int64
}

func newOrderByOperator(t *searchTask, _ map[string]any) (operator, error) {
	var groupByFieldId int64 = -1
	var groupSize int64 = 1
	if len(t.queryInfos) > 0 && t.queryInfos[0] != nil {
		queryInfo := t.queryInfos[0]
		groupByFieldId = queryInfo.GetGroupByFieldId()
		if ids := queryInfo.GetGroupByFieldIds(); len(ids) > 0 {
			groupByFieldId = ids[0]
		}
		groupSize = queryInfo.GetGroupSize()
	}
	return &orderByOperator{
		orderByFields:  t.orderByFields,
		groupByFieldId: groupByFieldId,
		groupSize:      groupSize,
	}, nil
}

func (op *orderByOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "orderByOperator")
	defer sp.End()

	result := inputs[0].(*milvuspb.SearchResults)

	if len(op.orderByFields) == 0 {
		return []any{result}, nil
	}

	numResults := len(result.GetResults().GetScores())
	if numResults == 0 {
		return []any{result}, nil
	}

	// Validate that all order_by fields exist in the result
	if err := op.validateOrderByFields(result); err != nil {
		return nil, err
	}

	// Get per-query result counts from Topks
	// Topks[i] contains the number of results for the i-th query
	topks := result.GetResults().GetTopks()

	// Validate that sum(Topks) matches numResults to prevent slice bounds panic
	var sumTopks int64
	for _, topk := range topks {
		sumTopks += topk
	}
	if int(sumTopks) != numResults {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("order_by: Topks sum (%d) does not match numResults (%d)", sumTopks, numResults))
	}

	// Build indices array for sorting
	indices := make([]int, numResults)
	for i := range indices {
		indices[i] = i
	}

	// Sort results per query - each query's results should be sorted independently
	// Results are stored sequentially: [q1_results..., q2_results..., ...]
	if len(topks) <= 1 {
		// Single query (nq=1) or no topks info - sort all results together
		if err := op.sortQueryResults(result, indices); err != nil {
			return nil, err
		}
	} else {
		// Multiple queries (nq>1) - sort each query's results independently
		offset := 0
		for _, topk := range topks {
			if topk > 0 {
				// Extract the slice of indices for this query
				queryIndices := indices[offset : offset+int(topk)]
				if err := op.sortQueryResults(result, queryIndices); err != nil {
					return nil, err
				}
			}
			offset += int(topk)
		}
	}

	// Reorder results based on sorted indices
	if err := op.reorderResults(result, indices); err != nil {
		return nil, err
	}

	return []any{result}, nil
}

// sortQueryResults sorts the given indices slice based on order_by fields.
// This handles both regular and group-by cases for a single query's results.
// Returns an error if comparison fails (e.g., index out of bounds).
func (op *orderByOperator) sortQueryResults(result *milvuspb.SearchResults, indices []int) error {
	if len(indices) == 0 {
		return nil
	}

	if op.groupByFieldId >= 0 && op.groupSize > 0 {
		// Group-by case: sort groups by the first row's value in each group
		return op.sortGroupsByOrderByFields(result, indices)
	}
	// Regular case: sort all results
	return op.sortResultsByOrderByFields(result, indices)
}

// validateOrderByFields checks that all order_by fields exist in the result
// Note: For dynamic fields, FieldName is "$meta" (set by proxy's default_limit_reducer from schema),
// not the user-requested subfield name like "age". See OrderByField struct comment for details.
func (op *orderByOperator) validateOrderByFields(result *milvuspb.SearchResults) error {
	fieldNames := make(map[string]bool)
	for _, field := range result.GetResults().GetFieldsData() {
		fieldNames[field.GetFieldName()] = true
	}

	for _, orderBy := range op.orderByFields {
		if !fieldNames[orderBy.FieldName] {
			return merr.WrapErrServiceInternal(fmt.Sprintf("order_by field '%s' not found in search results", orderBy.FieldName))
		}
	}
	return nil
}

// buildFieldDataMap builds a map from field name to field data for O(1) lookup
func buildFieldDataMap(result *milvuspb.SearchResults) map[string]*schemapb.FieldData {
	fieldMap := make(map[string]*schemapb.FieldData)
	for _, field := range result.GetResults().GetFieldsData() {
		fieldMap[field.GetFieldName()] = field
	}
	return fieldMap
}

// jsonValueCache caches pre-extracted JSON values to avoid repeated extraction during sorting.
// Key format: "fieldName\x00jsonPath" (using null byte as separator to avoid collisions)
// Value is a map from data index to extracted JSON value (sparse storage for nq>1 efficiency)
type jsonValueCache map[string]map[int]gjson.Result

// buildJSONValueCache pre-extracts JSON values for the specified indices only.
// This converts O(n log n) extractions during sorting to O(n) extractions upfront.
// Note: For dynamic fields, FieldName is "$meta" and JSONPath extracts the subfield (e.g., "/age").
//
// The indices parameter specifies which data indices to extract (e.g., [20,21,22] for q3 when nq>1).
// Only values at these indices are cached, avoiding unnecessary extraction for other queries' data.
func buildJSONValueCache(fieldMap map[string]*schemapb.FieldData, orderByFields []OrderByField, indices []int) jsonValueCache {
	cache := make(jsonValueCache)
	for _, orderBy := range orderByFields {
		if orderBy.JSONPath == "" {
			continue
		}
		field := fieldMap[orderBy.FieldName]
		if field == nil || field.GetType() != schemapb.DataType_JSON {
			continue
		}
		cacheKey := orderBy.FieldName + "\x00" + orderBy.JSONPath
		jsonData := field.GetScalars().GetJsonData().GetData()
		// Use a map for sparse storage - only cache values for indices we need
		values := make(map[int]gjson.Result, len(indices))
		for _, idx := range indices {
			if idx < len(jsonData) {
				values[idx] = extractJSONValue(jsonData[idx], orderBy.JSONPath)
			}
			// else values[idx] not set, getCachedJSONValue returns empty result
		}
		cache[cacheKey] = values
	}
	return cache
}

// getCachedJSONValue retrieves a cached JSON value, returning empty result if not found
func (c jsonValueCache) getCachedJSONValue(fieldName, jsonPath string, idx int) gjson.Result {
	cacheKey := fieldName + "\x00" + jsonPath
	if values, ok := c[cacheKey]; ok {
		if val, exists := values[idx]; exists {
			return val
		}
	}
	return gjson.Result{}
}

// extractJSONValue extracts a value from JSON bytes at the given JSON Pointer path
// Returns the gjson.Result for comparison
//
// JSON Pointer (RFC 6901) uses "/" as separator and escape sequences:
//   - ~0 represents literal "~"
//   - ~1 represents literal "/"
//
// gjson uses "." as separator and "\." for literal dots in keys.
func extractJSONValue(jsonData []byte, jsonPath string) gjson.Result {
	if jsonPath == "" || len(jsonData) == 0 {
		return gjson.Result{}
	}

	// Convert JSON Pointer format to gjson path
	// Example: "/user/name" -> "user.name"
	// Example: "/key~1with~1slash" -> "key/with/slash" (single key with slashes)
	gjsonPath := jsonPointerToGjsonPath(jsonPath)
	return gjson.GetBytes(jsonData, gjsonPath)
}

// jsonPointerToGjsonPath converts a JSON Pointer path to gjson path format
// Properly handles:
//   - Path separators: "/" in JSON Pointer -> "." in gjson
//   - Escaped slashes: "~1" -> literal "/" in key name
//   - Escaped tildes: "~0" -> literal "~" in key name
//   - Dots in keys: "." -> "\." in gjson (escaped)
func jsonPointerToGjsonPath(jsonPointer string) string {
	if jsonPointer == "" {
		return ""
	}

	// Remove leading "/" if present
	jsonPointer = strings.TrimPrefix(jsonPointer, "/")

	if jsonPointer == "" {
		return ""
	}

	// Split by "/" (the JSON Pointer separator)
	segments := strings.Split(jsonPointer, "/")

	// Process each segment: unescape and convert to gjson format
	gjsonSegments := make([]string, 0, len(segments))
	for _, segment := range segments {
		if segment == "" {
			continue
		}
		// Unescape JSON Pointer sequences (order matters: ~1 before ~0)
		segment = strings.ReplaceAll(segment, "~1", "/")
		segment = strings.ReplaceAll(segment, "~0", "~")

		// Escape special characters for gjson
		// gjson uses "." as path separator, so escape literal dots in keys
		segment = strings.ReplaceAll(segment, ".", "\\.")

		gjsonSegments = append(gjsonSegments, segment)
	}

	return strings.Join(gjsonSegments, ".")
}

// compareJSONValues compares two gjson.Result values
// Returns -1 if a < b, 0 if a == b, 1 if a > b
// Null handling: non-existent paths and explicit JSON null are treated as null (NULLS FIRST)
func compareJSONValues(a, b gjson.Result) int {
	// Check if values are null (non-existent or explicit JSON null)
	aIsNull := !a.Exists() || a.Type == gjson.Null
	bIsNull := !b.Exists() || b.Type == gjson.Null

	// Handle null values (NULLS FIRST semantics)
	if aIsNull && bIsNull {
		return 0
	}
	if aIsNull {
		return -1 // nulls first
	}
	if bIsNull {
		return 1
	}

	// Compare based on type
	switch {
	case a.Type == gjson.Number && b.Type == gjson.Number:
		af, bf := a.Float(), b.Float()
		if af < bf {
			return -1
		} else if af > bf {
			return 1
		}
		return 0
	case a.Type == gjson.String && b.Type == gjson.String:
		return strings.Compare(a.String(), b.String())
	case (a.Type == gjson.True || a.Type == gjson.False) && (b.Type == gjson.True || b.Type == gjson.False):
		// bool comparison: false < true
		aBool, bBool := a.Bool(), b.Bool()
		if !aBool && bBool {
			return -1
		} else if aBool && !bBool {
			return 1
		}
		return 0
	default:
		// Mixed types or unsupported types: fallback to raw JSON string comparison.
		// This means strings (with quotes) sort before numbers due to quote character ASCII value.
		// Users should ensure consistent types within the same JSON field for predictable ordering.
		return strings.Compare(a.Raw, b.Raw)
	}
}

// compareNullsFirst compares two indices for null values using ValidData.
// Returns (comparison result, true) if at least one value is null.
// Returns (0, false) if neither value is null, indicating caller should proceed with value comparison.
// Implements NULLS FIRST semantics: null values are sorted before non-null values.
func compareNullsFirst(validData []bool, i, j int) (int, bool) {
	if len(validData) == 0 {
		return 0, false
	}
	iNull := i < len(validData) && !validData[i]
	jNull := j < len(validData) && !validData[j]
	if iNull && jNull {
		return 0, true // both null, equal
	}
	if iNull {
		return -1, true // nulls first
	}
	if jNull {
		return 1, true
	}
	return 0, false // neither is null, proceed with value comparison
}

// compareOrderByField compares two values for an order_by field at given indices
// Handles both regular fields and JSON fields with paths
// The cache parameter contains pre-extracted JSON values to avoid repeated extraction during sorting
// Returns an error if indices are out of bounds.
func compareOrderByField(field *schemapb.FieldData, orderBy OrderByField, idxI, idxJ int, cache jsonValueCache) (int, error) {
	if orderBy.JSONPath != "" && field.GetType() == schemapb.DataType_JSON {
		if cmp, handled := compareNullsFirst(field.ValidData, idxI, idxJ); handled {
			return cmp, nil
		}
		// JSON subfield comparison using cached values
		// Use FieldName for cache key (e.g., "$meta" for dynamic fields)
		valI := cache.getCachedJSONValue(orderBy.FieldName, orderBy.JSONPath, idxI)
		valJ := cache.getCachedJSONValue(orderBy.FieldName, orderBy.JSONPath, idxJ)
		return compareJSONValues(valI, valJ), nil
	}
	// Regular field comparison
	return compareFieldDataAt(field, idxI, idxJ)
}

// sortResultsByOrderByFields sorts indices based on order_by fields for regular search results.
// The indices slice contains actual data indices (e.g., [3,4,5] for the second query when nq>1).
// Returns an error if comparison fails (e.g., index out of bounds).
func (op *orderByOperator) sortResultsByOrderByFields(result *milvuspb.SearchResults, indices []int) error {
	if len(indices) == 0 {
		return nil
	}
	fieldMap := buildFieldDataMap(result)
	// Pre-extract JSON values only for the indices we need (efficient for nq>1)
	cache := buildJSONValueCache(fieldMap, op.orderByFields, indices)

	// Capture any error that occurs during sorting
	var sortErr error
	sort.SliceStable(indices, func(i, j int) bool {
		if sortErr != nil {
			return false // Stop comparing if error occurred
		}
		idxI, idxJ := indices[i], indices[j]
		for _, orderBy := range op.orderByFields {
			// Use FieldName for lookup (e.g., "$meta" for dynamic fields, set by proxy reducer from schema)
			field := fieldMap[orderBy.FieldName]
			if field == nil {
				// This should never happen if validateOrderByFields passed.
				// Log and skip rather than panic to avoid crashing on edge cases.
				log.Warn("order_by field not found in fieldMap after validation, skipping",
					zap.String("fieldName", orderBy.FieldName))
				continue
			}
			cmp, err := compareOrderByField(field, orderBy, idxI, idxJ, cache)
			if err != nil {
				sortErr = err
				return false
			}
			if cmp != 0 {
				if orderBy.Ascending {
					return cmp < 0
				}
				return cmp > 0
			}
		}
		return false
	})
	return sortErr
}

// sortGroupsByOrderByFields sorts groups by the first row's value in each group.
// Groups are identified by GroupByFieldValue - consecutive results with the same value form a group.
//
// The indices slice contains actual data indices (e.g., [3,4,5] for the second query when nq>1).
// This function sorts the indices in-place based on group ordering.
// Returns an error if comparison fails (e.g., index out of bounds).
//
// IMPORTANT: This function assumes that results with the same GroupByFieldValue are already
// contiguous in the input (i.e., all rows of group A appear together, then all rows of group B, etc.).
// This invariant is guaranteed by the upstream search/reduce pipeline which groups results before
// returning them. If this invariant is violated (e.g., [A, B, A] instead of [A, A, B]), the function
// will treat non-contiguous occurrences as separate groups and produce incorrect ordering.
func (op *orderByOperator) sortGroupsByOrderByFields(result *milvuspb.SearchResults, indices []int) error {
	numResults := len(indices)
	if numResults == 0 {
		return nil
	}

	// All internal pipeline stages emit to the plural channel. The task
	// output boundary downgrades to singular for legacy-wire SDK clients,
	// which runs after orderBy, so this reader sees plural only. orderBy
	// inspects column 0 because orderBy + multi-field composite key is not
	// a pipeline combination constructed today.
	gbvs := result.GetResults().GetGroupByFieldValues()
	if len(gbvs) == 0 {
		// No group by field value, fall back to regular sort
		return op.sortResultsByOrderByFields(result, indices)
	}
	groupByValue := gbvs[0]

	// Find group boundaries by detecting when GroupByFieldValue changes
	// Each group is represented as [startLocalIdx, endLocalIdx) - indices into the 'indices' slice
	// We use actual data indices (indices[i]) to check group boundaries
	type group struct {
		start int // local index into 'indices' slice
		end   int // local index into 'indices' slice
	}
	var groups []group
	groupStart := 0
	for i := 1; i < numResults; i++ {
		// Use actual data indices to check if group changed
		if !isSameGroupByValue(groupByValue, indices[i-1], indices[i]) {
			groups = append(groups, group{start: groupStart, end: i})
			groupStart = i
		}
	}
	// Add the last group
	groups = append(groups, group{start: groupStart, end: numResults})

	// Sort groups based on the first row's value in each group
	fieldMap := buildFieldDataMap(result)
	// Pre-extract JSON values only for the indices we need (efficient for nq>1)
	cache := buildJSONValueCache(fieldMap, op.orderByFields, indices)

	// Capture any error that occurs during sorting
	var sortErr error
	sort.SliceStable(groups, func(i, j int) bool {
		if sortErr != nil {
			return false // Stop comparing if error occurred
		}
		// Use actual data indices for comparison
		dataIdxI, dataIdxJ := indices[groups[i].start], indices[groups[j].start]
		for _, orderBy := range op.orderByFields {
			// Use FieldName for lookup (e.g., "$meta" for dynamic fields, set by proxy reducer from schema)
			field := fieldMap[orderBy.FieldName]
			if field == nil {
				// This should never happen if validateOrderByFields passed.
				// Log and skip rather than panic to avoid crashing on edge cases.
				log.Warn("order_by field not found in fieldMap after validation, skipping",
					zap.String("fieldName", orderBy.FieldName))
				continue
			}
			cmp, err := compareOrderByField(field, orderBy, dataIdxI, dataIdxJ, cache)
			if err != nil {
				sortErr = err
				return false
			}
			if cmp != 0 {
				if orderBy.Ascending {
					return cmp < 0
				}
				return cmp > 0
			}
		}
		return false
	})
	if sortErr != nil {
		return sortErr
	}

	// Rebuild indices array based on sorted groups
	// Collect actual data indices in the new sorted order
	newIndices := make([]int, 0, numResults)
	for _, g := range groups {
		for localIdx := g.start; localIdx < g.end; localIdx++ {
			newIndices = append(newIndices, indices[localIdx])
		}
	}
	copy(indices, newIndices)
	return nil
}

// isSameGroupByValue checks if two indices have the same group by value.
// Note: Float/Double types are not supported for group_by due to floating-point
// precision issues that make equality comparison unreliable.
func isSameGroupByValue(field *schemapb.FieldData, i, j int) bool {
	switch field.GetType() {
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		data := field.GetScalars().GetIntData().GetData()
		if i >= len(data) || j >= len(data) {
			return false
		}
		return data[i] == data[j]
	case schemapb.DataType_Int64:
		data := field.GetScalars().GetLongData().GetData()
		if i >= len(data) || j >= len(data) {
			return false
		}
		return data[i] == data[j]
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		data := field.GetScalars().GetStringData().GetData()
		if i >= len(data) || j >= len(data) {
			return false
		}
		return data[i] == data[j]
	case schemapb.DataType_Bool:
		data := field.GetScalars().GetBoolData().GetData()
		if i >= len(data) || j >= len(data) {
			return false
		}
		return data[i] == data[j]
	}
	return false
}

// compareFieldDataAt compares two values in field data at indices i and j.
// Returns -1 if value[i] < value[j], 0 if equal, 1 if value[i] > value[j].
// Returns an error if indices are out of bounds (indicates a bug in the pipeline).
//
// Note on Float/Double NaN handling: This function does not explicitly handle NaN values
// because Milvus rejects NaN and Infinity at insert time via proxy validation
// (see task_insert.go withNANCheck() -> validate_util.go -> typeutil.VerifyFloat).
// Therefore, NaN values cannot exist in stored data and will never reach this comparison.
func compareFieldDataAt(field *schemapb.FieldData, i, j int) (int, error) {
	if cmp, handled := compareNullsFirst(field.ValidData, i, j); handled {
		return cmp, nil
	}

	switch field.GetType() {
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		data := field.GetScalars().GetIntData().GetData()
		if i >= len(data) || j >= len(data) {
			return 0, merr.WrapErrServiceInternal(fmt.Sprintf("compareFieldDataAt: index out of bounds for Int field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data)))
		}
		if data[i] < data[j] {
			return -1, nil
		} else if data[i] > data[j] {
			return 1, nil
		}
		return 0, nil
	case schemapb.DataType_Int64:
		data := field.GetScalars().GetLongData().GetData()
		if i >= len(data) || j >= len(data) {
			return 0, merr.WrapErrServiceInternal(fmt.Sprintf("compareFieldDataAt: index out of bounds for Int64 field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data)))
		}
		if data[i] < data[j] {
			return -1, nil
		} else if data[i] > data[j] {
			return 1, nil
		}
		return 0, nil
	case schemapb.DataType_Float:
		data := field.GetScalars().GetFloatData().GetData()
		if i >= len(data) || j >= len(data) {
			return 0, merr.WrapErrServiceInternal(fmt.Sprintf("compareFieldDataAt: index out of bounds for Float field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data)))
		}
		if data[i] < data[j] {
			return -1, nil
		} else if data[i] > data[j] {
			return 1, nil
		}
		return 0, nil
	case schemapb.DataType_Double:
		data := field.GetScalars().GetDoubleData().GetData()
		if i >= len(data) || j >= len(data) {
			return 0, merr.WrapErrServiceInternal(fmt.Sprintf("compareFieldDataAt: index out of bounds for Double field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data)))
		}
		if data[i] < data[j] {
			return -1, nil
		} else if data[i] > data[j] {
			return 1, nil
		}
		return 0, nil
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		data := field.GetScalars().GetStringData().GetData()
		if i >= len(data) || j >= len(data) {
			return 0, merr.WrapErrServiceInternal(fmt.Sprintf("compareFieldDataAt: index out of bounds for String field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data)))
		}
		if data[i] < data[j] {
			return -1, nil
		} else if data[i] > data[j] {
			return 1, nil
		}
		return 0, nil
	case schemapb.DataType_JSON:
		// JSON fields are sorted by their raw string value (lexicographic byte comparison),
		// not by semantic JSON value. For example, "2" > "10" because '2' > '1' in bytes.
		data := field.GetScalars().GetJsonData().GetData()
		if i >= len(data) || j >= len(data) {
			return 0, merr.WrapErrServiceInternal(fmt.Sprintf("compareFieldDataAt: index out of bounds for JSON field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data)))
		}
		return bytes.Compare(data[i], data[j]), nil
	case schemapb.DataType_Bool:
		data := field.GetScalars().GetBoolData().GetData()
		if i >= len(data) || j >= len(data) {
			return 0, merr.WrapErrServiceInternal(fmt.Sprintf("compareFieldDataAt: index out of bounds for Bool field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data)))
		}
		// false < true
		if !data[i] && data[j] {
			return -1, nil
		} else if data[i] && !data[j] {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, merr.WrapErrServiceInternal(fmt.Sprintf("compareFieldDataAt: unsupported field type %s for field %s", field.GetType().String(), field.GetFieldName()))
	}
}

// reorderResults reorders all result arrays based on the sorted indices
func (op *orderByOperator) reorderResults(result *milvuspb.SearchResults, indices []int) error {
	results := result.GetResults()
	n := len(indices)

	// Reorder IDs
	if ids := results.GetIds(); ids != nil {
		if intIds := ids.GetIntId(); intIds != nil {
			newData := make([]int64, n)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= len(intIds.Data) {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderResults: index %d out of bounds for int IDs (len=%d)", oldIdx, len(intIds.Data)))
				}
				newData[newIdx] = intIds.Data[oldIdx]
			}
			intIds.Data = newData
		} else if strIds := ids.GetStrId(); strIds != nil {
			newData := make([]string, n)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= len(strIds.Data) {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderResults: index %d out of bounds for string IDs (len=%d)", oldIdx, len(strIds.Data)))
				}
				newData[newIdx] = strIds.Data[oldIdx]
			}
			strIds.Data = newData
		}
	}

	// Reorder scores
	if len(results.Scores) > 0 {
		newScores := make([]float32, n)
		for newIdx, oldIdx := range indices {
			if oldIdx < 0 || oldIdx >= len(results.Scores) {
				return merr.WrapErrServiceInternal(fmt.Sprintf("reorderResults: index %d out of bounds for scores (len=%d)", oldIdx, len(results.Scores)))
			}
			newScores[newIdx] = results.Scores[oldIdx]
		}
		results.Scores = newScores
	}

	// Reorder field data
	for _, field := range results.FieldsData {
		if err := reorderFieldData(field, indices); err != nil {
			return err
		}
	}

	// Reorder every group-by column — all internal stages emit plural; the
	// task output boundary handles legacy-wire singular downgrade after.
	for _, gbv := range results.GetGroupByFieldValues() {
		if err := reorderFieldData(gbv, indices); err != nil {
			return err
		}
	}
	return nil
}

// reorderFieldData reorders field data based on indices
func reorderFieldData(field *schemapb.FieldData, indices []int) error {
	n := len(indices)
	switch field.GetType() {
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		if data := field.GetScalars().GetIntData(); data != nil && len(data.Data) > 0 {
			newData := make([]int32, n)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= len(data.Data) {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: index %d out of bounds for %s field %s (len=%d)", oldIdx, field.GetType().String(), field.GetFieldName(), len(data.Data)))
				}
				newData[newIdx] = data.Data[oldIdx]
			}
			data.Data = newData
		}
	case schemapb.DataType_Int64:
		if data := field.GetScalars().GetLongData(); data != nil && len(data.Data) > 0 {
			newData := make([]int64, n)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= len(data.Data) {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: index %d out of bounds for Int64 field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data)))
				}
				newData[newIdx] = data.Data[oldIdx]
			}
			data.Data = newData
		}
	case schemapb.DataType_Float:
		if data := field.GetScalars().GetFloatData(); data != nil && len(data.Data) > 0 {
			newData := make([]float32, n)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= len(data.Data) {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: index %d out of bounds for Float field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data)))
				}
				newData[newIdx] = data.Data[oldIdx]
			}
			data.Data = newData
		}
	case schemapb.DataType_Double:
		if data := field.GetScalars().GetDoubleData(); data != nil && len(data.Data) > 0 {
			newData := make([]float64, n)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= len(data.Data) {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: index %d out of bounds for Double field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data)))
				}
				newData[newIdx] = data.Data[oldIdx]
			}
			data.Data = newData
		}
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		if data := field.GetScalars().GetStringData(); data != nil && len(data.Data) > 0 {
			newData := make([]string, n)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= len(data.Data) {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: index %d out of bounds for %s field %s (len=%d)", oldIdx, field.GetType().String(), field.GetFieldName(), len(data.Data)))
				}
				newData[newIdx] = data.Data[oldIdx]
			}
			data.Data = newData
		}
	case schemapb.DataType_Bool:
		if data := field.GetScalars().GetBoolData(); data != nil && len(data.Data) > 0 {
			newData := make([]bool, n)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= len(data.Data) {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: index %d out of bounds for Bool field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data)))
				}
				newData[newIdx] = data.Data[oldIdx]
			}
			data.Data = newData
		}
	case schemapb.DataType_JSON:
		if data := field.GetScalars().GetJsonData(); data != nil && len(data.Data) > 0 {
			newData := make([][]byte, n)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= len(data.Data) {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: index %d out of bounds for JSON field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data)))
				}
				newData[newIdx] = data.Data[oldIdx]
			}
			data.Data = newData
		}
	case schemapb.DataType_Array:
		if data := field.GetScalars().GetArrayData(); data != nil && len(data.Data) > 0 {
			newData := make([]*schemapb.ScalarField, n)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= len(data.Data) {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: index %d out of bounds for Array field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data)))
				}
				newData[newIdx] = data.Data[oldIdx]
			}
			data.Data = newData
		}
	case schemapb.DataType_FloatVector:
		vectors := field.GetVectors()
		if vectors != nil && vectors.GetFloatVector() != nil {
			dim := int(vectors.GetDim())
			if dim <= 0 {
				return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: invalid dimension %d for FloatVector field %s", dim, field.GetFieldName()))
			}
			data := vectors.GetFloatVector().GetData()
			if len(data) != n*dim {
				return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: FloatVector field %s has %d elements, expected %d (n=%d, dim=%d)", field.GetFieldName(), len(data), n*dim, n, dim))
			}
			newData := make([]float32, n*dim)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= n {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: index %d out of bounds for FloatVector field %s (n=%d)", oldIdx, field.GetFieldName(), n))
				}
				srcStart := oldIdx * dim
				dstStart := newIdx * dim
				copy(newData[dstStart:dstStart+dim], data[srcStart:srcStart+dim])
			}
			vectors.GetFloatVector().Data = newData
		}
	case schemapb.DataType_BinaryVector:
		vectors := field.GetVectors()
		if vectors != nil && len(vectors.GetBinaryVector()) > 0 {
			dim := int(vectors.GetDim())
			bytesPerVector := dim / 8
			if bytesPerVector <= 0 {
				return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: invalid dimension %d for BinaryVector field %s", dim, field.GetFieldName()))
			}
			data := vectors.GetBinaryVector()
			if len(data) != n*bytesPerVector {
				return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: BinaryVector field %s has %d bytes, expected %d (n=%d, bytesPerVector=%d)", field.GetFieldName(), len(data), n*bytesPerVector, n, bytesPerVector))
			}
			newData := make([]byte, n*bytesPerVector)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= n {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: index %d out of bounds for BinaryVector field %s (n=%d)", oldIdx, field.GetFieldName(), n))
				}
				srcStart := oldIdx * bytesPerVector
				dstStart := newIdx * bytesPerVector
				copy(newData[dstStart:dstStart+bytesPerVector], data[srcStart:srcStart+bytesPerVector])
			}
			vectors.Data = &schemapb.VectorField_BinaryVector{BinaryVector: newData}
		}
	case schemapb.DataType_Float16Vector:
		vectors := field.GetVectors()
		if vectors != nil && len(vectors.GetFloat16Vector()) > 0 {
			dim := int(vectors.GetDim())
			if dim <= 0 {
				return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: invalid dimension %d for Float16Vector field %s", dim, field.GetFieldName()))
			}
			bytesPerVector := dim * 2 // 2 bytes per float16
			data := vectors.GetFloat16Vector()
			if len(data) != n*bytesPerVector {
				return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: Float16Vector field %s has %d bytes, expected %d (n=%d, bytesPerVector=%d)", field.GetFieldName(), len(data), n*bytesPerVector, n, bytesPerVector))
			}
			newData := make([]byte, n*bytesPerVector)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= n {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: index %d out of bounds for Float16Vector field %s (n=%d)", oldIdx, field.GetFieldName(), n))
				}
				srcStart := oldIdx * bytesPerVector
				dstStart := newIdx * bytesPerVector
				copy(newData[dstStart:dstStart+bytesPerVector], data[srcStart:srcStart+bytesPerVector])
			}
			vectors.Data = &schemapb.VectorField_Float16Vector{Float16Vector: newData}
		}
	case schemapb.DataType_BFloat16Vector:
		vectors := field.GetVectors()
		if vectors != nil && len(vectors.GetBfloat16Vector()) > 0 {
			dim := int(vectors.GetDim())
			if dim <= 0 {
				return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: invalid dimension %d for BFloat16Vector field %s", dim, field.GetFieldName()))
			}
			bytesPerVector := dim * 2 // 2 bytes per bfloat16
			data := vectors.GetBfloat16Vector()
			if len(data) != n*bytesPerVector {
				return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: BFloat16Vector field %s has %d bytes, expected %d (n=%d, bytesPerVector=%d)", field.GetFieldName(), len(data), n*bytesPerVector, n, bytesPerVector))
			}
			newData := make([]byte, n*bytesPerVector)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= n {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: index %d out of bounds for BFloat16Vector field %s (n=%d)", oldIdx, field.GetFieldName(), n))
				}
				srcStart := oldIdx * bytesPerVector
				dstStart := newIdx * bytesPerVector
				copy(newData[dstStart:dstStart+bytesPerVector], data[srcStart:srcStart+bytesPerVector])
			}
			vectors.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: newData}
		}
	case schemapb.DataType_SparseFloatVector:
		vectors := field.GetVectors()
		if vectors != nil && vectors.GetSparseFloatVector() != nil {
			sparseData := vectors.GetSparseFloatVector()
			contents := sparseData.GetContents()
			if len(contents) != n {
				return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: SparseFloatVector field %s has %d elements, expected %d", field.GetFieldName(), len(contents), n))
			}
			newContents := make([][]byte, n)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= n {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: index %d out of bounds for SparseFloatVector field %s (n=%d)", oldIdx, field.GetFieldName(), n))
				}
				newContents[newIdx] = contents[oldIdx]
			}
			sparseData.Contents = newContents
		}
	case schemapb.DataType_Int8Vector:
		vectors := field.GetVectors()
		if vectors != nil && len(vectors.GetInt8Vector()) > 0 {
			dim := int(vectors.GetDim())
			if dim <= 0 {
				return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: invalid dimension %d for Int8Vector field %s", dim, field.GetFieldName()))
			}
			data := vectors.GetInt8Vector()
			if len(data) != n*dim {
				return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: Int8Vector field %s has %d bytes, expected %d (n=%d, dim=%d)", field.GetFieldName(), len(data), n*dim, n, dim))
			}
			newData := make([]byte, n*dim)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= n {
					return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: index %d out of bounds for Int8Vector field %s (n=%d)", oldIdx, field.GetFieldName(), n))
				}
				srcStart := oldIdx * dim
				dstStart := newIdx * dim
				copy(newData[dstStart:dstStart+dim], data[srcStart:srcStart+dim])
			}
			vectors.Data = &schemapb.VectorField_Int8Vector{Int8Vector: newData}
		}
	default:
		return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: unhandled data type %s", field.GetType().String()))
	}

	// Reorder valid data if present
	if len(field.ValidData) > 0 {
		newValidData := make([]bool, n)
		for newIdx, oldIdx := range indices {
			if oldIdx < 0 || oldIdx >= len(field.ValidData) {
				return merr.WrapErrServiceInternal(fmt.Sprintf("reorderFieldData: index %d out of bounds for ValidData of field %s (len=%d)", oldIdx, field.GetFieldName(), len(field.ValidData)))
			}
			newValidData[newIdx] = field.ValidData[oldIdx]
		}
		field.ValidData = newValidData
	}
	return nil
}

func mergeIDsFunc(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	multipleMilvusResults := inputs[0].([]*milvuspb.SearchResults)
	idInt64Type := false
	idsList := lo.FilterMap(multipleMilvusResults, func(m *milvuspb.SearchResults, _ int) (*schemapb.IDs, bool) {
		if m.GetResults().GetIds().GetIntId() != nil {
			idInt64Type = true
		}
		return m.Results.Ids, true
	})

	uniqueIDs := &schemapb.IDs{}
	if idInt64Type {
		idsSet := typeutil.NewSet[int64]()
		for _, ids := range idsList {
			if data := ids.GetIntId().GetData(); data != nil {
				idsSet.Insert(data...)
			}
		}
		uniqueIDs.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: idsSet.Collect(),
			},
		}
	} else {
		idsSet := typeutil.NewSet[string]()
		for _, ids := range idsList {
			if data := ids.GetStrId().GetData(); data != nil {
				idsSet.Insert(data...)
			}
		}
		uniqueIDs.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: idsSet.Collect(),
			},
		}
	}
	return []any{uniqueIDs}, nil
}

type pipeline struct {
	name         string
	nodes        []*Node
	traceEnabled bool
}

func newPipeline(pipeDef *pipelineDef, t *searchTask) (*pipeline, error) {
	nodes := make([]*Node, len(pipeDef.nodes))
	for i, def := range pipeDef.nodes {
		node, err := NewNode(def, t)
		if err != nil {
			return nil, err
		}
		nodes[i] = node
	}
	return &pipeline{name: pipeDef.name, nodes: nodes, traceEnabled: t.traceEnabled}, nil
}

func (p *pipeline) AddNodes(t *searchTask, nodes ...*nodeDef) error {
	for _, def := range nodes {
		node, err := NewNode(def, t)
		if err != nil {
			return err
		}
		p.nodes = append(p.nodes, node)
	}
	return nil
}

func (p *pipeline) Run(ctx context.Context, span trace.Span, toReduceResults []*internalpb.SearchResults, storageCost segcore.StorageCost) (*milvuspb.SearchResults, segcore.StorageCost, error) {
	log.Ctx(ctx).Debug("SearchPipeline run", zap.Stringer("pipeline", p))
	pTrace := newPipelineTrace(p.traceEnabled)
	msg := opMsg{}
	msg[pipelineInput] = toReduceResults
	msg[pipelineStorageCost] = storageCost
	for _, node := range p.nodes {
		var err error
		log.Ctx(ctx).Debug("SearchPipeline run node", zap.String("node", node.name))
		msg, err = node.Run(ctx, span, msg)
		if err != nil {
			log.Ctx(ctx).Error("Run node failed: ", zap.String("err", err.Error()))
			return nil, storageCost, err
		}
		pTrace.TraceMsg(node.opName, msg)
	}
	pTrace.LogIfEnabled(ctx, p.name)
	return msg[pipelineOutput].(*milvuspb.SearchResults), msg[pipelineStorageCost].(segcore.StorageCost), nil
}

func (p *pipeline) String() string {
	buf := bytes.NewBufferString(fmt.Sprintf("SearchPipeline: %s", p.name))
	for _, node := range p.nodes {
		fmt.Fprintf(buf, "  %s -> %s", node.name, node.outputs)
	}
	return buf.String()
}

type pipelineDef struct {
	name  string
	nodes []*nodeDef
}

var endNode = &nodeDef{
	name:    "filter_field",
	inputs:  []string{"result", "reduced"},
	outputs: []string{pipelineOutput},
	opName:  endOp,
}

var highlightNode = &nodeDef{
	name:    "highlight",
	inputs:  []string{"result"},
	outputs: []string{pipelineOutput},
	opName:  highlightOp,
}

var searchWithAggPipe = &pipelineDef{
	name: "searchWithAgg",
	nodes: []*nodeDef{
		{
			// searchReduceOp performs cross-shard composite-key group reduce
			// (reduceSearchResultDataWithMultiGroupBy) before hierarchy compute.
			name:    "reduce",
			inputs:  []string{pipelineInput, pipelineStorageCost},
			outputs: []string{"reduced", "metrics"},
			opName:  searchReduceOp,
		},
		{
			name:    "agg",
			inputs:  []string{"reduced"},
			outputs: []string{pipelineOutput},
			opName:  aggOp,
		},
	},
}

var searchPipe = &pipelineDef{
	name: "search",
	nodes: []*nodeDef{
		{
			name:    "reduce",
			inputs:  []string{pipelineInput, pipelineStorageCost},
			outputs: []string{"reduced", "metrics"},
			opName:  searchReduceOp,
		},
		{
			name:    "pick",
			inputs:  []string{"reduced"},
			outputs: []string{"result"},
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					result := inputs[0].([]*milvuspb.SearchResults)[0]
					return []any{result}, nil
				},
			},
			opName: lambdaOp,
		},
	},
}

var searchWithRequeryPipe = &pipelineDef{
	name: "searchWithRequery",
	nodes: []*nodeDef{
		{
			name:    "reduce",
			inputs:  []string{pipelineInput, pipelineStorageCost},
			outputs: []string{"reduced", "metrics"},
			opName:  searchReduceOp,
		},
		{
			name:    "merge",
			inputs:  []string{"reduced"},
			outputs: []string{"unique_ids"},
			opName:  lambdaOp,
			params: map[string]any{
				lambdaParamKey: mergeIDsFunc,
			},
		},
		{
			name:    "requery",
			inputs:  []string{"unique_ids", pipelineStorageCost},
			outputs: []string{"fields", pipelineStorageCost},
			opName:  requeryOp,
		},
		{
			name:    "gen_ids",
			inputs:  []string{"reduced"},
			outputs: []string{"ids"},
			opName:  lambdaOp,
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					return []any{[]*schemapb.IDs{inputs[0].([]*milvuspb.SearchResults)[0].Results.Ids}}, nil
				},
			},
		},
		{
			name:    "organize",
			inputs:  []string{"fields", "ids"},
			outputs: []string{"organized_fields"},
			opName:  organizeOp,
		},
		{
			name:    "pick",
			inputs:  []string{"reduced", "organized_fields"},
			outputs: []string{"result"},
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					result := inputs[0].([]*milvuspb.SearchResults)[0]
					fields := inputs[1].([][]*schemapb.FieldData)
					result.Results.FieldsData = fields[0]
					return []any{result}, nil
				},
			},
			opName: lambdaOp,
		},
	},
}

var searchWithRerankPipe = &pipelineDef{
	name: "searchWithRerank",
	nodes: []*nodeDef{
		{
			name:    "reduce",
			inputs:  []string{pipelineInput, pipelineStorageCost},
			outputs: []string{"reduced", "metrics"},
			opName:  searchReduceOp,
		},
		{
			name:    "rerank",
			inputs:  []string{"reduced", "metrics"},
			outputs: []string{"rank_result"},
			opName:  rerankOp,
		},
		{
			name:    "pick",
			inputs:  []string{"reduced", "rank_result"},
			outputs: []string{"fields", "ids"},
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					return []any{
						inputs[0].([]*milvuspb.SearchResults)[0].Results.FieldsData,
						[]*schemapb.IDs{inputs[1].(*milvuspb.SearchResults).Results.Ids},
					}, nil
				},
			},
			opName: lambdaOp,
		},
		{
			name:    "organize",
			inputs:  []string{"fields", "ids"},
			outputs: []string{"organized_fields"},
			opName:  organizeOp,
		},
		{
			name:    "result",
			inputs:  []string{"rank_result", "organized_fields"},
			outputs: []string{"result"},
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					result := inputs[0].(*milvuspb.SearchResults)
					fields := inputs[1].([][]*schemapb.FieldData)
					result.Results.FieldsData = fields[0]
					return []any{result}, nil
				},
			},
			opName: lambdaOp,
		},
	},
}

var searchWithRerankRequeryPipe = &pipelineDef{
	name: "searchWithRerankRequery",
	nodes: []*nodeDef{
		{
			name:    "reduce",
			inputs:  []string{pipelineInput, pipelineStorageCost},
			outputs: []string{"reduced", "metrics"},
			opName:  searchReduceOp,
		},
		{
			name:    "rerank",
			inputs:  []string{"reduced", "metrics"},
			outputs: []string{"rank_result"},
			opName:  rerankOp,
		},
		{
			name:    "pick_ids",
			inputs:  []string{"rank_result"},
			outputs: []string{"ids"},
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					return []any{
						inputs[0].(*milvuspb.SearchResults).Results.Ids,
					}, nil
				},
			},
			opName: lambdaOp,
		},
		{
			name:    "requery",
			inputs:  []string{"ids", pipelineStorageCost},
			outputs: []string{"fields", pipelineStorageCost},
			opName:  requeryOp,
		},
		{
			name:    "to_ids_list",
			inputs:  []string{"ids"},
			outputs: []string{"ids"},
			opName:  lambdaOp,
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					return []any{[]*schemapb.IDs{inputs[0].(*schemapb.IDs)}}, nil
				},
			},
		},
		{
			name:    "organize",
			inputs:  []string{"fields", "ids"},
			outputs: []string{"organized_fields"},
			opName:  organizeOp,
		},
		{
			name:    "result",
			inputs:  []string{"rank_result", "organized_fields"},
			outputs: []string{"result"},
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					result := inputs[0].(*milvuspb.SearchResults)
					fields := inputs[1].([][]*schemapb.FieldData)
					result.Results.FieldsData = fields[0]
					return []any{result}, nil
				},
			},
			opName: lambdaOp,
		},
	},
}

var hybridSearchPipe = &pipelineDef{
	name: "hybridSearchPipe",
	nodes: []*nodeDef{
		{
			name:    "reduce",
			inputs:  []string{pipelineInput, pipelineStorageCost},
			outputs: []string{"reduced", "metrics"},
			opName:  hybridSearchReduceOp,
		},
		{
			name:    "rerank",
			inputs:  []string{"reduced", "metrics"},
			outputs: []string{"rank_result"},
			opName:  rerankOp,
		},
		{
			name:    "assemble",
			inputs:  []string{"reduced", "rank_result"},
			outputs: []string{"result"},
			opName:  hybridAssembleOp,
		},
	},
}

var hybridSearchWithRequeryAndRerankByFieldDataPipe = &pipelineDef{
	name: "hybridSearchWithRequeryAndRerankByDataPipe",
	nodes: []*nodeDef{
		{
			name:    "reduce",
			inputs:  []string{pipelineInput, pipelineStorageCost},
			outputs: []string{"reduced", "metrics"},
			opName:  hybridSearchReduceOp,
		},
		{
			name:    "merge_ids",
			inputs:  []string{"reduced"},
			outputs: []string{"ids"},
			opName:  lambdaOp,
			params: map[string]any{
				lambdaParamKey: mergeIDsFunc,
			},
		},
		{
			name:    "requery",
			inputs:  []string{"ids", pipelineStorageCost},
			outputs: []string{"fields", pipelineStorageCost},
			opName:  requeryOp,
		},
		{
			name:    "parse_ids",
			inputs:  []string{"reduced"},
			outputs: []string{"id_list"},
			opName:  lambdaOp,
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					multipleMilvusResults := inputs[0].([]*milvuspb.SearchResults)
					idsList := lo.FilterMap(multipleMilvusResults, func(m *milvuspb.SearchResults, _ int) (*schemapb.IDs, bool) {
						return m.Results.Ids, true
					})
					return []any{idsList}, nil
				},
			},
		},
		{
			name:    "organize_rank_data",
			inputs:  []string{"fields", "id_list"},
			outputs: []string{"organized_fields"},
			opName:  organizeOp,
		},
		{
			name:    "gen_rank_data",
			inputs:  []string{"reduced", "organized_fields"},
			outputs: []string{"rank_data"},
			opName:  lambdaOp,
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					results := inputs[0].([]*milvuspb.SearchResults)
					fields := inputs[1].([][]*schemapb.FieldData)
					for i := 0; i < len(results); i++ {
						results[i].Results.FieldsData = fields[i]
					}
					return []any{results}, nil
				},
			},
		},
		{
			name:    "rerank",
			inputs:  []string{"rank_data", "metrics"},
			outputs: []string{"rank_result"},
			opName:  rerankOp,
		},
		{
			name:    "pick_ids",
			inputs:  []string{"rank_result"},
			outputs: []string{"ids"},
			opName:  lambdaOp,
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					return []any{[]*schemapb.IDs{inputs[0].(*milvuspb.SearchResults).Results.Ids}}, nil
				},
			},
		},
		{
			name:    "organize_result",
			inputs:  []string{"fields", "ids"},
			outputs: []string{"organized_fields"},
			opName:  organizeOp,
		},
		{
			name:    "result",
			inputs:  []string{"rank_result", "organized_fields"},
			outputs: []string{"result"},
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					result := inputs[0].(*milvuspb.SearchResults)
					fields := inputs[1].([][]*schemapb.FieldData)
					result.Results.FieldsData = fields[0]
					return []any{result}, nil
				},
			},
			opName: lambdaOp,
		},
	},
}

var hybridSearchWithRequeryPipe = &pipelineDef{
	name: "hybridSearchWithRequeryPipe",
	nodes: []*nodeDef{
		{
			name:    "reduce",
			inputs:  []string{pipelineInput, pipelineStorageCost},
			outputs: []string{"reduced", "metrics"},
			opName:  hybridSearchReduceOp,
		},
		{
			name:    "rerank",
			inputs:  []string{"reduced", "metrics"},
			outputs: []string{"rank_result"},
			opName:  rerankOp,
		},
		{
			name:    "pick_ids",
			inputs:  []string{"rank_result"},
			outputs: []string{"ids"},
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					return []any{
						inputs[0].(*milvuspb.SearchResults).Results.Ids,
					}, nil
				},
			},
			opName: lambdaOp,
		},
		{
			name:    "requery",
			inputs:  []string{"ids", pipelineStorageCost},
			outputs: []string{"fields", pipelineStorageCost},
			opName:  requeryOp,
		},
		{
			name:    "organize",
			inputs:  []string{"fields", "ids"},
			outputs: []string{"organized_fields"},
			opName:  organizeOp,
		},
		{
			name:    "result",
			inputs:  []string{"rank_result", "organized_fields"},
			outputs: []string{"result"},
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					result := inputs[0].(*milvuspb.SearchResults)
					fields := inputs[1].([][]*schemapb.FieldData)
					result.Results.FieldsData = fields[0]
					return []any{result}, nil
				},
			},
			opName: lambdaOp,
		},
		// endOp node is appended by newSearchPipeline; do not add one here.
	},
}

// searchWithOrderByPipe: reduce → requery → organize → order_by
// For common search with order_by_fields
var searchWithOrderByPipe = &pipelineDef{
	name: "searchWithOrderBy",
	nodes: []*nodeDef{
		{
			name:    "reduce",
			inputs:  []string{pipelineInput, pipelineStorageCost},
			outputs: []string{"reduced", "metrics"},
			opName:  searchReduceOp,
		},
		{
			name:    "merge",
			inputs:  []string{"reduced"},
			outputs: []string{"unique_ids"},
			opName:  lambdaOp,
			params: map[string]any{
				lambdaParamKey: mergeIDsFunc,
			},
		},
		{
			name:    "requery",
			inputs:  []string{"unique_ids", pipelineStorageCost},
			outputs: []string{"fields", pipelineStorageCost},
			opName:  requeryOp,
		},
		{
			name:    "gen_ids",
			inputs:  []string{"reduced"},
			outputs: []string{"ids"},
			opName:  lambdaOp,
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					return []any{[]*schemapb.IDs{inputs[0].([]*milvuspb.SearchResults)[0].Results.Ids}}, nil
				},
			},
		},
		{
			name:    "organize",
			inputs:  []string{"fields", "ids"},
			outputs: []string{"organized_fields"},
			opName:  organizeOp,
		},
		{
			name:    "pick",
			inputs:  []string{"reduced", "organized_fields"},
			outputs: []string{"result"},
			params: map[string]any{
				lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
					result := inputs[0].([]*milvuspb.SearchResults)[0]
					fields := inputs[1].([][]*schemapb.FieldData)
					result.Results.FieldsData = fields[0]
					return []any{result}, nil
				},
			},
			opName: lambdaOp,
		},
		{
			name:    "order_by",
			inputs:  []string{"result"},
			outputs: []string{"result"},
			opName:  orderByOp,
		},
	},
}

func newBuiltInPipeline(t *searchTask) (*pipeline, error) {
	if t.aggCtx != nil {
		return newPipeline(searchWithAggPipe, t)
	}

	hasOrderBy := len(t.orderByFields) > 0

	// Common search with order_by: reduce → requery → order_by
	if !t.GetIsAdvanced() && hasOrderBy {
		return newPipeline(searchWithOrderByPipe, t)
	}

	hasRerank := t.rerankMeta != nil
	if !t.GetIsAdvanced() && !t.needRequery && !hasRerank {
		return newPipeline(searchPipe, t)
	}
	if !t.GetIsAdvanced() && t.needRequery && !hasRerank {
		return newPipeline(searchWithRequeryPipe, t)
	}
	if !t.GetIsAdvanced() && !t.needRequery && hasRerank {
		return newPipeline(searchWithRerankPipe, t)
	}
	if !t.GetIsAdvanced() && t.needRequery && hasRerank {
		return newPipeline(searchWithRerankRequeryPipe, t)
	}
	if t.GetIsAdvanced() && !t.needRequery {
		return newPipeline(hybridSearchPipe, t)
	}
	if t.GetIsAdvanced() && t.needRequery {
		if t.rerankMeta != nil && len(t.rerankMeta.GetInputFieldIDs()) > 0 {
			// When the function score need field data, we need to requery to fetch the field data before rerank.
			// The requery will fetch the field data of all search results,
			// so there's some memory overhead.
			return newPipeline(hybridSearchWithRequeryAndRerankByFieldDataPipe, t)
		} else {
			// Otherwise, we can rerank and limit the requery size to the limit.
			// so the memory overhead is less than the hybridSearchWithRequeryAndRerankByFieldDataPipe.
			return newPipeline(hybridSearchWithRequeryPipe, t)
		}
	}
	return nil, merr.WrapErrServiceInternal("unsupported pipeline")
}

func newSearchPipeline(t *searchTask) (*pipeline, error) {
	p, err := newBuiltInPipeline(t)
	if err != nil {
		return nil, err
	}

	if t.aggCtx != nil {
		return p, nil
	}

	if t.highlighter != nil {
		err := p.AddNodes(t, highlightNode, endNode)
		if err != nil {
			return nil, err
		}
	} else {
		err := p.AddNodes(t, endNode)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}

func aggregatedAllSearchCount(searchResults []*milvuspb.SearchResults) int64 {
	allSearchCount := int64(0)
	for _, sr := range searchResults {
		if sr != nil && sr.GetResults() != nil {
			allSearchCount += sr.GetResults().GetAllSearchCount()
		}
	}
	return allSearchCount
}
