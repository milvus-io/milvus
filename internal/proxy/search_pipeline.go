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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/search_agg"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	chaintypes "github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
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
			return nil, merr.WrapErrServiceInternalMsg("Node [%s]'s input %s not found", n.name, input)
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
		return nil, merr.WrapErrServiceInternalMsg("Node [%s] output size not match operator output size", n.name)
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
	searchReduceOp        = "search_reduce"
	hybridSearchReduceOp  = "hybrid_search_reduce"
	rerankOp              = "rerank"
	requeryOp             = "requery"
	organizeOp            = "organize"
	elementBestCollapseOp = "element_best_collapse"
	elementKeyRestoreOp   = "element_key_restore"
	hybridAssembleOp      = "hybrid_assemble"
	endOp                 = "end"
	lambdaOp              = "lambda"
	highlightOp           = "highlight"
	orderByOp             = "order_by"
)

const (
	pipelineOutput      = "output"
	pipelineInput       = "input"
	pipelineStorageCost = "storage_cost"
)

var opFactory = map[string]func(t *searchTask, params map[string]any) (operator, error){
	searchReduceOp:        newSearchReduceOperator,
	hybridSearchReduceOp:  newHybridSearchReduceOperator,
	aggOp:                 newAggregateOperator,
	rerankOp:              newRerankOperator,
	organizeOp:            newOrganizeOperator,
	elementBestCollapseOp: newElementBestCollapseOperator,
	elementKeyRestoreOp:   newElementKeyRestoreOperator,
	hybridAssembleOp:      newHybridAssembleOperator,
	requeryOp:             newRequeryOperator,
	lambdaOp:              newLambdaOperator,
	endOp:                 newEndOperator,
	highlightOp:           newHighlightOperator,
	orderByOp:             newOrderByOperator,
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

const reduceOffsetParamKey = "reduce_offset"

func newSearchReduceOperator(t *searchTask, params map[string]any) (operator, error) {
	pkField, err := t.schema.GetPkField()
	if err != nil {
		return nil, err
	}
	offset := t.GetOffset()
	if v, ok := params[reduceOffsetParamKey].(int64); ok {
		offset = v
	}
	return &searchReduceOperator{
		traceCtx:            t.TraceCtx(),
		primaryFieldSchema:  pkField,
		nq:                  t.GetNq(),
		topK:                t.GetTopk(),
		offset:              offset,
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

type elementBestCollapseOperator struct {
	configs            []elementCollapseConfig
	elementLevelHybrid bool
}

func newElementBestCollapseOperator(t *searchTask, _ map[string]any) (operator, error) {
	return &elementBestCollapseOperator{
		configs:            t.hybridCollapseConfigs(),
		elementLevelHybrid: t.hybridElementLevel,
	}, nil
}

func (t *searchTask) hybridCollapseConfigs() []elementCollapseConfig {
	if len(t.hybridSubSearchInfos) == 0 {
		return nil
	}
	configs := make([]elementCollapseConfig, len(t.hybridSubSearchInfos))
	for i, info := range t.hybridSubSearchInfos {
		configs[i] = info.Collapse
		if configs[i].Strategy == "" {
			configs[i] = defaultElementCollapseConfig()
		}
	}
	return configs
}

// elementBestCollapseOperator normalizes element-level hybrid sub-search results
// into row-level results before rerank, or prepares same-struct element-level
// hybrid results with proxy-internal element keys so rerank can distinguish
// different elements from the same row.
func (op *elementBestCollapseOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	if len(inputs) < 2 {
		return nil, merr.WrapErrServiceInternal("element best collapse: missing inputs")
	}
	results, ok := inputs[0].([]*milvuspb.SearchResults)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("element best collapse: inputs[0] must be []*SearchResults, got %T", inputs[0])
	}
	metrics, ok := inputs[1].([]string)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("element best collapse: inputs[1] must be []string, got %T", inputs[1])
	}
	if len(metrics) != len(results) {
		return nil, merr.WrapErrServiceInternalMsg("element best collapse: metrics length (%d) does not match results length (%d)", len(metrics), len(results))
	}

	collapsed := make([]*milvuspb.SearchResults, len(results))
	for i, result := range results {
		if op.elementLevelHybrid {
			var err error
			collapsed[i], err = prepareElementLevelHybridResult(result)
			if err != nil {
				return nil, err
			}
			continue
		}
		metricType := metrics[i]
		if result != nil && result.GetResults() != nil && result.GetResults().GetElementIndices() != nil && strings.TrimSpace(metricType) == "" {
			totalRows := int64(0)
			for _, topk := range result.GetResults().GetTopks() {
				totalRows += topk
			}
			if totalRows > 0 {
				return nil, merr.WrapErrServiceInternalMsg("element best collapse: missing metric type for element-level result[%d]", i)
			}
		}
		var err error
		config := defaultElementCollapseConfig()
		if i < len(op.configs) && op.configs[i].Strategy != "" {
			config = op.configs[i]
		}
		collapsed[i], err = collapseElementLevelResultByMetricType(result, metricType, config)
		if err != nil {
			return nil, err
		}
	}
	return []any{collapsed}, nil
}

type bestElementHit struct {
	rowIdx     int64
	score      float32
	order      int
	aggregate  float32
	groupCount int
}

type rowIdxComputeItem struct {
	outputIdx int
	rowIdx    int64
}

func computeFieldIdxsByOriginalOrder(rowIdxs []int64, compute func(int64) []int64) [][]int64 {
	items := make([]rowIdxComputeItem, 0, len(rowIdxs))
	for i, rowIdx := range rowIdxs {
		items = append(items, rowIdxComputeItem{
			outputIdx: i,
			rowIdx:    rowIdx,
		})
	}
	sort.SliceStable(items, func(i, j int) bool {
		return items[i].rowIdx < items[j].rowIdx
	})

	fieldIdxsByOutput := make([][]int64, len(rowIdxs))
	for _, item := range items {
		fieldIdxsByOutput[item.outputIdx] = append([]int64(nil), compute(item.rowIdx)...)
	}
	return fieldIdxsByOutput
}

func collapseElementLevelResultByBestScore(result *milvuspb.SearchResults, largerScoreIsBetter bool) (*milvuspb.SearchResults, error) {
	return collapseElementLevelResult(result, largerScoreIsBetter, defaultElementCollapseConfig())
}

func collapseElementLevelResult(result *milvuspb.SearchResults, largerScoreIsBetter bool, config elementCollapseConfig) (*milvuspb.SearchResults, error) {
	return collapseElementLevelResultWithMetricDirection(result, largerScoreIsBetter, true, config)
}

func collapseElementLevelResultByMetricType(result *milvuspb.SearchResults, metricType string, config elementCollapseConfig) (*milvuspb.SearchResults, error) {
	metricType = strings.TrimSpace(metricType)
	return collapseElementLevelResultWithMetricDirection(result, metric.PositivelyRelated(metricType), metricType != "", config)
}

func collapseElementLevelResultWithMetricDirection(result *milvuspb.SearchResults, largerScoreIsBetter bool, metricKnown bool, config elementCollapseConfig) (*milvuspb.SearchResults, error) {
	if result == nil || result.GetResults() == nil || result.GetResults().GetElementIndices() == nil {
		return result, nil
	}

	data := result.GetResults()
	topks := data.GetTopks()
	totalRows := int64(0)
	for _, topk := range topks {
		totalRows += topk
	}

	if isElementCollapseSumFamily(config.Strategy) && metricKnown && !largerScoreIsBetter {
		return nil, merr.WrapErrParameterInvalidMsg(
			"%s.collapse.strategy %s is only supported for positively related metrics",
			elementScopeKey, config.Strategy)
	}
	if totalRows == 0 {
		return copySearchResultsWithData(result, &schemapb.SearchResultData{
			NumQueries:              data.GetNumQueries(),
			TopK:                    0,
			Topks:                   append([]int64(nil), topks...),
			FieldsData:              []*schemapb.FieldData{},
			Scores:                  []float32{},
			OutputFields:            append([]string(nil), data.GetOutputFields()...),
			AllSearchCount:          data.GetAllSearchCount(),
			PrimaryFieldName:        data.GetPrimaryFieldName(),
			SearchIteratorV2Results: data.GetSearchIteratorV2Results(),
		}), nil
	}

	if !metricKnown {
		return nil, merr.WrapErrServiceInternal("element best collapse: missing metric type for element-level result")
	}

	if typeutil.GetSizeOfIDs(data.GetIds()) < int(totalRows) {
		return nil, merr.WrapErrServiceInternalMsg("element best collapse: ids length (%d) is less than total rows (%d)",
			typeutil.GetSizeOfIDs(data.GetIds()), totalRows)
	}
	if int64(len(data.GetScores())) < totalRows {
		return nil, merr.WrapErrServiceInternalMsg("element best collapse: scores length (%d) is less than total rows (%d)",
			len(data.GetScores()), totalRows)
	}
	if int64(len(data.GetElementIndices().GetData())) < totalRows {
		return nil, merr.WrapErrServiceInternalMsg("element best collapse: element_indices length (%d) is less than total rows (%d)",
			len(data.GetElementIndices().GetData()), totalRows)
	}
	if len(data.GetDistances()) > 0 && int64(len(data.GetDistances())) < totalRows {
		return nil, merr.WrapErrServiceInternalMsg("element best collapse: distances length (%d) is less than total rows (%d)",
			len(data.GetDistances()), totalRows)
	}
	if len(data.GetRecalls()) > 0 && int64(len(data.GetRecalls())) < totalRows {
		return nil, merr.WrapErrServiceInternalMsg("element best collapse: recalls length (%d) is less than total rows (%d)",
			len(data.GetRecalls()), totalRows)
	}

	output := &schemapb.SearchResultData{
		NumQueries:              data.GetNumQueries(),
		Topks:                   make([]int64, 0, len(topks)),
		FieldsData:              typeutil.PrepareResultFieldData(data.GetFieldsData(), totalRows),
		Scores:                  make([]float32, 0, totalRows),
		Ids:                     &schemapb.IDs{},
		OutputFields:            append([]string(nil), data.GetOutputFields()...),
		AllSearchCount:          data.GetAllSearchCount(),
		PrimaryFieldName:        data.GetPrimaryFieldName(),
		SearchIteratorV2Results: data.GetSearchIteratorV2Results(),
	}
	if len(data.GetDistances()) > 0 {
		output.Distances = make([]float32, 0, totalRows)
	}
	if len(data.GetRecalls()) > 0 {
		output.Recalls = make([]float32, 0, totalRows)
	}

	idxComputer := typeutil.NewFieldDataIdxComputer(data.GetFieldsData())
	offset := int64(0)
	for _, topk := range topks {
		grouped := make(map[any][]bestElementHit)
		groupOrder := make(map[any]int)
		for i := int64(0); i < topk; i++ {
			rowIdx := offset + i
			pk := typeutil.GetPK(data.GetIds(), rowIdx)
			if pk == nil {
				continue
			}
			score := data.GetScores()[rowIdx]
			if _, ok := grouped[pk]; !ok {
				groupOrder[pk] = int(i)
			}
			grouped[pk] = append(grouped[pk], bestElementHit{
				rowIdx: rowIdx,
				score:  score,
				order:  int(i),
			})
		}

		hits := make([]bestElementHit, 0, len(grouped))
		for pk, pkHits := range grouped {
			hit := aggregateElementHits(pkHits, config, largerScoreIsBetter)
			hit.order = groupOrder[pk]
			hits = append(hits, hit)
		}
		sort.SliceStable(hits, func(i, j int) bool {
			if hits[i].aggregate != hits[j].aggregate {
				return isBetterElementScore(hits[i].aggregate, hits[j].aggregate, largerScoreIsBetter)
			}
			return hits[i].order < hits[j].order
		})

		output.Topks = append(output.Topks, int64(len(hits)))
		if int64(len(hits)) > output.TopK {
			output.TopK = int64(len(hits))
		}

		var fieldIdxsByOutput [][]int64
		if len(data.GetFieldsData()) > 0 {
			rowIdxs := make([]int64, 0, len(hits))
			for _, hit := range hits {
				rowIdxs = append(rowIdxs, hit.rowIdx)
			}
			fieldIdxsByOutput = computeFieldIdxsByOriginalOrder(rowIdxs, idxComputer.Compute)
		}

		for i, hit := range hits {
			typeutil.AppendIDs(output.Ids, data.GetIds(), int(hit.rowIdx))
			output.Scores = append(output.Scores, hit.aggregate)
			// For aggregate collapse strategies, Score is the row aggregate while
			// Distance/Recall keep the representative best element's values.
			if len(data.GetDistances()) > 0 {
				output.Distances = append(output.Distances, data.GetDistances()[hit.rowIdx])
			}
			if len(data.GetRecalls()) > 0 {
				output.Recalls = append(output.Recalls, data.GetRecalls()[hit.rowIdx])
			}
			if len(data.GetFieldsData()) > 0 {
				typeutil.AppendFieldData(output.FieldsData, data.GetFieldsData(), hit.rowIdx, fieldIdxsByOutput[i]...)
			}
		}
		offset += topk
	}

	return copySearchResultsWithData(result, output), nil
}

func aggregateElementHits(hits []bestElementHit, config elementCollapseConfig, largerScoreIsBetter bool) bestElementHit {
	if len(hits) == 0 {
		return bestElementHit{}
	}

	bestHits := append([]bestElementHit(nil), hits...)
	sort.SliceStable(bestHits, func(i, j int) bool {
		if bestHits[i].score != bestHits[j].score {
			return isBetterElementScore(bestHits[i].score, bestHits[j].score, largerScoreIsBetter)
		}
		return bestHits[i].order < bestHits[j].order
	})

	switch config.Strategy {
	case elementCollapseSum, elementCollapseAvg:
		sum := float32(0)
		for _, hit := range hits {
			sum += hit.score
		}
		selected := bestHits[0]
		selected.aggregate = sum
		selected.groupCount = len(hits)
		if config.Strategy == elementCollapseAvg {
			selected.aggregate = sum / float32(len(hits))
		}
		return selected
	case elementCollapseTopKSum, elementCollapseTopKAvg:
		k := config.TopK
		if k <= 0 || k > len(bestHits) {
			k = len(bestHits)
		}
		sum := float32(0)
		for _, hit := range bestHits[:k] {
			sum += hit.score
		}
		selected := bestHits[0]
		selected.aggregate = sum
		selected.groupCount = k
		if config.Strategy == elementCollapseTopKAvg {
			selected.aggregate = sum / float32(k)
		}
		return selected
	case elementCollapseMax:
		fallthrough
	default:
		selected := bestHits[0]
		selected.aggregate = selected.score
		selected.groupCount = 1
		return selected
	}
}

func prepareElementLevelHybridResult(result *milvuspb.SearchResults) (*milvuspb.SearchResults, error) {
	if result == nil || result.GetResults() == nil {
		return result, nil
	}
	data := result.GetResults()
	totalRows := int64(0)
	for _, topk := range data.GetTopks() {
		totalRows += topk
	}
	if totalRows == 0 {
		output := &schemapb.SearchResultData{
			NumQueries:              data.GetNumQueries(),
			TopK:                    data.GetTopK(),
			Topks:                   append([]int64(nil), data.GetTopks()...),
			FieldsData:              data.GetFieldsData(),
			Scores:                  append([]float32(nil), data.GetScores()...),
			Ids:                     &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{}}},
			OutputFields:            append([]string(nil), data.GetOutputFields()...),
			AllSearchCount:          data.GetAllSearchCount(),
			PrimaryFieldName:        data.GetPrimaryFieldName(),
			ElementIndices:          &schemapb.LongArray{},
			GroupByFieldValues:      append([]*schemapb.FieldData(nil), data.GetGroupByFieldValues()...),
			GroupByFieldValue:       data.GetGroupByFieldValue(),
			SearchIteratorV2Results: data.GetSearchIteratorV2Results(),
		}
		if len(data.GetDistances()) > 0 {
			output.Distances = append([]float32(nil), data.GetDistances()...)
		}
		if len(data.GetRecalls()) > 0 {
			output.Recalls = append([]float32(nil), data.GetRecalls()...)
		}
		return copySearchResultsWithData(result, output), nil
	}
	if typeutil.GetSizeOfIDs(data.GetIds()) < int(totalRows) {
		return nil, merr.WrapErrServiceInternalMsg("element-level hybrid: ids length (%d) is less than total rows (%d)",
			typeutil.GetSizeOfIDs(data.GetIds()), totalRows)
	}
	if data.GetElementIndices() == nil {
		return nil, merr.WrapErrServiceInternal("element-level hybrid: missing element_indices")
	}
	if int64(len(data.GetElementIndices().GetData())) < totalRows {
		return nil, merr.WrapErrServiceInternalMsg("element-level hybrid: element_indices length (%d) is less than total rows (%d)",
			len(data.GetElementIndices().GetData()), totalRows)
	}

	keys := make([]string, 0, totalRows)
	for i := int64(0); i < totalRows; i++ {
		keys = append(keys, makeHybridElementKey(typeutil.GetPK(data.GetIds(), i), data.GetElementIndices().GetData()[i]))
	}
	output := &schemapb.SearchResultData{
		NumQueries:              data.GetNumQueries(),
		TopK:                    data.GetTopK(),
		Topks:                   append([]int64(nil), data.GetTopks()...),
		FieldsData:              data.GetFieldsData(),
		Scores:                  append([]float32(nil), data.GetScores()...),
		Ids:                     &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: keys}}},
		OutputFields:            append([]string(nil), data.GetOutputFields()...),
		AllSearchCount:          data.GetAllSearchCount(),
		PrimaryFieldName:        data.GetPrimaryFieldName(),
		ElementIndices:          data.GetElementIndices(),
		GroupByFieldValues:      append([]*schemapb.FieldData(nil), data.GetGroupByFieldValues()...),
		GroupByFieldValue:       data.GetGroupByFieldValue(),
		SearchIteratorV2Results: data.GetSearchIteratorV2Results(),
	}
	if len(data.GetDistances()) > 0 {
		output.Distances = append([]float32(nil), data.GetDistances()...)
	}
	if len(data.GetRecalls()) > 0 {
		output.Recalls = append([]float32(nil), data.GetRecalls()...)
	}
	return copySearchResultsWithData(result, output), nil
}

type elementKeyRestoreOperator struct {
	enabled bool
}

func newElementKeyRestoreOperator(t *searchTask, _ map[string]any) (operator, error) {
	return &elementKeyRestoreOperator{enabled: t.hybridElementLevel}, nil
}

func (op *elementKeyRestoreOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	if len(inputs) < 1 {
		return nil, merr.WrapErrServiceInternal("element key restore: missing inputs")
	}

	target := inputs[len(inputs)-1]
	if !op.enabled {
		return []any{target}, nil
	}

	switch v := target.(type) {
	case *milvuspb.SearchResults:
		if v == nil || v.GetResults() == nil {
			return []any{v}, nil
		}
		restored, err := restoreElementLevelHybridRankResult(v)
		if err != nil {
			return nil, err
		}
		return []any{restored}, nil
	case []*milvuspb.SearchResults:
		restored := make([]*milvuspb.SearchResults, len(v))
		for i, result := range v {
			if result == nil || result.GetResults() == nil {
				restored[i] = result
				continue
			}
			var err error
			restored[i], err = restoreElementLevelHybridRankResult(result)
			if err != nil {
				return nil, err
			}
		}
		return []any{restored}, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("element key restore: input must be *SearchResults or []*SearchResults, got %T", target)
	}
}

func restoreElementLevelHybridRankResult(rankResult *milvuspb.SearchResults) (*milvuspb.SearchResults, error) {
	data := rankResult.GetResults()
	size := typeutil.GetSizeOfIDs(data.GetIds())
	outputIDs := &schemapb.IDs{}
	elementIndices := make([]int64, 0, size)
	for i := 0; i < size; i++ {
		rawKey := typeutil.GetPK(data.GetIds(), int64(i))
		key, ok := rawKey.(string)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("element key restore: expected string element key, got %T", rawKey)
		}
		pk, elementIndex, ok := parseHybridElementKey(key)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("element key restore: invalid element key %q", key)
		}
		appendPK(outputIDs, pk)
		elementIndices = append(elementIndices, elementIndex)
	}

	output := &schemapb.SearchResultData{
		NumQueries:              data.GetNumQueries(),
		TopK:                    data.GetTopK(),
		Topks:                   append([]int64(nil), data.GetTopks()...),
		FieldsData:              data.GetFieldsData(),
		Scores:                  append([]float32(nil), data.GetScores()...),
		Ids:                     outputIDs,
		OutputFields:            append([]string(nil), data.GetOutputFields()...),
		AllSearchCount:          data.GetAllSearchCount(),
		PrimaryFieldName:        data.GetPrimaryFieldName(),
		ElementIndices:          &schemapb.LongArray{Data: elementIndices},
		GroupByFieldValues:      append([]*schemapb.FieldData(nil), data.GetGroupByFieldValues()...),
		GroupByFieldValue:       data.GetGroupByFieldValue(),
		SearchIteratorV2Results: data.GetSearchIteratorV2Results(),
	}
	if len(data.GetDistances()) > 0 {
		output.Distances = append([]float32(nil), data.GetDistances()...)
	}
	if len(data.GetRecalls()) > 0 {
		output.Recalls = append([]float32(nil), data.GetRecalls()...)
	}
	return copySearchResultsWithData(rankResult, output), nil
}

func appendPK(ids *schemapb.IDs, pk any) {
	switch v := pk.(type) {
	case int64:
		if ids.GetIntId() == nil {
			ids.IdField = &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}
		}
		ids.GetIntId().Data = append(ids.GetIntId().Data, v)
	case string:
		if ids.GetStrId() == nil {
			ids.IdField = &schemapb.IDs_StrId{StrId: &schemapb.StringArray{}}
		}
		ids.GetStrId().Data = append(ids.GetStrId().Data, v)
	}
}

func isBetterElementScore(candidate, current float32, largerScoreIsBetter bool) bool {
	if largerScoreIsBetter {
		return candidate > current
	}
	return candidate < current
}

func copySearchResultsWithData(src *milvuspb.SearchResults, data *schemapb.SearchResultData) *milvuspb.SearchResults {
	return &milvuspb.SearchResults{
		Status:         src.GetStatus(),
		Results:        data,
		CollectionName: src.GetCollectionName(),
		SessionTs:      src.GetSessionTs(),
	}
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
		return nil, merr.WrapErrServiceInternalMsg("aggregateOperator: expected []*milvuspb.SearchResults, got %T (pipeline wire)", inputs[0])
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
	allFields := typeutil.GetAllFieldSchemas(schema)
	fieldIDToName := make(map[int64]string, len(allFields))
	for _, field := range allFields {
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
	case *functionChainRerankMeta:
		buildCtx := chaintypes.FunctionBuildContext{}
		if searchParams != nil {
			buildCtx.ModelExtraInfo = searchParams.ModelExtraInfo
		}
		return chain.FuncChainFromReprWithContext(m.repr, alloc, buildCtx)
	case *legacyRerankMeta:
		return chain.BuildRerankChainWithLegacy(collSchema, m.legacyParams, metrics, searchParams, alloc)
	default:
		return nil, merr.WrapErrFunctionFailedMsg("rerank operator: unsupported rerankMeta type %T", meta)
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
		return nil, merr.WrapErrFunctionFailedMsg("rerank operator: rerankMeta is nil, cannot build rerank chain")
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

	// Execute chain. Column pruning is an execution optimization only;
	// final response projection is still handled by the end operator.
	resultDF, err := fc.ExecuteWithOptions(ctx, chain.ExecuteOptions{
		EnableColumnPruning: true,
		Downstream: chain.DownstreamSpec{
			RequiredColumns: neededFields,
		},
		SystemColumnPolicy: chain.SystemColumnPolicy{
			KeepAllSystemColumns: true,
		},
	}, dataframes...)
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
	queryChannelsNode  map[string]int64
	consistencyLevel   commonpb.ConsistencyLevel
	guaranteeTimestamp uint64
	namespace          *string
	planNamespace      *string

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
	queryChannelsNode := make(map[string]int64)
	if t.queryChannelsNode != nil {
		t.queryChannelsNode.Range(func(channel string, nodeID int64) bool {
			queryChannelsNode[channel] = nodeID
			return true
		})
	}
	return &requeryOperator{
		traceCtx:           t.TraceCtx(),
		outputFieldNames:   outputFieldNames.Collect(),
		timestamp:          t.BeginTs(),
		dbName:             t.request.GetDbName(),
		collectionName:     t.request.GetCollectionName(),
		primaryFieldSchema: pkField,
		queryChannelsTs:    t.queryChannelsTs,
		queryChannelsNode:  queryChannelsNode,
		consistencyLevel:   t.GetConsistencyLevel(),
		guaranteeTimestamp: t.GetGuaranteeTimestamp(),
		notReturnAllMeta:   t.request.GetNotReturnAllMeta(),
		partitionNames:     t.request.GetPartitionNames(),
		partitionIDs:       t.GetPartitionIDs(),
		node:               t.node,
		namespace:          t.request.Namespace,
		planNamespace:      namespaceForPlan(t.schema.CollectionSchema, t.request.Namespace),
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
	plan.Namespace = op.planNamespace
	channelsMvcc := make(map[string]Timestamp)
	for k, v := range op.queryChannelsTs {
		channelsMvcc[k] = v
	}
	preferredNodes := make(map[string]int64)
	for k, v := range op.queryChannelsNode {
		preferredNodes[k] = v
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
		preferredNodes: preferredNodes,
		fastSkip:       true,
		reQuery:        true,
		chMgr:          op.node.(*Proxy).chMgr,
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
	schema             *schemapb.CollectionSchema
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
		schema:             t.schema.CollectionSchema,
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
		if fieldData, err := pickFieldData(ids, offsets, fields, op.schema, op.collectionID); err != nil {
			return nil, err
		} else {
			allFieldData[idx] = fieldData
		}
	}
	return []any{allFieldData}, nil
}

func pickFieldData(ids *schemapb.IDs, pkOffset map[any]int, fields []*schemapb.FieldData, schema *schemapb.CollectionSchema, collectionID int64) ([]*schemapb.FieldData, error) {
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
	idxComputer := typeutil.NewFieldDataIdxComputerWithSchema(fields, schema)

	size := typeutil.GetSizeOfIDs(ids)
	rowIdxs := make([]int64, 0, size)
	for i := 0; i < size; i++ {
		id := typeutil.GetPK(ids, int64(i))
		if _, ok := pkOffset[id]; !ok {
			return nil, merr.WrapErrInconsistentRequery(fmt.Sprintf("incomplete query result, missing id %s, len(searchIDs) = %d, len(queryIDs) = %d, collection=%d",
				id, typeutil.GetSizeOfIDs(ids), len(pkOffset), collectionID))
		}
		rowIdxs = append(rowIdxs, int64(pkOffset[id]))
	}

	fieldIdxsByOutput := computeFieldIdxsByOriginalOrder(rowIdxs, idxComputer.Compute)
	for i, rowIdx := range rowIdxs {
		typeutil.AppendFieldData(fieldsData, fields, rowIdx, fieldIdxsByOutput[i]...)
	}

	return fieldsData, nil
}

// hybridAssembleOperator picks field data for reranked IDs directly from multiple
// sub-search results using a PK index, avoiding the full data copy that
// merging all FieldsData would require.
type hybridAssembleOperator struct {
	collectionID       int64
	elementLevelHybrid bool
}

func newHybridAssembleOperator(t *searchTask, _ map[string]any) (operator, error) {
	return &hybridAssembleOperator{
		collectionID:       t.GetCollectionID(),
		elementLevelHybrid: t.hybridElementLevel,
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

	type pkLoc struct{ resultIdx, rowIdx int }

	// Build candidate-key -> (resultIdx, rowIdx) index across all sub-search results.
	// Row-level hybrid keys by PK; element-level hybrid keys by (PK, element_index).
	pkIndex := make(map[any]pkLoc)
	for rIdx, result := range reducedResults {
		ids := result.GetResults().GetIds()
		for i := 0; i < typeutil.GetSizeOfIDs(ids); i++ {
			key := typeutil.GetPK(ids, int64(i))
			if op.elementLevelHybrid {
				if rawKey, ok := key.(string); ok {
					key = rawKey
				}
			}
			pkIndex[key] = pkLoc{rIdx, i}
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

	locs := make([]pkLoc, numReranked)
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
	itemsByResult := make([][]rowIdxComputeItem, len(reducedResults))
	for i := 0; i < numReranked; i++ {
		candidateKey := typeutil.GetPK(rerankedIDs, int64(i))
		if op.elementLevelHybrid {
			elementIndices := rankResult.GetResults().GetElementIndices().GetData()
			if i >= len(elementIndices) {
				return nil, merr.WrapErrServiceInternalMsg("hybrid assemble: missing element index for reranked row %d, collection=%d", i, op.collectionID)
			}
			candidateKey = makeHybridElementKey(candidateKey, elementIndices[i])
		}
		loc, ok := pkIndex[candidateKey]
		if !ok {
			return nil, merr.WrapErrInconsistentRequery(
				fmt.Sprintf("hybrid assemble: missing id %v, collection=%d", candidateKey, op.collectionID))
		}
		if computers[loc.resultIdx] == nil {
			return nil, merr.WrapErrServiceInternalMsg(
				"hybrid assemble: sub-result[%d] has empty FieldsData but contributed reranked id %v; "+
					"all sub-results that contribute ids must share the same FieldsData layout, "+
					"collection=%d", loc.resultIdx, candidateKey, op.collectionID)
		}
		locs[i] = loc
		itemsByResult[loc.resultIdx] = append(itemsByResult[loc.resultIdx], rowIdxComputeItem{
			outputIdx: i,
			rowIdx:    int64(loc.rowIdx),
		})
	}

	fieldIdxsByOutput := make([][]int64, numReranked)
	for resultIdx, items := range itemsByResult {
		if len(items) == 0 {
			continue
		}
		rowIdxs := make([]int64, 0, len(items))
		for _, item := range items {
			rowIdxs = append(rowIdxs, item.rowIdx)
		}
		fieldIdxs := computeFieldIdxsByOriginalOrder(rowIdxs, computers[resultIdx].Compute)
		for i, item := range items {
			fieldIdxsByOutput[item.outputIdx] = fieldIdxs[i]
		}
	}

	fieldsData := make([]*schemapb.FieldData, len(templateFields))
	for i, loc := range locs {
		srcFields := reducedResults[loc.resultIdx].GetResults().GetFieldsData()
		typeutil.AppendFieldData(fieldsData, srcFields, int64(loc.rowIdx), fieldIdxsByOutput[i]...)
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
	limit          int64
	offset         int64
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
		limit:          t.GetTopk() - t.GetOffset(),
		offset:         t.GetOffset(),
	}, nil
}

func (op *orderByOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "orderByOperator")
	defer sp.End()

	result := inputs[0].(*milvuspb.SearchResults)
	resultData := result.GetResults()

	if len(op.orderByFields) == 0 {
		return []any{result}, nil
	}

	numResults := len(resultData.GetScores())
	if numResults == 0 {
		return []any{result}, nil
	}

	// Validate that all order_by fields exist in the result
	if err := op.validateOrderByFields(result); err != nil {
		return nil, err
	}

	// Get per-query result counts from Topks
	// Topks[i] contains the number of results for the i-th query
	topks := resultData.GetTopks()

	// Validate that sum(Topks) matches numResults to prevent slice bounds panic
	var sumTopks int64
	for _, topk := range topks {
		sumTopks += topk
	}
	if int(sumTopks) != numResults {
		return nil, merr.WrapErrServiceInternalMsg("order_by: Topks sum (%d) does not match numResults (%d)", sumTopks, numResults)
	}

	// Build indices array for sorting
	indices := make([]int, numResults)
	for i := range indices {
		indices[i] = i
	}

	selectedIndices := make([]int, 0, numResults)
	newTopks := make([]int64, 0, len(topks))
	queryOffset := 0
	for _, topk := range topks {
		queryIndices := indices[queryOffset : queryOffset+int(topk)]
		if topk > 0 {
			var err error
			queryIndices, err = op.sortQueryResults(result, queryIndices)
			if err != nil {
				return nil, err
			}
		}
		selectedIndices = append(selectedIndices, queryIndices...)
		newTopks = append(newTopks, int64(len(queryIndices)))
		queryOffset += int(topk)
	}

	if err := op.reorderResults(result, selectedIndices); err != nil {
		return nil, err
	}

	var maxTopK int64
	for _, topk := range newTopks {
		if topk > maxTopK {
			maxTopK = topk
		}
	}
	resultData.Topks = newTopks
	resultData.TopK = maxTopK

	return []any{result}, nil
}

func getOrderByGroupByFieldValue(resultData *schemapb.SearchResultData) *schemapb.FieldData {
	if resultData == nil {
		return nil
	}
	if gbvs := resultData.GetGroupByFieldValues(); len(gbvs) > 0 {
		return gbvs[0]
	}
	return resultData.GetGroupByFieldValue()
}

func paginateSortedRows(indices []int, offset, limit int64) []int {
	start := int(offset)
	if start > len(indices) {
		start = len(indices)
	}
	end := len(indices)
	if limit > 0 && start+int(limit) < end {
		end = start + int(limit)
	}
	return indices[start:end]
}

// sortQueryResults sorts the given indices slice based on order_by fields.
// This handles both regular and group-by cases for a single query's results.
// Returns the sorted and paginated indices, or an error if comparison fails.
func (op *orderByOperator) sortQueryResults(result *milvuspb.SearchResults, indices []int) ([]int, error) {
	if len(indices) == 0 {
		return indices, nil
	}

	if op.groupByFieldId >= 0 && op.groupSize > 0 {
		return op.sortGroupsByOrderByFields(result, indices)
	}
	if err := op.sortResultsByOrderByFields(result, indices); err != nil {
		return nil, err
	}
	return paginateSortedRows(indices, op.offset, op.limit), nil
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
			return merr.WrapErrServiceInternalMsg("order_by field '%s' not found in search results", orderBy.FieldName)
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

// compareJSONValues compares two gjson.Result values.
// Non-existent paths and explicit JSON null are treated as null.
func isJSONNull(v gjson.Result) bool {
	return !v.Exists() || v.Type == gjson.Null
}

func compareJSONValues(a, b gjson.Result, nullsFirst bool) int {
	aIsNull := isJSONNull(a)
	bIsNull := isJSONNull(b)

	if aIsNull && bIsNull {
		return 0
	}
	if aIsNull {
		if nullsFirst {
			return -1
		}
		return 1
	}
	if bIsNull {
		if nullsFirst {
			return 1
		}
		return -1
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

// compareNulls compares two indices for null values using ValidData.
// Returns (0, false) if neither value is null, indicating caller should proceed with value comparison.
func compareNulls(validData []bool, i, j int, nullsFirst bool) (int, bool) {
	if len(validData) == 0 {
		return 0, false
	}
	iNull := i < len(validData) && !validData[i]
	jNull := j < len(validData) && !validData[j]
	if iNull && jNull {
		return 0, true
	}
	if iNull {
		if nullsFirst {
			return -1, true
		}
		return 1, true
	}
	if jNull {
		if nullsFirst {
			return 1, true
		}
		return -1, true
	}
	return 0, false
}

// compareOrderByField compares two values for an order_by field at given indices.
// It returns the final order comparison after applying ASC/DESC to non-null values.
func compareOrderByField(field *schemapb.FieldData, orderBy OrderByField, idxI, idxJ int, cache jsonValueCache) (int, error) {
	if orderBy.JSONPath != "" && field.GetType() == schemapb.DataType_JSON {
		if cmp, handled := compareNulls(field.ValidData, idxI, idxJ, orderBy.NullsFirst); handled {
			return cmp, nil
		}
		valI := cache.getCachedJSONValue(orderBy.FieldName, orderBy.JSONPath, idxI)
		valJ := cache.getCachedJSONValue(orderBy.FieldName, orderBy.JSONPath, idxJ)
		cmp := compareJSONValues(valI, valJ, orderBy.NullsFirst)
		if cmp != 0 && !orderBy.Ascending && !isJSONNull(valI) && !isJSONNull(valJ) {
			cmp = -cmp
		}
		return cmp, nil
	}

	if cmp, handled := compareNulls(field.ValidData, idxI, idxJ, orderBy.NullsFirst); handled {
		return cmp, nil
	}
	cmp, err := compareFieldDataAt(field, idxI, idxJ, orderBy.NullsFirst)
	if err != nil {
		return 0, err
	}
	if cmp != 0 && !orderBy.Ascending {
		cmp = -cmp
	}
	return cmp, nil
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
				mlog.Warn(context.TODO(), "order_by field not found in fieldMap after validation, skipping",
					mlog.String("fieldName", orderBy.FieldName))
				continue
			}
			cmp, err := compareOrderByField(field, orderBy, idxI, idxJ, cache)
			if err != nil {
				sortErr = err
				return false
			}
			if cmp != 0 {
				return cmp < 0
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
func (op *orderByOperator) sortGroupsByOrderByFields(result *milvuspb.SearchResults, indices []int) ([]int, error) {
	numResults := len(indices)
	if numResults == 0 {
		return indices, nil
	}

	groupByValue := getOrderByGroupByFieldValue(result.GetResults())
	if groupByValue == nil {
		if err := op.sortResultsByOrderByFields(result, indices); err != nil {
			return nil, err
		}
		return paginateSortedRows(indices, op.offset, op.limit), nil
	}

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
				mlog.Warn(context.TODO(), "order_by field not found in fieldMap after validation, skipping",
					mlog.String("fieldName", orderBy.FieldName))
				continue
			}
			cmp, err := compareOrderByField(field, orderBy, dataIdxI, dataIdxJ, cache)
			if err != nil {
				sortErr = err
				return false
			}
			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})
	if sortErr != nil {
		return nil, sortErr
	}

	selected := make([]int, 0, numResults)
	for groupIdx, g := range groups {
		if int64(groupIdx) < op.offset {
			continue
		}
		if op.limit > 0 && int64(groupIdx) >= op.offset+op.limit {
			break
		}
		selected = append(selected, indices[g.start:g.end]...)
	}
	return selected, nil
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
func compareFieldDataAt(field *schemapb.FieldData, i, j int, nullsFirst bool) (int, error) {
	if cmp, handled := compareNulls(field.ValidData, i, j, nullsFirst); handled {
		return cmp, nil
	}

	switch field.GetType() {
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		data := field.GetScalars().GetIntData().GetData()
		if i >= len(data) || j >= len(data) {
			return 0, merr.WrapErrServiceInternalMsg("compareFieldDataAt: index out of bounds for Int field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data))
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
			return 0, merr.WrapErrServiceInternalMsg("compareFieldDataAt: index out of bounds for Int64 field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data))
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
			return 0, merr.WrapErrServiceInternalMsg("compareFieldDataAt: index out of bounds for Float field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data))
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
			return 0, merr.WrapErrServiceInternalMsg("compareFieldDataAt: index out of bounds for Double field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data))
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
			return 0, merr.WrapErrServiceInternalMsg("compareFieldDataAt: index out of bounds for String field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data))
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
			return 0, merr.WrapErrServiceInternalMsg("compareFieldDataAt: index out of bounds for JSON field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data))
		}
		return bytes.Compare(data[i], data[j]), nil
	case schemapb.DataType_Bool:
		data := field.GetScalars().GetBoolData().GetData()
		if i >= len(data) || j >= len(data) {
			return 0, merr.WrapErrServiceInternalMsg("compareFieldDataAt: index out of bounds for Bool field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data))
		}
		// false < true
		if !data[i] && data[j] {
			return -1, nil
		} else if data[i] && !data[j] {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, merr.WrapErrServiceInternalMsg("compareFieldDataAt: unsupported field type %s for field %s", field.GetType().String(), field.GetFieldName())
	}
}

// reorderResults reorders all result arrays based on the sorted indices
func (op *orderByOperator) reorderResults(result *milvuspb.SearchResults, indices []int) error {
	results := result.GetResults()
	n := len(indices)

	var intIDs *schemapb.LongArray
	var strIDs *schemapb.StringArray
	var newIntIDs []int64
	var newStrIDs []string
	if ids := results.GetIds(); ids != nil {
		if intIDData := ids.GetIntId(); intIDData != nil {
			intIDs = intIDData
			newIntIDs = make([]int64, n)
		} else if strIDData := ids.GetStrId(); strIDData != nil {
			strIDs = strIDData
			newStrIDs = make([]string, n)
		}
	}

	var newScores []float32
	if len(results.Scores) > 0 {
		newScores = make([]float32, n)
	}
	var newDistances []float32
	if len(results.Distances) > 0 {
		newDistances = make([]float32, n)
	}
	var newRecalls []float32
	if len(results.Recalls) > 0 {
		newRecalls = make([]float32, n)
	}
	var elemIndices *schemapb.LongArray
	var newElementIndices []int64
	if data := results.GetElementIndices(); data != nil && len(data.GetData()) > 0 {
		elemIndices = data
		newElementIndices = make([]int64, n)
	}

	for newIdx, oldIdx := range indices {
		if intIDs != nil {
			if oldIdx < 0 || oldIdx >= len(intIDs.Data) {
				return merr.WrapErrServiceInternalMsg("reorderResults: index %d out of bounds for int IDs (len=%d)", oldIdx, len(intIDs.Data))
			}
			newIntIDs[newIdx] = intIDs.Data[oldIdx]
		}
		if strIDs != nil {
			if oldIdx < 0 || oldIdx >= len(strIDs.Data) {
				return merr.WrapErrServiceInternalMsg("reorderResults: index %d out of bounds for string IDs (len=%d)", oldIdx, len(strIDs.Data))
			}
			newStrIDs[newIdx] = strIDs.Data[oldIdx]
		}
		if newScores != nil {
			if oldIdx < 0 || oldIdx >= len(results.Scores) {
				return merr.WrapErrServiceInternalMsg("reorderResults: index %d out of bounds for scores (len=%d)", oldIdx, len(results.Scores))
			}
			newScores[newIdx] = results.Scores[oldIdx]
		}
		if newDistances != nil {
			if oldIdx < 0 || oldIdx >= len(results.Distances) {
				return merr.WrapErrServiceInternalMsg("reorderResults: index %d out of bounds for distances (len=%d)", oldIdx, len(results.Distances))
			}
			newDistances[newIdx] = results.Distances[oldIdx]
		}
		if newRecalls != nil {
			if oldIdx < 0 || oldIdx >= len(results.Recalls) {
				return merr.WrapErrServiceInternalMsg("reorderResults: index %d out of bounds for recalls (len=%d)", oldIdx, len(results.Recalls))
			}
			newRecalls[newIdx] = results.Recalls[oldIdx]
		}
		if newElementIndices != nil {
			if oldIdx < 0 || oldIdx >= len(elemIndices.GetData()) {
				return merr.WrapErrServiceInternalMsg("reorderResults: index %d out of bounds for element indices (len=%d)", oldIdx, len(elemIndices.GetData()))
			}
			newElementIndices[newIdx] = elemIndices.GetData()[oldIdx]
		}
	}

	if intIDs != nil {
		intIDs.Data = newIntIDs
	}
	if strIDs != nil {
		strIDs.Data = newStrIDs
	}
	if newScores != nil {
		results.Scores = newScores
	}
	if newDistances != nil {
		results.Distances = newDistances
	}
	if newRecalls != nil {
		results.Recalls = newRecalls
	}
	if newElementIndices != nil {
		elemIndices.Data = newElementIndices
	}

	// Reorder field data
	for _, field := range results.FieldsData {
		if err := reorderFieldData(field, indices); err != nil {
			return err
		}
	}

	if gbv := getOrderByGroupByFieldValue(results); gbv != nil {
		if err := reorderFieldData(gbv, indices); err != nil {
			return err
		}
	}
	return nil
}

func prepareNullableFieldDataReorder(field *schemapb.FieldData, indices []int) ([]bool, []int, int, error) {
	validData := field.GetValidData()
	if len(validData) == 0 {
		return nil, nil, 0, nil
	}

	logicalToPhysical, validCount := typeutil.BuildNullableVectorDataIndices(validData)

	newValidData := make([]bool, len(indices))
	for newIdx, oldIdx := range indices {
		if oldIdx < 0 || oldIdx >= len(validData) {
			return nil, nil, 0, merr.WrapErrServiceInternalMsg("reorderFieldData: index %d out of bounds for ValidData of field %s (len=%d)", oldIdx, field.GetFieldName(), len(validData))
		}
		newValidData[newIdx] = validData[oldIdx]
	}
	return newValidData, logicalToPhysical, validCount, nil
}

func countValidRows(validData []bool) int {
	validCount := 0
	for _, valid := range validData {
		if valid {
			validCount++
		}
	}
	return validCount
}

func validateReorderIndex(oldIdx, originalRows int, field *schemapb.FieldData) error {
	if oldIdx < 0 || oldIdx >= originalRows {
		return merr.WrapErrServiceInternalMsg("reorderFieldData: index %d out of bounds for %s field %s (rows=%d)", oldIdx, field.GetType().String(), field.GetFieldName(), originalRows)
	}
	return nil
}

func reorderNullableFloatVectorData(field *schemapb.FieldData, data []float32, width int, indices []int, newValidData []bool, logicalToPhysical []int, validCount int) ([]float32, error) {
	expected := validCount * width
	if len(data) != expected {
		return nil, merr.WrapErrServiceInternalMsg("reorderFieldData: nullable FloatVector field %s has %d elements, expected compact %d (valid=%d, dim=%d)", field.GetFieldName(), len(data), expected, validCount, width)
	}
	newData := make([]float32, 0, countValidRows(newValidData)*width)
	for _, oldIdx := range indices {
		if !field.ValidData[oldIdx] {
			continue
		}
		physicalIdx := logicalToPhysical[oldIdx]
		srcStart := physicalIdx * width
		newData = append(newData, data[srcStart:srcStart+width]...)
	}
	return newData, nil
}

func reorderNullableByteVectorData(field *schemapb.FieldData, typeName string, data []byte, width int, indices []int, newValidData []bool, logicalToPhysical []int, validCount int) ([]byte, error) {
	expected := validCount * width
	if len(data) != expected {
		return nil, merr.WrapErrServiceInternalMsg("reorderFieldData: nullable %s field %s has %d bytes, expected compact %d (valid=%d, width=%d)", typeName, field.GetFieldName(), len(data), expected, validCount, width)
	}
	newData := make([]byte, 0, countValidRows(newValidData)*width)
	for _, oldIdx := range indices {
		if !field.ValidData[oldIdx] {
			continue
		}
		physicalIdx := logicalToPhysical[oldIdx]
		srcStart := physicalIdx * width
		newData = append(newData, data[srcStart:srcStart+width]...)
	}
	return newData, nil
}

func reorderNullableSparseVectorData(field *schemapb.FieldData, contents [][]byte, indices []int, newValidData []bool, logicalToPhysical []int, validCount int) ([][]byte, error) {
	if len(contents) != validCount {
		return nil, merr.WrapErrServiceInternalMsg("reorderFieldData: nullable SparseFloatVector field %s has %d elements, expected compact %d", field.GetFieldName(), len(contents), validCount)
	}
	newContents := make([][]byte, 0, countValidRows(newValidData))
	for _, oldIdx := range indices {
		if !field.ValidData[oldIdx] {
			continue
		}
		newContents = append(newContents, contents[logicalToPhysical[oldIdx]])
	}
	return newContents, nil
}

// reorderFieldData reorders field data based on indices
func reorderFieldData(field *schemapb.FieldData, indices []int) error {
	n := len(indices)
	newValidData, logicalToPhysical, validCount, err := prepareNullableFieldDataReorder(field, indices)
	if err != nil {
		return err
	}
	switch field.GetType() {
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		if data := field.GetScalars().GetIntData(); data != nil && len(data.Data) > 0 {
			newData := make([]int32, n)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= len(data.Data) {
					return merr.WrapErrServiceInternalMsg("reorderFieldData: index %d out of bounds for %s field %s (len=%d)", oldIdx, field.GetType().String(), field.GetFieldName(), len(data.Data))
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
					return merr.WrapErrServiceInternalMsg("reorderFieldData: index %d out of bounds for Int64 field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data))
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
					return merr.WrapErrServiceInternalMsg("reorderFieldData: index %d out of bounds for Float field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data))
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
					return merr.WrapErrServiceInternalMsg("reorderFieldData: index %d out of bounds for Double field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data))
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
					return merr.WrapErrServiceInternalMsg("reorderFieldData: index %d out of bounds for %s field %s (len=%d)", oldIdx, field.GetType().String(), field.GetFieldName(), len(data.Data))
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
					return merr.WrapErrServiceInternalMsg("reorderFieldData: index %d out of bounds for Bool field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data))
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
					return merr.WrapErrServiceInternalMsg("reorderFieldData: index %d out of bounds for JSON field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data))
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
					return merr.WrapErrServiceInternalMsg("reorderFieldData: index %d out of bounds for Array field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data))
				}
				newData[newIdx] = data.Data[oldIdx]
			}
			data.Data = newData
		}
	case schemapb.DataType_FloatVector:
		vectors := field.GetVectors()
		if vectors != nil && (newValidData != nil || vectors.GetFloatVector() != nil) {
			dim := int(vectors.GetDim())
			if dim <= 0 {
				return merr.WrapErrServiceInternalMsg("reorderFieldData: invalid dimension %d for FloatVector field %s", dim, field.GetFieldName())
			}
			var data []float32
			if vectors.GetFloatVector() != nil {
				data = vectors.GetFloatVector().GetData()
			}
			if newValidData != nil {
				newData, err := reorderNullableFloatVectorData(field, data, dim, indices, newValidData, logicalToPhysical, validCount)
				if err != nil {
					return err
				}
				vectors.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: newData}}
				break
			}
			if len(data)%dim != 0 {
				return merr.WrapErrServiceInternalMsg("reorderFieldData: FloatVector field %s has %d elements not divisible by dim %d", field.GetFieldName(), len(data), dim)
			}
			originalRows := len(data) / dim
			newData := make([]float32, n*dim)
			for newIdx, oldIdx := range indices {
				if err := validateReorderIndex(oldIdx, originalRows, field); err != nil {
					return err
				}
				srcStart := oldIdx * dim
				dstStart := newIdx * dim
				copy(newData[dstStart:dstStart+dim], data[srcStart:srcStart+dim])
			}
			vectors.GetFloatVector().Data = newData
		}
	case schemapb.DataType_BinaryVector:
		vectors := field.GetVectors()
		if vectors != nil && (newValidData != nil || len(vectors.GetBinaryVector()) > 0) {
			dim := int(vectors.GetDim())
			bytesPerVector := dim / 8
			if bytesPerVector <= 0 {
				return merr.WrapErrServiceInternalMsg("reorderFieldData: invalid dimension %d for BinaryVector field %s", dim, field.GetFieldName())
			}
			data := vectors.GetBinaryVector()
			if newValidData != nil {
				newData, err := reorderNullableByteVectorData(field, "BinaryVector", data, bytesPerVector, indices, newValidData, logicalToPhysical, validCount)
				if err != nil {
					return err
				}
				vectors.Data = &schemapb.VectorField_BinaryVector{BinaryVector: newData}
				break
			}
			if len(data)%bytesPerVector != 0 {
				return merr.WrapErrServiceInternalMsg("reorderFieldData: BinaryVector field %s has %d bytes not divisible by width %d", field.GetFieldName(), len(data), bytesPerVector)
			}
			originalRows := len(data) / bytesPerVector
			newData := make([]byte, n*bytesPerVector)
			for newIdx, oldIdx := range indices {
				if err := validateReorderIndex(oldIdx, originalRows, field); err != nil {
					return err
				}
				srcStart := oldIdx * bytesPerVector
				dstStart := newIdx * bytesPerVector
				copy(newData[dstStart:dstStart+bytesPerVector], data[srcStart:srcStart+bytesPerVector])
			}
			vectors.Data = &schemapb.VectorField_BinaryVector{BinaryVector: newData}
		}
	case schemapb.DataType_Float16Vector:
		vectors := field.GetVectors()
		if vectors != nil && (newValidData != nil || len(vectors.GetFloat16Vector()) > 0) {
			dim := int(vectors.GetDim())
			if dim <= 0 {
				return merr.WrapErrServiceInternalMsg("reorderFieldData: invalid dimension %d for Float16Vector field %s", dim, field.GetFieldName())
			}
			bytesPerVector := dim * 2 // 2 bytes per float16
			data := vectors.GetFloat16Vector()
			if newValidData != nil {
				newData, err := reorderNullableByteVectorData(field, "Float16Vector", data, bytesPerVector, indices, newValidData, logicalToPhysical, validCount)
				if err != nil {
					return err
				}
				vectors.Data = &schemapb.VectorField_Float16Vector{Float16Vector: newData}
				break
			}
			if len(data)%bytesPerVector != 0 {
				return merr.WrapErrServiceInternalMsg("reorderFieldData: Float16Vector field %s has %d bytes not divisible by width %d", field.GetFieldName(), len(data), bytesPerVector)
			}
			originalRows := len(data) / bytesPerVector
			newData := make([]byte, n*bytesPerVector)
			for newIdx, oldIdx := range indices {
				if err := validateReorderIndex(oldIdx, originalRows, field); err != nil {
					return err
				}
				srcStart := oldIdx * bytesPerVector
				dstStart := newIdx * bytesPerVector
				copy(newData[dstStart:dstStart+bytesPerVector], data[srcStart:srcStart+bytesPerVector])
			}
			vectors.Data = &schemapb.VectorField_Float16Vector{Float16Vector: newData}
		}
	case schemapb.DataType_BFloat16Vector:
		vectors := field.GetVectors()
		if vectors != nil && (newValidData != nil || len(vectors.GetBfloat16Vector()) > 0) {
			dim := int(vectors.GetDim())
			if dim <= 0 {
				return merr.WrapErrServiceInternalMsg("reorderFieldData: invalid dimension %d for BFloat16Vector field %s", dim, field.GetFieldName())
			}
			bytesPerVector := dim * 2 // 2 bytes per bfloat16
			data := vectors.GetBfloat16Vector()
			if newValidData != nil {
				newData, err := reorderNullableByteVectorData(field, "BFloat16Vector", data, bytesPerVector, indices, newValidData, logicalToPhysical, validCount)
				if err != nil {
					return err
				}
				vectors.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: newData}
				break
			}
			if len(data)%bytesPerVector != 0 {
				return merr.WrapErrServiceInternalMsg("reorderFieldData: BFloat16Vector field %s has %d bytes not divisible by width %d", field.GetFieldName(), len(data), bytesPerVector)
			}
			originalRows := len(data) / bytesPerVector
			newData := make([]byte, n*bytesPerVector)
			for newIdx, oldIdx := range indices {
				if err := validateReorderIndex(oldIdx, originalRows, field); err != nil {
					return err
				}
				srcStart := oldIdx * bytesPerVector
				dstStart := newIdx * bytesPerVector
				copy(newData[dstStart:dstStart+bytesPerVector], data[srcStart:srcStart+bytesPerVector])
			}
			vectors.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: newData}
		}
	case schemapb.DataType_SparseFloatVector:
		vectors := field.GetVectors()
		if vectors != nil && (newValidData != nil || vectors.GetSparseFloatVector() != nil) {
			sparseData := vectors.GetSparseFloatVector()
			if sparseData == nil {
				sparseData = &schemapb.SparseFloatArray{Dim: vectors.GetDim()}
			}
			contents := sparseData.GetContents()
			if newValidData != nil {
				newContents, err := reorderNullableSparseVectorData(field, contents, indices, newValidData, logicalToPhysical, validCount)
				if err != nil {
					return err
				}
				sparseData.Contents = newContents
				vectors.Data = &schemapb.VectorField_SparseFloatVector{SparseFloatVector: sparseData}
				break
			}
			newContents := make([][]byte, n)
			for newIdx, oldIdx := range indices {
				if err := validateReorderIndex(oldIdx, len(contents), field); err != nil {
					return err
				}
				newContents[newIdx] = contents[oldIdx]
			}
			sparseData.Contents = newContents
		}
	case schemapb.DataType_Int8Vector:
		vectors := field.GetVectors()
		if vectors != nil && (newValidData != nil || len(vectors.GetInt8Vector()) > 0) {
			dim := int(vectors.GetDim())
			if dim <= 0 {
				return merr.WrapErrServiceInternalMsg("reorderFieldData: invalid dimension %d for Int8Vector field %s", dim, field.GetFieldName())
			}
			data := vectors.GetInt8Vector()
			if newValidData != nil {
				newData, err := reorderNullableByteVectorData(field, "Int8Vector", data, dim, indices, newValidData, logicalToPhysical, validCount)
				if err != nil {
					return err
				}
				vectors.Data = &schemapb.VectorField_Int8Vector{Int8Vector: newData}
				break
			}
			if len(data)%dim != 0 {
				return merr.WrapErrServiceInternalMsg("reorderFieldData: Int8Vector field %s has %d bytes not divisible by dim %d", field.GetFieldName(), len(data), dim)
			}
			originalRows := len(data) / dim
			newData := make([]byte, n*dim)
			for newIdx, oldIdx := range indices {
				if err := validateReorderIndex(oldIdx, originalRows, field); err != nil {
					return err
				}
				srcStart := oldIdx * dim
				dstStart := newIdx * dim
				copy(newData[dstStart:dstStart+dim], data[srcStart:srcStart+dim])
			}
			vectors.Data = &schemapb.VectorField_Int8Vector{Int8Vector: newData}
		}
	default:
		return merr.WrapErrServiceInternalMsg("reorderFieldData: unhandled data type %s", field.GetType().String())
	}

	// Reorder valid data if present
	if newValidData != nil {
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

// restoreGroupByFieldData overwrites group-by columns in FieldsData with the
// search-time group-by values carried on the result data (GroupByFieldValues,
// or the legacy singular GroupByFieldValue). The group-by decision (which rows
// survive group_size) is made on the rows returned by search, but output fields
// are re-fetched by PK only via requery/organize; when duplicate PKs exist, the
// re-fetched row can be a different physical row with a different group-by
// value, surfacing duplicated group values in the final response even with
// group_size=1. Restoring the search-time values keeps the response consistent
// with the grouping decision. Results without group-by are left untouched.
func restoreGroupByFieldData(data *schemapb.SearchResultData) {
	if data == nil {
		return
	}
	groupByValues := data.GetGroupByFieldValues()
	if len(groupByValues) == 0 && data.GetGroupByFieldValue() != nil {
		groupByValues = []*schemapb.FieldData{data.GetGroupByFieldValue()}
	}
	for _, gbv := range groupByValues {
		if gbv == nil {
			continue
		}
		for i, fd := range data.GetFieldsData() {
			// A type mismatch means the group-by key is an extraction (e.g. a
			// JSON path or dynamic key) rather than the whole column; keep the
			// re-fetched column in that case.
			if fd == nil || fd.GetType() != gbv.GetType() {
				continue
			}
			if (gbv.GetFieldId() != 0 && fd.GetFieldId() == gbv.GetFieldId()) ||
				(gbv.GetFieldId() == 0 && gbv.GetFieldName() != "" && fd.GetFieldName() == gbv.GetFieldName()) {
				data.FieldsData[i] = proto.Clone(gbv).(*schemapb.FieldData)
				break
			}
		}
	}
}

// attachOrganizedFieldsFunc is the shared "result" node body of rerank-based
// pipelines: attach the organized output fields to the rank result, then
// restore group-by columns so they reflect the values the grouping was
// computed on (see restoreGroupByFieldData).
func attachOrganizedFieldsFunc(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	result := inputs[0].(*milvuspb.SearchResults)
	fields := inputs[1].([][]*schemapb.FieldData)
	result.Results.FieldsData = fields[0]
	restoreGroupByFieldData(result.GetResults())
	return []any{result}, nil
}

// pickOrganizedFieldsFunc is attachOrganizedFieldsFunc for pipelines whose
// carrying result is the reduced result list instead of a rank result.
func pickOrganizedFieldsFunc(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	result := inputs[0].([]*milvuspb.SearchResults)[0]
	fields := inputs[1].([][]*schemapb.FieldData)
	result.Results.FieldsData = fields[0]
	restoreGroupByFieldData(result.GetResults())
	return []any{result}, nil
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
	mlog.Debug(ctx, "SearchPipeline run", mlog.Stringer("pipeline", p))
	pTrace := newPipelineTrace(p.traceEnabled)
	msg := opMsg{}
	msg[pipelineInput] = toReduceResults
	msg[pipelineStorageCost] = storageCost
	for _, node := range p.nodes {
		var err error
		mlog.Debug(ctx, "SearchPipeline run node", mlog.String("node", node.name))
		msg, err = node.Run(ctx, span, msg)
		if err != nil {
			mlog.Error(ctx, "Run node failed: ", mlog.String("err", err.Error()))
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
				lambdaParamKey: pickOrganizedFieldsFunc,
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
				lambdaParamKey: attachOrganizedFieldsFunc,
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
				lambdaParamKey: attachOrganizedFieldsFunc,
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
			name:    "element_best_collapse",
			inputs:  []string{"reduced", "metrics"},
			outputs: []string{"collapsed"},
			opName:  elementBestCollapseOp,
		},
		{
			name:    "rerank",
			inputs:  []string{"collapsed", "metrics"},
			outputs: []string{"rank_result"},
			opName:  rerankOp,
		},
		{
			name:    "restore_element_keys",
			inputs:  []string{"collapsed", "rank_result"},
			outputs: []string{"rank_result"},
			opName:  elementKeyRestoreOp,
		},
		{
			name:    "assemble",
			inputs:  []string{"collapsed", "rank_result"},
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
			name:    "element_best_collapse",
			inputs:  []string{"reduced", "metrics"},
			outputs: []string{"collapsed"},
			opName:  elementBestCollapseOp,
		},
		{
			name:    "restore_element_keys_for_requery",
			inputs:  []string{"collapsed"},
			outputs: []string{"requery_data"},
			opName:  elementKeyRestoreOp,
		},
		{
			name:    "merge_ids",
			inputs:  []string{"requery_data"},
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
			inputs:  []string{"requery_data"},
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
			inputs:  []string{"collapsed", "organized_fields"},
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
			name:    "restore_element_keys",
			inputs:  []string{"rank_data", "rank_result"},
			outputs: []string{"rank_result"},
			opName:  elementKeyRestoreOp,
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
				lambdaParamKey: attachOrganizedFieldsFunc,
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
			name:    "element_best_collapse",
			inputs:  []string{"reduced", "metrics"},
			outputs: []string{"collapsed"},
			opName:  elementBestCollapseOp,
		},
		{
			name:    "rerank",
			inputs:  []string{"collapsed", "metrics"},
			outputs: []string{"rank_result"},
			opName:  rerankOp,
		},
		{
			name:    "restore_element_keys",
			inputs:  []string{"collapsed", "rank_result"},
			outputs: []string{"rank_result"},
			opName:  elementKeyRestoreOp,
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
				lambdaParamKey: attachOrganizedFieldsFunc,
			},
			opName: lambdaOp,
		},
		// endOp node is appended by newSearchPipeline; do not add one here.
	},
}

// searchWithOrderByPipe: reduce without offset → requery → organize → order_by
// For common search with order_by_fields
var searchWithOrderByPipe = &pipelineDef{
	name: "searchWithOrderBy",
	nodes: []*nodeDef{
		{
			name:    "reduce",
			inputs:  []string{pipelineInput, pipelineStorageCost},
			outputs: []string{"reduced", "metrics"},
			params: map[string]any{
				reduceOffsetParamKey: int64(0),
			},
			opName: searchReduceOp,
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
				lambdaParamKey: pickOrganizedFieldsFunc,
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

	// Common search with order_by: reduce without offset, then order and slice.
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
