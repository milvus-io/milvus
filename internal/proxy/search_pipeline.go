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

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/function/rerank"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	inputs  []string
	outputs []string

	op operator
}

func (n *Node) unpackInputs(msg opMsg) ([]any, error) {
	for _, input := range n.inputs {
		if _, ok := msg[input]; !ok {
			return nil, fmt.Errorf("node [%s]'s input %s not found", n.name, input)
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
		return nil, fmt.Errorf("node [%s] output size not match operator output size", n.name)
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
)

const (
	pipelineOutput      = "output"
	pipelineInput       = "input"
	pipelineStorageCost = "storage_cost"
)

var opFactory = map[string]func(t *searchTask, params map[string]any) (operator, error){
	searchReduceOp:        newSearchReduceOperator,
	hybridSearchReduceOp:  newHybridSearchReduceOperator,
	rerankOp:              newRerankOperator,
	organizeOp:            newOrganizeOperator,
	elementBestCollapseOp: newElementBestCollapseOperator,
	elementKeyRestoreOp:   newElementKeyRestoreOperator,
	hybridAssembleOp:      newHybridAssembleOperator,
	requeryOp:             newRequeryOperator,
	lambdaOp:              newLambdaOperator,
	endOp:                 newEndOperator,
	highlightOp:           newHighlightOperator,
}

func NewNode(info *nodeDef, t *searchTask) (*Node, error) {
	n := Node{
		name:    info.name,
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
	traceCtx           context.Context
	primaryFieldSchema *schemapb.FieldSchema
	nq                 int64
	topK               int64
	offset             int64
	collectionID       int64
	partitionIDs       []int64
	queryInfos         []*planpb.QueryInfo
}

func newSearchReduceOperator(t *searchTask, _ map[string]any) (operator, error) {
	pkField, err := t.schema.GetPkField()
	if err != nil {
		return nil, err
	}
	return &searchReduceOperator{
		traceCtx:           t.TraceCtx(),
		primaryFieldSchema: pkField,
		nq:                 t.GetNq(),
		topK:               t.GetTopk(),
		offset:             t.GetOffset(),
		collectionID:       t.GetCollectionID(),
		partitionIDs:       t.GetPartitionIDs(),
		queryInfos:         t.queryInfos,
	}, nil
}

func (op *searchReduceOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	_, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "searchReduceOperator")
	defer sp.End()
	toReduceResults := inputs[0].([]*internalpb.SearchResults)
	metricType := getMetricType(toReduceResults)
	result, err := reduceResults(
		op.traceCtx, toReduceResults, op.nq, op.topK, op.offset,
		metricType, op.primaryFieldSchema.GetDataType(), op.queryInfos[0], false, op.collectionID, op.partitionIDs)
	if err != nil {
		return nil, err
	}
	return []any{[]*milvuspb.SearchResults{result}, []string{metricType}}, nil
}

type hybridSearchReduceOperator struct {
	traceCtx           context.Context
	subReqs            []*internalpb.SubSearchRequest
	primaryFieldSchema *schemapb.FieldSchema
	collectionID       int64
	partitionIDs       []int64
	queryInfos         []*planpb.QueryInfo
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
			op.primaryFieldSchema.GetDataType(), op.queryInfos[index], true, op.collectionID, op.partitionIDs)
		if err != nil {
			return nil, err
		}
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
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("element best collapse: metrics length (%d) does not match results length (%d)", len(metrics), len(results)))
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
				return nil, merr.WrapErrServiceInternal(fmt.Sprintf("element best collapse: missing metric type for element-level result[%d]", i))
			}
		}
		var err error
		config := defaultElementCollapseConfig()
		if i < len(op.configs) && op.configs[i].Strategy != "" {
			config = op.configs[i]
		}
		collapsed[i], err = collapseElementLevelResult(result, metric.PositivelyRelated(metricType), config)
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
	if result == nil || result.GetResults() == nil || result.GetResults().GetElementIndices() == nil {
		return result, nil
	}

	data := result.GetResults()
	topks := data.GetTopks()
	totalRows := int64(0)
	for _, topk := range topks {
		totalRows += topk
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

	if isElementCollapseSumFamily(config.Strategy) && !largerScoreIsBetter {
		return nil, merr.WrapErrParameterInvalidMsg(
			"%s.collapse.strategy %s is only supported for positively related metrics",
			elementScopeKey, config.Strategy)
	}

	if typeutil.GetSizeOfIDs(data.GetIds()) < int(totalRows) {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("element best collapse: ids length (%d) is less than total rows (%d)",
			typeutil.GetSizeOfIDs(data.GetIds()), totalRows))
	}
	if int64(len(data.GetScores())) < totalRows {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("element best collapse: scores length (%d) is less than total rows (%d)",
			len(data.GetScores()), totalRows))
	}
	if int64(len(data.GetElementIndices().GetData())) < totalRows {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("element best collapse: element_indices length (%d) is less than total rows (%d)",
			len(data.GetElementIndices().GetData()), totalRows))
	}
	if len(data.GetDistances()) > 0 && int64(len(data.GetDistances())) < totalRows {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("element best collapse: distances length (%d) is less than total rows (%d)",
			len(data.GetDistances()), totalRows))
	}
	if len(data.GetRecalls()) > 0 && int64(len(data.GetRecalls())) < totalRows {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("element best collapse: recalls length (%d) is less than total rows (%d)",
			len(data.GetRecalls()), totalRows))
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
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("element-level hybrid: ids length (%d) is less than total rows (%d)",
			typeutil.GetSizeOfIDs(data.GetIds()), totalRows))
	}
	if data.GetElementIndices() == nil {
		return nil, merr.WrapErrServiceInternal("element-level hybrid: missing element_indices")
	}
	if int64(len(data.GetElementIndices().GetData())) < totalRows {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("element-level hybrid: element_indices length (%d) is less than total rows (%d)",
			len(data.GetElementIndices().GetData()), totalRows))
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
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("element key restore: expected string element key, got %T", rawKey))
		}
		pk, elementIndex, ok := parseHybridElementKey(key)
		if !ok {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("element key restore: invalid element key %q", key))
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

type rerankOperator struct {
	nq              int64
	topK            int64
	offset          int64
	roundDecimal    int64
	groupByFieldId  int64
	groupSize       int64
	strictGroupSize bool
	groupScorerStr  string

	functionScore *rerank.FunctionScore
}

func newRerankOperator(t *searchTask, params map[string]any) (operator, error) {
	if t.GetIsAdvanced() {
		return &rerankOperator{
			nq:              t.GetNq(),
			topK:            t.rankParams.limit,
			offset:          t.rankParams.offset,
			roundDecimal:    t.rankParams.roundDecimal,
			groupByFieldId:  t.rankParams.groupByFieldId,
			groupSize:       t.rankParams.groupSize,
			strictGroupSize: t.rankParams.strictGroupSize,
			groupScorerStr:  getGroupScorerStr(t.request.GetSearchParams()),
			functionScore:   t.functionScore,
		}, nil
	}
	return &rerankOperator{
		nq:              t.GetNq(),
		topK:            t.GetTopk(),
		offset:          0, // Search performs Offset in the reduce phase
		roundDecimal:    t.queryInfos[0].RoundDecimal,
		groupByFieldId:  t.queryInfos[0].GroupByFieldId,
		groupSize:       t.queryInfos[0].GroupSize,
		strictGroupSize: t.queryInfos[0].StrictGroupSize,
		groupScorerStr:  getGroupScorerStr(t.request.GetSearchParams()),
		functionScore:   t.functionScore,
	}, nil
}

func (op *rerankOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "rerankOperator")
	defer sp.End()

	reducedResults := inputs[0].([]*milvuspb.SearchResults)
	metrics := inputs[1].([]string)
	rankInputs := []*milvuspb.SearchResults{}
	rankMetrics := []string{}
	for idx, ret := range reducedResults {
		rankInputs = append(rankInputs, ret)
		rankMetrics = append(rankMetrics, metrics[idx])
	}
	params := rerank.NewSearchParams(op.nq, op.topK, op.offset, op.roundDecimal, op.groupByFieldId,
		op.groupSize, op.strictGroupSize, op.groupScorerStr, rankMetrics)
	ret, err := op.functionScore.Process(ctx, params, rankInputs)
	if err != nil {
		return nil, err
	}
	return []any{ret}, nil
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

	node types.ProxyComponent
}

func newRequeryOperator(t *searchTask, _ map[string]any) (operator, error) {
	pkField, err := t.schema.GetPkField()
	if err != nil {
		return nil, err
	}
	outputFieldNames := typeutil.NewSet(t.translatedOutputFields...)
	if t.GetIsAdvanced() {
		outputFieldNames.Insert(t.functionScore.GetAllInputFieldNames()...)
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
	}
	plan := planparserv2.CreateRequeryPlan(op.primaryFieldSchema, ids)
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

// hybridAssembleOperator picks field data for reranked IDs directly from
// multiple sub-search results, avoiding a full field-data merge.
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

	var templateFields []*schemapb.FieldData
	for _, result := range reducedResults {
		if len(result.GetResults().GetFieldsData()) > 0 {
			templateFields = result.GetResults().GetFieldsData()
			break
		}
	}
	if templateFields == nil {
		return []any{rankResult}, nil
	}

	locs := make([]pkLoc, numReranked)
	computers := make([]*typeutil.FieldDataIdxComputer, len(reducedResults))
	for i, result := range reducedResults {
		if len(result.GetResults().GetFieldsData()) > 0 {
			computers[i] = typeutil.NewFieldDataIdxComputer(result.GetResults().GetFieldsData())
		}
	}

	itemsByResult := make([][]rowIdxComputeItem, len(reducedResults))
	for i := 0; i < numReranked; i++ {
		candidateKey := typeutil.GetPK(rerankedIDs, int64(i))
		if op.elementLevelHybrid {
			elementIndices := rankResult.GetResults().GetElementIndices().GetData()
			if i >= len(elementIndices) {
				return nil, merr.WrapErrServiceInternal(fmt.Sprintf("hybrid assemble: missing element index for reranked row %d, collection=%d", i, op.collectionID))
			}
			candidateKey = makeHybridElementKey(candidateKey, elementIndices[i])
		}
		loc, ok := pkIndex[candidateKey]
		if !ok {
			return nil, merr.WrapErrInconsistentRequery(
				fmt.Sprintf("hybrid assemble: missing id %v, collection=%d", candidateKey, op.collectionID))
		}
		if computers[loc.resultIdx] == nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf(
				"hybrid assemble: sub-result[%d] has empty FieldsData but contributed reranked id %v; "+
					"all sub-results that contribute ids must share the same FieldsData layout, "+
					"collection=%d", loc.resultIdx, candidateKey, op.collectionID))
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
	name  string
	nodes []*Node
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
	return &pipeline{name: pipeDef.name, nodes: nodes}, nil
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
	log.Ctx(ctx).Debug("SearchPipeline run", zap.String("pipeline", p.String()))
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
	}
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

var filterFieldNode = &nodeDef{
	name:    "filter_field",
	inputs:  []string{"result", "reduced"},
	outputs: []string{"output"},
	opName:  endOp,
}

var highlightNode = &nodeDef{
	name:    "highlight",
	inputs:  []string{"result"},
	outputs: []string{"output"},
	opName:  highlightOp,
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
			outputs: []string{"ids", "fields"},
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
			inputs:  []string{"ids", "fields"},
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

func newBuiltInPipeline(t *searchTask) (*pipeline, error) {
	if !t.GetIsAdvanced() && !t.needRequery && t.functionScore == nil {
		return newPipeline(searchPipe, t)
	}
	if !t.GetIsAdvanced() && t.needRequery && t.functionScore == nil {
		return newPipeline(searchWithRequeryPipe, t)
	}
	if !t.GetIsAdvanced() && !t.needRequery && t.functionScore != nil {
		return newPipeline(searchWithRerankPipe, t)
	}
	if !t.GetIsAdvanced() && t.needRequery && t.functionScore != nil {
		return newPipeline(searchWithRerankRequeryPipe, t)
	}
	if t.GetIsAdvanced() && !t.needRequery {
		return newPipeline(hybridSearchPipe, t)
	}
	if t.GetIsAdvanced() && t.needRequery {
		if len(t.functionScore.GetAllInputFieldIDs()) > 0 {
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
	return nil, fmt.Errorf("unsupported pipeline")
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

func newSearchPipeline(t *searchTask) (*pipeline, error) {
	p, err := newBuiltInPipeline(t)
	if err != nil {
		return nil, err
	}

	if t.highlighter != nil {
		err := p.AddNodes(t, highlightNode, filterFieldNode)
		if err != nil {
			return nil, err
		}
	} else {
		err := p.AddNodes(t, filterFieldNode)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}
