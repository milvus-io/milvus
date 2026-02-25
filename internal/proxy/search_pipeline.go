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
	"github.com/tidwall/gjson"
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
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
			return nil, fmt.Errorf("Node [%s]'s input %s not found", n.name, input)
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
		return nil, fmt.Errorf("Node [%s] output size not match operator output size", n.name)
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
	searchReduceOp       = "search_reduce"
	hybridSearchReduceOp = "hybrid_search_reduce"
	rerankOp             = "rerank"
	requeryOp            = "requery"
	organizeOp           = "organize"
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
	rerankOp:             newRerankOperator,
	organizeOp:           newOrganizeOperator,
	requeryOp:            newRequeryOperator,
	lambdaOp:             newLambdaOperator,
	endOp:                newEndOperator,
	highlightOp:          newHighlightOperator,
	orderByOp:            newOrderByOperator,
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
	if t.SearchRequest.GetIsAdvanced() {
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
		nq:              t.SearchRequest.GetNq(),
		topK:            t.SearchRequest.GetTopk(),
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
	if t.SearchRequest.GetIsAdvanced() {
		outputFieldNames.Insert(t.functionScore.GetAllInputFieldNames()...)
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
		consistencyLevel:   t.SearchRequest.GetConsistencyLevel(),
		guaranteeTimestamp: t.SearchRequest.GetGuaranteeTimestamp(),
		notReturnAllMeta:   t.request.GetNotReturnAllMeta(),
		partitionNames:     t.request.GetPartitionNames(),
		partitionIDs:       t.SearchRequest.GetPartitionIDs(),
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
		collectionID:       t.SearchRequest.GetCollectionID(),
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
		groupByFieldId = t.queryInfos[0].GetGroupByFieldId()
		groupSize = t.queryInfos[0].GetGroupSize()
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
		return nil, fmt.Errorf("order_by: Topks sum (%d) does not match numResults (%d)", sumTopks, numResults)
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
			return fmt.Errorf("order_by field '%s' not found in search results", orderBy.FieldName)
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

	groupByValue := result.GetResults().GetGroupByFieldValue()
	if groupByValue == nil {
		// No group by field value, fall back to regular sort
		return op.sortResultsByOrderByFields(result, indices)
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
			return 0, fmt.Errorf("compareFieldDataAt: index out of bounds for Int field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data))
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
			return 0, fmt.Errorf("compareFieldDataAt: index out of bounds for Int64 field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data))
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
			return 0, fmt.Errorf("compareFieldDataAt: index out of bounds for Float field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data))
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
			return 0, fmt.Errorf("compareFieldDataAt: index out of bounds for Double field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data))
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
			return 0, fmt.Errorf("compareFieldDataAt: index out of bounds for String field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data))
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
			return 0, fmt.Errorf("compareFieldDataAt: index out of bounds for JSON field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data))
		}
		return bytes.Compare(data[i], data[j]), nil
	case schemapb.DataType_Bool:
		data := field.GetScalars().GetBoolData().GetData()
		if i >= len(data) || j >= len(data) {
			return 0, fmt.Errorf("compareFieldDataAt: index out of bounds for Bool field %s (i=%d, j=%d, len=%d)", field.GetFieldName(), i, j, len(data))
		}
		// false < true
		if !data[i] && data[j] {
			return -1, nil
		} else if data[i] && !data[j] {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("compareFieldDataAt: unsupported field type %s for field %s", field.GetType().String(), field.GetFieldName())
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
					return fmt.Errorf("reorderResults: index %d out of bounds for int IDs (len=%d)", oldIdx, len(intIds.Data))
				}
				newData[newIdx] = intIds.Data[oldIdx]
			}
			intIds.Data = newData
		} else if strIds := ids.GetStrId(); strIds != nil {
			newData := make([]string, n)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= len(strIds.Data) {
					return fmt.Errorf("reorderResults: index %d out of bounds for string IDs (len=%d)", oldIdx, len(strIds.Data))
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
				return fmt.Errorf("reorderResults: index %d out of bounds for scores (len=%d)", oldIdx, len(results.Scores))
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

	// Reorder group by field value if present
	if groupByValue := results.GetGroupByFieldValue(); groupByValue != nil {
		if err := reorderFieldData(groupByValue, indices); err != nil {
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
					return fmt.Errorf("reorderFieldData: index %d out of bounds for %s field %s (len=%d)", oldIdx, field.GetType().String(), field.GetFieldName(), len(data.Data))
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
					return fmt.Errorf("reorderFieldData: index %d out of bounds for Int64 field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data))
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
					return fmt.Errorf("reorderFieldData: index %d out of bounds for Float field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data))
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
					return fmt.Errorf("reorderFieldData: index %d out of bounds for Double field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data))
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
					return fmt.Errorf("reorderFieldData: index %d out of bounds for %s field %s (len=%d)", oldIdx, field.GetType().String(), field.GetFieldName(), len(data.Data))
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
					return fmt.Errorf("reorderFieldData: index %d out of bounds for Bool field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data))
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
					return fmt.Errorf("reorderFieldData: index %d out of bounds for JSON field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data))
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
					return fmt.Errorf("reorderFieldData: index %d out of bounds for Array field %s (len=%d)", oldIdx, field.GetFieldName(), len(data.Data))
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
				return fmt.Errorf("reorderFieldData: invalid dimension %d for FloatVector field %s", dim, field.GetFieldName())
			}
			data := vectors.GetFloatVector().GetData()
			if len(data) != n*dim {
				return fmt.Errorf("reorderFieldData: FloatVector field %s has %d elements, expected %d (n=%d, dim=%d)", field.GetFieldName(), len(data), n*dim, n, dim)
			}
			newData := make([]float32, n*dim)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= n {
					return fmt.Errorf("reorderFieldData: index %d out of bounds for FloatVector field %s (n=%d)", oldIdx, field.GetFieldName(), n)
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
				return fmt.Errorf("reorderFieldData: invalid dimension %d for BinaryVector field %s", dim, field.GetFieldName())
			}
			data := vectors.GetBinaryVector()
			if len(data) != n*bytesPerVector {
				return fmt.Errorf("reorderFieldData: BinaryVector field %s has %d bytes, expected %d (n=%d, bytesPerVector=%d)", field.GetFieldName(), len(data), n*bytesPerVector, n, bytesPerVector)
			}
			newData := make([]byte, n*bytesPerVector)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= n {
					return fmt.Errorf("reorderFieldData: index %d out of bounds for BinaryVector field %s (n=%d)", oldIdx, field.GetFieldName(), n)
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
				return fmt.Errorf("reorderFieldData: invalid dimension %d for Float16Vector field %s", dim, field.GetFieldName())
			}
			bytesPerVector := dim * 2 // 2 bytes per float16
			data := vectors.GetFloat16Vector()
			if len(data) != n*bytesPerVector {
				return fmt.Errorf("reorderFieldData: Float16Vector field %s has %d bytes, expected %d (n=%d, bytesPerVector=%d)", field.GetFieldName(), len(data), n*bytesPerVector, n, bytesPerVector)
			}
			newData := make([]byte, n*bytesPerVector)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= n {
					return fmt.Errorf("reorderFieldData: index %d out of bounds for Float16Vector field %s (n=%d)", oldIdx, field.GetFieldName(), n)
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
				return fmt.Errorf("reorderFieldData: invalid dimension %d for BFloat16Vector field %s", dim, field.GetFieldName())
			}
			bytesPerVector := dim * 2 // 2 bytes per bfloat16
			data := vectors.GetBfloat16Vector()
			if len(data) != n*bytesPerVector {
				return fmt.Errorf("reorderFieldData: BFloat16Vector field %s has %d bytes, expected %d (n=%d, bytesPerVector=%d)", field.GetFieldName(), len(data), n*bytesPerVector, n, bytesPerVector)
			}
			newData := make([]byte, n*bytesPerVector)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= n {
					return fmt.Errorf("reorderFieldData: index %d out of bounds for BFloat16Vector field %s (n=%d)", oldIdx, field.GetFieldName(), n)
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
				return fmt.Errorf("reorderFieldData: SparseFloatVector field %s has %d elements, expected %d", field.GetFieldName(), len(contents), n)
			}
			newContents := make([][]byte, n)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= n {
					return fmt.Errorf("reorderFieldData: index %d out of bounds for SparseFloatVector field %s (n=%d)", oldIdx, field.GetFieldName(), n)
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
				return fmt.Errorf("reorderFieldData: invalid dimension %d for Int8Vector field %s", dim, field.GetFieldName())
			}
			data := vectors.GetInt8Vector()
			if len(data) != n*dim {
				return fmt.Errorf("reorderFieldData: Int8Vector field %s has %d bytes, expected %d (n=%d, dim=%d)", field.GetFieldName(), len(data), n*dim, n, dim)
			}
			newData := make([]byte, n*dim)
			for newIdx, oldIdx := range indices {
				if oldIdx < 0 || oldIdx >= n {
					return fmt.Errorf("reorderFieldData: index %d out of bounds for Int8Vector field %s (n=%d)", oldIdx, field.GetFieldName(), n)
				}
				srcStart := oldIdx * dim
				dstStart := newIdx * dim
				copy(newData[dstStart:dstStart+dim], data[srcStart:srcStart+dim])
			}
			vectors.Data = &schemapb.VectorField_Int8Vector{Int8Vector: newData}
		}
	default:
		return fmt.Errorf("reorderFieldData: unhandled data type %s", field.GetType().String())
	}

	// Reorder valid data if present
	if len(field.ValidData) > 0 {
		newValidData := make([]bool, n)
		for newIdx, oldIdx := range indices {
			if oldIdx < 0 || oldIdx >= len(field.ValidData) {
				return fmt.Errorf("reorderFieldData: index %d out of bounds for ValidData of field %s (len=%d)", oldIdx, field.GetFieldName(), len(field.ValidData))
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
	log.Ctx(ctx).Debug("SearchPipeline run", zap.String("pipeline", p.name))
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
		buf.WriteString(fmt.Sprintf("  %s -> %s", node.name, node.outputs))
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
			name:    "rerank",
			inputs:  []string{"reduced", "metrics"},
			outputs: []string{"result"},
			opName:  rerankOp,
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
		{
			name:    "filter_field",
			inputs:  []string{"result", "reduced"},
			outputs: []string{pipelineOutput},
			opName:  endOp,
		},
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
	hasOrderBy := len(t.orderByFields) > 0

	// Common search with order_by: reduce → requery → order_by
	if !t.SearchRequest.GetIsAdvanced() && hasOrderBy {
		return newPipeline(searchWithOrderByPipe, t)
	}

	if !t.SearchRequest.GetIsAdvanced() && !t.needRequery && t.functionScore == nil {
		return newPipeline(searchPipe, t)
	}
	if !t.SearchRequest.GetIsAdvanced() && t.needRequery && t.functionScore == nil {
		return newPipeline(searchWithRequeryPipe, t)
	}
	if !t.SearchRequest.GetIsAdvanced() && !t.needRequery && t.functionScore != nil {
		return newPipeline(searchWithRerankPipe, t)
	}
	if !t.SearchRequest.GetIsAdvanced() && t.needRequery && t.functionScore != nil {
		return newPipeline(searchWithRerankRequeryPipe, t)
	}
	if t.SearchRequest.GetIsAdvanced() && !t.needRequery {
		return newPipeline(hybridSearchPipe, t)
	}
	if t.SearchRequest.GetIsAdvanced() && t.needRequery {
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
	return nil, fmt.Errorf("Unsupported pipeline")
}

func newSearchPipeline(t *searchTask) (*pipeline, error) {
	p, err := newBuiltInPipeline(t)
	if err != nil {
		return nil, err
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
