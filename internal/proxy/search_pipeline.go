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
	for i := 0; i < typeutil.GetSizeOfIDs(ids); i++ {
		id := typeutil.GetPK(ids, int64(i))
		if _, ok := pkOffset[id]; !ok {
			return nil, merr.WrapErrInconsistentRequery(fmt.Sprintf("incomplete query result, missing id %s, len(searchIDs) = %d, len(queryIDs) = %d, collection=%d",
				id, typeutil.GetSizeOfIDs(ids), len(pkOffset), collectionID))
		}
		typeutil.AppendFieldData(fieldsData, fields, int64(pkOffset[id]))
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
	},
}

func newBuiltInPipeline(t *searchTask) (*pipeline, error) {
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
