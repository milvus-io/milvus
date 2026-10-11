// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dql

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	chaintypes "github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/transformio"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const searchQueryColumn = "query"

type searchTransformMeta struct {
	plan       *schemapb.FunctionChain
	repr       *chain.ChainRepr
	executor   transform.Executor
	inputNames []string
	inputIDs   []int64
	jsonPlan   *chain.DataFrameInputPlan
}

func newSearchTransformMeta(plan *schemapb.FunctionChain, schema *schemaInfo, dbName string) (*searchTransformMeta, error) {
	if err := transform.ValidatePlan(plan); err != nil {
		return nil, err
	}
	repr, err := chain.ProtoChainToRepr(plan)
	if err != nil {
		return nil, err
	}
	buildCtx := chaintypes.FunctionBuildContext{ModelExtraInfo: &models.ModelExtraInfo{
		ClusterID: paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), DBName: dbName,
	}}
	meta := &searchTransformMeta{
		plan: proto.Clone(plan).(*schemapb.FunctionChain), repr: repr,
		executor: chain.NewTransformExecutor(memory.DefaultAllocator, buildCtx),
	}
	if plan.GetStage() == schemapb.FunctionChainStage_FunctionChainStagePreProcess {
		for _, input := range repr.Info.RequiredInputs {
			if input != searchQueryColumn {
				return nil, merr.WrapErrParameterInvalidMsg("PRE_PROCESS input %q is unavailable; the query payload column is %q", input, searchQueryColumn)
			}
		}
	} else {
		if schema == nil || schema.CollectionSchema == nil {
			return nil, merr.WrapErrServiceInternalMsg("POST_PROCESS collection schema is missing")
		}
		if err := meta.planPostInputs(schema); err != nil {
			return nil, err
		}
	}
	// Bind inputs before constructing functions, then validate availability even
	// when a search later returns no hits. Invalid field references must not
	// trigger a Python resource download or model initialization.
	if _, err := chain.FuncChainFromReprWithContext(repr, memory.DefaultAllocator, buildCtx); err != nil {
		return nil, err
	}
	return meta, nil
}

// planPostInputs resolves physical roots before fetching, while runtime-created
// columns remain local to the executor. Nested JSON paths use the existing typed
// JSON projection; arrays/vectors are converted by the endpoint's Arrow adapter.
func (meta *searchTransformMeta) planPostInputs(schema *schemaInfo) error {
	names := typeutil.NewSet[string]()
	ids := typeutil.NewSet[int64]()
	jsonRepr := &chain.ChainRepr{Stage: chaintypes.StagePostProcess}
	for _, name := range meta.repr.Info.RequiredInputs {
		switch name {
		case chaintypes.IDFieldName, chaintypes.ScoreFieldName, chaintypes.ElementIndicesFieldName:
			if err := meta.validatePostSourceHint(name, schemapb.DataType_None); err != nil {
				return err
			}
			continue
		}
		if field := schema.SchemaHelper.GetStructArrayFieldFromName(name); field != nil {
			if err := meta.validatePostSourceHint(name, schemapb.DataType_ArrayOfStruct); err != nil {
				return err
			}
			names.Insert(name)
			for _, child := range field.GetFields() {
				if child.GetDataType() == schemapb.DataType_ArrayOfVector {
					return merr.WrapErrParameterInvalidMsg("POST_PROCESS does not yet support ArrayOfVector source %q in struct %q", child.GetName(), name)
				}
				ids.Insert(child.GetFieldID())
			}
			continue
		}
		if field, err := schema.SchemaHelper.GetFieldFromName(name); err == nil {
			if field.GetDataType() == schemapb.DataType_ArrayOfVector {
				return merr.WrapErrParameterInvalidMsg("POST_PROCESS does not yet support ArrayOfVector source %q", name)
			}
			if err := meta.validatePostSourceHint(name, field.GetDataType()); err != nil {
				return err
			}
			names.Insert(field.GetName())
			ids.Insert(field.GetFieldID())
			continue
		}
		// Preserve occurrence-aligned JSON type hints and let the existing
		// planner detect conflicting hints for the same projected path.
		for _, op := range meta.repr.Operators {
			for index, input := range op.Inputs {
				if input != name {
					continue
				}
				hint := schemapb.DataType_None
				if len(op.InputDataTypes) > index {
					hint = op.InputDataTypes[index]
				}
				jsonRepr.Operators = append(jsonRepr.Operators, chain.OperatorRepr{
					Type: chaintypes.OpTypeMap, Inputs: []string{name}, InputDataTypes: []schemapb.DataType{hint},
				})
			}
		}
	}
	if len(jsonRepr.Operators) > 0 {
		plan, err := chain.CompileDataFrameInputPlanWithSchemaHelper(jsonRepr, schema.SchemaHelper)
		if err != nil {
			return merr.Wrap(err, "POST_PROCESS inputs")
		}
		meta.jsonPlan = plan
		names.Insert(plan.PhysicalFieldNames()...)
		ids.Insert(plan.PhysicalFieldIDs()...)
	}
	meta.inputNames, meta.inputIDs = names.Collect(), ids.Collect()
	return nil
}

func (meta *searchTransformMeta) validatePostSourceHint(name string, dataType schemapb.DataType) error {
	for _, op := range meta.repr.Operators {
		for index, input := range op.Inputs {
			if input != name || len(op.InputDataTypes) <= index || op.InputDataTypes[index] == schemapb.DataType_None {
				continue
			}
			hint := op.InputDataTypes[index]
			if dataType == schemapb.DataType_None {
				return merr.WrapErrParameterInvalidMsg("POST_PROCESS protected input %q does not accept a type hint", name)
			}
			if hint == dataType {
				continue
			}
			actualArrow, actualErr := chain.ToArrowType(dataType)
			hintArrow, hintErr := chain.ToArrowType(hint)
			if actualErr != nil || hintErr != nil || !arrow.TypeEqual(actualArrow, hintArrow) {
				return merr.WrapErrParameterInvalidMsg("POST_PROCESS input %q type hint %s is incompatible with source type %s", name, hint, dataType)
			}
		}
		for _, output := range op.Outputs {
			if output == name {
				// Subsequent reads refer to an executor-produced value, which may
				// intentionally have a different type from the stored source.
				return nil
			}
		}
	}
	return nil
}

func hasSearchTransforms(request *milvuspb.SearchRequest) bool {
	for _, stage := range []schemapb.FunctionChainStage{
		schemapb.FunctionChainStage_FunctionChainStagePreProcess,
		schemapb.FunctionChainStage_FunctionChainStagePostProcess,
	} {
		if hasFunctionChainStage(request.GetFunctionChains(), stage) {
			return true
		}
		for _, sub := range request.GetSubReqs() {
			if hasFunctionChainStage(sub.GetFunctionChains(), stage) {
				return true
			}
		}
	}
	return false
}

func hasTransformIterator(params []*commonpb.KeyValuePair) bool {
	for _, kv := range params {
		if kv.GetKey() == IteratorField || kv.GetKey() == SearchIterV2Key {
			if enabled, err := strconv.ParseBool(kv.GetValue()); err == nil && enabled {
				return true
			}
		}
	}
	return false
}

func (t *SearchTask) prepareSearchTransforms() error {
	if t.transformsPrepared || !hasSearchTransforms(t.request) {
		return nil
	}
	if err := validateFunctionChainSearchRequest(t.request, t.GetIsAdvanced()); err != nil {
		return err
	}
	if t.request.GetIds() != nil {
		return merr.WrapErrParameterInvalidMsg("PRE_PROCESS and POST_PROCESS are not supported with search by IDs")
	}
	if hasTransformIterator(t.request.GetSearchParams()) {
		return merr.WrapErrParameterInvalidMsg("PRE_PROCESS and POST_PROCESS are not supported with search iterator")
	}
	if t.GetIsAdvanced() && len(t.request.GetSubReqs()) > defaultMaxSearchRequest {
		return merr.WrapErrParameterInvalidMsg("maximum of ann search requests is %d", defaultMaxSearchRequest)
	}
	if hasFunctionChainStage(t.request.GetFunctionChains(), schemapb.FunctionChainStage_FunctionChainStagePostProcess) && t.request.GetSearchAggregation() != nil {
		return merr.WrapErrParameterInvalidMsg("POST_PROCESS requires candidate results and is not supported with search_aggregation")
	}
	for index, sub := range t.request.GetSubReqs() {
		if _, _, err := splitFunctionChainsByStage(sub.GetFunctionChains()); err != nil {
			return merr.Wrapf(err, "sub-search[%d]", index)
		}
		if hasFunctionChainStage(sub.GetFunctionChains(), schemapb.FunctionChainStage_FunctionChainStagePostProcess) {
			return merr.WrapErrParameterInvalidMsg("sub-search[%d]: POST_PROCESS belongs to the top-level hybrid request", index)
		}
		if hasFunctionChainStage(sub.GetFunctionChains(), schemapb.FunctionChainStage_FunctionChainStageL2Rerank) {
			return merr.WrapErrParameterInvalidMsg("sub-search[%d] function chains support PRE_PROCESS, L0 and L1", index)
		}
		if hasTransformIterator(sub.GetSearchParams()) {
			return merr.WrapErrParameterInvalidMsg("PRE_PROCESS and POST_PROCESS are not supported with search iterator")
		}
	}
	// The task owns transformed placeholders. Automatic RPC retries must start
	// from the client's original inputs, rather than transform a previous result.
	t.request = proto.Clone(t.request).(*milvuspb.SearchRequest)
	t.preProcess = make([]*searchTransformMeta, 1)
	if t.GetIsAdvanced() {
		t.preProcess = make([]*searchTransformMeta, len(t.request.GetSubReqs()))
	}
	for index := range t.preProcess {
		plans := t.request.GetFunctionChains()
		if t.GetIsAdvanced() {
			plans = t.request.GetSubReqs()[index].GetFunctionChains()
		}
		pre := functionChainsAtStage(plans, schemapb.FunctionChainStage_FunctionChainStagePreProcess)
		if len(pre) > 0 {
			meta, err := newSearchTransformMeta(pre[0], t.schema, t.request.GetDbName())
			if err != nil {
				return merr.Wrapf(err, "PRE_PROCESS[%d]", index)
			}
			t.preProcess[index] = meta
		}
	}
	post := functionChainsAtStage(t.request.GetFunctionChains(), schemapb.FunctionChainStage_FunctionChainStagePostProcess)
	if len(post) > 0 {
		meta, err := newSearchTransformMeta(post[0], t.schema, t.request.GetDbName())
		if err != nil {
			return merr.Wrap(err, "POST_PROCESS")
		}
		t.postProcess = meta
	}
	t.transformsPrepared = true
	return nil
}

func (t *SearchTask) executeSearchPreProcess(ctx context.Context) error {
	if t.preProcessed {
		return nil
	}
	if err := t.prepareQueryContext(); err != nil {
		return err
	}
	for index, meta := range t.preProcess {
		if meta == nil {
			continue
		}
		raw := t.request.GetPlaceholderGroup()
		if t.GetIsAdvanced() {
			raw = t.request.GetSubReqs()[index].GetPlaceholderGroup()
		}
		if len(raw) == 0 {
			return merr.WrapErrParameterInvalidMsg("PRE_PROCESS requires query payloads; search by IDs is not supported")
		}
		processed, err := meta.processQueryInput(ctx, raw, t.GetNq(), t.queryContext)
		if err != nil {
			return merr.Wrapf(err, "PRE_PROCESS[%d]", index)
		}
		if t.GetIsAdvanced() {
			t.request.SubReqs[index].PlaceholderGroup = processed
		} else {
			t.request.SearchInput = &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: processed}
		}
	}
	if t.queryContext != nil {
		queries, err := t.snapshotQueryInputs()
		if err != nil {
			return searchTransformOutputError(err, "query context after PRE_PROCESS")
		}
		t.queryContext = &chaintypes.QueryContext{OriginalQueries: t.queryContext.OriginalQueries, Queries: queries}
	}
	t.preProcessed = true
	return nil
}

func (meta *searchTransformMeta) processQueryInput(ctx context.Context, raw []byte, nq int64, queries *chaintypes.QueryContext) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	phg := &commonpb.PlaceholderGroup{}
	if err := proto.Unmarshal(raw, phg); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("PRE_PROCESS placeholder group is malformed")
	}
	if len(phg.GetPlaceholders()) != 1 || int64(len(phg.GetPlaceholders()[0].GetValues())) != nq {
		return nil, merr.WrapErrParameterInvalidMsg("PRE_PROCESS requires one placeholder with exactly %d query values", nq)
	}
	fd, err := queryPlaceholderFieldData(phg.Placeholders[0])
	if err != nil {
		return nil, err
	}
	table, err := transformio.FromFieldData([]*schemapb.FieldData{fd}, nq, memory.DefaultAllocator)
	if err != nil {
		return nil, merr.Wrap(err, "PRE_PROCESS decode query inputs")
	}
	defer table.Release()
	// Query inputs are rows of one request group, allowing the function to
	// batch translation/embedding across NQ. Candidate POST groups remain per
	// query. Identity lineage still protects every original query row.
	input := transform.Batch{Groups: []arrow.Table{table}, QueryContext: queries}
	result, err := meta.executor.Execute(ctx, meta.plan, input)
	if err != nil {
		return nil, err
	}
	defer releaseSearchTransformBatch(result.Data)
	if err := transform.ValidateResult(meta.plan.GetStage(), input, result); err != nil {
		return nil, err
	}
	combined, err := concatenateSearchTransformGroups(result.Data)
	if err != nil {
		return nil, err
	}
	defer combined.Release()
	queryIndexes := combined.Schema().FieldIndices(searchQueryColumn)
	if len(queryIndexes) != 1 {
		return nil, merr.WrapErrFunctionFailedMsg("PRE_PROCESS output must contain one %q payload column", searchQueryColumn)
	}
	query := combined.Column(queryIndexes[0])
	queryType, err := queryOutputType(query, fd)
	if err != nil {
		return nil, err
	}
	// Intermediate values are not query payloads. Only the explicitly named
	// query column is bound back to the ordinary search input contract.
	queryTable := array.NewTable(arrow.NewSchema([]arrow.Field{query.Field()}, nil), []arrow.Column{*query}, combined.NumRows())
	defer queryTable.Release()
	fields, err := transformio.ToFieldData(queryTable, []*schemapb.FieldSchema{queryType})
	if err != nil {
		return nil, searchTransformOutputError(err, "PRE_PROCESS bind query outputs")
	}
	if len(fields) != 1 || query.Data().NullN() != 0 {
		return nil, merr.WrapErrFunctionFailedMsg("PRE_PROCESS must return one non-null query value per input")
	}
	if err := validateGeneratedSearchField(fields[0]); err != nil {
		return nil, err
	}
	encoded, count, err := funcutil.FieldDataToPlaceholderGroupBytesWithCount(fields[0])
	if err != nil {
		return nil, searchTransformOutputError(err, "PRE_PROCESS encode query outputs")
	}
	if int64(count) != nq {
		return nil, merr.WrapErrFunctionFailedMsg("PRE_PROCESS changed query count from %d to %d", nq, count)
	}
	output := &commonpb.PlaceholderGroup{}
	if err := proto.Unmarshal(encoded, output); err != nil || len(output.GetPlaceholders()) != 1 {
		return nil, merr.WrapErrServiceInternalMsg("PRE_PROCESS output placeholder encoding failed")
	}
	output.Placeholders[0].Tag = phg.Placeholders[0].GetTag()
	encoded, err = proto.Marshal(output)
	if err != nil {
		return nil, merr.WrapErrFunctionFailed(err, "PRE_PROCESS serialize query output")
	}
	return encoded, nil
}

func queryPlaceholderFieldData(ph *commonpb.PlaceholderValue) (*schemapb.FieldData, error) {
	fd := &schemapb.FieldData{FieldName: searchQueryColumn}
	switch ph.GetType() {
	case commonpb.PlaceholderType_VarChar:
		values := make([]string, len(ph.GetValues()))
		for index, value := range ph.GetValues() {
			if !utf8.Valid(value) {
				return nil, merr.WrapErrParameterInvalidMsg("PRE_PROCESS query %d is not valid UTF-8", index)
			}
			values[index] = string(value)
		}
		fd.Type = schemapb.DataType_VarChar
		fd.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}}}
	case commonpb.PlaceholderType_FloatVector:
		dim := 0
		values := make([]float32, 0)
		for index, value := range ph.GetValues() {
			if len(value) == 0 || len(value)%4 != 0 || dim != 0 && len(value)/4 != dim {
				return nil, merr.WrapErrParameterInvalidMsg("PRE_PROCESS float query %d has inconsistent dimension or encoding", index)
			}
			dim = len(value) / 4
			for offset := 0; offset < len(value); offset += 4 {
				values = append(values, math.Float32frombits(binary.LittleEndian.Uint32(value[offset:offset+4])))
			}
		}
		fd.Type = schemapb.DataType_FloatVector
		fd.Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: int64(dim), Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: values}}}}
	case commonpb.PlaceholderType_BinaryVector, commonpb.PlaceholderType_Float16Vector,
		commonpb.PlaceholderType_BFloat16Vector, commonpb.PlaceholderType_Int8Vector:
		width := 0
		values := make([]byte, 0)
		for index, value := range ph.GetValues() {
			if len(value) == 0 || width != 0 && len(value) != width {
				return nil, merr.WrapErrParameterInvalidMsg("PRE_PROCESS vector query %d has inconsistent byte width", index)
			}
			width = len(value)
			values = append(values, value...)
		}
		vectors := &schemapb.VectorField{}
		switch ph.GetType() {
		case commonpb.PlaceholderType_BinaryVector:
			fd.Type = schemapb.DataType_BinaryVector
			vectors.Dim, vectors.Data = int64(width*8), &schemapb.VectorField_BinaryVector{BinaryVector: values}
		case commonpb.PlaceholderType_Int8Vector:
			fd.Type = schemapb.DataType_Int8Vector
			vectors.Dim, vectors.Data = int64(width), &schemapb.VectorField_Int8Vector{Int8Vector: values}
		case commonpb.PlaceholderType_Float16Vector, commonpb.PlaceholderType_BFloat16Vector:
			if width%2 != 0 {
				return nil, merr.WrapErrParameterInvalidMsg("PRE_PROCESS 16-bit query vector has odd byte width")
			}
			vectors.Dim = int64(width / 2)
			if ph.GetType() == commonpb.PlaceholderType_Float16Vector {
				fd.Type = schemapb.DataType_Float16Vector
				vectors.Data = &schemapb.VectorField_Float16Vector{Float16Vector: values}
			} else {
				fd.Type = schemapb.DataType_BFloat16Vector
				vectors.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: values}
			}
		}
		fd.Field = &schemapb.FieldData_Vectors{Vectors: vectors}
	case commonpb.PlaceholderType_SparseFloatVector:
		for _, value := range ph.GetValues() {
			if err := typeutil.ValidateSparseFloatRows(value); err != nil {
				return nil, merr.Wrap(err, "PRE_PROCESS sparse query input")
			}
		}
		fd.Type = schemapb.DataType_SparseFloatVector
		fd.Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_SparseFloatVector{
			SparseFloatVector: &schemapb.SparseFloatArray{Contents: ph.GetValues()},
		}}}
	default:
		return nil, merr.WrapErrParameterInvalidMsg("PRE_PROCESS does not support query placeholder type %s", ph.GetType())
	}
	return fd, nil
}

func queryOutputType(column *arrow.Column, source *schemapb.FieldData) (*schemapb.FieldSchema, error) {
	dataType := column.DataType()
	field := &schemapb.FieldSchema{Name: searchQueryColumn}
	switch typed := dataType.(type) {
	case *arrow.StringType, *arrow.LargeStringType:
		field.DataType = schemapb.DataType_VarChar
	case *arrow.FixedSizeListType:
		if typed.Elem().ID() != arrow.FLOAT32 && typed.Elem().ID() != arrow.FLOAT64 {
			return nil, merr.WrapErrFunctionFailedMsg("PRE_PROCESS vector elements must be Float32 or Float64")
		}
		field.DataType = schemapb.DataType_FloatVector
		field.TypeParams = []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.Itoa(int(typed.Len()))}}
	case *arrow.ListType, *arrow.LargeListType:
		listType := typed.(arrow.ListLikeType)
		if listType.Elem().ID() != arrow.FLOAT32 && listType.Elem().ID() != arrow.FLOAT64 {
			return nil, merr.WrapErrFunctionFailedMsg("PRE_PROCESS vector elements must be Float32 or Float64")
		}
		dim := int64(0)
		for _, chunk := range column.Data().Chunks() {
			if chunk.Len() == 0 {
				continue
			}
			switch values := chunk.(type) {
			case *array.List:
				start, end := values.ValueOffsets(0)
				dim = end - start
			case *array.LargeList:
				start, end := values.ValueOffsets(0)
				dim = end - start
			}
			break
		}
		if dim <= 0 {
			return nil, merr.WrapErrFunctionFailedMsg("PRE_PROCESS returned an empty query vector")
		}
		field.DataType = schemapb.DataType_FloatVector
		field.TypeParams = []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.FormatInt(dim, 10)}}
	case *arrow.BinaryType, *arrow.FixedSizeBinaryType:
		if !typeutil.IsVectorType(source.GetType()) || source.GetType() == schemapb.DataType_FloatVector {
			return nil, merr.WrapErrFunctionFailedMsg("PRE_PROCESS binary query output requires a binary-encoded vector input type")
		}
		field.DataType = source.GetType()
		if source.GetType() != schemapb.DataType_SparseFloatVector {
			field.TypeParams = []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.FormatInt(source.GetVectors().GetDim(), 10)}}
		}
	default:
		return nil, merr.WrapErrFunctionFailedMsg("PRE_PROCESS query output must be text or a supported vector; got %s", dataType)
	}
	return field, nil
}

// bindPreProcessVectorOutput validates generated vectors against their target
// before the ordinary native placeholder conversion. A malformed function
// result is a function failure even when downstream search would classify the
// same bytes, supplied directly by a client, as an invalid request.
func (t *SearchTask) bindPreProcessVectorOutput(index int, raw []byte, fieldID int64) error {
	if len(t.preProcess) <= index || t.preProcess[index] == nil {
		return nil
	}
	phg := &commonpb.PlaceholderGroup{}
	if err := proto.Unmarshal(raw, phg); err != nil {
		return merr.WrapErrFunctionFailed(err, "PRE_PROCESS generated malformed query placeholders")
	}
	if len(phg.GetPlaceholders()) != 1 {
		return merr.WrapErrFunctionFailedMsg("PRE_PROCESS output requires exactly one query placeholder")
	}
	placeholder := phg.GetPlaceholders()[0]
	outputType, isVector := placeholderTypeToDataType[placeholder.GetType()]
	if !isVector {
		// Text still enters collection-native embedding/BM25 processing.
		return nil
	}
	field := typeutil.GetFieldByID(t.schema.CollectionSchema, fieldID)
	if field == nil {
		return merr.WrapErrServiceInternalMsg("PRE_PROCESS target search field %d is missing from collection schema", fieldID)
	}
	fieldType := field.GetDataType()
	if fieldType == schemapb.DataType_ArrayOfVector {
		fieldType = field.GetElementType()
	}
	compatible := outputType == fieldType || outputType == schemapb.DataType_FloatVector &&
		(fieldType == schemapb.DataType_Float16Vector || fieldType == schemapb.DataType_BFloat16Vector)
	if !compatible {
		return merr.WrapErrFunctionFailedMsg("PRE_PROCESS generated query type %s is incompatible with target field %q type %s", outputType, field.GetName(), fieldType)
	}
	if outputType == schemapb.DataType_SparseFloatVector {
		return nil
	}
	dim, err := typeutil.GetDim(field)
	if err != nil {
		return merr.WrapErrServiceInternalErr(err, "PRE_PROCESS target vector dimension is missing")
	}
	width := dim
	switch outputType {
	case schemapb.DataType_FloatVector:
		width *= 4
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		width *= 2
	case schemapb.DataType_BinaryVector:
		width /= 8
	}
	for query, value := range placeholder.GetValues() {
		if int64(len(value)) != width {
			return merr.WrapErrFunctionFailedMsg("PRE_PROCESS generated query %d has %d bytes; target field %q requires %d", query, len(value), field.GetName(), width)
		}
		if outputType == schemapb.DataType_FloatVector {
			for offset := 0; offset < len(value); offset += 4 {
				component := float64(math.Float32frombits(binary.LittleEndian.Uint32(value[offset : offset+4])))
				if math.IsNaN(component) || math.IsInf(component, 0) {
					return merr.WrapErrFunctionFailedMsg("PRE_PROCESS generated a non-finite float component in query %d", query)
				}
			}
		}
	}
	return nil
}

func splitSearchTransformTable(table arrow.Table, counts []int64) (transform.Batch, error) {
	total := int64(0)
	for _, count := range counts {
		if count < 0 || count > math.MaxInt64-total {
			return transform.Batch{}, merr.WrapErrServiceInternalMsg("search transform has invalid logical group sizes")
		}
		total += count
	}
	if total != table.NumRows() {
		return transform.Batch{}, merr.WrapErrServiceInternalMsg("search transform group rows %d do not match Arrow rows %d", total, table.NumRows())
	}
	reader := array.NewTableReader(table, 0)
	defer reader.Release()
	var record arrow.Record
	position := int64(0)
	groups := make([]arrow.Table, 0, len(counts))
	for _, count := range counts {
		remaining := count
		parts := make([]arrow.Record, 0)
		for remaining > 0 {
			for record == nil || position == record.NumRows() {
				if !reader.Next() {
					for _, part := range parts {
						part.Release()
					}
					releaseSearchTransformBatch(transform.Batch{Groups: groups})
					return transform.Batch{}, merr.WrapErrServiceInternalMsg("search transform Arrow rows ended before logical groups")
				}
				record, position = reader.Record(), 0
			}
			size := min(remaining, record.NumRows()-position)
			columns := make([]arrow.Array, record.NumCols())
			for index := range columns {
				columns[index] = array.NewSlice(record.Column(index), position, position+size)
			}
			part := array.NewRecord(table.Schema(), columns, size)
			for _, column := range columns {
				column.Release()
			}
			parts = append(parts, part)
			position += size
			remaining -= size
		}
		groups = append(groups, array.NewTableFromRecords(table.Schema(), parts))
		for _, part := range parts {
			part.Release()
		}
	}
	return transform.Batch{Groups: groups}, nil
}

func releaseSearchTransformBatch(batch transform.Batch) {
	for _, table := range batch.Groups {
		if table != nil {
			table.Release()
		}
	}
}

func concatenateSearchTransformGroups(batch transform.Batch) (arrow.Table, error) {
	if len(batch.Groups) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("search transform has no logical query groups")
	}
	schema := batch.Groups[0].Schema()
	records := make([]arrow.Record, 0)
	defer func() {
		for _, record := range records {
			record.Release()
		}
	}()
	for _, table := range batch.Groups {
		if !schema.Equal(table.Schema()) {
			return nil, merr.WrapErrFunctionFailedMsg("search transform produced inconsistent schemas across queries")
		}
		reader := array.NewTableReader(table, 0)
		for reader.Next() {
			record := reader.Record()
			record.Retain()
			records = append(records, record)
		}
		reader.Release()
	}
	return array.NewTableFromRecords(schema, records), nil
}

func (t *SearchTask) executeSearchPostProcess(ctx context.Context, result *milvuspb.SearchResults) error {
	if t.postProcess == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if result == nil || result.GetResults() == nil {
		return merr.WrapErrServiceInternalMsg("POST_PROCESS search results are missing")
	}
	data := result.GetResults()
	if int64(len(data.GetTopks())) != t.GetNq() {
		return merr.WrapErrServiceInternalMsg("POST_PROCESS candidate query boundaries do not match request NQ")
	}
	rows := int64(0)
	for _, count := range data.GetTopks() {
		if count < 0 || count > math.MaxInt64-rows {
			return merr.WrapErrServiceInternalMsg("POST_PROCESS candidate counts are malformed")
		}
		rows += count
	}
	if int64(len(data.GetScores())) != rows || int64(typeutil.GetSizeOfIDs(data.GetIds())) != rows {
		return merr.WrapErrServiceInternalMsg("POST_PROCESS candidate IDs/scores do not align with row counts")
	}
	// Struct-array reconstruction normally happens after the pipeline. POST
	// needs the public nested shape before its expressions execute.
	fillFieldNames(t.schema.CollectionSchema, data)
	for _, field := range data.GetFieldsData() {
		if schema, err := t.schema.SchemaHelper.GetFieldFromID(field.GetFieldId()); err == nil {
			field.IsDynamic = schema.GetIsDynamic()
		}
	}
	reconstructStructFieldDataForSearch(result, t.schema.CollectionSchema)
	fields, err := t.postProcessFields(data, rows)
	if err != nil {
		return err
	}
	sourceNames := typeutil.NewSet(t.postProcess.inputNames...)
	chainFields := make([]*schemapb.FieldData, 0, len(sourceNames)+3)
	for _, field := range fields {
		if sourceNames.Contain(field.GetFieldName()) || chain.IsFunctionChainSystemName(field.GetFieldName()) {
			chainFields = append(chainFields, field)
		}
	}
	// Only declared inputs enter Arrow. Untouched requested payloads retain
	// their original wire representation, including types the chain engine
	// cannot consume yet, and do not incur a second copy of large vectors.
	table, err := transformio.FromFieldData(chainFields, rows, memory.DefaultAllocator)
	if err != nil {
		if merr.GetErrorType(err) == merr.InputError {
			// The converter also serves INSERT user inputs. Here the values are
			// produced by QueryNode/requery, so malformed values are internal
			// component output, not an invalid client request.
			return merr.WrapErrServiceInternalErr(err, "malformed POST_PROCESS candidate fields")
		}
		return merr.Wrap(err, "POST_PROCESS decode candidate fields")
	}
	defer table.Release()
	for _, field := range chainFields {
		if err := validateGeneratedSearchField(field); err != nil {
			return merr.WrapErrServiceInternalErr(err, "POST_PROCESS source contains malformed text or JSON")
		}
	}
	if t.postProcess.jsonPlan != nil {
		projected, err := chain.FromSearchResultData(data, memory.DefaultAllocator, t.postProcess.jsonPlan)
		if err != nil {
			return err
		}
		defer projected.Release()
		withPaths, err := appendPostJSONColumns(table, projected, t.postProcess.jsonPlan)
		if err != nil {
			return err
		}
		table = withPaths
		defer withPaths.Release()
	}
	input, err := splitSearchTransformTable(table, data.GetTopks())
	if err != nil {
		return err
	}
	defer releaseSearchTransformBatch(input)
	input.QueryContext = t.queryContext
	processed, err := t.postProcess.executor.Execute(ctx, t.postProcess.plan, input)
	if err != nil {
		return merr.Wrap(err, "POST_PROCESS")
	}
	defer releaseSearchTransformBatch(processed.Data)
	if err := transform.ValidateResult(t.postProcess.plan.GetStage(), input, processed); err != nil {
		return err
	}
	if err := validatePostIdentity(input, processed.Data); err != nil {
		return err
	}
	combined, err := concatenateSearchTransformGroups(processed.Data)
	if err != nil {
		return err
	}
	defer combined.Release()
	return t.exportPostPayload(data, fields, combined)
}

func (t *SearchTask) postProcessFields(data *schemapb.SearchResultData, rows int64) ([]*schemapb.FieldData, error) {
	fields := append([]*schemapb.FieldData(nil), data.GetFieldsData()...)
	byName := make(map[string]*schemapb.FieldData, len(fields))
	for _, field := range fields {
		byName[field.GetFieldName()] = field
	}
	pk, err := t.schema.GetPkField()
	if err != nil {
		return nil, err
	}
	pkData := &schemapb.FieldData{FieldName: pk.GetName(), FieldId: pk.GetFieldID(), Type: pk.GetDataType()}
	if pk.GetDataType() == schemapb.DataType_Int64 {
		pkData.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data.GetIds().GetIntId().GetData()}}}}
	} else {
		pkData.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: data.GetIds().GetStrId().GetData()}}}}
	}
	if byName[pk.GetName()] == nil {
		fields = append(fields, pkData)
		byName[pk.GetName()] = pkData
	}
	sources := typeutil.NewSet(t.postProcess.inputNames...)
	if rows == 0 {
		sources.Insert(t.translatedOutputFields...)
		// Materialize complete explicitly referenced parents before child
		// aliases, so source iteration order cannot alter an empty schema.
		for _, name := range sources.Collect() {
			if t.schema.SchemaHelper.GetStructArrayFieldFromName(name) == nil || byName[name] != nil {
				continue
			}
			empty, err := t.emptyPostSourceFieldData(name)
			if err != nil {
				return nil, err
			}
			fields = append(fields, empty)
			byName[name] = empty
		}
	}
	for _, name := range sources.Collect() {
		if byName[name] != nil {
			continue
		}
		if parent, child := postStructChild(t.schema.CollectionSchema, name); child != nil {
			root := byName[parent.GetName()]
			if root == nil && rows == 0 {
				root = &schemapb.FieldData{
					FieldName: parent.GetName(), FieldId: parent.GetFieldID(), Type: schemapb.DataType_ArrayOfStruct,
					Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{}},
				}
				fields = append(fields, root)
				byName[parent.GetName()] = root
			}
			var childData *schemapb.FieldData
			for _, candidate := range root.GetStructArrays().GetFields() {
				if candidate.GetFieldId() == child.GetFieldID() {
					childData = candidate
					break
				}
			}
			if childData == nil && rows == 0 {
				childData, err = emptyPostFieldData(child)
				if err != nil {
					return nil, err
				}
				if originalName, extractErr := extractOriginalFieldName(child.GetName()); extractErr == nil {
					childData.FieldName = originalName
				}
				root.GetStructArrays().Fields = append(root.GetStructArrays().Fields, childData)
			}
			if childData == nil {
				return nil, merr.WrapErrServiceInternalMsg("POST_PROCESS source field %q is missing after requery", name)
			}
			// The response reconstructs child arrays into their parent. Retain
			// an executor-local alias for a chain that explicitly reads a
			// physical child name, without flattening the response shape.
			alias := proto.Clone(childData).(*schemapb.FieldData)
			alias.FieldName = child.GetName()
			fields = append(fields, alias)
			byName[name] = alias
			continue
		}
		if rows != 0 {
			return nil, merr.WrapErrServiceInternalMsg("POST_PROCESS source field %q is missing after requery", name)
		}
		empty, err := t.emptyPostSourceFieldData(name)
		if err != nil {
			return nil, err
		}
		fields = append(fields, empty)
		byName[name] = empty
	}
	// Candidate identity is transported to the executor as protected columns.
	// The ordinary response keeps the original dedicated wire fields unchanged.
	// Always derive protected identity from the dedicated candidate IDs. A
	// separately fetched primary-key payload cannot redefine candidate identity.
	id := proto.Clone(pkData).(*schemapb.FieldData)
	id.FieldName, id.FieldId = chaintypes.IDFieldName, 0
	fields = append(fields, id, &schemapb.FieldData{
		FieldName: chaintypes.ScoreFieldName, Type: schemapb.DataType_Float,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: data.GetScores()}}}},
	})
	if data.GetElementIndices() != nil {
		if int64(len(data.GetElementIndices().GetData())) != rows {
			return nil, merr.WrapErrServiceInternalMsg("POST_PROCESS element indexes do not align with candidate rows")
		}
		fields = append(fields, &schemapb.FieldData{
			FieldName: chaintypes.ElementIndicesFieldName, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: data.GetElementIndices()}}},
		})
	}
	return fields, nil
}

func postStructChild(schema *schemapb.CollectionSchema, name string) (*schemapb.StructArrayFieldSchema, *schemapb.FieldSchema) {
	for _, parent := range schema.GetStructArrayFields() {
		for _, child := range parent.GetFields() {
			if child.GetName() == name {
				return parent, child
			}
		}
	}
	return nil, nil
}

func (t *SearchTask) emptyPostSourceFieldData(name string) (*schemapb.FieldData, error) {
	if parent := t.schema.SchemaHelper.GetStructArrayFieldFromName(name); parent != nil {
		children := make([]*schemapb.FieldData, 0, len(parent.GetFields()))
		for _, child := range parent.GetFields() {
			data, err := emptyPostFieldData(child)
			if err != nil {
				return nil, err
			}
			if originalName, extractErr := extractOriginalFieldName(child.GetName()); extractErr == nil {
				data.FieldName = originalName
			}
			children = append(children, data)
		}
		return &schemapb.FieldData{
			FieldName: name, FieldId: parent.GetFieldID(), Type: schemapb.DataType_ArrayOfStruct,
			Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: children}},
		}, nil
	}
	field, err := t.schema.SchemaHelper.GetFieldFromName(name)
	if err != nil {
		return nil, merr.WrapErrServiceInternalMsg("POST_PROCESS resolved source field %q is missing from schema", name)
	}
	return emptyPostFieldData(field)
}

func emptyPostFieldData(field *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	fd := &schemapb.FieldData{FieldName: field.GetName(), FieldId: field.GetFieldID(), Type: field.GetDataType(), IsDynamic: field.GetIsDynamic()}
	scalar := &schemapb.ScalarField{}
	switch field.GetDataType() {
	case schemapb.DataType_Bool:
		scalar.Data = &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{}}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		scalar.Data = &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{}}
	case schemapb.DataType_Int64:
		scalar.Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{}}
	case schemapb.DataType_Timestamptz:
		scalar.Data = &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{}}
	case schemapb.DataType_Float:
		scalar.Data = &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{}}
	case schemapb.DataType_Double:
		scalar.Data = &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{}}
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		scalar.Data = &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{}}
	case schemapb.DataType_JSON:
		scalar.Data = &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{}}
	case schemapb.DataType_Geometry:
		scalar.Data = &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{}}
	case schemapb.DataType_Array:
		scalar.Data = &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: field.GetElementType()}}
	case schemapb.DataType_FloatVector:
		dim, err := typeutil.GetDim(field)
		if err != nil {
			return nil, merr.Wrap(err, "POST_PROCESS empty vector schema")
		}
		fd.Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{}}}}
		return fd, nil
	case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector:
		dim, err := typeutil.GetDim(field)
		if err != nil {
			return nil, merr.Wrap(err, "POST_PROCESS empty vector schema")
		}
		vector := &schemapb.VectorField{Dim: dim}
		switch field.GetDataType() {
		case schemapb.DataType_BinaryVector:
			vector.Data = &schemapb.VectorField_BinaryVector{}
		case schemapb.DataType_Float16Vector:
			vector.Data = &schemapb.VectorField_Float16Vector{}
		case schemapb.DataType_BFloat16Vector:
			vector.Data = &schemapb.VectorField_Bfloat16Vector{}
		case schemapb.DataType_Int8Vector:
			vector.Data = &schemapb.VectorField_Int8Vector{}
		}
		fd.Field = &schemapb.FieldData_Vectors{Vectors: vector}
		return fd, nil
	case schemapb.DataType_SparseFloatVector:
		fd.Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{}}}}
		return fd, nil
	case schemapb.DataType_ArrayOfVector:
		dim, err := typeutil.GetDim(field)
		if err != nil {
			return nil, merr.WrapErrServiceInternalErr(err, "POST_PROCESS empty vector-array schema")
		}
		fd.Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: dim, Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Dim: dim, ElementType: field.GetElementType()}}}}
		return fd, nil
	default:
		return nil, merr.Wrapf(merr.ErrServiceUnimplemented, "POST_PROCESS cannot materialize empty field type %s", field.GetDataType())
	}
	fd.Field = &schemapb.FieldData_Scalars{Scalars: scalar}
	return fd, nil
}

func appendPostJSONColumns(table arrow.Table, projected *chain.DataFrame, plan *chain.DataFrameInputPlan) (arrow.Table, error) {
	fields := table.Schema().Fields()
	columns := make([]arrow.Column, 0, table.NumCols()+int64(len(plan.Inputs)))
	for index := 0; index < int(table.NumCols()); index++ {
		columns = append(columns, *table.Column(index))
	}
	owned := make([]*arrow.Column, 0, len(plan.Inputs))
	defer func() {
		for _, column := range owned {
			column.Release()
		}
	}()
	for _, input := range plan.Inputs {
		if len(table.Schema().FieldIndices(input.LogicalName)) != 0 {
			continue
		}
		data := projected.Column(input.LogicalName)
		if data == nil {
			return nil, merr.WrapErrServiceInternalMsg("POST_PROCESS projected input %q is missing", input.LogicalName)
		}
		field := arrow.Field{Name: input.LogicalName, Type: data.DataType(), Nullable: true}
		column := arrow.NewColumn(field, data)
		owned = append(owned, column)
		columns = append(columns, *column)
		fields = append(fields, field)
	}
	return array.NewTable(arrow.NewSchema(fields, nil), columns, table.NumRows()), nil
}

func validatePostIdentity(input, output transform.Batch) error {
	for group, before := range input.Groups {
		after := output.Groups[group]
		for _, name := range []string{chaintypes.IDFieldName, chaintypes.ScoreFieldName, chaintypes.ElementIndicesFieldName} {
			index := before.Schema().FieldIndices(name)
			if len(index) == 0 {
				continue
			}
			other := after.Schema().FieldIndices(name)
			if len(other) != 1 || !equalPostIdentityColumn(before.Column(index[0]).Data(), after.Column(other[0]).Data(), name) {
				return merr.WrapErrFunctionFailedMsg("POST_PROCESS changed protected column %q in query %d", name, group)
			}
		}
	}
	return nil
}

func equalPostIdentityColumn(before, after *arrow.Chunked, name string) bool {
	if name != chaintypes.ScoreFieldName {
		return array.ChunkedEqual(before, after)
	}
	if before.DataType().ID() != arrow.FLOAT32 || after.DataType().ID() != arrow.FLOAT32 || before.Len() != after.Len() || before.NullN() != 0 || after.NullN() != 0 {
		return false
	}
	values := make([]uint32, 0, before.Len())
	for _, chunk := range before.Chunks() {
		for _, value := range chunk.(*array.Float32).Float32Values() {
			values = append(values, math.Float32bits(value))
		}
	}
	position := 0
	for _, chunk := range after.Chunks() {
		for _, value := range chunk.(*array.Float32).Float32Values() {
			if math.Float32bits(value) != values[position] {
				return false
			}
			position++
		}
	}
	return true
}

func (t *SearchTask) exportPostPayload(data *schemapb.SearchResultData, original []*schemapb.FieldData, table arrow.Table) error {
	byName := make(map[string]*schemapb.FieldData, len(original))
	for _, field := range original {
		byName[field.GetFieldName()] = field
	}
	changed := typeutil.NewSet[string]()
	hasSelect := false
	for _, op := range t.postProcess.repr.Operators {
		if op.Type == transform.OpSelect {
			hasSelect = true
			for index, output := range op.Outputs {
				if len(op.Inputs) > index && output != op.Inputs[index] {
					changed.Insert(output)
				}
			}
		} else {
			changed.Insert(op.Outputs...)
		}
	}
	wanted := typeutil.NewSet(t.translatedOutputFields...)
	if t.userRequestedPkFieldExplicitly {
		pk, _ := t.schema.GetPkField()
		wanted.Insert(pk.GetName())
	}
	wanted.Insert(changed.Collect()...)
	if !hasSelect {
		if len(t.userDynamicFields) > 0 && !typeutil.NewSet(t.userOutputFields...).Contain(common.MetaFieldName) && !changed.Contain(common.MetaFieldName) {
			if root := byName[common.MetaFieldName]; root != nil {
				visible, err := projectPostDynamicPayload(root, t.userDynamicFields)
				if err != nil {
					return err
				}
				byName[common.MetaFieldName] = visible
			}
		}
		requested := typeutil.NewSet(t.translatedOutputFields...)
		for _, parent := range t.schema.GetStructArrayFields() {
			root := byName[parent.GetName()]
			if root == nil {
				continue
			}
			children := make([]*schemapb.FieldData, 0, len(root.GetStructArrays().GetFields()))
			for _, child := range parent.GetFields() {
				if !changed.Contain(child.GetName()) {
					wanted.Remove(child.GetName())
				}
				if !requested.Contain(child.GetName()) {
					continue
				}
				for _, data := range root.GetStructArrays().GetFields() {
					if data.GetFieldId() == child.GetFieldID() {
						children = append(children, data)
						break
					}
				}
			}
			if len(children) > 0 {
				wanted.Insert(parent.GetName())
				visible := proto.Clone(root).(*schemapb.FieldData)
				visible.GetStructArrays().Fields = children
				byName[parent.GetName()] = visible
			}
		}
	}
	payloadNames := make([]string, 0, table.NumCols())
	payloadSet := typeutil.NewSet[string]()
	if !hasSelect {
		for _, field := range original {
			name := field.GetFieldName()
			if chain.IsFunctionChainSystemName(name) || !wanted.Contain(name) || changed.Contain(name) || payloadSet.Contain(name) {
				continue
			}
			payloadNames = append(payloadNames, name)
			payloadSet.Insert(name)
		}
	}
	generatedFields := make([]arrow.Field, 0)
	generatedColumns := make([]arrow.Column, 0)
	for index, field := range table.Schema().Fields() {
		if chain.IsFunctionChainSystemName(field.Name) || !hasSelect && !wanted.Contain(field.Name) {
			continue
		}
		if !payloadSet.Contain(field.Name) {
			payloadNames = append(payloadNames, field.Name)
			payloadSet.Insert(field.Name)
		}
		if byName[field.Name] == nil || changed.Contain(field.Name) {
			generatedFields = append(generatedFields, field)
			generatedColumns = append(generatedColumns, *table.Column(index))
		}
	}
	generated := make(map[string]*schemapb.FieldData, len(generatedFields))
	if len(generatedFields) > 0 {
		payload := array.NewTable(arrow.NewSchema(generatedFields, nil), generatedColumns, table.NumRows())
		defer payload.Release()
		fields, err := transformio.ToFieldData(payload, nil)
		if err != nil {
			return searchTransformOutputError(err, "POST_PROCESS encode generated payload")
		}
		for _, field := range fields {
			if err := validateGeneratedSearchField(field); err != nil {
				return err
			}
			field.FieldId = 0
			if source := byName[field.GetFieldName()]; source != nil {
				field.IsDynamic = source.GetIsDynamic()
			}
			generated[field.GetFieldName()] = field
		}
	}
	data.FieldsData = make([]*schemapb.FieldData, 0, len(payloadNames))
	for _, name := range payloadNames {
		field := generated[name]
		if field == nil {
			field = byName[name]
		}
		if field == nil {
			return merr.WrapErrServiceInternalMsg("POST_PROCESS payload field %q was not serialized", name)
		}
		data.FieldsData = append(data.FieldsData, field)
	}
	if hasSelect {
		t.postOutputFields = payloadNames
	} else {
		t.postOutputFields = make([]string, 0, len(t.userOutputFields)+len(changed))
		seen := typeutil.NewSet[string]()
		for _, name := range t.userOutputFields {
			if parent, child := postStructChild(t.schema.CollectionSchema, name); child != nil && !changed.Contain(name) {
				name = parent.GetName()
			}
			if !seen.Contain(name) {
				t.postOutputFields = append(t.postOutputFields, name)
				seen.Insert(name)
			}
		}
		for _, name := range payloadNames {
			if changed.Contain(name) && !seen.Contain(name) {
				t.postOutputFields = append(t.postOutputFields, name)
				seen.Insert(name)
			}
		}
	}
	data.OutputFields = append([]string(nil), t.postOutputFields...)
	return nil
}

func projectPostDynamicPayload(field *schemapb.FieldData, names []string) (*schemapb.FieldData, error) {
	visible := proto.Clone(field).(*schemapb.FieldData)
	values := visible.GetScalars().GetJsonData()
	if values == nil {
		return nil, merr.WrapErrServiceInternalMsg("POST_PROCESS dynamic payload is missing JSON data")
	}
	valid := typeutil.GetFieldDataValidData(visible)
	for row, value := range values.GetData() {
		if len(valid) == len(values.GetData()) && !valid[row] {
			continue
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(value, &object); err != nil {
			return nil, merr.WrapErrServiceInternalErr(err, "POST_PROCESS dynamic source is malformed")
		}
		if object == nil {
			continue
		}
		projected := make(map[string]json.RawMessage, len(names))
		for _, name := range names {
			if content, exists := object[name]; exists {
				projected[name] = content
			}
		}
		encoded, err := json.Marshal(projected)
		if err != nil {
			return nil, merr.WrapErrServiceInternalErr(err, "POST_PROCESS dynamic source projection failed")
		}
		values.Data[row] = encoded
	}
	return visible, nil
}

// Arrow strings and binary-backed JSON may contain bytes that protobuf/client
// parsers cannot interpret. Validate function-produced payloads at the output
// boundary, including scalar values nested inside arrays and struct arrays.
func validateGeneratedSearchField(field *schemapb.FieldData) error {
	if field.GetStructArrays() != nil {
		for _, child := range field.GetStructArrays().GetFields() {
			if err := validateGeneratedSearchField(child); err != nil {
				return err
			}
		}
		return nil
	}
	scalar := field.GetScalars()
	valid := typeutil.GetFieldDataValidData(field)
	physicalLen := 0
	switch {
	case scalar.GetStringData() != nil:
		physicalLen = len(scalar.GetStringData().GetData())
	case scalar.GetJsonData() != nil:
		physicalLen = len(scalar.GetJsonData().GetData())
	case scalar.GetArrayData() != nil:
		physicalLen = len(scalar.GetArrayData().GetData())
	}
	validRow := func(row int) bool { return len(valid) != physicalLen || valid[row] }
	if values := scalar.GetStringData(); values != nil {
		for row, value := range values.GetData() {
			if validRow(row) && !utf8.ValidString(value) {
				return merr.WrapErrFunctionFailedMsg("search transform generated invalid UTF-8 in field %q row %d", field.GetFieldName(), row)
			}
		}
	}
	if values := scalar.GetJsonData(); values != nil {
		for row, value := range values.GetData() {
			if validRow(row) && !json.Valid(value) {
				return merr.WrapErrFunctionFailedMsg("search transform generated invalid JSON in field %q row %d", field.GetFieldName(), row)
			}
		}
	}
	if values := scalar.GetArrayData(); values != nil {
		for row, value := range values.GetData() {
			if !validRow(row) {
				continue
			}
			if err := validateGeneratedSearchField(&schemapb.FieldData{
				FieldName: field.GetFieldName(), Type: values.GetElementType(), Field: &schemapb.FieldData_Scalars{Scalars: value},
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func searchTransformOutputError(err error, context string) error {
	if merr.GetErrorType(err) == merr.InputError {
		// A converter's user-input code is intentionally translated at this
		// output boundary: the function generated the invalid value.
		return merr.WrapErrFunctionFailed(err, "%s", context)
	}
	return merr.Wrap(err, context)
}
