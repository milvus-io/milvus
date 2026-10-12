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
	"bytes"
	"context"
	"math"
	"sort"
	"unicode/utf8"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	chaintypes "github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/transformio"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	postQueryIndexColumn = "$query_index"
	postANNIndexColumn   = "$ann_index"
)

func (t *SearchTask) hasPostTable() bool {
	return t.postProcess != nil && transform.IsTablePlan(t.postProcess.plan)
}

func (t *SearchTask) hasANNInputTable() bool {
	return t.hasPostTable() && t.postProcess.plan.GetPostProcessOptions().GetInput() == schemapb.PostProcessInput_PostProcessInputAnnInputs
}

func validatePostTableSearchOptions(request *milvuspb.SearchRequest) error {
	for _, plan := range request.GetFunctionChains() {
		if !transform.IsTablePlan(plan) || plan.GetPostProcessOptions().GetInput() != schemapb.PostProcessInput_PostProcessInputAnnInputs {
			continue
		}
		if len(request.GetSubReqs()) == 0 {
			return merr.WrapErrParameterInvalidMsg("POST_PROCESS ANN_INPUTS requires HybridSearch")
		}
		if request.GetFunctionScore() != nil ||
			hasFunctionChainStage(request.GetFunctionChains(), schemapb.FunctionChainStage_FunctionChainStageL2Rerank) ||
			hasExplicitLegacyReranker(request.GetSearchParams()) {
			return merr.WrapErrParameterInvalidMsg("POST_PROCESS ANN_INPUTS cannot be combined with a top-level ranker")
		}
		if request.GetHighlighter() != nil {
			return merr.WrapErrParameterInvalidMsg("POST_PROCESS ANN_INPUTS does not support the hit highlighter")
		}
		for _, key := range []string{GroupByFieldKey, GroupByFieldsKey, GroupSizeKey, OrderByFieldsKey} {
			if value, err := funcutil.GetAttrByKeyFromRepeatedKV(key, request.GetSearchParams()); err == nil && value != "" {
				return merr.WrapErrParameterInvalidMsg("POST_PROCESS ANN_INPUTS does not support top-level %s; use TABLE operators", key)
			}
		}
	}
	return nil
}

// buildPostTableInput creates one typed group per original NQ, including empty
// groups. The source result remains owned by the search pipeline. TABLE has no
// hit-shaped export and therefore converts only declared inputs and provenance.
func (t *SearchTask) buildPostTableInput(ctx context.Context, result *milvuspb.SearchResults, annIndex int64, query bool) (transform.Batch, error) {
	if err := ctx.Err(); err != nil {
		return transform.Batch{}, err
	}
	if result == nil || result.GetResults() == nil {
		return transform.Batch{}, merr.WrapErrServiceInternalMsg("POST TABLE source results are missing")
	}
	result = proto.Clone(result).(*milvuspb.SearchResults)
	data := result.GetResults()
	if int64(len(data.GetTopks())) != t.GetNq() {
		return transform.Batch{}, merr.WrapErrServiceInternalMsg("POST TABLE query boundaries do not match source NQ")
	}
	rows := int64(0)
	for _, count := range data.GetTopks() {
		if count < 0 || count > math.MaxInt64-rows {
			return transform.Batch{}, merr.WrapErrServiceInternalMsg("POST TABLE source row counts are malformed")
		}
		rows += count
	}
	if int64(typeutil.GetSizeOfIDs(data.GetIds())) != rows || !query && int64(len(data.GetScores())) != rows {
		return transform.Batch{}, merr.WrapErrServiceInternalMsg("POST TABLE source IDs/scores do not align with rows")
	}
	fillFieldNames(t.schema.CollectionSchema, data)
	for _, field := range data.GetFieldsData() {
		if schema, err := t.schema.SchemaHelper.GetFieldFromID(field.GetFieldId()); err == nil {
			field.IsDynamic = schema.GetIsDynamic()
		}
	}
	reconstructStructFieldDataForSearch(result, t.schema.CollectionSchema)
	if rows == 0 {
		// Requery organization returns metadata-only fields for an entirely
		// empty ANN branch. Complete their schema-backed payloads before the
		// generic post input resolver treats an existing name as available.
		if err := t.completeEmptyPostTableSources(data.GetFieldsData()); err != nil {
			return transform.Batch{}, err
		}
	}
	fields, err := t.postProcessFields(data, rows)
	if err != nil {
		return transform.Batch{}, err
	}
	sources := typeutil.NewSet(t.postProcess.inputNames...)
	chainFields := make([]*schemapb.FieldData, 0, len(sources)+5)
	hasElementIndices := false
	for _, field := range fields {
		name := field.GetFieldName()
		if query && (name == chaintypes.ScoreFieldName || name == chaintypes.ElementIndicesFieldName) {
			continue
		}
		if sources.Contain(name) || chain.IsFunctionChainSystemName(name) {
			chainFields = append(chainFields, field)
			hasElementIndices = hasElementIndices || name == chaintypes.ElementIndicesFieldName
		}
	}
	// Mixed row/element ANN branches share a nullable element-index schema.
	// Missing element identity is null, never an invented element ordinal.
	if !query && !hasElementIndices {
		chainFields = append(chainFields, &schemapb.FieldData{
			FieldName: chaintypes.ElementIndicesFieldName, Type: schemapb.DataType_Int64,
			ValidData: make([]bool, rows),
			Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{}}}},
		})
	}
	queryIndexes := make([]int64, 0, rows)
	annIndexes := make([]int64, 0, rows)
	for q, count := range data.GetTopks() {
		queryIndex := int64(q)
		if query {
			queryIndex = -1
		}
		for row := int64(0); row < count; row++ {
			queryIndexes, annIndexes = append(queryIndexes, queryIndex), append(annIndexes, annIndex)
		}
	}
	for index, name := range []string{postQueryIndexColumn, postANNIndexColumn} {
		values := queryIndexes
		if index == 1 {
			values = annIndexes
		}
		chainFields = append(chainFields, &schemapb.FieldData{
			FieldName: name, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}}},
		})
	}
	for _, field := range chainFields {
		if err := validateGeneratedSearchField(field); err != nil {
			return transform.Batch{}, merr.WrapErrServiceInternalErr(err, "POST TABLE source contains malformed text or JSON")
		}
	}
	sort.SliceStable(chainFields, func(i, j int) bool { return chainFields[i].GetFieldName() < chainFields[j].GetFieldName() })
	table, err := transformio.FromFieldData(chainFields, rows, memory.DefaultAllocator, t.schema.GetFields()...)
	if err != nil {
		if merr.GetErrorType(err) == merr.InputError {
			return transform.Batch{}, merr.WrapErrServiceInternalErr(err, "malformed POST TABLE source fields")
		}
		return transform.Batch{}, merr.Wrap(err, "POST TABLE decode source fields")
	}
	defer table.Release()
	normalized, err := normalizePostTableSourceSchema(table, t.schema)
	if err != nil {
		return transform.Batch{}, err
	}
	defer normalized.Release()
	table = normalized
	if t.postProcess.jsonPlan != nil {
		projected, err := chain.FromSearchResultData(data, memory.DefaultAllocator, t.postProcess.jsonPlan)
		if err != nil {
			return transform.Batch{}, err
		}
		defer projected.Release()
		withPaths, err := appendPostJSONColumns(table, projected, t.postProcess.jsonPlan)
		if err != nil {
			return transform.Batch{}, err
		}
		table = withPaths
		defer withPaths.Release()
	}
	return splitSearchTransformTable(table, data.GetTopks())
}

func (t *SearchTask) completeEmptyPostTableSources(fields []*schemapb.FieldData) error {
	for index, field := range fields {
		if field == nil {
			return merr.WrapErrServiceInternalMsg("POST TABLE empty source contains a nil field")
		}
		missingPayload := field.GetField() == nil
		switch payload := field.GetField().(type) {
		case *schemapb.FieldData_Scalars:
			missingPayload = payload.Scalars == nil || payload.Scalars.GetData() == nil
		case *schemapb.FieldData_Vectors:
			missingPayload = payload.Vectors == nil || payload.Vectors.GetData() == nil
		case *schemapb.FieldData_StructArrays:
			missingPayload = payload.StructArrays == nil || len(payload.StructArrays.GetFields()) == 0
			if !missingPayload {
				if err := t.completeEmptyPostTableSources(payload.StructArrays.GetFields()); err != nil {
					return err
				}
			}
		}
		if !missingPayload {
			continue
		}
		var empty *schemapb.FieldData
		var err error
		if t.schema.SchemaHelper.GetStructArrayFieldFromName(field.GetFieldName()) != nil {
			empty, err = t.emptyPostSourceFieldData(field.GetFieldName())
		} else {
			source, sourceErr := t.schema.SchemaHelper.GetFieldFromID(field.GetFieldId())
			if sourceErr != nil {
				source, sourceErr = t.schema.SchemaHelper.GetFieldFromName(field.GetFieldName())
			}
			if sourceErr != nil {
				// Undeclared runtime fields are not TABLE inputs. Leave them to
				// the normal dependency filter rather than invent a stored type.
				continue
			}
			empty, err = emptyPostFieldData(source)
		}
		if err != nil {
			return err
		}
		// A reconstructed struct child uses its local member name; physical
		// aliases are added later by postProcessFields using the field ID.
		if field.GetFieldName() != "" {
			empty.FieldName = field.GetFieldName()
		}
		fields[index] = empty
	}
	return nil
}

// Wire ValidData may be absent in an empty branch or a branch without nulls.
// Collection nullability and the optional element source remain stable across
// branches, so their Arrow schemas do not depend on which rows were selected.
func normalizePostTableSourceSchema(table arrow.Table, schema *schemaInfo) (arrow.Table, error) {
	fields := table.Schema().Fields()
	columns := make([]arrow.Column, len(fields))
	for index, field := range fields {
		if field.Name == chaintypes.ElementIndicesFieldName {
			field.Nullable = true
		} else if source, err := schema.SchemaHelper.GetFieldFromName(field.Name); err == nil {
			field.Nullable = source.GetNullable()
		}
		field, err := chain.RebindArrowField(field, field.Name, field.Nullable)
		if err != nil {
			return nil, merr.Wrap(err, "POST TABLE source schema")
		}
		fields[index] = field
		column := arrow.NewColumn(field, table.Column(index).Data())
		columns[index] = *column
		defer column.Release()
	}
	metadata := table.Schema().Metadata()
	return array.NewTable(arrow.NewSchema(fields, &metadata), columns, table.NumRows()), nil
}

func executePostTable(ctx context.Context, meta *searchTransformMeta, input transform.Batch) (*schemapb.FunctionChainTableResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	processed, err := meta.executor.Execute(ctx, meta.plan, input)
	if err != nil {
		return nil, merr.Wrap(err, "POST TABLE")
	}
	defer releaseSearchTransformBatch(processed.Data)
	if err := transform.ValidateResultForPlan(meta.plan, input, processed); err != nil {
		return nil, err
	}
	combined, err := concatenateSearchTransformGroups(processed.Data)
	if err != nil {
		return nil, err
	}
	defer combined.Release()
	return encodePostTable(ctx, combined)
}

type postTableIPCWriter struct {
	ctx context.Context
	buf bytes.Buffer
	max int64
}

func (w *postTableIPCWriter) Write(data []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if w.max >= 0 && int64(len(data)) > w.max-int64(w.buf.Len()) {
		return 0, merr.WrapErrFunctionFailedMsg("POST TABLE IPC result exceeds maxOutputSize %d", w.max)
	}
	return w.buf.Write(data)
}

// Even a zero-row table is written through IPC so clients receive its complete
// schema. Bound bytes while writing instead of copying an oversized response.
func encodePostTable(ctx context.Context, table arrow.Table) (*schemapb.FunctionChainTableResult, error) {
	if err := validatePostTableFields(table.Schema().Fields()); err != nil {
		return nil, err
	}
	for column := 0; column < int(table.NumCols()); column++ {
		for _, values := range table.Column(column).Data().Chunks() {
			if err := validatePostTableArray(ctx, values); err != nil {
				return nil, err
			}
		}
	}
	dst := &postTableIPCWriter{ctx: ctx, max: paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()}
	writer := ipc.NewWriter(dst, ipc.WithSchema(table.Schema()), ipc.WithAllocator(memory.DefaultAllocator))
	reader := array.NewTableReader(table, 65536)
	defer reader.Release()
	for reader.Next() {
		if err := writer.Write(reader.Record()); err != nil {
			_ = writer.Close()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if merr.IsMilvusError(err) {
				return nil, merr.Wrap(err, "POST TABLE IPC encoding")
			}
			return nil, merr.WrapErrFunctionFailed(err, "POST TABLE IPC encoding")
		}
	}
	if err := writer.Close(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if merr.IsMilvusError(err) {
			return nil, merr.Wrap(err, "POST TABLE IPC finalization")
		}
		return nil, merr.WrapErrFunctionFailed(err, "POST TABLE IPC finalization")
	}
	return &schemapb.FunctionChainTableResult{ArrowIpc: dst.buf.Bytes(), RowCount: table.NumRows()}, nil
}

func validatePostTableFields(fields []arrow.Field) error {
	seen := typeutil.NewSet[string]()
	for _, field := range fields {
		if !utf8.ValidString(field.Name) {
			return merr.WrapErrFunctionFailedMsg("POST TABLE contains an invalid UTF-8 field name")
		}
		if seen.Contain(field.Name) {
			return merr.WrapErrFunctionFailedMsg("POST TABLE contains duplicate field %q", field.Name)
		}
		seen.Insert(field.Name)
		dataType := field.Type
		if dictionary, ok := dataType.(*arrow.DictionaryType); ok {
			dataType = dictionary.ValueType
		}
		if nested, ok := dataType.(interface{ Fields() []arrow.Field }); ok {
			if err := validatePostTableFields(nested.Fields()); err != nil {
				return err
			}
		}
	}
	return nil
}

// IPC accepts arbitrary bytes in a String array. Reject malformed generated
// strings (also inside nested arrays) at the server's FunctionFailed boundary,
// before a client encounters an Arrow decoding or to_pylist failure.
func validatePostTableArray(ctx context.Context, values arrow.Array) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = merr.WrapErrFunctionFailedMsg("POST TABLE contains malformed Arrow values (%T)", recovered)
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings, ok := values.(interface{ Value(int) string }); ok {
		for row := 0; row < values.Len(); row++ {
			if row%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if !values.IsNull(row) && !utf8.ValidString(strings.Value(row)) {
				return merr.WrapErrFunctionFailedMsg("POST TABLE contains invalid UTF-8 in %s row %d", values.DataType(), row)
			}
		}
	}
	children := append([]arrow.ArrayData(nil), values.Data().Children()...)
	if dictionary, ok := values.(*array.Dictionary); ok {
		if err := validatePostTableArray(ctx, dictionary.Dictionary()); err != nil {
			return err
		}
	}
	for _, data := range children {
		child := array.MakeFromData(data)
		err := validatePostTableArray(ctx, child)
		child.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

// ANN_INPUTS replaces only the top-level Hybrid fusion pipeline. Branch PRE,
// L0/L1, candidate budgets and distributed reduce retain their ordinary paths.
func newANNInputTablePipeline(t *SearchTask) (*pipeline, error) {
	p, err := newPipeline(&pipelineDef{name: "hybrid-ann-post-table", nodes: []*nodeDef{{
		name: "reduce", inputs: []string{pipelineInput, pipelineStorageCost},
		outputs: []string{"reduced", "metrics"}, opName: hybridSearchReduceOp,
	}}}, t)
	if err != nil {
		return nil, err
	}
	err = p.AddNodes(t, &nodeDef{
		name: "ann_post_table", inputs: []string{"reduced", pipelineStorageCost},
		outputs: []string{pipelineOutput, pipelineStorageCost}, opName: lambdaOp,
		params: map[string]any{lambdaParamKey: func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
			candidates := inputs[0].([]*milvuspb.SearchResults)
			t.tableAllSearchCount = aggregatedAllSearchCount(candidates)
			cost := inputs[1].(segcore.StorageCost)
			if t.needRequery && len(t.postProcess.inputNames) > 0 {
				merged, err := mergeIDsFunc(ctx, span, candidates)
				if err != nil {
					return nil, err
				}
				requery, err := newRequeryOperator(t, nil)
				if err != nil {
					return nil, err
				}
				// A TABLE fetch has no implicit ordinary output fields.
				requery.(*requeryOperator).outputFieldNames = append([]string(nil), t.postProcess.inputNames...)
				fetched, err := requery.run(ctx, span, merged[0], cost)
				if err != nil {
					if merr.GetErrorType(err) == merr.InputError {
						return nil, merr.WrapErrServiceInternalErr(err, "malformed internal POST TABLE requery")
					}
					return nil, err
				}
				cost = fetched[1].(segcore.StorageCost)
				organize, err := newOrganizeOperator(t, nil)
				if err != nil {
					return nil, err
				}
				ids := make([]*schemapb.IDs, len(candidates))
				for index, candidate := range candidates {
					ids[index] = candidate.GetResults().GetIds()
				}
				organized, err := organize.run(ctx, span, fetched[0], ids)
				if err != nil {
					if merr.GetErrorType(err) == merr.InputError {
						return nil, merr.WrapErrServiceInternalErr(err, "malformed POST TABLE fetched candidate fields")
					}
					return nil, err
				}
				fields := organized[0].([][]*schemapb.FieldData)
				for index, candidate := range candidates {
					candidate.Results.FieldsData = fields[index]
				}
			}
			batch := transform.Batch{QueryContext: t.queryContext}
			defer func() { releaseSearchTransformBatch(batch) }()
			for index, candidate := range candidates {
				input, err := t.buildPostTableInput(ctx, candidate, int64(index), false)
				if err != nil {
					return nil, err
				}
				batch.Groups = append(batch.Groups, input.Groups...)
			}
			table, err := executePostTable(ctx, t.postProcess, batch)
			if err != nil {
				return nil, err
			}
			return []any{&milvuspb.SearchResults{Status: merr.Success(), PostProcessResult: table}, cost}, nil
		}},
	})
	return p, err
}

func (t *QueryTask) prepareQueryPostProcess() error {
	if len(t.request.GetFunctionChains()) == 0 {
		return nil
	}
	if t.reQuery || t.preserveRawFields || t.ids != nil {
		return merr.WrapErrParameterInvalidMsg("Query POST TABLE is unavailable for internal retrieval")
	}
	if t.queryParams.isIterator {
		return merr.WrapErrParameterInvalidMsg("Query POST TABLE does not support query iterator")
	}
	if len(t.request.GetFunctionChains()) != 1 {
		return merr.WrapErrParameterInvalidMsg("Query requires exactly one POST TABLE function chain")
	}
	plan := t.request.GetFunctionChains()[0]
	if err := transform.ValidatePlan(plan); err != nil {
		return err
	}
	if !transform.IsTablePlan(plan) || plan.GetPostProcessOptions().GetInput() != schemapb.PostProcessInput_PostProcessInputFused {
		return merr.WrapErrParameterInvalidMsg("Query function chains require POST TABLE with FUSED input")
	}
	_, _, _, aggregates, _, err := translateOutputFields(t.request.GetOutputFields(), t.schema, false)
	if err != nil {
		return err
	}
	if len(t.queryParams.groupByFields) > 0 || len(aggregates) > 0 {
		return merr.WrapErrParameterInvalidMsg("Query POST TABLE does not support native Query aggregation; aggregate in the function chain")
	}
	repr, err := chain.ProtoChainToRepr(plan)
	if err != nil {
		return err
	}
	for _, input := range repr.Info.RequiredInputs {
		if input == chaintypes.ScoreFieldName || input == chaintypes.ElementIndicesFieldName {
			return merr.WrapErrParameterInvalidMsg("Query POST TABLE source %q is unavailable", input)
		}
	}
	meta, err := newSearchTransformMeta(plan, t.schema, t.request.GetDbName())
	if err != nil {
		return err
	}
	t.postProcess = meta
	return nil
}

func (t *QueryTask) planQueryPostInputs() error {
	pk, err := t.schema.GetPkField()
	if err != nil {
		return err
	}
	ids := typeutil.NewSet(t.postProcess.inputIDs...)
	ids.Insert(pk.GetFieldID(), common.TimeStampField)
	t.OutputFieldsId = ids.Collect()
	t.plan.OutputFieldIds = append([]int64(nil), t.OutputFieldsId...)
	t.translatedOutputFields = append([]string(nil), t.postProcess.inputNames...)
	t.plan.DynamicFields = postTableDynamicFields(t.postProcess)
	return nil
}

func postTableDynamicFields(meta *searchTransformMeta) []string {
	for _, input := range meta.repr.Info.RequiredInputs {
		if input == common.MetaFieldName {
			return nil
		}
	}
	keys := typeutil.NewSet[string]()
	if meta.jsonPlan != nil {
		for _, input := range meta.jsonPlan.Inputs {
			if input.FieldName == common.MetaFieldName && len(input.NestedPath) > 0 {
				keys.Insert(input.NestedPath[0])
			}
		}
	}
	return keys.Collect()
}

func (t *QueryTask) executeQueryPostProcess(ctx context.Context) error {
	pk, err := t.schema.GetPkField()
	if err != nil {
		return err
	}
	pkData, err := typeutil.GetPrimaryFieldData(t.result.GetFieldsData(), pk)
	if err != nil {
		return merr.WrapErrServiceInternalErr(err, "Query POST TABLE source primary key is missing")
	}
	ids := &schemapb.IDs{}
	if pk.GetDataType() == schemapb.DataType_Int64 {
		ids.IdField = &schemapb.IDs_IntId{IntId: pkData.GetScalars().GetLongData()}
	} else {
		ids.IdField = &schemapb.IDs_StrId{StrId: pkData.GetScalars().GetStringData()}
	}
	rows := int64(typeutil.GetSizeOfIDs(ids))
	adapter := &SearchTask{
		schema: t.schema, postProcess: t.postProcess,
		SearchRequest: &internalpb.SearchRequest{Nq: 1},
	}
	// Query has one logical row batch; it has no search score or NQ identity.
	input, err := adapter.buildPostTableInput(ctx, &milvuspb.SearchResults{Results: &schemapb.SearchResultData{
		FieldsData: t.result.GetFieldsData(), Ids: ids, Topks: []int64{rows}, NumQueries: 1,
	}}, -1, true)
	if err != nil {
		return err
	}
	defer releaseSearchTransformBatch(input)
	result, err := executePostTable(ctx, t.postProcess, input)
	if err != nil {
		return err
	}
	t.result = &milvuspb.QueryResults{Status: merr.Success(), CollectionName: t.collectionName, PostProcessResult: result}
	return nil
}
