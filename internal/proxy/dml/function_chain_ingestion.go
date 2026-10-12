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

package dml

import (
	"context"
	"math"
	"sync/atomic"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/dql"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/ingestion"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/transformio"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	ingestionBeforeAppend uint32 = iota
	ingestionAppendStarted
	ingestionOutcomeKnown
	ingestionOutcomeUnknown
	ingestionCanceledBeforeAppend
)

// IngestionFailureResult snapshots a failed RPC without reading data that the
// scheduler may still be changing. Stopping before append wins a CAS against
// Execute, so cancellation cannot race a later successful WAL write unnoticed.
func (it *InsertTask) IngestionFailureResult(err error, stopBeforeAppend bool) *milvuspb.MutationResult {
	for {
		phase := atomic.LoadUint32(&it.ingestionWritePhase)
		if stopBeforeAppend && phase == ingestionBeforeAppend {
			if !atomic.CompareAndSwapUint32(&it.ingestionWritePhase, phase, ingestionCanceledBeforeAppend) {
				continue
			}
			phase = ingestionCanceledBeforeAppend
		}
		inputRows := atomic.LoadUint64(&it.ingestionInputRows)
		if inputRows == 0 {
			return nil
		}
		var result *milvuspb.MutationResult
		if phase == ingestionOutcomeKnown || phase == ingestionOutcomeUnknown {
			// Settlement publishes all response fields before the phase store.
			result = proto.Clone(it.result).(*milvuspb.MutationResult)
		} else {
			result = &milvuspb.MutationResult{IngestionResult: &milvuspb.IngestionFunctionChainResult{
				InputRows: uint32(inputRows - 1), OutcomeUnknown: phase == ingestionAppendStarted,
				OutputSources: &milvuspb.FunctionChainRowMapping{SourceOffsets: []uint64{0}},
			}}
			result.ErrIndex = make([]uint32, inputRows-1)
			for row := range result.ErrIndex {
				result.ErrIndex[row] = uint32(row)
			}
		}
		result.Status = merr.Status(err)
		if phase == ingestionAppendStarted || phase == ingestionOutcomeKnown || phase == ingestionOutcomeUnknown {
			result.Status.Retriable = false
		}
		return result
	}
}

func (it *InsertTask) settleIngestionAppend() {
	phase := ingestionOutcomeKnown
	if it.result.GetIngestionResult().GetOutcomeUnknown() {
		phase = ingestionOutcomeUnknown
	}
	atomic.StoreUint32(&it.ingestionWritePhase, phase)
}

func (it *InsertTask) applyIngestion(ctx context.Context, schema *schemaInfo) error {
	if len(schema.GetFunctionChains()) == 0 {
		return nil
	}
	inputRows := it.insertMsg.NRows()
	if inputRows > math.MaxUint32 {
		return merr.WrapErrParameterInvalidMsg("ingestion input row count exceeds uint32")
	}
	// Zero denotes a task whose collection binding has not been discovered yet.
	atomic.StoreUint64(&it.ingestionInputRows, uint64(inputRows)+1)
	it.result.IngestionResult = &milvuspb.IngestionFunctionChainResult{InputRows: uint32(inputRows), OutputSources: &milvuspb.FunctionChainRowMapping{SourceOffsets: []uint64{0}}}
	if atomic.LoadUint32(&it.ingestionWritePhase) == ingestionCanceledBeforeAppend {
		return merr.Wrap(context.Canceled, "ingestion insertion canceled before transform")
	}
	// Deduplicating the materialized WAL write cannot prevent an external model
	// from executing again, or reconstruct its original cardinality on retry.
	// Reject before either transform execution or WAL side effects in v1.
	if it.idempotencyKey != "" {
		return merr.WrapErrAsInputError(merr.WrapErrOperationNotSupportedMsg("idempotency keys are not supported for ingestion function chains"))
	}
	if err := ingestion.ValidateCollection(schema.CollectionSchema); err != nil {
		return merr.WrapErrAsSysError(merr.Wrap(err, "persisted ingestion schema is invalid"))
	}
	input, err := transformio.FromFieldDataWithTimezone(it.insertMsg.GetFieldsData(), int64(inputRows), memory.DefaultAllocator, schema.SchemaHelper.GetTimezone(), schema.GetFields()...)
	if err != nil {
		return err
	}
	defer input.Release()
	batch := transform.Batch{Groups: []arrow.Table{input}}
	executor := chain.NewTransformExecutor(memory.DefaultAllocator, types.FunctionBuildContext{ModelExtraInfo: &models.ModelExtraInfo{ClusterID: paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), DBName: it.insertMsg.GetDbName()}})
	output, err := executor.Execute(ctx, schema.GetFunctionChains()[0], batch)
	if err != nil {
		return merr.Wrap(err, "execute ingestion function chain")
	}
	defer chain.ReleaseArrowBatch(output.Data)
	if err := transform.ValidateResult(schemapb.FunctionChainStage_FunctionChainStageIngestion, batch, output); err != nil {
		return err
	}
	table := output.Data.Groups[0]
	if table.NumRows() > math.MaxUint32 {
		return merr.WrapErrFunctionFailedMsg("ingestion output row count exceeds uint32")
	}
	fields := append([]*schemapb.FieldSchema(nil), schema.GetFields()...)
	for _, field := range schema.GetStructArrayFields() {
		fields = append(fields, &schemapb.FieldSchema{Name: field.GetName(), FieldID: field.GetFieldID(), DataType: schemapb.DataType_ArrayOfStruct, Nullable: field.GetNullable()})
	}
	converted, err := transformio.ToFieldData(table, fields, schema.GetStructArrayFields()...)
	if err != nil {
		return err
	}
	// The existing insertion validator consumes the public timestamp string
	// format and canonicalizes it exactly once before WAL encoding.
	if err := dql.FormatTimestamptzFields(converted, schema.SchemaHelper.GetTimezone()); err != nil {
		return err
	}
	if err := validateExpandedPrimaryKeys(schema.CollectionSchema, converted, output.Lineage[0]); err != nil {
		return err
	}
	it.insertMsg.FieldsData = converted
	it.insertMsg.NumRows = uint64(table.NumRows())
	it.ingestionLineage = output.Lineage[0]
	return nil
}

func validateExpandedPrimaryKeys(schema *schemapb.CollectionSchema, fields []*schemapb.FieldData, lineage []transform.RowLineage) error {
	primary, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return err
	}
	if primary.GetAutoID() {
		return nil
	}
	parents := make(map[int64]struct{}, len(lineage))
	expanded := false
	for _, sources := range lineage {
		if len(sources) == 1 {
			if _, seen := parents[sources[0].Row]; seen {
				expanded = true
			}
			parents[sources[0].Row] = struct{}{}
		}
	}
	if !expanded {
		return nil
	}
	for _, field := range fields {
		if field.GetFieldName() != primary.GetName() {
			continue
		}
		if primary.GetDataType() == schemapb.DataType_Int64 {
			seen := make(map[int64]struct{})
			for _, id := range field.GetScalars().GetLongData().GetData() {
				if _, duplicate := seen[id]; duplicate {
					return merr.WrapErrAsInputError(merr.WrapErrOperationNotSupportedMsg("ingestion expansion requires distinct child primary keys; use autoID or generate a primary key for each child"))
				}
				seen[id] = struct{}{}
			}
		} else {
			seen := make(map[string]struct{})
			for _, id := range field.GetScalars().GetStringData().GetData() {
				if _, duplicate := seen[id]; duplicate {
					return merr.WrapErrAsInputError(merr.WrapErrOperationNotSupportedMsg("ingestion expansion requires distinct child primary keys; use autoID or generate a primary key for each child"))
				}
				seen[id] = struct{}{}
			}
		}
	}
	return nil
}

func emptyIngestionIDs(schema *schemapb.CollectionSchema) (*schemapb.IDs, error) {
	primary, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	if primary.GetDataType() == schemapb.DataType_VarChar {
		return &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{}}}, nil
	}
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}}, nil
}

// finalizeIngestion reports only outputs confirmed by WAL. Input success/error
// indexes remain in original request order; a source with an unconfirmed child
// is an error even when some of its other child IDs are returned as committed.
// Sources removed by a filter are successful no-ops.
func (it *InsertTask) finalizeIngestion(confirmed []bool) error {
	info := it.result.GetIngestionResult()
	if info == nil {
		return nil
	}
	if len(confirmed) != len(it.ingestionLineage) {
		return merr.WrapErrServiceInternalMsg("ingestion confirmation count differs from lineage")
	}
	selected := make([]int, 0, len(confirmed))
	failed := make([]bool, info.GetInputRows())
	info.OutputSources = &milvuspb.FunctionChainRowMapping{SourceOffsets: []uint64{0}}
	mapping := info.OutputSources
	for row, sources := range it.ingestionLineage {
		if len(sources) == 0 {
			return merr.WrapErrServiceInternalMsg("ingestion output has no source")
		}
		for _, source := range sources {
			if source.Group != 0 || source.Row < 0 || source.Row >= int64(info.GetInputRows()) {
				return merr.WrapErrServiceInternalMsg("ingestion output source is out of bounds")
			}
		}
		if !confirmed[row] {
			for _, source := range sources {
				failed[source.Row] = true
			}
			continue
		}
		selected = append(selected, row)
		for _, source := range sources {
			mapping.SourceRowIndexes = append(mapping.SourceRowIndexes, uint32(source.Row))
		}
		mapping.SourceOffsets = append(mapping.SourceOffsets, uint64(len(mapping.SourceRowIndexes)))
	}
	ids, err := idsByOffsets(it.result.GetIDs(), selected)
	if err != nil {
		return err
	}
	it.result.IDs = ids
	info.OutputRows = uint32(len(selected))
	it.result.InsertCnt = int64(len(selected))
	it.result.SuccIndex = nil
	it.result.ErrIndex = nil
	for source, isFailed := range failed {
		if isFailed {
			it.result.ErrIndex = append(it.result.ErrIndex, uint32(source))
		} else {
			it.result.SuccIndex = append(it.result.SuccIndex, uint32(source))
		}
	}
	return nil
}

// confirmIngestion uses write-unit success, not the request's optimistic IDs.
// AppendMessagesWithOptions groups each VChannel into one ProduceGuard and
// maps that guard's outcome back to all messages belonging to the VChannel.
func (it *InsertTask) confirmIngestion(msgs []message.MutableMessage, channels []string, responses streaming.AppendResponses) error {
	if it.result.GetIngestionResult() == nil {
		return nil
	}
	if len(msgs) != len(responses.Responses) {
		return merr.WrapErrServiceInternalMsg("ingestion WAL response count differs from message count")
	}
	byChannel := make(map[string]bool, len(channels))
	unknown := false
	for index, msg := range msgs {
		response := responses.Responses[index]
		if response.AppendNotAttempted && (response.Error == nil || response.AppendResult != nil) {
			return merr.WrapErrServiceInternalMsg("ingestion WAL response contradicts append provenance")
		}
		if response.Error == nil && response.AppendResult == nil {
			return merr.WrapErrServiceInternalMsg("successful ingestion WAL response has no append result")
		}
		confirmed := response.Error == nil && response.AppendResult != nil
		unknown = unknown || (response.Error != nil && !response.AppendNotAttempted)
		if previous, exists := byChannel[msg.VChannel()]; exists && previous != confirmed {
			return merr.WrapErrServiceInternalMsg("ingestion WAL responses disagree within a write unit")
		}
		byChannel[msg.VChannel()] = confirmed
	}
	if len(it.insertMsg.HashValues) != len(it.ingestionLineage) {
		return merr.WrapErrServiceInternalMsg("ingestion routing count differs from lineage")
	}
	confirmed := make([]bool, len(it.ingestionLineage))
	for row, channel := range it.insertMsg.HashValues {
		if int(channel) >= len(channels) {
			return merr.WrapErrServiceInternalMsg("ingestion row channel is out of bounds")
		}
		var exists bool
		confirmed[row], exists = byChannel[channels[channel]]
		if !exists {
			return merr.WrapErrServiceInternalMsg("ingestion output row has no WAL write unit")
		}
	}
	it.result.IngestionResult.OutcomeUnknown = unknown
	return it.finalizeIngestion(confirmed)
}
