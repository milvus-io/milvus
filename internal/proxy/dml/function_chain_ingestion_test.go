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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	streamingtypes "github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func ingestionTestStringArray(values ...string) *schemapb.ScalarField {
	return &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}}
}

func ingestionTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Name: "coll", Fields: []*schemapb.FieldSchema{
		{Name: "pk", FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
		{Name: "doc_id", FieldID: 101, DataType: schemapb.DataType_Int64},
		{Name: "text", FieldID: 102, DataType: schemapb.DataType_VarChar},
		{Name: "chunk_index", FieldID: 103, DataType: schemapb.DataType_Int64},
	}, FunctionChains: []*schemapb.FunctionChain{{Name: "chunk", Stage: schemapb.FunctionChainStage_FunctionChainStageIngestion, Ops: []*schemapb.FunctionChainOp{
		{Op: "explode", Inputs: []string{"sections"}, Outputs: []string{"chunk", "chunk_index"}},
		{Op: "select", Inputs: []string{"doc_id", "chunk.text", "chunk_index"}, Outputs: []string{"doc_id", "text", "chunk_index"}},
	}}}}
}

func ingestionTestTask() *InsertTask {
	text := &schemapb.FieldData{FieldName: "text", Type: schemapb.DataType_Array, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_VarChar, Data: []*schemapb.ScalarField{ingestionTestStringArray("a", "b"), ingestionTestStringArray("c")}}}}}}
	return &InsertTask{result: &milvuspb.MutationResult{}, insertMsg: &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{NumRows: 2, Version: msgpb.InsertDataVersion_ColumnBased, FieldsData: []*schemapb.FieldData{
		{FieldName: "doc_id", Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{7, 8}}}}}},
		{FieldName: "sections", Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{text}}}},
	}}}}
}

func TestApplyIngestionExplodesStructBeforeIDAllocation(t *testing.T) {
	task := ingestionTestTask()
	require.NoError(t, task.applyIngestion(context.Background(), mustNewSchemaInfo(ingestionTestSchema())))
	require.Equal(t, uint64(3), task.insertMsg.GetNumRows())
	require.Empty(t, task.insertMsg.GetRowIDs(), "transform occurs before ID allocation")
	require.Equal(t, []int64{7, 7, 8}, task.insertMsg.FieldsData[0].GetScalars().GetLongData().GetData())
	require.Equal(t, []string{"a", "b", "c"}, task.insertMsg.FieldsData[1].GetScalars().GetStringData().GetData())
	require.Equal(t, []int64{0, 1, 0}, task.insertMsg.FieldsData[2].GetScalars().GetLongData().GetData())
	require.Equal(t, []transform.RowLineage{{{Group: 0, Row: 0}}, {{Group: 0, Row: 0}}, {{Group: 0, Row: 1}}}, task.ingestionLineage)
	require.Equal(t, uint32(2), task.result.GetIngestionResult().GetInputRows())
	require.Zero(t, task.result.GetIngestionResult().GetOutputRows(), "no output is confirmed before WAL")
}

func TestIngestionBindsTargetStructChildSchemaBeforeWriting(t *testing.T) {
	for _, elementType := range []schemapb.DataType{schemapb.DataType_VarChar, schemapb.DataType_Int64} {
		t.Run(elementType.String(), func(t *testing.T) {
			task := ingestionTestTask()
			original := proto.Clone(task.insertMsg.InsertRequest)
			schema := ingestionTestSchema()
			schema.Fields = schema.Fields[:2]
			schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{{Name: "documents", FieldID: 200, Fields: []*schemapb.FieldSchema{
				{Name: "documents[text]", FieldID: 201, DataType: schemapb.DataType_Array, ElementType: elementType},
			}}}
			schema.FunctionChains[0].Ops = []*schemapb.FunctionChainOp{{Op: "select", Inputs: []string{"doc_id", "sections"}, Outputs: []string{"doc_id", "documents"}}}
			err := task.applyIngestion(context.Background(), mustNewSchemaInfo(schema))
			if elementType == schemapb.DataType_Int64 {
				require.ErrorIs(t, err, merr.ErrFunctionFailed)
				require.True(t, proto.Equal(original, task.insertMsg.InsertRequest))
				require.Empty(t, task.ingestionLineage)
				require.Zero(t, task.result.GetIngestionResult().GetOutputRows())
				return
			}
			require.NoError(t, err)
			field := task.insertMsg.FieldsData[1]
			require.EqualValues(t, 200, field.GetFieldId())
			require.Len(t, field.GetStructArrays().GetFields(), 1)
			child := field.GetStructArrays().GetFields()[0]
			require.EqualValues(t, 201, child.GetFieldId())
			require.Equal(t, schemapb.DataType_VarChar, child.GetScalars().GetArrayData().GetElementType())
			require.Equal(t, []string{"a", "b"}, child.GetScalars().GetArrayData().GetData()[0].GetStringData().GetData())
			require.Equal(t, []string{"c"}, child.GetScalars().GetArrayData().GetData()[1].GetStringData().GetData())
			require.Empty(t, task.insertMsg.GetRowIDs(), "schema binding precedes IDs and WAL")
		})
	}
}

func TestIngestionIdempotencyRejectsBeforeInputAndUDF(t *testing.T) {
	task := ingestionTestTask()
	task.idempotencyKey = "retry-key"
	task.insertMsg.FieldsData = nil // conversion would fail if reached
	err := task.applyIngestion(context.Background(), mustNewSchemaInfo(ingestionTestSchema()))
	require.ErrorIs(t, err, merr.ErrOperationNotSupported)
	require.Equal(t, merr.InputError, merr.GetErrorType(err))
	require.Empty(t, task.ingestionLineage)
	require.Zero(t, task.result.GetIngestionResult().GetOutputRows())
}

func TestIngestionConfirmedIDsAndCSRForPartialOutcome(t *testing.T) {
	task := &InsertTask{result: &milvuspb.MutationResult{IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{101, 102, 103}}}}, IngestionResult: &milvuspb.IngestionFunctionChainResult{InputRows: 3}}, ingestionLineage: []transform.RowLineage{{{Group: 0, Row: 0}}, {{Group: 0, Row: 0}}, {{Group: 0, Row: 1}}}}
	require.NoError(t, task.finalizeIngestion([]bool{true, false, true}))
	require.Equal(t, []int64{101, 103}, task.result.GetIDs().GetIntId().GetData())
	require.Equal(t, int64(2), task.result.GetInsertCnt())
	require.Equal(t, uint32(2), task.result.GetIngestionResult().GetOutputRows())
	require.Equal(t, []uint64{0, 1, 2}, task.result.GetIngestionResult().GetOutputSources().GetSourceOffsets())
	require.Equal(t, []uint32{0, 1}, task.result.GetIngestionResult().GetOutputSources().GetSourceRowIndexes())
	require.Equal(t, []uint32{0}, task.result.GetErrIndex())
	require.Equal(t, []uint32{1, 2}, task.result.GetSuccIndex(), "filtered source 2 is a successful no-op")
}

func TestIngestionEmptyOutput(t *testing.T) {
	task := &InsertTask{result: &milvuspb.MutationResult{IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}}, IngestionResult: &milvuspb.IngestionFunctionChainResult{InputRows: 2}}}
	require.NoError(t, task.finalizeIngestion(nil))
	require.Empty(t, task.result.GetIDs().GetIntId().GetData())
	require.Zero(t, task.result.GetInsertCnt())
	require.Equal(t, []uint64{0}, task.result.GetIngestionResult().GetOutputSources().GetSourceOffsets())
	require.Equal(t, []uint32{0, 1}, task.result.GetSuccIndex())
}

func TestIngestionWALConfirmationUsesChannelOutcomes(t *testing.T) {
	task := &InsertTask{result: &milvuspb.MutationResult{IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{101, 102, 103}}}}, IngestionResult: &milvuspb.IngestionFunctionChainResult{InputRows: 2}}, insertMsg: &msgstream.InsertMsg{BaseMsg: msgstream.BaseMsg{HashValues: []uint32{0, 1, 0}}}, ingestionLineage: []transform.RowLineage{{{Group: 0, Row: 0}}, {{Group: 0, Row: 0}}, {{Group: 0, Row: 1}}}}
	channels := []string{"channel_a", "channel_b"}
	msgs := make([]message.MutableMessage, 2)
	for i, channel := range channels {
		msg, err := message.NewInsertMessageBuilderV1().WithVChannel(channel).WithHeader(&message.InsertMessageHeader{CollectionId: 1, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 1, Rows: 1}}}).WithBody(&msgpb.InsertRequest{NumRows: 1}).BuildMutable()
		require.NoError(t, err)
		msgs[i] = msg
	}
	responses := streaming.AppendResponses{Responses: []streamingtypes.AppendResponse{{AppendResult: &streamingtypes.AppendResult{TimeTick: 42}}, {Error: merr.ErrServiceUnavailable}}}
	require.NoError(t, task.confirmIngestion(msgs, channels, responses))
	require.Equal(t, []int64{101, 103}, task.result.GetIDs().GetIntId().GetData())
	require.Equal(t, []uint32{0}, task.result.GetErrIndex())
	require.Equal(t, []uint32{1}, task.result.GetSuccIndex())
	require.Equal(t, uint32(2), task.result.GetIngestionResult().GetOutputRows())
}

func TestIngestionExpansionRequiresIndependentPrimaryKeys(t *testing.T) {
	schema := ingestionTestSchema()
	schema.Fields[0].AutoID = false
	field := &schemapb.FieldData{FieldName: "pk", Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{7, 7}}}}}}
	lineage := []transform.RowLineage{{{Group: 0, Row: 0}}, {{Group: 0, Row: 0}}}
	require.ErrorIs(t, validateExpandedPrimaryKeys(schema, []*schemapb.FieldData{field}, lineage), merr.ErrOperationNotSupported)
	field.GetScalars().GetLongData().Data[1] = 8
	require.NoError(t, validateExpandedPrimaryKeys(schema, []*schemapb.FieldData{field}, lineage))
	schema.Fields[0].AutoID = true
	require.NoError(t, validateExpandedPrimaryKeys(schema, nil, lineage))
}

func TestIngestionExecutePartialAndMalformedWALOutcomes(t *testing.T) {
	channels := []string{"channel_a", "channel_b"}
	newMessage := func(channel string) message.MutableMessage {
		msg, err := message.NewInsertMessageBuilderV1().WithVChannel(channel).WithHeader(&message.InsertMessageHeader{CollectionId: 1, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 1, Rows: 1}}}).WithBody(&msgpb.InsertRequest{NumRows: 1}).BuildMutable()
		require.NoError(t, err)
		return msg
	}
	success := streamingtypes.AppendResponse{AppendResult: &streamingtypes.AppendResult{TimeTick: 42}}
	failure := streamingtypes.AppendResponse{Error: merr.ErrServiceUnavailable}
	beforeAppend := streamingtypes.AppendResponse{Error: merr.ErrServiceUnavailable, AppendNotAttempted: true}
	for _, test := range []struct {
		name      string
		messages  []message.MutableMessage
		responses []streamingtypes.AppendResponse
		partial   bool
		knownFail bool
	}{
		{name: "partial confirms child IDs", messages: []message.MutableMessage{newMessage(channels[0]), newMessage(channels[1])}, responses: []streamingtypes.AppendResponse{success, failure}, partial: true},
		{name: "all write units rejected before append", messages: []message.MutableMessage{newMessage(channels[0]), newMessage(channels[1])}, responses: []streamingtypes.AppendResponse{beforeAppend, beforeAppend}, knownFail: true},
		{name: "missing response", messages: []message.MutableMessage{newMessage(channels[0])}},
		{name: "success has no append result", messages: []message.MutableMessage{newMessage(channels[0])}, responses: []streamingtypes.AppendResponse{{}}},
		{name: "missing routed write unit", messages: []message.MutableMessage{newMessage(channels[0])}, responses: []streamingtypes.AppendResponse{success}},
		{name: "write unit responses disagree", messages: []message.MutableMessage{newMessage(channels[0]), newMessage(channels[0])}, responses: []streamingtypes.AppendResponse{failure, success}},
		{name: "append provenance contradicts result", messages: []message.MutableMessage{newMessage(channels[0])}, responses: []streamingtypes.AppendResponse{{AppendResult: success.AppendResult, AppendNotAttempted: true}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			pack := mockey.Mock(repackInsertDataForStreamingService).Return(test.messages, nil).Build()
			defer pack.UnPatch()
			fakeWAL := newPartialUpdateCASTestWAL(t, 1)
			fakeWAL.appendHook = func(context.Context, ...message.MutableMessage) streaming.AppendResponses {
				return streaming.AppendResponses{Responses: test.responses}
			}
			oldWAL := streaming.WAL()
			streaming.SetWALForTest(fakeWAL)
			t.Cleanup(func() { streaming.SetWALForTest(oldWAL) })
			task := &InsertTask{
				baseTask: baseTask{MetaCache: NewMockCache(t)}, ctx: context.Background(), collectionID: 1, vChannels: channels, schema: ingestionTestSchema(),
				insertMsg:          &msgstream.InsertMsg{BaseMsg: msgstream.BaseMsg{HashValues: []uint32{0, 1, 0}}, InsertRequest: &msgpb.InsertRequest{Base: &commonpb.MsgBase{}, NumRows: 3, Version: msgpb.InsertDataVersion_ColumnBased}},
				result:             &milvuspb.MutationResult{Status: merr.Success(), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{101, 102, 103}}}}, IngestionResult: &milvuspb.IngestionFunctionChainResult{InputRows: 2}},
				ingestionLineage:   []transform.RowLineage{{{Group: 0, Row: 0}}, {{Group: 0, Row: 0}}, {{Group: 0, Row: 1}}},
				ingestionInputRows: 3,
			}
			require.NoError(t, task.Execute(context.Background()))
			require.Equal(t, 1, fakeWAL.appendCalls)
			require.False(t, task.result.GetStatus().GetRetriable(), "WAL has been reached and transformed output cannot be reused")
			require.Equal(t, !test.knownFail, task.result.GetIngestionResult().GetOutcomeUnknown())
			if test.partial {
				require.ErrorIs(t, merr.Error(task.result.GetStatus()), merr.ErrServiceUnavailable)
				require.Equal(t, []int64{101, 103}, task.result.GetIDs().GetIntId().GetData())
				require.Equal(t, int64(2), task.result.GetInsertCnt())
				require.Equal(t, uint32(2), task.result.GetIngestionResult().GetOutputRows())
				require.Equal(t, uint64(42), task.result.GetTimestamp())
				require.Equal(t, []uint32{0}, task.result.GetErrIndex())
			} else {
				if test.knownFail {
					require.ErrorIs(t, merr.Error(task.result.GetStatus()), merr.ErrServiceUnavailable)
				} else {
					require.ErrorIs(t, merr.Error(task.result.GetStatus()), merr.ErrServiceInternal)
				}
				require.Empty(t, task.result.GetIDs().GetIntId().GetData())
				require.Zero(t, task.result.GetInsertCnt())
				require.Zero(t, task.result.GetIngestionResult().GetOutputRows())
				require.Equal(t, []uint64{0}, task.result.GetIngestionResult().GetOutputSources().GetSourceOffsets())
				require.Equal(t, []uint32{0, 1}, task.result.GetErrIndex())
			}
			interrupted := task.IngestionFailureResult(context.Canceled, true)
			require.True(t, proto.Equal(task.result.GetIDs(), interrupted.GetIDs()), "settled failure keeps every confirmed ID")
			require.True(t, proto.Equal(task.result.GetIngestionResult(), interrupted.GetIngestionResult()))
		})
	}
}

func TestIngestionWaitInterruptionCoordinatesWithWAL(t *testing.T) {
	msg, err := message.NewInsertMessageBuilderV1().WithVChannel("channel_a").WithHeader(&message.InsertMessageHeader{CollectionId: 1, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 1, Rows: 1}}}).WithBody(&msgpb.InsertRequest{NumRows: 1}).BuildMutable()
	require.NoError(t, err)
	for _, interruptBeforeAppend := range []bool{true, false} {
		t.Run(map[bool]string{true: "waiter stops a future append", false: "in-flight append is unknown until settlement"}[interruptBeforeAppend], func(t *testing.T) {
			pack := mockey.Mock(repackInsertDataForStreamingService).Return([]message.MutableMessage{msg}, nil).Build()
			defer pack.UnPatch()
			fakeWAL := newPartialUpdateCASTestWAL(t, 1)
			oldWAL := streaming.WAL()
			streaming.SetWALForTest(fakeWAL)
			t.Cleanup(func() { streaming.SetWALForTest(oldWAL) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			task := &InsertTask{
				baseTask: baseTask{MetaCache: NewMockCache(t)}, ctx: ctx, Condition: NewTaskCondition(ctx), collectionID: 1, vChannels: []string{"channel_a"}, schema: ingestionTestSchema(),
				insertMsg:        &msgstream.InsertMsg{BaseMsg: msgstream.BaseMsg{HashValues: []uint32{0}}, InsertRequest: &msgpb.InsertRequest{Base: &commonpb.MsgBase{}, NumRows: 1, Version: msgpb.InsertDataVersion_ColumnBased}},
				result:           &milvuspb.MutationResult{Status: merr.Success(), IDs: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{101}}}}, IngestionResult: &milvuspb.IngestionFunctionChainResult{InputRows: 1}},
				ingestionLineage: []transform.RowLineage{{{Group: 0, Row: 0}}}, ingestionInputRows: 2,
			}
			if interruptBeforeAppend {
				cancel()
				require.ErrorIs(t, task.WaitToFinish(), context.Canceled)
				failed := task.IngestionFailureResult(ctx.Err(), true)
				require.False(t, failed.GetIngestionResult().GetOutcomeUnknown())
				require.Empty(t, failed.GetIDs().GetIntId().GetData(), "allocated IDs are not confirmed")
				require.Equal(t, []uint32{0}, failed.GetErrIndex())
				require.ErrorIs(t, task.Execute(ctx), context.Canceled)
				require.Zero(t, fakeWAL.appendCalls)
				return
			}
			started := make(chan struct{})
			resume := make(chan struct{})
			fakeWAL.appendHook = func(context.Context, ...message.MutableMessage) streaming.AppendResponses {
				close(started)
				<-resume
				return streaming.AppendResponses{Responses: []streamingtypes.AppendResponse{{AppendResult: &streamingtypes.AppendResult{TimeTick: 42}}}}
			}
			finished := make(chan error, 1)
			go func() { finished <- task.Execute(ctx) }()
			<-started
			cancel()
			require.ErrorIs(t, task.WaitToFinish(), context.Canceled)
			failed := task.IngestionFailureResult(ctx.Err(), true)
			require.True(t, failed.GetIngestionResult().GetOutcomeUnknown())
			require.Zero(t, failed.GetIngestionResult().GetOutputRows())
			require.Empty(t, failed.GetIDs().GetIntId().GetData())
			require.False(t, failed.GetStatus().GetRetriable())
			close(resume)
			require.NoError(t, <-finished)
			settled := task.IngestionFailureResult(ctx.Err(), true)
			require.False(t, settled.GetIngestionResult().GetOutcomeUnknown())
			require.Equal(t, []int64{101}, settled.GetIDs().GetIntId().GetData())
			require.Equal(t, uint32(1), settled.GetIngestionResult().GetOutputRows())
			require.Equal(t, []uint64{0, 1}, settled.GetIngestionResult().GetOutputSources().GetSourceOffsets())
		})
	}
}
