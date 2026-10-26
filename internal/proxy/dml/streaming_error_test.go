package dml

import (
	"context"
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestStreamingMutationErrorProjection(t *testing.T) {
	missing := errors.Wrap(status.NewTransactionNotFound(42), "append")
	cas := status.NewPartialUpdateRetryable("conflict")
	schema := status.NewSchemaVersionMismatch("schema changed")
	for _, tc := range []struct {
		name string
		errs []error
		want error
	}{
		{"success", []error{nil, nil}, nil},
		{"missing", []error{missing}, merr.ErrServiceWriteOutcomeUnknown},
		{"cas then missing", []error{cas, missing}, merr.ErrServiceWriteOutcomeUnknown},
		{"schema then missing", []error{schema, missing}, merr.ErrServiceWriteOutcomeUnknown},
		{"missing then schema", []error{missing, schema}, merr.ErrServiceWriteOutcomeUnknown},
		{"multiple cas", []error{cas, cas}, cas},
		{"cas then transport", []error{cas, io.ErrUnexpectedEOF}, io.ErrUnexpectedEOF},
		{"transport then cas", []error{io.ErrUnexpectedEOF, cas}, io.ErrUnexpectedEOF},
		{"deadline", []error{context.DeadlineExceeded}, context.DeadlineExceeded},
		{"explicit expiry", []error{status.NewTransactionExpired("expired")}, nil},
		{"same text without code", []error{status.NewUnrecoverableError(missing.Error())}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := streaming.AppendResponses{}
			for _, err := range tc.errs {
				resp.Responses = append(resp.Responses, streaming.AppendResponse{Error: err})
			}
			want := tc.want
			if want == nil {
				want = tc.errs[0]
			}
			err := unwrapStreamingMutationError(resp)
			require.ErrorIs(t, err, want)
			if want == merr.ErrServiceWriteOutcomeUnknown {
				assertWriteOutcomeUnknownStatus(t, merr.Status(err))
			}
		})
	}
}

func assertWriteOutcomeUnknownStatus(t *testing.T, st *commonpb.Status) {
	t.Helper()
	// Exercise the actual MutationResult payload seen by SDKs, not only Go errors.
	wire, err := proto.Marshal(&milvuspb.MutationResult{Status: st})
	require.NoError(t, err)
	var decoded milvuspb.MutationResult
	require.NoError(t, proto.Unmarshal(wire, &decoded))
	st = decoded.GetStatus()
	require.Equal(t, int32(13), st.GetCode())
	require.False(t, st.GetRetriable())
	require.NotEqual(t, "true", st.GetExtraInfo()[merr.InputErrorFlagKey])
	require.Contains(t, st.GetReason(), "write outcome is unknown")
	require.Contains(t, st.GetReason(), "write may have completed")
	require.Contains(t, st.GetReason(), "verify the result before retrying")
	require.NotContains(t, st.GetReason(), "STREAMING_CODE")
	require.NotContains(t, st.GetReason(), "transaction 42")
	require.ErrorIs(t, merr.Error(st), merr.ErrServiceWriteOutcomeUnknown)
}

func TestMutationTasksReportWriteOutcomeUnknown(t *testing.T) {
	for _, operation := range []string{"insert", "delete", "upsert", "partial replace", "partial relative"} {
		t.Run(operation, func(t *testing.T) {
			// Supply an error after the same serialization used by the server.
			wire, err := proto.Marshal(status.NewTransactionNotFound(42).AsPBError())
			require.NoError(t, err)
			var decoded streamingpb.StreamingError
			require.NoError(t, proto.Unmarshal(wire, &decoded))
			fakeWAL := newPartialUpdateCASTestWAL(t, 1)
			fakeWAL.appendHook = func(context.Context, ...message.MutableMessage) streaming.AppendResponses {
				return streaming.AppendResponses{Responses: []streaming.AppendResponse{
					{Error: status.NewPartialUpdateRetryable("another shard rejected CAS")},
					{Error: status.New(decoded.Code, decoded.Cause)},
				}}
			}
			oldWAL := streaming.WAL()
			streaming.SetWALForTest(fakeWAL)
			t.Cleanup(func() { streaming.SetWALForTest(oldWAL) })

			var st *commonpb.Status
			switch operation {
			case "insert":
				patch := mockey.Mock(repackInsertDataForStreamingService).Return([]message.MutableMessage{}, nil).Build()
				defer patch.UnPatch()
				task := &InsertTask{
					ctx: context.Background(), vChannels: []string{"test-vchannel"},
					insertMsg: &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{Base: &commonpb.MsgBase{}}},
					result:    &milvuspb.MutationResult{}, schema: &schemapb.CollectionSchema{},
				}
				require.NoError(t, task.Execute(context.Background()))
				st = task.result.Status
			case "delete":
				patch := mockey.Mock(repackDeleteMsgByHash).Return(map[uint32][]*msgstream.DeleteMsg{}, int64(0), nil).Build()
				defer patch.UnPatch()
				task := &DeleteTask{req: &milvuspb.DeleteRequest{Expr: "id in [1]"}}
				st = merr.Status(task.Execute(context.Background()))
			default:
				partial := operation != "upsert"
				task, msgs, _ := partialUpdateCASTestTask(t, partial, []int64{10}, []int64{10}, []int64{10})
				if partial {
					preparePartialUpdateCASTestGroups(t, task)
					msgs, _ = buildPartialUpdateCASTestMessages(t, task.collectionID, partialUpdateCASTestVChannels,
						[]int64{10}, []int64{10}, task.partialUpdateCASGroups)
				}
				if operation == "partial replace" {
					task.req.FieldOps[0].Op = schemapb.FieldPartialUpdateOp_REPLACE
				}
				insertPatch := mockey.Mock((*UpsertTask).packInsertMessage).Return(msgs, nil).Build()
				defer insertPatch.UnPatch()
				deletePatch := mockey.Mock((*UpsertTask).packDeleteMessage).Return(nil, nil).Build()
				defer deletePatch.UnPatch()
				st = merr.Status(task.Execute(context.Background()))
			}
			assertWriteOutcomeUnknownStatus(t, st)
			require.Equal(t, 1, fakeWAL.appendCalls, "unknown outcomes must not replay the write")
		})
	}
}
