package dml

import (
	"context"
	"fmt"
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
			var channels []string
			for i, err := range tc.errs {
				channels = append(channels, fmt.Sprintf("v%d", i))
				resp.Responses = append(resp.Responses, streaming.AppendResponse{Error: err})
			}
			want := tc.want
			if want == nil {
				want = tc.errs[0]
			}
			err := unwrapStreamingMutationError(resp, newStreamingErrorTestMessages(channels...))
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
	require.Contains(t, st.GetReason(), "write result is unknown because the transaction outcome could not be determined")
	require.Contains(t, st.GetReason(), "write may have succeeded")
	require.Contains(t, st.GetReason(), "retrying may result in duplicate data")
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
			var failedVChannel string
			fakeWAL.appendHook = func(_ context.Context, msgs ...message.MutableMessage) streaming.AppendResponses {
				require.NotEmpty(t, msgs)
				failedVChannel = msgs[len(msgs)-1].VChannel()
				resp := streaming.AppendResponses{}
				for _, msg := range msgs {
					var err error = status.NewPartialUpdateRetryable("another shard rejected CAS")
					if msg.VChannel() == failedVChannel {
						err = status.New(decoded.Code, decoded.Cause)
					}
					resp.Responses = append(resp.Responses, streaming.AppendResponse{Error: err})
				}
				return resp
			}
			oldWAL := streaming.WAL()
			streaming.SetWALForTest(fakeWAL)
			t.Cleanup(func() { streaming.SetWALForTest(oldWAL) })

			var st *commonpb.Status
			switch operation {
			case "insert":
				patch := mockey.Mock(repackInsertDataForStreamingService).Return(newStreamingErrorTestMessages("insert-v1", "insert-v2"), nil).Build()
				defer patch.UnPatch()
				task := &InsertTask{
					ctx: context.Background(), vChannels: []string{"test-vchannel"},
					insertMsg: &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{Base: &commonpb.MsgBase{}}},
					result:    &milvuspb.MutationResult{}, schema: &schemapb.CollectionSchema{},
				}
				require.NoError(t, task.Execute(context.Background()))
				st = task.result.Status
			case "delete":
				patch := mockey.Mock(repackDeleteMsgByHash).Return(map[uint32][]*msgstream.DeleteMsg{0: {{DeleteRequest: &msgpb.DeleteRequest{CollectionID: 1, NumRows: 1}}}}, int64(1), nil).Build()
				defer patch.UnPatch()
				task := &DeleteTask{req: &milvuspb.DeleteRequest{Expr: "id in [1]"}, vChannels: []string{"delete-v1"}}
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
			require.Contains(t, st.GetReason(), "vchannel="+failedVChannel)
			require.Equal(t, 1, fakeWAL.appendCalls, "unknown outcomes must not replay the write")
		})
	}
}

func newStreamingErrorTestMessages(channels ...string) []message.MutableMessage {
	msgs := make([]message.MutableMessage, 0, len(channels))
	for _, channel := range channels {
		msgs = append(msgs, message.NewInsertMessageBuilderV1().WithHeader(&message.InsertMessageHeader{CollectionId: 1}).WithBody(&msgpb.InsertRequest{CollectionID: 1}).WithVChannel(channel).MustBuildMutable())
	}
	return msgs
}

func TestStreamingMutationErrorCarriesFailingVChannel(t *testing.T) {
	for _, channels := range [][]string{{"healthy-v", "failed-v", "healthy-v"}, {"healthy-v", "healthy-v", "failed-v"}, {"failed-v", "healthy-v", "failed-v"}} {
		msgs := newStreamingErrorTestMessages(channels...)
		resp := streaming.AppendResponses{}
		for _, channel := range channels {
			var err error = status.NewPartialUpdateRetryable("conflict")
			if channel == "failed-v" {
				err = status.NewTransactionNotFound(42)
			}
			resp.Responses = append(resp.Responses, streaming.AppendResponse{Error: err})
		}
		st := merr.Status(unwrapStreamingMutationError(resp, msgs))
		assertWriteOutcomeUnknownStatus(t, st)
		require.Contains(t, st.GetReason(), "vchannel=failed-v")
		require.NotContains(t, st.GetReason(), "healthy-v")
	}
}
