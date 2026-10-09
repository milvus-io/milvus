package broadcaster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

const (
	appendExtraTestSource  = "p0_1v0"
	appendExtraTestTarget  = "p1_1v1"
	appendExtraTestControl = "p1_vcchan"
)

// initAppendExtraTestResource wires the catalog and mixcoord the broadcast task
// persists through.
func initAppendExtraTestResource(t *testing.T) {
	paramtable.Init()
	registry.ResetRegistration()
	meta := mock_metastore.NewMockStreamingCoordCataLog(t)
	meta.EXPECT().SaveBroadcastTask(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	rc := idalloc.NewMockRootCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(rc)
	resource.InitForTest(resource.OptStreamingCatalog(meta), resource.OptMixCoordClient(f))
}

// appendExtraTestBroadcast is a SplitShard broadcast of one source, one target
// and the control channel.
func appendExtraTestBroadcast() message.BroadcastMutableMessage {
	return createNewSplitShardBroadcastMsg(
		[]string{appendExtraTestSource, appendExtraTestTarget, appendExtraTestControl},
		appendExtraTestSource,
	).WithBroadcastID(800)
}

// newAppendExtraTestTask is the primary's pending task for appendExtraTestBroadcast.
func newAppendExtraTestTask(t *testing.T) *broadcastTask {
	initAppendExtraTestResource(t)
	taskProto := createNewWaitAckBroadcastTaskFromMessage(appendExtraTestBroadcast(),
		streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING, []byte{0x00, 0x00, 0x00})
	task := newBroadcastTaskFromProto(taskProto, newBroadcasterMetrics(), newAckCallbackScheduler(mlog.With()))
	task.SetLogger(mlog.With())
	return task
}

func appendExtraTestSwitchExtra(t *testing.T, switchTimeTick uint64) *anypb.Any {
	extra, err := anypb.New(&message.SplitShardExtraResponse{SplitTimeTick: switchTimeTick})
	require.NoError(t, err)
	return extra
}

// walRecordsOf is what the WAL holds for each replica of msg after the append:
// the source's record carries the extra append response its StreamingNode
// stamped on it.
func walRecordsOf(msg message.BroadcastMutableMessage, sourceExtra *anypb.Any) map[string]message.ImmutableMessage {
	records := make(map[string]message.ImmutableMessage)
	for i, replica := range msg.SplitIntoMutableMessage() {
		if replica.VChannel() == appendExtraTestSource {
			message.SetAppendExtra(replica, sourceExtra)
		}
		records[replica.VChannel()] = replica.WithTimeTick(uint64(100 + i)).
			WithLastConfirmedUseMessageID().
			IntoImmutableMessage(walimplstest.NewTestMessageID(int64(i + 1)))
	}
	return records
}

// assertCallbackSeesTheSourceExtra asserts that the ack callback of task would
// read wantSwitchTimeTick from the source's result, both from the live task and
// from the task recovered out of its persisted proto -- a coordinator restart
// between the ack and the callback must not lose it.
func assertCallbackSeesTheSourceExtra(t *testing.T, task *broadcastTask, wantSwitchTimeTick uint64) {
	t.Helper()
	check := func(task *broadcastTask) {
		_, results := task.BroadcastResult()
		callbackResults := toCallbackAppendResults(results)
		require.NotNil(t, callbackResults[appendExtraTestSource].Extra, "the source result must carry its extra append response")
		resp := &message.SplitShardExtraResponse{}
		require.NoError(t, callbackResults[appendExtraTestSource].Extra.UnmarshalTo(resp))
		assert.Equal(t, wantSwitchTimeTick, resp.GetSplitTimeTick())
		// A replica without an extra response reports none.
		assert.Nil(t, callbackResults[appendExtraTestTarget].Extra)
		assert.Nil(t, callbackResults[appendExtraTestControl].Extra)
	}
	check(task)

	persisted, err := proto.Marshal(task.task)
	require.NoError(t, err)
	restored := &streamingpb.BroadcastTask{}
	require.NoError(t, proto.Unmarshal(persisted, restored))
	check(newBroadcastTaskFromProto(restored, newBroadcasterMetrics(), newAckCallbackScheduler(mlog.With())))
}

// TestBroadcastTaskPersistsTheAppendExtraOfItsOwnAppend: on the primary the
// source replica is acknowledged by the broadcaster's own append -- AckPartial
// for the append-first source, FastAck for the rest -- whose results carry the
// producer's extra append response.
func TestBroadcastTaskPersistsTheAppendExtraOfItsOwnAppend(t *testing.T) {
	task := newAppendExtraTestTask(t)
	ctx := context.Background()

	require.NoError(t, task.AckPartial(ctx, map[string]*types.AppendResult{
		appendExtraTestSource: {
			MessageID:              walimplstest.NewTestMessageID(1),
			LastConfirmedMessageID: walimplstest.NewTestMessageID(1),
			TimeTick:               100,
			Extra:                  appendExtraTestSwitchExtra(t, 90),
		},
	}))
	require.NoError(t, task.FastAck(ctx, map[string]*types.AppendResult{
		appendExtraTestTarget:  {MessageID: walimplstest.NewTestMessageID(2), LastConfirmedMessageID: walimplstest.NewTestMessageID(2), TimeTick: 101},
		appendExtraTestControl: {MessageID: walimplstest.NewTestMessageID(3), LastConfirmedMessageID: walimplstest.NewTestMessageID(3), TimeTick: 102},
	}))

	assertCallbackSeesTheSourceExtra(t, task, 90)
}

// TestBroadcastTaskPersistsTheAppendExtraOfAConsumerSideAck: the source replica
// may instead be acknowledged first by its StreamingNode's consumer, which sends
// the WAL record itself. The extra append response travels on that record.
func TestBroadcastTaskPersistsTheAppendExtraOfAConsumerSideAck(t *testing.T) {
	task := newAppendExtraTestTask(t)
	records := walRecordsOf(appendExtraTestBroadcast(), appendExtraTestSwitchExtra(t, 90))
	for _, vchannel := range []string{appendExtraTestSource, appendExtraTestTarget, appendExtraTestControl} {
		require.NoError(t, task.Ack(context.Background(), records[vchannel]))
	}

	assertCallbackSeesTheSourceExtra(t, task, 90)
}

// TestReplicatedBroadcastTaskPersistsTheAppendExtra is the secondary cluster's
// path: there is no broadcaster append at all. The task is created from the
// first acked WAL record and every ack is a consumer-side one, so the only
// carrier of the source's T_switch is the record the secondary's own
// StreamingNode stamped.
func TestReplicatedBroadcastTaskPersistsTheAppendExtra(t *testing.T) {
	initAppendExtraTestResource(t)
	records := walRecordsOf(appendExtraTestBroadcast(), appendExtraTestSwitchExtra(t, 90))

	// The source's record arrives first and creates the task.
	task := newBroadcastTaskFromImmutableMessage(records[appendExtraTestSource], newBroadcasterMetrics(), newAckCallbackScheduler(mlog.With()))
	task.SetLogger(mlog.With())
	assert.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED, task.State())
	// The task's own message is split into replicas again, so it must not keep
	// the source's extra append response for every other replica to inherit.
	assert.Nil(t, message.AppendExtraOf(task.BroadcastMessage()))

	for _, vchannel := range []string{appendExtraTestSource, appendExtraTestTarget, appendExtraTestControl} {
		require.NoError(t, task.Ack(context.Background(), records[vchannel]))
	}

	assertCallbackSeesTheSourceExtra(t, task, 90)
}
