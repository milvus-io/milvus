package vchannel

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestDeleteReplayScannerUsesTransformLogStreamManager(t *testing.T) {
	ctx := context.Background()
	transformLog := transformlog.New(transformlog.Config{VChannel: "v1"})
	transformLog.SwitchIntoMetaAndData()
	require.NotNil(t, transformLog.ObserveMessage(ctx, newDeleteReplayTestMessage(t, "v1", 10)).Data)
	require.NotNil(t, transformLog.ObserveMessage(ctx, newDeleteReplayTestMessage(t, "v1", 20)).Data)
	require.NotNil(t, transformLog.ObserveMessage(ctx, newDeleteReplayTestMessage(t, "v1", 30)).Data)
	manager := transformlog.NewStreamManager("test-pchannel")
	manager.Register("v1", transformLog)

	scanner := newDeleteReplayScanner(ctx, manager, "test-pchannel", "v1", 0, 20)
	defer scanner.Close()

	first := recvDeleteReplayEvent(t, scanner.Chan())
	require.NotNil(t, first.Entry)
	assert.Equal(t, uint64(10), first.Entry.GetTimeTick())
	second := recvDeleteReplayEvent(t, scanner.Chan())
	require.NotNil(t, second.Entry)
	assert.Equal(t, uint64(20), second.Entry.GetTimeTick())
	caughtUp := recvDeleteReplayEvent(t, scanner.Chan())
	require.NotNil(t, caughtUp.CaughtUp)

	require.Eventually(t, func() bool {
		select {
		case <-scanner.Done():
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	assert.NoError(t, scanner.Error())
}

func newDeleteReplayTestMessage(t *testing.T, vchannel string, timetick uint64) message.ImmutableDeleteMessageV1 {
	t.Helper()
	mutableMsg := message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: 1,
			Rows:         1,
		}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  10,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
			Timestamps:   []uint64{timetick},
		}).
		MustBuildMutable()
	msg := mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	return message.MustAsImmutableDeleteMessageV1(msg)
}

func recvDeleteReplayEvent(t *testing.T, ch <-chan wal.TransformLogEvent) wal.TransformLogEvent {
	t.Helper()
	select {
	case event := <-ch:
		return event
	case <-time.After(time.Second):
		t.Fatal("timeout waiting delete replay event")
		return wal.TransformLogEvent{}
	}
}
