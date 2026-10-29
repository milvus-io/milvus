package utility

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestMessagePositionUsesSafeSeekAndConsumerChannel(t *testing.T) {
	msg := message.NewFlushAllMessageBuilderV2().WithVChannel("").
		WithHeader(&message.FlushAllMessageHeader{}).WithBody(&message.FlushAllMessageBody{}).
		MustBuildMutable().WithTimeTick(100).WithLastConfirmed(rmq.NewRmqID(40)).IntoImmutableMessage(rmq.NewRmqID(50))
	pos := NewMessagePosition(msg, "p1_1v0")
	require.Equal(t, "p1_1v0", pos.GetChannelName())
	require.Equal(t, uint64(100), pos.GetTimestamp())
	require.Equal(t, commonpb.WALName_RocksMQ, pos.GetWALName())
	id := adaptor.MustGetMessageIDFromMQWrapperIDBytesWithWALName(message.WALName(pos.GetWALName()), pos.GetMsgID())
	require.True(t, id.EQ(msg.LastConfirmedMessageID()))
	require.False(t, id.EQ(msg.MessageID()))
}
