package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestBroadcastService(t *testing.T) {
	fb := syncutil.NewFuture[broadcaster.Broadcaster]()
	mba := mock_broadcaster.NewMockBroadcastAPI(t)
	mb := mock_broadcaster.NewMockBroadcaster(t)
	fb.Set(mb)
	mba.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(&types.BroadcastAppendResult{}, nil)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Return(mba, nil)
	mb.EXPECT().Ack(mock.Anything, mock.Anything).Return(nil)
	mb.EXPECT().LegacyAck(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	broadcast.Register(mb)

	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		WithBroadcast([]string{"v1"}, message.NewCollectionNameResourceKey("r1")).
		MustBuildBroadcast()

	service := NewBroadcastService()
	service.Broadcast(context.Background(), &streamingpb.BroadcastRequest{
		Message: msg.IntoMessageProto(),
	})
	service.Ack(context.Background(), &streamingpb.BroadcastAckRequest{
		BroadcastId: 1,
		Vchannel:    "v1",
	})
	service.Ack(context.Background(), &streamingpb.BroadcastAckRequest{
		BroadcastId: 1,
		Vchannel:    "v1",
		Message: &commonpb.ImmutableMessage{
			Id:         walimplstest.NewTestMessageID(1).IntoProto(),
			Payload:    []byte("payload"),
			Properties: map[string]string{"key": "value"},
		},
	})
}
