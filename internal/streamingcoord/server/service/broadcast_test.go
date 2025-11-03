package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestBroadcastService(t *testing.T) {
	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()
	// Set up the balancer
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	})
	b.EXPECT().Close().Return().Maybe()
	balance.Register(b)

	fb := syncutil.NewFuture[broadcaster.Broadcaster]()
	mba := mock_broadcaster.NewMockBroadcastAPI(t)
	mba.EXPECT().Close().Return()
	mb := mock_broadcaster.NewMockBroadcaster(t)
	fb.Set(mb)
	mba.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(&types.BroadcastAppendResult{}, nil)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Return(mba, nil)
	mb.EXPECT().Ack(mock.Anything, mock.Anything).Return(nil)
	mb.EXPECT().LegacyAck(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mb.EXPECT().Close().Return().Maybe()
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

	ctx, cancel := context.WithCancel(context.Background())
	reached := make(chan struct{})
	done := make(chan struct{})
	mb.EXPECT().Ack(mock.Anything, mock.Anything).Unset()
	mb.EXPECT().Ack(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, msg message.ImmutableMessage) error {
		close(reached)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return nil
		}
	})
	go func() {
		<-reached
		cancel()
		time.Sleep(10 * time.Millisecond)
		close(done)
	}()
	_, err := service.Ack(ctx, &streamingpb.BroadcastAckRequest{
		BroadcastId: 1,
		Vchannel:    "v1",
		Message: &commonpb.ImmutableMessage{
			Id:         walimplstest.NewTestMessageID(1).IntoProto(),
			Payload:    []byte("payload"),
			Properties: map[string]string{"key": "value"},
		},
	})

	assert.NoError(t, err)
}
