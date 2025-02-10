package broadcast

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
)

func TestBroadcast(t *testing.T) {
	s := newMockServer(t, 0)
	bs := NewGRPCBroadcastService(walimplstest.WALName, s)
	msg, _ := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{"v1"}, message.NewCollectionNameResourceKey("r1")).
		BuildBroadcast()
	_, err := bs.Broadcast(context.Background(), msg)
	assert.NoError(t, err)
	err = bs.Ack(context.Background(), types.BroadcastAckRequest{
		VChannel:    "v1",
		BroadcastID: 1,
	})
	assert.NoError(t, err)
	err = bs.BlockUntilEvent(context.Background(), message.NewResourceKeyAckOneBroadcastEvent(message.NewCollectionNameResourceKey("r1")))
	assert.NoError(t, err)
	bs.Close()
}
