package flusherimpl

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
)

func TestPChannelCheckpointManager(t *testing.T) {
	snMeta := mock_metastore.NewMockStreamingNodeCataLog(t)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snMeta))
	snMeta.EXPECT().GetConsumeCheckpoint(mock.Anything, mock.Anything).Return(&streamingpb.WALCheckpoint{
		MessageID: &messagespb.MessageID{Id: rmq.NewRmqID(0).Marshal()},
	}, nil)
	minimumOne := atomic.NewPointer[message.MessageID](nil)
	snMeta.EXPECT().SaveConsumeCheckpoint(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, ckpt *streamingpb.WALCheckpoint) error {
		id, _ := message.UnmarshalMessageID("rocksmq", ckpt.MessageID.Id)
		minimumOne.Store(&id)
		return nil
	})

	exists, vchannel, minimum := generateRandomExistsMessageID()
	p, err := recoverPChannelCheckpointManager(context.Background(), "rocksmq", "test", exists)
	assert.True(t, p.StartMessageID().EQ(rmq.NewRmqID(0)))

	assert.NoError(t, err)
	assert.NotNil(t, p)
	assert.Eventually(t, func() bool {
		newMinimum := minimumOne.Load()
		return newMinimum != nil && (*newMinimum).EQ(minimum)
	}, 10*time.Second, 10*time.Millisecond)

	p.AddVChannel("vchannel-999", rmq.NewRmqID(1000000))
	p.DropVChannel("vchannel-1000")
	for _, vchannel := range vchannel {
		p.Update(vchannel, rmq.NewRmqID(1000001))
	}

	assert.Eventually(t, func() bool {
		newMinimum := minimumOne.Load()
		return !(*newMinimum).EQ(minimum)
	}, 10*time.Second, 10*time.Millisecond)

	p.Close()
}
