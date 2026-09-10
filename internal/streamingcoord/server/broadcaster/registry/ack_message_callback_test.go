package registry

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestMessageCallbackRegistration(t *testing.T) {
	// Reset callbacks before test
	resetMessageAckCallbacks()

	// Test registering a callback
	called := false
	callback := func(ctx context.Context, msg message.BroadcastResultDropPartitionMessageV1) error {
		called = true
		return nil
	}

	RegisterDropPartitionV1AckCallback(callback)

	// Verify callback was registered
	callbackFuture, ok := messageAckCallbacks[message.MessageTypeDropPartitionV1]
	assert.True(t, ok)
	assert.NotNil(t, callbackFuture)

	// Create a mock message
	msg := message.NewDropPartitionMessageBuilderV1().
		WithHeader(&message.DropPartitionMessageHeader{}).
		WithBody(&message.DropPartitionRequest{}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()

	// Call the callback
	err := CallMessageAckCallback(context.Background(), msg, map[string]*message.AppendResult{
		"v1": {
			MessageID:              walimplstest.NewTestMessageID(1),
			LastConfirmedMessageID: walimplstest.NewTestMessageID(1),
			TimeTick:               1,
		},
	})
	assert.NoError(t, err)
	assert.True(t, called)

	resetMessageAckCallbacks()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err = CallMessageAckCallback(ctx, msg, nil)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}

// TestSplitShardAckCallbackIsRegistrable pins that a SplitShard broadcast can
// carry an ack callback at all: the registry keys its callbacks by message type,
// so a type absent from the map panics on registration rather than silently
// never firing. The callback is what commits a split's routing post-image, so a
// missing entry would leave every split fenced and never committed.
func TestSplitShardAckCallbackIsRegistrable(t *testing.T) {
	resetMessageAckCallbacks()
	defer resetMessageAckCallbacks()

	var got message.BroadcastResultSplitShardMessageV2
	called := false
	RegisterSplitShardV2AckCallback(func(ctx context.Context, result message.BroadcastResultSplitShardMessageV2) error {
		called = true
		got = result
		return nil
	})

	msg := message.NewSplitShardMessageBuilderV2().
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    1,
			SplitTaskId:     2,
			RoutingModulus:  2,
			SourceVchannels: []string{"v1"},
			PartitionIds:    []int64{10},
			Targets: []*message.SplitShardTarget{
				{Vchannel: "v2", Routing: &schemapb.HashRouting{Buckets: []uint64{0}}},
				{Vchannel: "v3", Routing: &schemapb.HashRouting{Buckets: []uint64{1}}},
			},
		}).
		WithBody(&message.SplitShardMessageBody{
			Genesis: &msgpb.CreateCollectionRequest{CollectionSchema: &schemapb.CollectionSchema{Name: "c"}},
			Routing: &message.AlterCollectionMessageUpdates{VirtualChannelNames: []string{"v2", "v3"}},
		}).
		WithBroadcast([]string{"v1", "v2", "v3"}, message.OptBuildBroadcastAppendFirst("v1")).
		MustBuildBroadcast()

	err := CallMessageAckCallback(context.Background(), msg, map[string]*message.AppendResult{
		"v1": {MessageID: walimplstest.NewTestMessageID(1), TimeTick: 100},
		"v2": {MessageID: walimplstest.NewTestMessageID(2), TimeTick: 101},
		"v3": {MessageID: walimplstest.NewTestMessageID(3), TimeTick: 102},
	})
	assert.NoError(t, err)
	assert.True(t, called)
	assert.EqualValues(t, 2, got.Message.Header().GetSplitTaskId())
	assert.EqualValues(t, 100, got.Results["v1"].TimeTick)
}
