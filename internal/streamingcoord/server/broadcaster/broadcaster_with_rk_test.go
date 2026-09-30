//go:build test && dynamic

package broadcaster

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestBroadcasterWithRK_AddsControlChannel(t *testing.T) {
	defer mockey.UnPatchAll()
	const cchannel = "by-dev-rootcoord-dml_0_vcchan"

	var capturedMsg message.BroadcastMutableMessage
	mockey.Mock((*broadcastTaskManager).broadcast).To(
		func(_ *broadcastTaskManager, _ context.Context, msg message.BroadcastMutableMessage, _ uint64, _ *lockGuards) (*types.BroadcastAppendResult, error) {
			capturedMsg = msg
			return &types.BroadcastAppendResult{}, nil
		}).Build()

	newB := func() *broadcasterWithRK {
		return &broadcasterWithRK{
			broadcaster:    &broadcastTaskManager{},
			broadcastID:    11,
			controlChannel: cchannel,
			guards:         buildTestLockGuards(message.NewExclusiveCollectionNameResourceKey("db", "collection")),
		}
	}
	build := func(vchannels []string) message.BroadcastMutableMessage {
		b := message.NewDropCollectionMessageBuilderV1().
			WithHeader(&messagespb.DropCollectionMessageHeader{}).
			WithBody(&msgpb.DropCollectionRequest{})
		if len(vchannels) > 0 {
			b.WithBroadcast(vchannels)
		} else {
			b.WithControlChannelBroadcast()
		}
		return b.MustBuildBroadcast()
	}

	// The caller omits the control channel.
	_, err := newB().Broadcast(context.Background(), build([]string{"v1", "v2"}))
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"v1", "v2", cchannel}, capturedMsg.BroadcastHeader().VChannels)

	// The caller supplies it: no duplicate.
	_, err = newB().Broadcast(context.Background(), build([]string{"v1", cchannel}))
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"v1", cchannel}, capturedMsg.BroadcastHeader().VChannels)

	// Control channel only.
	_, err = newB().Broadcast(context.Background(), build(nil))
	assert.NoError(t, err)
	assert.Equal(t, []string{cchannel}, capturedMsg.BroadcastHeader().VChannels)
}

func buildTestLockGuards(keys ...message.ResourceKey) *lockGuards {
	guards := &lockGuards{}
	for _, key := range keys {
		guards.append(&lockGuard{key: key})
	}
	return guards
}
