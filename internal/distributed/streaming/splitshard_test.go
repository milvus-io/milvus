package streaming_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func newSplitShardParam() streaming.SplitShardParam {
	return streaming.SplitShardParam{
		CollectionID:   1,
		SourceVChannel: "v0",
		SplitTaskID:    100,
		FlushTs:        1000,
		Targets: []*message.SplitShardTarget{
			{Vchannel: "v1", KeyRange: &message.KeyRange{Upper: []byte{0x80}}},
			{Vchannel: "v2", KeyRange: &message.KeyRange{Lower: []byte{0x80}}},
		},
	}
}

func newManualFlushAppendResult(t *testing.T, segmentIDs []int64) *types.AppendResult {
	extra, err := anypb.New(&messagespb.ManualFlushExtraResponse{SegmentIds: segmentIDs})
	assert.NoError(t, err)
	return &types.AppendResult{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1500,
		Extra:     extra,
	}
}

func TestSplitShard(t *testing.T) {
	w := mock_streaming.NewMockWALAccesser(t)
	var barrierTimeTick uint64
	w.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
		return msg.MessageType() == message.MessageTypeManualFlush && msg.VChannel() == "v0"
	}), mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.MutableMessage, opts ...streaming.AppendOption) (*types.AppendResult, error) {
			if len(opts) > 0 {
				barrierTimeTick = opts[0].BarrierTimeTick
			}
			return newManualFlushAppendResult(t, []int64{7, 8}), nil
		}).Once()
	w.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
		if msg.MessageType() != message.MessageTypeSplitShard || msg.VChannel() != "v0" {
			return false
		}
		header := message.MustAsMutableSplitShardMessageV2(msg).Header()
		return header.GetCollectionId() == 1 && header.GetSplitTaskId() == 100 && len(header.GetTargets()) == 2
	})).Return(&types.AppendResult{
		MessageID: rmq.NewRmqID(2),
		TimeTick:  2000,
	}, nil).Once()

	result, err := streaming.SplitShard(context.Background(), w, newSplitShardParam())
	assert.NoError(t, err)
	assert.Equal(t, uint64(2000), result.SwitchTimeTick)
	assert.Equal(t, []int64{7, 8}, result.FlushedSegmentIDs)
	// the manual flush message carries the flush ts as the barrier time tick.
	assert.Equal(t, uint64(1000), barrierTimeTick)
}

func TestSplitShardOnFencedVChannel(t *testing.T) {
	w := mock_streaming.NewMockWALAccesser(t)
	// the retried ManualFlush is a harmless no-op on a fenced vchannel.
	w.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
		return msg.MessageType() == message.MessageTypeManualFlush
	}), mock.Anything).Return(newManualFlushAppendResult(t, nil), nil).Once()
	// the split message hits the fence of the previous split.
	w.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
		return msg.MessageType() == message.MessageTypeSplitShard
	})).Return(nil, status.NewShardFenced("v0")).Once()

	result, err := streaming.SplitShard(context.Background(), w, newSplitShardParam())
	assert.Nil(t, result)
	assert.ErrorIs(t, err, streaming.ErrSourceVChannelFenced)
}

func TestSplitShardAppendFailure(t *testing.T) {
	// the manual flush append fails.
	w := mock_streaming.NewMockWALAccesser(t)
	w.EXPECT().RawAppend(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("mock append error")).Once()
	result, err := streaming.SplitShard(context.Background(), w, newSplitShardParam())
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.NotErrorIs(t, err, streaming.ErrSourceVChannelFenced)

	// the split shard append fails with a non-fenced error.
	w = mock_streaming.NewMockWALAccesser(t)
	w.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
		return msg.MessageType() == message.MessageTypeManualFlush
	}), mock.Anything).Return(newManualFlushAppendResult(t, nil), nil).Once()
	w.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
		return msg.MessageType() == message.MessageTypeSplitShard
	})).Return(nil, errors.New("mock append error")).Once()
	result, err = streaming.SplitShard(context.Background(), w, newSplitShardParam())
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.NotErrorIs(t, err, streaming.ErrSourceVChannelFenced)
}

func TestSplitShardParamValidate(t *testing.T) {
	// valid param.
	param := newSplitShardParam()
	assert.NoError(t, param.Validate())

	// collection id must be positive.
	param = newSplitShardParam()
	param.CollectionID = 0
	assert.Error(t, param.Validate())

	// source vchannel must be set.
	param = newSplitShardParam()
	param.SourceVChannel = ""
	assert.Error(t, param.Validate())

	// at least two targets.
	param = newSplitShardParam()
	param.Targets = param.Targets[:1]
	assert.Error(t, param.Validate())

	// target vchannel must be set.
	param = newSplitShardParam()
	param.Targets[0].Vchannel = ""
	assert.Error(t, param.Validate())

	// the target vchannel must not duplicate the source.
	param = newSplitShardParam()
	param.Targets[0].Vchannel = "v0"
	assert.Error(t, param.Validate())

	// the target vchannels must not duplicate each other.
	param = newSplitShardParam()
	param.Targets[1].Vchannel = param.Targets[0].Vchannel
	assert.Error(t, param.Validate())

	// validation failure happens before any append.
	w := mock_streaming.NewMockWALAccesser(t)
	result, err := streaming.SplitShard(context.Background(), w, param)
	assert.Nil(t, result)
	assert.Error(t, err)
}
