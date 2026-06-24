package streaming_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func newSplitShardParam() streaming.SplitShardParam {
	return streaming.SplitShardParam{
		CollectionID:   1,
		SourceVChannel: "v0",
		SplitTaskID:    100,
		Targets: []*message.SplitShardTarget{
			{Vchannel: "v1", KeyRange: &message.KeyRange{Upper: []byte{0x80}}},
			{Vchannel: "v2", KeyRange: &message.KeyRange{Lower: []byte{0x80}}},
		},
	}
}

func TestSplitShard(t *testing.T) {
	w := mock_streaming.NewMockWALAccesser(t)
	// a single SplitShard message fences the source vchannel; the source
	// streamingnode auto-flushes the growing segments, so no separate
	// ManualFlush is appended.
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
}

func TestSplitShardOnFencedVChannel(t *testing.T) {
	w := mock_streaming.NewMockWALAccesser(t)
	// the split message hits the fence of the previous split; the fenced error
	// carries the recorded T_switch so the caller still recovers it.
	w.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
		return msg.MessageType() == message.MessageTypeSplitShard
	})).Return(nil, status.NewShardFenced("v0", 2000)).Once()

	result, err := streaming.SplitShard(context.Background(), w, newSplitShardParam())
	assert.ErrorIs(t, err, streaming.ErrSourceVChannelFenced)
	// even on the fenced path the result carries T_switch recovered from the error.
	assert.NotNil(t, result)
	assert.Equal(t, uint64(2000), result.SwitchTimeTick)
}

func TestSplitShardAppendFailure(t *testing.T) {
	// the split shard append fails with a non-fenced error.
	w := mock_streaming.NewMockWALAccesser(t)
	w.EXPECT().RawAppend(mock.Anything, mock.MatchedBy(func(msg message.MutableMessage) bool {
		return msg.MessageType() == message.MessageTypeSplitShard
	})).Return(nil, errors.New("mock append error")).Once()
	result, err := streaming.SplitShard(context.Background(), w, newSplitShardParam())
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.NotErrorIs(t, err, streaming.ErrSourceVChannelFenced)
}

func newInitSplitTargetParam() streaming.InitSplitTargetVChannelsParam {
	return streaming.InitSplitTargetVChannelsParam{
		CollectionID:   1,
		DBID:           2,
		DBName:         "db",
		CollectionName: "col",
		Schema: &schemapb.CollectionSchema{
			Name: "col",
		},
		PartitionIDs:    []int64{10, 11},
		SplitTaskID:     100,
		SourceVChannel:  "by-dev-rootcoord-dml_0_1v0",
		BarrierTimeTick: 2000,
		Targets: []*message.SplitShardTarget{
			{Vchannel: "by-dev-rootcoord-dml_1_1v1", KeyRange: &message.KeyRange{Upper: []byte{0x80}}},
			{Vchannel: "by-dev-rootcoord-dml_2_1v2", KeyRange: &message.KeyRange{Lower: []byte{0x80}}},
		},
	}
}

func TestInitSplitTargetVChannels(t *testing.T) {
	w := mock_streaming.NewMockWALAccesser(t)
	param := newInitSplitTargetParam()

	initialized := make(map[string]uint64, 2)
	w.EXPECT().RawAppend(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.MutableMessage, opts ...streaming.AppendOption) (*types.AppendResult, error) {
			assert.Equal(t, message.MessageTypeCreateVChannel, msg.MessageType())
			createMsg := message.MustAsMutableCreateVChannelMessageV2(msg)
			header := createMsg.Header()
			assert.Equal(t, int64(1), header.GetCollectionId())
			assert.Equal(t, []int64{10, 11}, header.GetPartitionIds())
			assert.Equal(t, int64(100), header.GetSplitTaskId())
			assert.Equal(t, "by-dev-rootcoord-dml_0_1v0", header.GetSplitSourceVchannel())
			assert.NotNil(t, header.GetKeyRange())
			body, err := createMsg.Body()
			assert.NoError(t, err)
			assert.Equal(t, "col", body.GetCollectionSchema().GetName())
			assert.Equal(t, []string{msg.VChannel()}, body.GetVirtualChannelNames())
			if len(opts) > 0 {
				initialized[msg.VChannel()] = opts[0].BarrierTimeTick
			}
			return &types.AppendResult{MessageID: rmq.NewRmqID(1), TimeTick: 2100, LastConfirmedMessageID: rmq.NewRmqID(7)}, nil
		}).Times(2)

	err := streaming.InitSplitTargetVChannels(context.Background(), w, param)
	assert.NoError(t, err)
	// every target vchannel is created with T_switch as the barrier. No consume
	// start position is returned: a split target is an ordinary vchannel whose
	// genesis checkpoint reaches datacoord through the normal WAL-open report.
	assert.Equal(t, map[string]uint64{
		"by-dev-rootcoord-dml_1_1v1": 2000,
		"by-dev-rootcoord-dml_2_1v2": 2000,
	}, initialized)
}

func TestInitSplitTargetVChannelsAppendFailure(t *testing.T) {
	w := mock_streaming.NewMockWALAccesser(t)
	w.EXPECT().RawAppend(mock.Anything, mock.Anything, mock.Anything).Return(&types.AppendResult{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  2100,
	}, errors.New("mock append error")).Once()

	err := streaming.InitSplitTargetVChannels(context.Background(), w, newInitSplitTargetParam())
	assert.Error(t, err)
}

func TestInitSplitTargetVChannelsParamValidate(t *testing.T) {
	param := newInitSplitTargetParam()
	assert.NoError(t, param.Validate())

	param = newInitSplitTargetParam()
	param.CollectionID = 0
	assert.Error(t, param.Validate())

	param = newInitSplitTargetParam()
	param.Schema = nil
	assert.Error(t, param.Validate())

	param = newInitSplitTargetParam()
	param.PartitionIDs = nil
	assert.Error(t, param.Validate())

	param = newInitSplitTargetParam()
	param.SourceVChannel = ""
	assert.Error(t, param.Validate())

	param = newInitSplitTargetParam()
	param.BarrierTimeTick = 0
	assert.Error(t, param.Validate())

	param = newInitSplitTargetParam()
	param.Targets = nil
	assert.Error(t, param.Validate())

	param = newInitSplitTargetParam()
	param.Targets[0].Vchannel = ""
	assert.Error(t, param.Validate())

	// the target must not duplicate the source.
	param = newInitSplitTargetParam()
	param.Targets[0].Vchannel = param.SourceVChannel
	assert.Error(t, param.Validate())

	// the targets must not duplicate each other.
	param = newInitSplitTargetParam()
	param.Targets[1].Vchannel = param.Targets[0].Vchannel
	assert.Error(t, param.Validate())

	// validation failure happens before any append.
	w := mock_streaming.NewMockWALAccesser(t)
	err := streaming.InitSplitTargetVChannels(context.Background(), w, param)
	assert.Error(t, err)
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
