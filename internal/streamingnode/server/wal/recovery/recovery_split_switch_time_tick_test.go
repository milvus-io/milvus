package recovery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// newStampedSourceFenceMessage is a source fence record as the source handler
// appends it: its own tick, plus the T_switch it reported stamped on the record
// (nil extra = a record carrying none).
func newStampedSourceFenceMessage(t *testing.T, timetick uint64, extra *anypb.Any) message.ImmutableSplitShardMessageV2 {
	t.Helper()
	msg := message.NewSplitShardMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    1,
			SplitTaskId:     100,
			SourceVchannel:  "v1",
			TargetVchannels: []string{"v1-target1", "v1-target2"},
		}).
		WithBody(&message.SplitShardMessageBody{
			Genesis: &msgpb.CreateCollectionRequest{CollectionSchema: &schemapb.CollectionSchema{Name: "col"}},
		}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmedUseMessageID()
	if extra != nil {
		message.SetAppendExtra(msg, extra)
	}
	return message.MustAsImmutableSplitShardMessageV2(msg.IntoImmutableMessage(rmq.NewRmqID(3)))
}

func splitSwitchExtra(t *testing.T, tick uint64) *anypb.Any {
	t.Helper()
	extra, err := anypb.New(&message.SplitShardExtraResponse{SplitTimeTick: tick})
	require.NoError(t, err)
	return extra
}

func newNormalVChannelRecoveryInfo() *vchannelRecoveryInfo {
	return &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel:       "v1",
			State:          streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: 1},
		},
	}
}

// TestObserveSplitShardTakesTSwitchFromTheRecord: the source handler fences in
// memory before it appends, at the first attempt's tick. When that append did
// not persist, the first fence record in the WAL is the re-drive, whose own
// tick is later than the T_switch it reports. SplitTimeTick must be the reported
// one -- DataCoord records it, and a tombstone rebuilt from this meta after a
// restart reports it again -- while CheckpointTimeTick stays the record's own.
func TestObserveSplitShardTakesTSwitchFromTheRecord(t *testing.T) {
	t.Run("re-drive is the first record", func(t *testing.T) {
		info := newNormalVChannelRecoveryInfo()
		info.ObserveSplitShard(newStampedSourceFenceMessage(t, 200, splitSwitchExtra(t, 100)))
		assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, info.meta.State)
		assert.Equal(t, uint64(100), info.meta.SplitTimeTick)
		assert.Equal(t, uint64(200), info.meta.CheckpointTimeTick)
		assert.Equal(t, int64(100), info.meta.SplitTaskId)
	})

	t.Run("first record persisted, then the re-drive", func(t *testing.T) {
		info := newNormalVChannelRecoveryInfo()
		info.ObserveSplitShard(newStampedSourceFenceMessage(t, 100, splitSwitchExtra(t, 100)))
		info.ObserveSplitShard(newStampedSourceFenceMessage(t, 200, splitSwitchExtra(t, 100)))
		assert.Equal(t, uint64(100), info.meta.SplitTimeTick)
		assert.Equal(t, uint64(100), info.meta.CheckpointTimeTick)
	})

	t.Run("a record without a usable extra falls back to its own tick", func(t *testing.T) {
		wrongType, err := anypb.New(&message.ManualFlushExtraResponse{SegmentIds: []int64{1}})
		require.NoError(t, err)
		for name, extra := range map[string]*anypb.Any{
			"none":       nil,
			"zero":       splitSwitchExtra(t, 0),
			"wrong type": wrongType,
		} {
			t.Run(name, func(t *testing.T) {
				info := newNormalVChannelRecoveryInfo()
				info.ObserveSplitShard(newStampedSourceFenceMessage(t, 300, extra))
				assert.Equal(t, uint64(300), info.meta.SplitTimeTick)
			})
		}
	})
}
