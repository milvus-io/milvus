package streaming

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// ErrSourceVChannelFenced is returned by SplitShard when the source vchannel
// has already been fenced by a previous split message. The caller should
// treat the split as already switched and recover T_switch from its own
// persisted state.
var ErrSourceVChannelFenced = errors.New("source vchannel is already fenced by shard split")

// SplitShardParam is the parameter of SplitShard.
type SplitShardParam struct {
	CollectionID   int64
	SourceVChannel string
	// SplitTaskID is the unique split task id allocated by the coordinator,
	// used for idempotency and split task correlation.
	SplitTaskID int64
	// FlushTs is a timestamp allocated from the TSO by the caller; the
	// ManualFlush message is appended with it as the barrier time tick.
	FlushTs uint64
	// Targets are the target shards the source shard splits into. Their key
	// ranges must be disjoint and exactly cover the source shard's range,
	// which is guaranteed by the coordinator.
	Targets []*message.SplitShardTarget
}

// Validate validates the parameter.
func (p *SplitShardParam) Validate() error {
	if p.CollectionID <= 0 {
		return errors.New("collection id must be positive")
	}
	if p.SourceVChannel == "" {
		return errors.New("source vchannel must be set")
	}
	if len(p.Targets) < 2 {
		return errors.New("shard split requires at least two targets")
	}
	vchannels := make(map[string]struct{}, len(p.Targets)+1)
	vchannels[p.SourceVChannel] = struct{}{}
	for _, target := range p.Targets {
		if target.GetVchannel() == "" {
			return errors.New("target vchannel must be set")
		}
		if _, ok := vchannels[target.GetVchannel()]; ok {
			return errors.Errorf("duplicated vchannel %s in shard split", target.GetVchannel())
		}
		vchannels[target.GetVchannel()] = struct{}{}
	}
	return nil
}

// SplitShardResult is the result of SplitShard.
type SplitShardResult struct {
	// SwitchTimeTick is T_switch: the time tick of the SplitShard message.
	// The source vchannel holds only messages <= T_switch, and every message
	// of the target vchannels is strictly greater than it.
	SwitchTimeTick uint64
	// FlushedSegmentIDs are the growing segments sealed by the ManualFlush
	// message written right before the split message.
	FlushedSegmentIDs []int64
}

// SplitShard executes the write switch of a shard split on the source
// vchannel: it appends a ManualFlush message sealing every growing segment,
// then appends the SplitShard message that fences the source vchannel
// forever. The returned SwitchTimeTick is T_switch.
//
// The call is idempotent: a retry on an already-fenced source vchannel
// returns ErrSourceVChannelFenced (the ManualFlush retry is a harmless
// no-op), and the caller recovers T_switch from its persisted task state.
func SplitShard(ctx context.Context, w WALAccesser, param SplitShardParam) (*SplitShardResult, error) {
	if err := param.Validate(); err != nil {
		return nil, err
	}

	// 1. Seal every growing segment of the source vchannel. The barrier
	// time tick guarantees the flush time tick is not smaller than FlushTs.
	flushMsg, err := message.NewManualFlushMessageBuilderV2().
		WithVChannel(param.SourceVChannel).
		WithHeader(&message.ManualFlushMessageHeader{
			CollectionId: param.CollectionID,
			FlushTs:      param.FlushTs,
		}).
		WithBody(&message.ManualFlushMessageBody{}).
		BuildMutable()
	if err != nil {
		return nil, errors.Wrap(err, "build manual flush message failed")
	}
	flushResult, err := w.RawAppend(ctx, flushMsg, AppendOption{BarrierTimeTick: param.FlushTs})
	if err != nil {
		return nil, errors.Wrap(err, "append manual flush message failed")
	}
	var flushExtra message.ManualFlushExtraResponse
	if err := flushResult.GetExtra(&flushExtra); err != nil {
		return nil, errors.Wrap(err, "get extra from manual flush append result failed")
	}

	// 2. Append the SplitShard message: the write fence of the source
	// vchannel. Its time tick is T_switch.
	splitMsg, err := message.NewSplitShardMessageBuilderV2().
		WithVChannel(param.SourceVChannel).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId: param.CollectionID,
			SplitTaskId:  param.SplitTaskID,
			Targets:      param.Targets,
		}).
		WithBody(&message.SplitShardMessageBody{}).
		BuildMutable()
	if err != nil {
		return nil, errors.Wrap(err, "build split shard message failed")
	}
	splitResult, err := w.RawAppend(ctx, splitMsg)
	if err != nil {
		if status.AsStreamingError(err).IsShardFenced() {
			return nil, errors.Wrapf(ErrSourceVChannelFenced, "%s", err.Error())
		}
		return nil, errors.Wrap(err, "append split shard message failed")
	}

	return &SplitShardResult{
		SwitchTimeTick:    splitResult.TimeTick,
		FlushedSegmentIDs: flushExtra.GetSegmentIds(),
	}, nil
}
