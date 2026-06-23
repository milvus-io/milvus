package streaming

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// ErrSourceVChannelFenced is returned by SplitShard when the source vchannel
// has already been fenced by a previous split message. The caller treats this
// as success — the fence holds — and rolls forward: the target vchannels are
// created with a freshly allocated barrier, so no exact T_switch is needed.
var ErrSourceVChannelFenced = errors.New("source vchannel is already fenced by shard split")

// SplitShardParam is the parameter of SplitShard.
type SplitShardParam struct {
	CollectionID   int64
	SourceVChannel string
	// SplitTaskID is the unique split task id allocated by the coordinator,
	// used for idempotency and split task correlation.
	SplitTaskID int64
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
}

// SplitShard executes the write switch of a shard split on the source
// vchannel: it appends a single SplitShard message that fences the source
// vchannel forever. The source StreamingNode's shard handler auto-flushes
// every growing segment of the vchannel as of the message's time tick and
// embeds the sealed segment ids into the message header, so no separate
// ManualFlush is needed. The returned SwitchTimeTick is T_switch.
//
// The call is idempotent: a retry on an already-fenced source vchannel
// returns ErrSourceVChannelFenced together with a result carrying the recorded
// T_switch, so the caller recovers T_switch even after a crash that lost it.
func SplitShard(ctx context.Context, w WALAccesser, param SplitShardParam) (*SplitShardResult, error) {
	if err := param.Validate(); err != nil {
		return nil, err
	}

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
		if streamErr := status.AsStreamingError(err); streamErr.IsShardFenced() {
			// the source is already fenced by a previous split message; the
			// streamingnode carries the recorded T_switch back on the error,
			// so the caller still recovers T_switch even after a crash that
			// lost the persisted value.
			return &SplitShardResult{SwitchTimeTick: streamErr.FencedTimeTick}, errors.Wrapf(ErrSourceVChannelFenced, "%s", err.Error())
		}
		return nil, errors.Wrap(err, "append split shard message failed")
	}

	return &SplitShardResult{
		SwitchTimeTick: splitResult.TimeTick,
	}, nil
}

// InitSplitTargetVChannelsParam is the parameter of InitSplitTargetVChannels.
type InitSplitTargetVChannelsParam struct {
	CollectionID   int64
	DBID           int64
	DBName         string
	CollectionName string
	// Schema is the current schema of the collection; the new vchannels'
	// schema history starts from it.
	Schema *schemapb.CollectionSchema
	// PartitionIDs is the current partition snapshot of the collection.
	// Partitions created concurrently with the initialization must be
	// reconciled by the coordinator afterwards (appending the missed
	// CreatePartition messages is idempotent).
	PartitionIDs []int64
	// SplitTaskID and SourceVChannel record the origin of the new vchannels.
	SplitTaskID    int64
	SourceVChannel string
	// BarrierTimeTick is a freshly allocated time tick (always greater than
	// T_switch). It is carried as the barrier time tick of the CreateVChannel
	// messages, so every message of the new WALs is strictly greater than
	// T_switch even if the hosting node holds an older prefetched TSO batch.
	BarrierTimeTick uint64
	// Targets are the target shards to create, each with its vchannel and the
	// routing key range it owns (embedded into the CreateVChannel header).
	Targets []*message.SplitShardTarget
}

// Validate validates the parameter.
func (p *InitSplitTargetVChannelsParam) Validate() error {
	if p.CollectionID <= 0 {
		return errors.New("collection id must be positive")
	}
	if p.Schema == nil {
		return errors.New("collection schema must be set")
	}
	if len(p.PartitionIDs) == 0 {
		return errors.New("partition snapshot must not be empty")
	}
	if p.SourceVChannel == "" {
		return errors.New("source vchannel must be set")
	}
	if p.BarrierTimeTick == 0 {
		return errors.New("barrier time tick must be set")
	}
	if len(p.Targets) == 0 {
		return errors.New("targets must not be empty")
	}
	vchannels := make(map[string]struct{}, len(p.Targets)+1)
	vchannels[p.SourceVChannel] = struct{}{}
	for _, target := range p.Targets {
		if target.GetVchannel() == "" {
			return errors.New("target vchannel must be set")
		}
		if _, ok := vchannels[target.GetVchannel()]; ok {
			return errors.Errorf("duplicated vchannel %s in split target initialization", target.GetVchannel())
		}
		vchannels[target.GetVchannel()] = struct{}{}
	}
	return nil
}

// InitSplitTargetVChannels creates every target vchannel of a shard split by
// appending a CreateVChannel message — the dedicated genesis message that the
// shard manager, the recovery storage and the flusher handle — carrying the
// collection's current schema and partition snapshot, the target's routing key
// range, and BarrierTimeTick = T_switch (so every message of the new WAL is
// strictly greater than T_switch, and creation doubles as activation).
//
// It returns the marshaled consume start position (the LastConfirmedMessageID
// of each append) per target vchannel; the child delegators consume the new
// WAL from there.
//
// The call is idempotent: every consumer of the CreateVChannel message skips an
// already-known vchannel, so a retry after a partial failure is safe.
func InitSplitTargetVChannels(ctx context.Context, w WALAccesser, param InitSplitTargetVChannelsParam) (map[string]string, error) {
	if err := param.Validate(); err != nil {
		return nil, err
	}
	startPositions := make(map[string]string, len(param.Targets))
	for _, target := range param.Targets {
		vchannel := target.GetVchannel()
		msg, err := message.NewCreateVChannelMessageBuilderV2().
			WithVChannel(vchannel).
			WithHeader(&message.CreateVChannelMessageHeader{
				CollectionId:        param.CollectionID,
				PartitionIds:        param.PartitionIDs,
				DbId:                param.DBID,
				SplitTaskId:         param.SplitTaskID,
				SplitSourceVchannel: param.SourceVChannel,
				KeyRange:            target.GetKeyRange(),
			}).
			WithBody(&message.CreateCollectionRequest{
				DbName:               param.DBName,
				CollectionName:       param.CollectionName,
				DbID:                 param.DBID,
				CollectionID:         param.CollectionID,
				CollectionSchema:     param.Schema,
				VirtualChannelNames:  []string{vchannel},
				PhysicalChannelNames: []string{funcutil.ToPhysicalChannel(vchannel)},
			}).
			BuildMutable()
		if err != nil {
			return nil, errors.Wrapf(err, "build create vchannel message for target %s failed", vchannel)
		}
		result, err := w.RawAppend(ctx, msg, AppendOption{BarrierTimeTick: param.BarrierTimeTick})
		if err != nil {
			return nil, errors.Wrapf(err, "create split target vchannel %s failed", vchannel)
		}
		startPositions[vchannel] = result.LastConfirmedMessageID.Marshal()
	}
	return startPositions, nil
}
