package streaming

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ErrSourceVChannelFenced is returned by SplitShard when the source vchannel
// has already been fenced by a previous split message. The caller treats this
// as success -- the fence holds -- and rolls forward.
//
// The result returned alongside it carries the recorded T_switch, read back off
// the error (see SplitShard). The BARRIER does not need it: it is freshly
// allocated and is only ever a lower bound. The redistribution DRAIN does --
// it gates on channelCheckpoint(source) >= T_switch -- so a caller that lost
// the persisted value recovers it here rather than proceeding with zero.
var ErrSourceVChannelFenced = errors.New("source vchannel is already fenced by shard split")

// SplitShardParam is the parameter of SplitShard.
type SplitShardParam struct {
	CollectionID   int64
	SourceVChannel string
	// SplitTaskID is the unique split task id allocated by the coordinator,
	// used for idempotency and split task correlation.
	SplitTaskID int64
	// Targets are the target shards the source shard splits into. Their
	// residues must be disjoint and exactly cover the source shard's residues,
	// which is guaranteed by the coordinator.
	Targets []*message.SplitShardTarget
	// RoutingModulus is the collection's routing modulus the targets' residues
	// are taken against. Recorded in the fence message because the record is
	// permanent while the collection's modulus moves.
	RoutingModulus uint64
}

// Validate validates the parameter.
func (p *SplitShardParam) Validate() error {
	if p.CollectionID <= 0 {
		return merr.WrapErrParameterInvalidMsg("collection id must be positive, got %d", p.CollectionID)
	}
	if p.SourceVChannel == "" {
		return merr.WrapErrParameterMissingMsg("source vchannel must be set")
	}
	if p.RoutingModulus == 0 {
		return merr.WrapErrParameterMissingMsg("routing modulus must be set")
	}
	// No lower bound on the target count. Targets here are the shards THIS source
	// must front during the split window, not the shards the split produces: a
	// source of a rehash fronts only its share of them, which may be one or, when
	// the collection shrinks, none at all. "A split produces at least two shards"
	// is a property of the task and is checked where the task is prepared; a
	// fence message cannot see the other sources' shares to check it here.
	vchannels := make(map[string]struct{}, len(p.Targets)+1)
	vchannels[p.SourceVChannel] = struct{}{}
	for _, target := range p.Targets {
		if target.GetVchannel() == "" {
			return merr.WrapErrParameterMissingMsg("target vchannel must be set")
		}
		if _, ok := vchannels[target.GetVchannel()]; ok {
			return merr.WrapErrParameterInvalidMsg("duplicated vchannel %s in shard split", target.GetVchannel())
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
			CollectionId:   param.CollectionID,
			SplitTaskId:    param.SplitTaskID,
			Targets:        param.Targets,
			RoutingModulus: param.RoutingModulus,
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
	// SplitTaskID and SourceVChannels record the origin of the new vchannels.
	// More than one source when the collection is rehashed to an arbitrary shard
	// count: every target is then carved out of every source at once.
	SplitTaskID     int64
	SourceVChannels []string
	// BarrierTimeTick is the barrier the CreateVChannel appends are held
	// behind: the hosting streamingnode blocks each one until its TSO has
	// passed this tick, so every message of the new WALs is strictly greater
	// than it even if that node holds an older prefetched TSO batch.
	//
	// The caller sets it to T_switch (the largest one, when the targets are
	// carved out of several sources). Any value >= T_switch is correct; a
	// larger one only makes the targets wait longer to be born.
	BarrierTimeTick uint64
	// Targets are the target shards to create, each with its vchannel and the
	// residues it owns (embedded into the CreateVChannel header).
	Targets []*message.SplitShardTarget
	// RoutingModulus is the collection's routing modulus the targets' residues
	// are taken against, recorded alongside them for the same reason.
	RoutingModulus uint64
}

// Validate validates the parameter.
func (p *InitSplitTargetVChannelsParam) Validate() error {
	if p.CollectionID <= 0 {
		return merr.WrapErrParameterInvalidMsg("collection id must be positive, got %d", p.CollectionID)
	}
	if p.Schema == nil {
		return merr.WrapErrParameterMissingMsg("collection schema must be set")
	}
	if len(p.PartitionIDs) == 0 {
		return merr.WrapErrParameterMissingMsg("partition snapshot must not be empty")
	}
	if len(p.SourceVChannels) == 0 {
		return merr.WrapErrParameterMissingMsg("source vchannels must be set")
	}
	if p.BarrierTimeTick == 0 {
		return merr.WrapErrParameterMissingMsg("barrier time tick must be set")
	}
	if len(p.Targets) == 0 {
		return merr.WrapErrParameterMissingMsg("targets must not be empty")
	}
	if p.RoutingModulus == 0 {
		return merr.WrapErrParameterMissingMsg("routing modulus must be set")
	}
	vchannels := make(map[string]struct{}, len(p.Targets)+len(p.SourceVChannels))
	for _, source := range p.SourceVChannels {
		if source == "" {
			return merr.WrapErrParameterMissingMsg("source vchannel must be set")
		}
		vchannels[source] = struct{}{}
	}
	for _, target := range p.Targets {
		if target.GetVchannel() == "" {
			return merr.WrapErrParameterMissingMsg("target vchannel must be set")
		}
		if _, ok := vchannels[target.GetVchannel()]; ok {
			return merr.WrapErrParameterInvalidMsg("duplicated vchannel %s in split target initialization", target.GetVchannel())
		}
		vchannels[target.GetVchannel()] = struct{}{}
	}
	return nil
}

// InitSplitTargetVChannels creates every target vchannel of a shard split by
// appending one CreateVChannel message per target — the dedicated genesis
// message that the shard manager, the recovery storage and the flusher handle —
// carrying the collection's current schema and partition snapshot, the target's
// routing residues, and BarrierTimeTick = T_switch (so every message of the new
// WAL is strictly greater than T_switch, and creation doubles as activation).
// It returns the WAL position each target vchannel was born at.
//
// The call is idempotent: every consumer of the CreateVChannel message skips an
// already-known vchannel, so a retry after a partial failure is safe. A retry
// does report the RE-appended genesis position rather than the original one;
// that is still a position from which every message the vchannel carries is
// readable, because a target takes no write until the coordinator commits the
// routing, which happens after this call returns.
//
// The positions are not a convenience. A vchannel that exists but has never
// been written to has no checkpoint, and datacoord's seek-position fallback
// then reaches for the earliest segment's DML position — which on a target
// holding only rewrite output carries a timestamp but neither a message ID nor
// a WAL name, because a rewritten segment is produced by compaction rather than
// consumed from the WAL. A dispatcher built on that position skips its Seek and
// panics the querynode the first time it reads. Recording the genesis position
// as the channel's first checkpoint means the fallback never has to guess: the
// CreateVChannel message id is the one position from which every message the
// vchannel will ever carry is readable, since the vchannel begins with it.
func InitSplitTargetVChannels(ctx context.Context, w WALAccesser, param InitSplitTargetVChannelsParam) ([]*msgpb.MsgPosition, error) {
	if err := param.Validate(); err != nil {
		return nil, err
	}
	genesis := make([]*msgpb.MsgPosition, 0, len(param.Targets))
	for _, target := range param.Targets {
		vchannel := target.GetVchannel()
		msg, err := message.NewCreateVChannelMessageBuilderV2().
			WithVChannel(vchannel).
			WithHeader(&message.CreateVChannelMessageHeader{
				CollectionId:         param.CollectionID,
				PartitionIds:         param.PartitionIDs,
				DbId:                 param.DBID,
				SplitTaskId:          param.SplitTaskID,
				SplitSourceVchannels: param.SourceVChannels,
				Routing:              target.GetRouting(),
				RoutingModulus:       param.RoutingModulus,
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
		genesis = append(genesis, splitTargetGenesisPosition(vchannel, result))
	}
	return genesis, nil
}

// splitTargetGenesisPosition turns the CreateVChannel append result into the
// target vchannel's first checkpoint.
//
// The message id is used rather than the last-confirmed one, for the reason
// collection creation uses it (ddl_callbacks_create_collection.go): a zero
// last-confirmed id serializes to nil under WoodPecker and downstream
// assertions panic on a nil position, while the message id is just as complete
// here — the vchannel is created BY this message, so nothing precedes it.
func splitTargetGenesisPosition(vchannel string, result *types.AppendResult) *msgpb.MsgPosition {
	return &msgpb.MsgPosition{
		ChannelName: vchannel,
		MsgID:       adaptor.MustGetMQWrapperIDFromMessage(result.MessageID).Serialize(),
		// Carried explicitly: the delegator's Seek deserializes the id with
		// MustGetMessageIDFromMQWrapperIDBytesWithWALName, so a position whose
		// WAL name is Unknown panics there instead of failing.
		WALName:   commonpb.WALName(result.MessageID.WALName()),
		Timestamp: result.TimeTick,
	}
}

// DropSplitVChannelParam is the parameter of DropSplitVChannel.
type DropSplitVChannelParam struct {
	CollectionID int64
	DBID         int64
	// VChannel is the retired split source being reclaimed.
	VChannel string
	// SplitTaskID correlates the teardown with the split that retired it.
	SplitTaskID int64
}

// Validate validates the parameter.
func (p *DropSplitVChannelParam) Validate() error {
	if p.CollectionID <= 0 {
		return merr.WrapErrParameterInvalidMsg("collection id must be positive, got %d", p.CollectionID)
	}
	if p.VChannel == "" {
		return merr.WrapErrParameterMissingMsg("vchannel must be set")
	}
	return nil
}

// DropSplitVChannel retires one vchannel a shard split left behind: the
// streamingnode tears down its shard-manager entry, recovery info and flusher.
//
// It must be appended BEFORE the coordinator drops the vchannel from the
// collection, and that order is not cosmetic. DropCollection is broadcast to
// exactly collection.VirtualChannelNames, so a vchannel removed from that list
// first would never receive another teardown message of any kind — its
// streamingnode state would outlive the collection itself, with no code path
// left to clean it.
//
// PRECONDITION, enforced by the caller: the source must be DRAINED, i.e.
// channelCheckpoint(source) >= T_switch. The streamingnode side only checks that
// the vchannel is SPLITTED; the teardown then closes the data sync service with
// drop=false, which discards the write buffer WITHOUT syncing it, and the
// coordinator's DropVirtualChannel marks any residual segment dropped. Appending
// this while the source buffer still holds fence-sealed data therefore loses
// writes from before T_switch, silently and with a successful append. The drain
// gate that establishes the precondition lives in the split task, not here.
func DropSplitVChannel(ctx context.Context, w WALAccesser, param DropSplitVChannelParam) error {
	if err := param.Validate(); err != nil {
		return err
	}
	msg, err := message.NewDropVChannelMessageBuilderV2().
		WithVChannel(param.VChannel).
		WithHeader(&message.DropVChannelMessageHeader{
			CollectionId: param.CollectionID,
			DbId:         param.DBID,
			SplitTaskId:  param.SplitTaskID,
		}).
		WithBody(&message.DropVChannelMessageBody{}).
		BuildMutable()
	if err != nil {
		return errors.Wrapf(err, "build drop vchannel message for %s failed", param.VChannel)
	}
	if _, err := w.RawAppend(ctx, msg); err != nil {
		return errors.Wrapf(err, "drop split vchannel %s failed", param.VChannel)
	}
	return nil
}
