package streaming

import (
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The SplitShard broadcast built here REPLICATES. A secondary cluster must end
// up with the same shard topology as the primary, so the split travels down the
// replicate stream like any other DDL rather than being withheld from it.
//
// Two mechanisms on the receiving side make that safe, and neither belongs in
// this file -- both live where a replica is received, replicate_service.go:
//
//  1. Name remap. Every channel name this message carries -- the sources and
//     targets in the header, the routing post-image and the genesis in the body,
//     and the broadcast header's append_first_vchannels -- names a channel of
//     the PRIMARY. The secondary rewrites all of them into its own namespace
//     before the replica is appended. The message-body names are rewritten by
//     replicate_service.go's per-message-type table; append_first_vchannels is
//     not in that table at all -- it is rewritten generically, for every
//     broadcast message type, by messageImpl.OverwriteReplicateVChannel, which
//     maps it through the very vchannel mapping the replicate service supplies
//     for the broadcast header (and refuses a header whose append-first list is
//     not a subset of that list).
//  2. The append gate. The primary's ordering (sources appended and persisted
//     first) is produced by the broadcaster and is not carried by the replicate
//     streams, which deliver each pchannel independently. The secondary
//     reproduces it by holding every non-append-first replica until the
//     append-first ones have landed there.
//
// The names are what makes this message replicable at all; the order is what
// makes it correct. Losing either would corrupt the secondary silently rather
// than loudly, which is why the two are described here and not only there.

// SplitShardParam is the parameter of NewSplitShardBroadcastMessage.
//
// It merges what used to be three separate appends -- the source fence, the
// targets' genesis, and their routing commit -- into the one broadcast this
// package now builds: the source vchannels are appended first (so the fence
// lands before any target can be observed), the targets' genesis travels in
// the body alongside the routing post-image the ack callback commits.
type SplitShardParam struct {
	CollectionID int64
	DBID         int64
	// SplitTaskID is the unique split task id allocated by the coordinator,
	// used for idempotency and split task correlation.
	SplitTaskID int64
	// SourceVChannels are the vchannels this split fences. One for a split or a
	// doubling, every shard of the collection for a rehash. They are appended
	// (and therefore persisted) before any other replica of the broadcast.
	SourceVChannels []string
	// CollectionVChannels is the collection's current vchannel list -- every
	// shard it has BEFORE this split, sources included. The broadcast reaches
	// every one of them (not only the sources, the targets and the control
	// channel), so that a shard the split does not act on -- a bystander --
	// still observes it: every source must be one of these, and no target may
	// already be one, since a target is a NEW shard this split creates.
	CollectionVChannels []string
	// Targets are the target shards the sources split into, each with its
	// vchannel and the residues it owns. Their residues must be disjoint and
	// exactly cover the sources' residues, which is guaranteed by the
	// coordinator.
	Targets []*message.SplitShardTarget
	// RoutingModulus is the collection's routing modulus the targets' residues
	// are taken against.
	RoutingModulus uint64
	// Schema is the current schema of the collection; the targets' genesis
	// (and their schema history) starts from it.
	Schema *schemapb.CollectionSchema
	// PartitionIDs is the current partition snapshot of the collection,
	// registered on every target's genesis.
	PartitionIDs []int64
	// Routing is the routing post-image the ack callback commits to the
	// collection meta once every replica has landed: the grown vchannel list,
	// every shard's state and residues, the modulus and shard_by. Carried
	// whole so a replay never depends on mutable meta.
	Routing *message.AlterCollectionMessageUpdates
	// ControlChannel is the collection's control channel; it receives the
	// broadcast like any other vchannel but is never an append-first target.
	ControlChannel string
}

// Validate validates the parameter.
//
// Every failure here is reported as a System error (WrapErrServiceInternalMsg),
// never as a caller mistake: the only caller of this package is DataCoord's own
// FSM, so a malformed param is a Milvus bug, not user input (the blame test in
// docs/dev/error_handling_guide.md never puts fault on request content that
// doesn't exist at this layer).
func (p *SplitShardParam) Validate() error {
	if p.CollectionID <= 0 {
		return merr.WrapErrServiceInternalMsg("collection id must be positive, got %d", p.CollectionID)
	}
	if p.SplitTaskID <= 0 {
		return merr.WrapErrServiceInternalMsg("split task id must be positive, got %d", p.SplitTaskID)
	}
	if p.DBID <= 0 {
		return merr.WrapErrServiceInternalMsg("db id must be positive, got %d", p.DBID)
	}
	if len(p.SourceVChannels) == 0 {
		return merr.WrapErrServiceInternalMsg("source vchannels must be set")
	}
	// A target registered with no partition accepts no insert: every insert
	// resolves its target partition from this list, so an empty one silently
	// makes the target unwritable rather than failing loudly here.
	if len(p.PartitionIDs) == 0 {
		return merr.WrapErrServiceInternalMsg("partition ids must be set")
	}
	if p.RoutingModulus == 0 {
		return merr.WrapErrServiceInternalMsg("routing modulus must be set")
	}
	if p.Schema == nil {
		return merr.WrapErrServiceInternalMsg("collection schema must be set")
	}
	if p.ControlChannel == "" {
		return merr.WrapErrServiceInternalMsg("control channel must be set")
	}
	if p.Routing == nil {
		return merr.WrapErrServiceInternalMsg("routing post-image must be set")
	}

	// No lower bound on the target count. Targets here are the shards THIS set
	// of sources must front during the split window, not the shards the split
	// produces: a source of a rehash fronts only its share of them, which may
	// be one or, when the collection shrinks, none at all. "A split produces at
	// least two shards" is a property of the task and is checked where the
	// task is prepared; this parameter cannot see the other sources' shares to
	// check it here.
	collectionVChannels := make(map[string]struct{}, len(p.CollectionVChannels))
	for _, vchannel := range p.CollectionVChannels {
		collectionVChannels[vchannel] = struct{}{}
	}

	vchannels := make(map[string]struct{}, len(p.SourceVChannels)+len(p.Targets))
	for _, source := range p.SourceVChannels {
		if source == "" {
			return merr.WrapErrServiceInternalMsg("source vchannel must be set")
		}
		if _, ok := vchannels[source]; ok {
			return merr.WrapErrServiceInternalMsg("duplicated source vchannel %s in shard split", source)
		}
		vchannels[source] = struct{}{}
		// A source is fenced by this split, so it must be a real, pre-existing
		// shard of the collection -- one the coordinator listed as such --
		// never a vchannel this split invents for the occasion.
		if _, ok := collectionVChannels[source]; !ok {
			return merr.WrapErrServiceInternalMsg("source vchannel %s must be one of the collection's current vchannels", source)
		}
	}
	routingTargets := make(map[string]struct{}, len(p.Routing.GetVirtualChannelNames()))
	for _, vchannel := range p.Routing.GetVirtualChannelNames() {
		routingTargets[vchannel] = struct{}{}
	}
	for _, target := range p.Targets {
		vchannel := target.GetVchannel()
		if vchannel == "" {
			return merr.WrapErrServiceInternalMsg("target vchannel must be set")
		}
		if _, ok := vchannels[vchannel]; ok {
			return merr.WrapErrServiceInternalMsg("duplicated vchannel %s in shard split", vchannel)
		}
		vchannels[vchannel] = struct{}{}
		// A target is a NEW shard this split creates, so it must not already
		// be one of the collection's current vchannels -- that would make the
		// broadcast overwrite a live shard's genesis with another one's.
		if _, ok := collectionVChannels[vchannel]; ok {
			return merr.WrapErrServiceInternalMsg("target vchannel %s must not already be one of the collection's current vchannels", vchannel)
		}
		// The message is a PERMANENT record -- it is what a replay derives the
		// window's fronting assignment from -- so a target without residues, or
		// with one that is not below the modulus they are taken against, is
		// refused here rather than written and puzzled over later.
		if len(target.GetRouting().GetBuckets()) == 0 {
			return merr.WrapErrServiceInternalMsg("target %s carries no residue", vchannel)
		}
		for _, residue := range target.GetRouting().GetBuckets() {
			if residue >= p.RoutingModulus {
				return merr.WrapErrServiceInternalMsg(
					"target %s owns residue %d, which is not below the routing modulus %d",
					vchannel, residue, p.RoutingModulus)
			}
		}
		if _, ok := routingTargets[vchannel]; !ok {
			return merr.WrapErrServiceInternalMsg("routing post-image is missing target vchannel %s", vchannel)
		}
	}
	return nil
}

// SplitShardResult is the decoded result of the SplitShard broadcast.
type SplitShardResult struct {
	// SwitchTimeTicks is T_switch, keyed by source vchannel: the time tick of
	// the SplitShard message on that replica. The source vchannel holds only
	// messages <= T_switch, and every message of the target vchannels is
	// strictly greater than it.
	SwitchTimeTicks map[string]uint64
	// GenesisPositions is the target vchannels' first checkpoint, one per
	// target, in the order of SplitShardParam.Targets: the position the
	// flusher/delegator start from, since the vchannel begins with this
	// message.
	GenesisPositions []*msgpb.MsgPosition
}

// NewSplitShardBroadcastMessage builds the SplitShard broadcast: the source
// vchannels are named append-first (so the fence lands before any other
// replica is observed), the targets' genesis and the routing post-image travel
// in the body, and the whole thing is deduplicated by a collection-scoped
// idempotency key derived from the split task id -- a retry of the same task
// against the same collection is therefore the SAME broadcast, not a new one.
//
// The message is replicable; see the top of this file for what the secondary
// does with it.
func NewSplitShardBroadcastMessage(param SplitShardParam) (message.BroadcastMutableMessage, error) {
	if err := param.Validate(); err != nil {
		return nil, err
	}
	// The broadcast reaches every vchannel of the collection, so that a
	// bystander shard -- one this split neither fences nor creates -- still
	// observes it: the union of CollectionVChannels, SourceVChannels, the
	// targets and the control channel, deduplicated.
	seen := make(map[string]struct{}, len(param.CollectionVChannels)+len(param.SourceVChannels)+len(param.Targets)+1)
	vchannels := make([]string, 0, len(param.CollectionVChannels)+len(param.Targets)+1)
	addVChannel := func(vchannel string) {
		if _, ok := seen[vchannel]; ok {
			return
		}
		seen[vchannel] = struct{}{}
		vchannels = append(vchannels, vchannel)
	}
	for _, vchannel := range param.CollectionVChannels {
		addVChannel(vchannel)
	}
	for _, source := range param.SourceVChannels {
		addVChannel(source)
	}
	for _, target := range param.Targets {
		addVChannel(target.GetVchannel())
	}
	addVChannel(param.ControlChannel)

	msg, err := message.NewSplitShardMessageBuilderV2().
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    param.CollectionID,
			SplitTaskId:     param.SplitTaskID,
			Targets:         param.Targets,
			RoutingModulus:  param.RoutingModulus,
			SourceVchannels: param.SourceVChannels,
			PartitionIds:    param.PartitionIDs,
			DbId:            param.DBID,
		}).
		WithBody(&message.SplitShardMessageBody{
			Genesis: &msgpb.CreateCollectionRequest{CollectionSchema: param.Schema},
			Routing: param.Routing,
		}).
		WithBroadcast(vchannels, message.OptBuildBroadcastAppendFirst(param.SourceVChannels...)).
		WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(param.CollectionID, fmt.Sprintf("shard-split-%d", param.SplitTaskID))).
		BuildBroadcast()
	if err != nil {
		return nil, errors.Wrap(err, "build split shard broadcast message failed")
	}
	return msg, nil
}

// SplitShardResultFrom decodes a SplitShard broadcast's append result into
// each source's T_switch and each target's genesis position.
//
// A result missing a source or a target is a Milvus-internal bug -- the
// broadcaster acks a broadcast only once every one of its vchannels has been
// appended -- so it is reported as a System error (WrapErrServiceInternalMsg),
// never as a caller mistake, and is not retriable: retrying the same broadcast
// append result decode cannot make a missing entry appear.
func SplitShardResultFrom(param SplitShardParam, result *types.BroadcastAppendResult) (*SplitShardResult, error) {
	switchTimeTicks := make(map[string]uint64, len(param.SourceVChannels))
	for _, source := range param.SourceVChannels {
		appendResult := result.GetAppendResult(source)
		if appendResult == nil {
			return nil, merr.WrapErrServiceInternalMsg(
				"split shard broadcast result is missing source vchannel %s", source)
		}
		switchTimeTicks[source] = appendResult.TimeTick
	}
	genesisPositions := make([]*msgpb.MsgPosition, 0, len(param.Targets))
	for _, target := range param.Targets {
		vchannel := target.GetVchannel()
		appendResult := result.GetAppendResult(vchannel)
		if appendResult == nil {
			return nil, merr.WrapErrServiceInternalMsg(
				"split shard broadcast result is missing target vchannel %s", vchannel)
		}
		genesisPositions = append(genesisPositions, splitTargetGenesisPosition(vchannel, appendResult))
	}
	return &SplitShardResult{
		SwitchTimeTicks:  switchTimeTicks,
		GenesisPositions: genesisPositions,
	}, nil
}

// SplitTargetGenesisPosition turns a target vchannel's landed SplitShard
// replica into that vchannel's first checkpoint.
//
// The message id is used rather than the last-confirmed one, for the reason
// collection creation uses it (ddl_callbacks_create_collection.go): a zero
// last-confirmed id serializes to nil under WoodPecker and downstream
// assertions panic on a nil position, while the message id is just as complete
// here -- the vchannel is created BY this message, so nothing precedes it.
//
// Exported because the recipe has two callers on two different result types:
// the append-time decode below (types.AppendResult) and rootcoord's SplitShard
// ack callback (message.AppendResult), which seeds the same positions at
// datacoord. A second copy of the recipe is exactly what must not exist -- a
// position that disagrees with the one already seeded would rewind or skip the
// target's readers.
func SplitTargetGenesisPosition(vchannel string, messageID message.MessageID, timeTick uint64) *msgpb.MsgPosition {
	return &msgpb.MsgPosition{
		ChannelName: vchannel,
		MsgID:       adaptor.MustGetMQWrapperIDFromMessage(messageID).Serialize(),
		// Carried explicitly: the delegator's Seek deserializes the id with
		// MustGetMessageIDFromMQWrapperIDBytesWithWALName, so a position whose
		// WAL name is Unknown panics there instead of failing.
		WALName:   commonpb.WALName(messageID.WALName()),
		Timestamp: timeTick,
	}
}

// splitTargetGenesisPosition adapts an append result to the recipe above.
func splitTargetGenesisPosition(vchannel string, result *types.AppendResult) *msgpb.MsgPosition {
	return SplitTargetGenesisPosition(vchannel, result.MessageID, result.TimeTick)
}
