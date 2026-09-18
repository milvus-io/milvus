package streaming

import (
	"fmt"
	"slices"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The SplitShard broadcast built here REPLICATES. A secondary cluster must end
// up with the same shard topology as the primary, so the split travels down the
// replicate stream like any other DDL rather than being withheld from it.
//
// Two mechanisms on the receiving side make that safe, and neither belongs in
// this file -- both live where a replica is received, replicate_service.go:
//
//  1. Name remap. Every channel name this message carries -- the source and
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
//  2. The append gate. The primary's ordering (source appended and persisted
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
// package now builds: the source vchannel is appended first (so the fence
// lands before any target can be observed), the targets' genesis travels in
// the body alongside the routing post-image the ack callback commits.
type SplitShardParam struct {
	CollectionID int64
	// SplitTaskID is the unique split task id allocated by the coordinator,
	// used for idempotency and split task correlation.
	SplitTaskID int64
	// SourceVChannel names the one vchannel this split fences. It is appended
	// (and therefore persisted) before any other replica of the broadcast.
	SourceVChannel string
	// TargetVChannels names the two vchannels the source splits into. The
	// residues each one owns are not here: the routing post-image carries them,
	// and it is their only copy.
	TargetVChannels []string
	// Schema is the current schema of the collection; the targets' genesis
	// (and their schema history) starts from it. Its Properties must carry the
	// collection's properties: the namespace admission check (§3.1) reads
	// namespace.sharding.enabled and namespace.mode from them, both before the
	// broadcast and in the ack callback. Its EnableNamespace must be the
	// collection's too: a namespace collection is refused (§1.3).
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
	// A target registered with no partition accepts no insert: every insert
	// resolves its target partition from this list, so an empty one silently
	// makes the target unwritable rather than failing loudly here.
	if len(p.PartitionIDs) == 0 {
		return merr.WrapErrServiceInternalMsg("partition ids must be set")
	}
	if p.Schema == nil {
		return merr.WrapErrServiceInternalMsg("collection schema must be set")
	}
	if p.ControlChannel == "" {
		return merr.WrapErrServiceInternalMsg("control channel must be set")
	}
	// The control channel is found in the broadcast result BY NAME
	// (BroadcastResult.GetControlChannelResult keys on funcutil.IsControlChannel),
	// so a plain vchannel passed here is not "a control channel under another
	// name": the broadcast simply has no CChannel replica. The ack callback then
	// finds no control-channel result to take its commit tick from, and the
	// replica that was meant to be the CChannel lands on a StreamingNode as a
	// SplitShard of a vchannel that is neither source nor target -- refused as
	// unrecoverable, and retried forever by the broadcaster with the collection
	// key held. Refused here, before anything is fenced.
	if !funcutil.IsControlChannel(p.ControlChannel) {
		return merr.WrapErrServiceInternalMsg("control channel %s is not a control channel", p.ControlChannel)
	}
	if p.Routing == nil {
		return merr.WrapErrServiceInternalMsg("routing post-image must be set")
	}
	// Every self-consistency check of the message, run on the very header and
	// body this param will put on the wire. The ack callback runs the SAME
	// function on the message once every replica has landed -- but by then the
	// source is fenced, so a message it refuses can only be retried, never
	// withdrawn. Running it here is what turns that wedge into a refused build.
	if err := ValidateSplitShardMessage(p.header(), p.body()); err != nil {
		return err
	}

	// Target PLACEMENT, checked here because it can only be refused cheaply
	// here. A shard manager holds ONE entry per collection per pchannel, so a
	// target placed on a pchannel the collection still occupies is refused by
	// the target replica's own handler (ErrVChannelConflict, unrecoverable) --
	// but that happens AFTER the source replica has landed and been persisted
	// by AckPartial. The broadcaster then retries that append forever, holding
	// the collection's exclusive resource key, with the source fenced and the
	// target never created: the residues this split moved are permanently
	// unwritable and every later DDL of the collection queues behind it. There
	// is no rollback from there, so the placement is refused before the fence
	// or never.
	//
	// The SOURCE's pchannel is free for a target to take: the fence removes the
	// source's collection entry in the same critical section that marks it
	// SPLITTED, which is what lets a shard split back onto its own pchannel.
	// Every OTHER vchannel of the collection -- every post-image shard that is
	// neither the source nor a target -- keeps its entry for the whole split
	// window, and so does each target once registered.
	occupiedPChannels := make(map[string]string, len(p.Routing.GetVirtualChannelNames()))
	for _, vchannel := range p.Routing.GetVirtualChannelNames() {
		if vchannel == p.SourceVChannel || slices.Contains(p.TargetVChannels, vchannel) {
			continue
		}
		occupiedPChannels[funcutil.ToPhysicalChannel(vchannel)] = vchannel
	}
	for _, vchannel := range p.TargetVChannels {
		pchannel := funcutil.ToPhysicalChannel(vchannel)
		if incumbent, ok := occupiedPChannels[pchannel]; ok {
			return merr.WrapErrServiceInternalMsg(
				"target vchannel %s and vchannel %s of the same collection are both placed on pchannel %s, "+
					"but a collection holds at most one vchannel per pchannel",
				vchannel, incumbent, pchannel)
		}
		occupiedPChannels[pchannel] = vchannel
	}
	return nil
}

// splitShardTargets is the only target count a shard split has: one shard
// fenced, two shards created whose residues tile the source's. Rehash, shrink
// and a split that creates no target are not shard splits and are refused, not
// special-cased.
const splitShardTargets = 2

// header is the SplitShard message header this param produces. Validate and
// the build below share it, so the header checked before the broadcast is the
// one the ack callback re-checks after it.
func (p *SplitShardParam) header() *message.SplitShardMessageHeader {
	return &message.SplitShardMessageHeader{
		CollectionId:    p.CollectionID,
		SplitTaskId:     p.SplitTaskID,
		SourceVchannel:  p.SourceVChannel,
		TargetVchannels: p.TargetVChannels,
		PartitionIds:    p.PartitionIDs,
	}
}

// body is the SplitShard message body this param produces, shared by Validate
// and the build for the same reason as header.
func (p *SplitShardParam) body() *message.SplitShardMessageBody {
	return &message.SplitShardMessageBody{
		Genesis: &msgpb.CreateCollectionRequest{CollectionSchema: p.Schema},
		Routing: p.Routing,
	}
}

// ValidateSplitShardMessage runs every check on a SplitShard message that the
// message alone can answer -- every refusal the ack callback and the routing
// apply could otherwise make only after the source is fenced, except the ones
// that need the collection's current meta:
//
//   - names: one source and two distinct targets, none of them a control
//     channel, all named by the post-image;
//   - shape (routing.CheckPostImageShape): the post-image's arrays are
//     parallel and non-empty and no vchannel is listed twice;
//   - modulus and tiling (routing.CheckPostImageTiling): an explicit, capped
//     modulus at which the writable shards tile the key space without gap or
//     overlap, each target owning at least one residue. A gap silently drops
//     the writes of the residues nobody claims; an overlap sends one key to
//     two shards;
//   - states: the source is Splitting (it stays listed, fenced, until adoption),
//     each target is Creating (the only state a new vchannel may arrive in), and
//     no listed shard is Dropped (Dropped is reached only by delisting);
//   - namespace admission (§3.1): a hash($namespace_id) post-image only for a
//     collection whose genesis properties say its rows were placed by namespace,
//     at a modulus that divides its partition-key buckets (the header's
//     partition snapshot), so the split relabels whole buckets;
//   - namespace deferral (§1.3): no split of a collection whose genesis schema
//     has enable_namespace set, whatever its shard_by.
//
// The header carries only names; the residues and the modulus exist once, in
// the post-image, so there is no second copy to cross-check.
//
// What stays with the apply (routing.JudgeCommit) is every
// comparison against the collection meta: states moving backwards, the modulus
// shrinking relative to the collection's, a shard delisted from a live state,
// and the namespace admission re-read from the meta's own properties.
//
// It lives here, next to the builder, because it has two callers that must not
// diverge: SplitShardParam.Validate runs it BEFORE the broadcast, where a
// malformed message is still a refused build, and rootcoord's SplitShard ack
// callback runs it after, before anything is committed, as the last line of
// defense against a message that reached the WAL some other way. A second copy
// would let the pre-fence gate drift away from the post-fence one, which is
// exactly the drift that leaves a fenced source behind a permanently retried
// callback.
//
// Every refusal is a System error: the message is derived by the split
// coordinator, never by a user request, so a bad one is a Milvus bug.
func ValidateSplitShardMessage(header *message.SplitShardMessageHeader, body *message.SplitShardMessageBody) error {
	postImage := body.GetRouting()
	if postImage == nil {
		return merr.WrapErrServiceInternalMsg("split shard message carries no routing post-image")
	}
	source := header.GetSourceVchannel()
	if source == "" {
		return merr.WrapErrServiceInternalMsg("a shard split fences exactly one source vchannel, got none")
	}
	// A control channel is neither a shard to fence nor one to create. The
	// builder names the source append-first, and OptBuildBroadcastAppendFirst
	// panics on a control channel rather than returning an error; a target
	// named like one would be created as a shard nothing routes to and, on the
	// broadcast result, be mistaken for the control-channel replica whose tick
	// orders the commit.
	if funcutil.IsControlChannel(source) {
		return merr.WrapErrServiceInternalMsg("source vchannel %s is a control channel", source)
	}
	targets := header.GetTargetVchannels()
	if len(targets) != splitShardTargets {
		return merr.WrapErrServiceInternalMsg(
			"a shard split creates exactly %d target vchannels, got %d", splitShardTargets, len(targets))
	}
	for i, target := range targets {
		if target == "" {
			return merr.WrapErrServiceInternalMsg("target vchannel must be set")
		}
		if funcutil.IsControlChannel(target) {
			return merr.WrapErrServiceInternalMsg("target vchannel %s is a control channel", target)
		}
		if target == source || slices.Contains(targets[:i], target) {
			return merr.WrapErrServiceInternalMsg("duplicated vchannel %s in shard split", target)
		}
	}

	// Shape and tiling are the checks every routing post-image passes, shared
	// with routing.JudgeCommit so the split gate and the adoption gate cannot
	// drift. The modulus check between them is the split's own: a legacy
	// post-image without a modulus is a valid collection meta but never a valid
	// split, since a split's targets are named by residue.
	if err := routing.CheckPostImageShape(postImage); err != nil {
		return merr.Wrap(err, "split shard routing post-image")
	}
	if postImage.GetRoutingModulus() == 0 {
		return merr.WrapErrServiceInternalMsg("split shard routing post-image: routing modulus must be set")
	}
	if err := routing.CheckPostImageTiling(postImage); err != nil {
		return merr.Wrap(err, "split shard routing post-image")
	}
	vchannels := postImage.GetVirtualChannelNames()

	// A fenced source stays listed, Splitting, until adoption: a post-image
	// that lists it in any other state would either keep routing writes to a
	// fenced shard or skip the fence in the meta entirely.
	sourceIdx := slices.Index(vchannels, source)
	if sourceIdx < 0 {
		return merr.WrapErrServiceInternalMsg("split shard routing post-image: source %s is not named by the post-image", source)
	}
	if state := postImage.GetShardInfos()[sourceIdx].GetState(); state != schemapb.ShardState_ShardSplitting {
		return merr.WrapErrServiceInternalMsg("split shard routing post-image: source %s is %s, a fenced source is Splitting", source, state.String())
	}
	// A target is born Creating -- the apply refuses a new vchannel in any other
	// state -- and a target without residues is a shard nothing can ever be
	// written to.
	for _, target := range targets {
		idx := slices.Index(vchannels, target)
		if idx < 0 {
			return merr.WrapErrServiceInternalMsg("split shard routing post-image: target %s is not named by the post-image", target)
		}
		info := postImage.GetShardInfos()[idx]
		if info.GetState() != schemapb.ShardState_ShardCreating {
			return merr.WrapErrServiceInternalMsg("split shard routing post-image: target %s is %s, a split target is born Creating", target, info.GetState().String())
		}
		if len(info.GetHashRouting().GetBuckets()) == 0 {
			return merr.WrapErrServiceInternalMsg("split shard routing post-image: target %s owns no residue", target)
		}
	}
	// Dropped is reachable only by delisting: an adoption removes a drained
	// source from the list, and only a delist is reached and drain-gated. A
	// post-image that lists a shard as Dropped would retire it without either.
	if err := routing.CheckNoListedDroppedShard(vchannels, postImage.GetShardInfos()); err != nil {
		return merr.Wrap(err, "split shard routing post-image")
	}

	// Namespace admission reads the collection properties the genesis schema
	// carries; both are immutable after creation, so they are the same facts
	// the collection meta holds.
	properties := body.GetGenesis().GetCollectionSchema().GetProperties()
	if err := routing.CheckShardByAdmission(postImage.GetShardBy(), properties); err != nil {
		return merr.Wrap(err, "split shard routing post-image")
	}
	if err := routing.CheckNamespaceRelabelGranularity(postImage.GetShardBy(), postImage.GetRoutingModulus(), len(header.GetPartitionIds())); err != nil {
		return merr.Wrap(err, "split shard routing post-image")
	}
	// Namespace collections are not split yet (design §1.3), under any shard_by
	// and in either namespace.mode: until the namespace layout supports
	// relabel, splitting one would take a rewrite of every row. Checked last,
	// so every refusal above keeps its own message. It reads the genesis
	// schema, which carries enable_namespace like the collection meta and is
	// immutable after creation, so it answers the same way before the fence
	// (SplitShardParam.Validate, CheckSplitShardAgainstCollection) and in the
	// ack callback, both of which run this function on the same message.
	if body.GetGenesis().GetCollectionSchema().GetEnableNamespace() {
		return merr.WrapErrServiceInternalMsg(
			"split shard of collection %d: namespace collections are not split until the namespace layout supports relabel (design §1.3)",
			header.GetCollectionId())
	}
	return nil
}

// CheckSplitShardAgainstCollection is the check a split planner must run,
// under the collection's lock and against the collection meta it holds there,
// immediately before it broadcasts the SplitShard message. It makes before the
// fence every refusal the SplitShard ack callback and the routing apply would
// otherwise make only after it, when the source is already fenced and the
// refusal can only be retried forever:
//
//   - every message-only check (ValidateSplitShardMessage);
//   - the genesis properties agree with the meta's on namespace admission
//     (routing.CheckAdmissionPropertiesAgree), so both sides of the fence
//     answer admission the same way;
//   - the source is a shard of the collection and every shard of the
//     collection is still listed: a split retires nothing, so a post-image
//     that forgets a live shard is a planning bug, not (as the apply would read
//     it) a retirement this cluster has not applied;
//   - the routing apply's own judgement (routing.JudgeCommit) with the split's
//     own delta -- the header's source fenced, the header's two targets
//     created -- and nothing else changed: namespace admission against the
//     meta's properties, a modulus that neither vanishes nor shrinks and that
//     the collection's divides, every untouched shard at the state and residues
//     the collection holds;
//   - what JudgeCommit lets a redelivery keep but a new split must not: the
//     source is Normal in the meta (a legacy shard without a shard info serves
//     as Normal), and no target already exists in the collection.
//
// A post-image the collection already carries passes: the split was committed
// and a re-issue under the same idempotency key resolves to that broadcast. An
// error marked routing.ErrCommitAheadOfCollection is retriable -- the meta the
// planner holds lacks a split its post-image assumes. Every other refusal is a
// System error naming a planning bug.
//
// The function is the one implementation of these checks; a planner that
// re-derives any of them keeps the drift this function exists to prevent. What
// it cannot cover is anything that changes between this call and the append,
// which is why it must run under the lock the broadcast is issued with.
func CheckSplitShardAgainstCollection(coll *model.Collection, header *message.SplitShardMessageHeader, body *message.SplitShardMessageBody) error {
	if coll == nil {
		return merr.WrapErrServiceInternalMsg("split shard of collection %d checked against no collection meta", header.GetCollectionId())
	}
	if header.GetCollectionId() != coll.CollectionID {
		return merr.WrapErrServiceInternalMsg("split shard names collection %d but was checked against the meta of collection %d",
			header.GetCollectionId(), coll.CollectionID)
	}
	if err := ValidateSplitShardMessage(header, body); err != nil {
		return err
	}
	if err := routing.CheckAdmissionPropertiesAgree(body.GetGenesis().GetCollectionSchema().GetProperties(), coll.Properties); err != nil {
		return merr.Wrapf(err, "split shard of collection %d", coll.CollectionID)
	}
	source := header.GetSourceVchannel()
	if !slices.Contains(coll.VirtualChannelNames, source) {
		return merr.WrapErrServiceInternalMsg(
			"split shard of collection %d: source %s is not a shard of the collection", coll.CollectionID, source)
	}
	for _, vchannel := range coll.VirtualChannelNames {
		if !slices.Contains(body.GetRouting().GetVirtualChannelNames(), vchannel) {
			return merr.WrapErrServiceInternalMsg(
				"split shard of collection %d: the post-image does not list shard %s, and a split retires nothing",
				coll.CollectionID, vchannel)
		}
	}
	// The planner holds the collection lock, so the split task it is about to
	// broadcast is not applied here: recorded=false, and the judge cannot read
	// the source's absence as a retired split (it was refused above anyway).
	err := routing.JudgeCommit(coll, body.GetRouting(), routing.SplitDelta(source, header.GetTargetVchannels(), false))
	if errors.Is(err, routing.ErrCommitAlreadyApplied) {
		return nil
	}
	// Named before the judge's verdict: the judge reads a Creating source or an
	// existing target as a commit ahead of, or past, the meta, which for a NEW
	// split is a planning bug rather than something to wait for.
	if info, ok := coll.ShardInfos[source]; ok && info.State != schemapb.ShardState_ShardNormal {
		return merr.WrapErrServiceInternalMsg(
			"split shard of collection %d: source %s is %s in the collection meta, only a Normal shard may be split",
			coll.CollectionID, source, info.State.String())
	}
	for _, target := range header.GetTargetVchannels() {
		if slices.Contains(coll.VirtualChannelNames, target) {
			return merr.WrapErrServiceInternalMsg(
				"split shard of collection %d: target %s already exists in the collection", coll.CollectionID, target)
		}
	}
	if err != nil {
		return merr.Wrapf(err, "split shard of collection %d against its meta", coll.CollectionID)
	}
	return nil
}

// SplitShardTargetBuckets returns the residues the post-image gives vchannel,
// nil when the post-image does not name it. The post-image is the only copy of
// a target's residues.
func SplitShardTargetBuckets(postImage *message.AlterCollectionMessageUpdates, vchannel string) []uint64 {
	idx := slices.Index(postImage.GetVirtualChannelNames(), vchannel)
	if idx < 0 || idx >= len(postImage.GetShardInfos()) {
		return nil
	}
	return postImage.GetShardInfos()[idx].GetHashRouting().GetBuckets()
}

// NewSplitShardBroadcastMessage builds the SplitShard broadcast: the source
// vchannel is named append-first (so the fence lands before any other
// replica is observed), the targets' genesis and the routing post-image travel
// in the body, and the whole thing is deduplicated by a collection-scoped
// idempotency key derived from the split task id -- a retry of the same task
// against the same collection is therefore the SAME broadcast, not a new one.
//
// The message is replicable; see the top of this file for what the secondary
// does with it.
//
// It refuses to build anything while dataCoord.shardSplit.enable is off.
func NewSplitShardBroadcastMessage(param SplitShardParam) (message.BroadcastMutableMessage, error) {
	// The switch gates ISSUING a split, and this builder is the one place a new
	// split is made. Nothing that carries a split already in the WAL reads it:
	// not the StreamingNode handlers, recovery or flusher, not the SplitShard and
	// routing AlterCollection ack callbacks or DataCoord's CommitShardSplit and
	// drain check, not a secondary's remap and append gate, not a force-promoted
	// re-drive. Once the source may be fenced a split can only roll forward, so a
	// switch turned off afterwards -- or never turned on, on a secondary -- must
	// not stop it.
	//
	// A disabled feature is a System refusal, not the caller's input: the only
	// caller is a coordinator. OperationNotSupported is not retriable, since
	// nothing changes until an operator turns the switch on.
	if enable := &paramtable.Get().DataCoordCfg.ShardSplitEnable; !enable.GetAsBool() {
		return nil, merr.WrapErrOperationNotSupportedMsg(
			"shard split of collection %d is disabled by %s", param.CollectionID, enable.Key)
	}
	if err := param.Validate(); err != nil {
		return nil, err
	}
	// The broadcast reaches exactly the vchannels the split acts on: the
	// source (fenced), the targets (created) and the control channel (which
	// orders the ack callback). A shard the split leaves alone gets no replica:
	// no consumer has anything to do with one, and SplitShard is
	// ExclusiveRequired, so a replica there would only force-fail that shard's
	// transactions and cost an append and a fresh TSO batch. Validate has
	// already refused duplicates among these.
	vchannels := make([]string, 0, len(param.TargetVChannels)+2)
	vchannels = append(vchannels, param.SourceVChannel)
	vchannels = append(vchannels, param.TargetVChannels...)
	vchannels = append(vchannels, param.ControlChannel)

	msg, err := message.NewSplitShardMessageBuilderV2().
		WithHeader(param.header()).
		WithBody(param.body()).
		WithBroadcast(vchannels, message.OptBuildBroadcastAppendFirst(param.SourceVChannel)).
		WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(param.CollectionID, fmt.Sprintf("shard-split-%d", param.SplitTaskID))).
		BuildBroadcast()
	if err != nil {
		return nil, errors.Wrap(err, "build split shard broadcast message failed")
	}
	return msg, nil
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
// Exported for rootcoord's SplitShard ack callback, which seeds these positions
// at datacoord; it lives next to the builder so the recipe has one home. A
// second copy of it is exactly what must not exist -- a position that disagrees
// with the one already seeded would rewind or skip the target's readers.
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
