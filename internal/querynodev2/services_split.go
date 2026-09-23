// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querynodev2

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// make sure QueryNode implements the shard-split child spawner.
var _ delegator.ChildSpawner = (*QueryNode)(nil)

// SpawnSplitChild creates and starts an in-process child delegator for a
// shard-split target vchannel. The child is born as an un-adopted split child
// (frontingParent set, adopted=false), so GetDataDistribution skips it
// (IsUnadoptedSplitChild) and no proxy read reaches it until querycoord adopts
// it; the source delegator reaches it only through the returned in-process
// handle. It is started so it consumes its WAL and serves fronted reads, after
// it has loaded what its target already persisted before the checkpoint it
// seeks from (loadSplitChildRecovery).
//
// It is idempotent: a re-consume of the fence finds the source's child already
// registered and returns it. A registered delegator is never replaced; one
// fronted by nobody is attached, one fronted by another source refused (see
// reuseSplitChild).
func (node *QueryNode) SpawnSplitChild(ctx context.Context, params delegator.SpawnChildParams) (delegator.ShardDelegator, error) {
	targetVChannel := params.TargetVChannel
	log := mlog.With(
		mlog.Int64("collectionID", params.CollectionID),
		mlog.String("sourceVChannel", params.SourceVChannel),
		mlog.String("targetVChannel", targetVChannel),
	)

	if existing, ok := node.delegators.Get(targetVChannel); ok {
		// This source's own child is complete once registered: reuse it. Any
		// other delegator is looked at only under the channel claim, which a
		// WatchDmChannels still building it holds (see reuseRegisteredUnderClaim).
		if params.Parent != nil && existing.FrontingParent() == params.Parent {
			return reuseSplitChild(ctx, existing, params)
		}
		return node.reuseRegisteredUnderClaim(ctx, params)
	}

	collection := node.manager.Collection.Get(params.CollectionID)
	if collection == nil {
		return nil, merr.WrapErrCollectionNotFound(params.CollectionID, "source collection missing while spawning split child")
	}

	// The target vchannel is created just after the fence, so its recovery info
	// (channel list + channel-checkpoint seek) may not be visible yet; wait for
	// it, the same seek path any delegator uses.
	channelInfo, err := node.waitSplitTargetRecovery(params.CollectionID, targetVChannel)
	if err != nil {
		return nil, err
	}
	seekPosition := channelInfo.GetSeekPosition()

	// The wait can take minutes, and querycoord may watch the target meanwhile.
	// Claim the channel the way WatchDmChannels does, so neither registers a
	// delegator for it while the other is part-way through, then look again: a
	// delegator registered during the wait is never overwritten (and so never
	// removed from the node by this spawn's failure cleanup).
	//
	// While this claim is held, a concurrent WatchDmChannels for the same
	// target cannot take it either; it returns merr.Success() having watched
	// nothing (the "channel subscribing..." branch in services.go), so
	// querycoord's watch task waits out queryCoord.channelTaskTimeout (120s)
	// before its channel checker re-watches. By then the claim is gone: the
	// retry finds the spawned child already registered and adopts it instead of
	// building it fresh. Reads stay correct throughout that whole window
	// because the source delegator fronts the child until it is adopted.
	if !node.claimSplitTarget(params) {
		return nil, errSplitTargetBeingWatched(params)
	}
	defer node.subscribingChannels.Remove(targetVChannel)
	if existing, ok := node.delegators.Get(targetVChannel); ok {
		return reuseSplitChild(ctx, existing, params)
	}

	// keep the collection alive for the child's lifetime.
	node.manager.Collection.Ref(params.CollectionID, 1)
	success := false
	defer func() {
		if !success {
			node.manager.Collection.Unref(params.CollectionID, 1)
		}
	}()

	queryView := delegator.NewChannelQueryView(nil, nil, collection.GetPartitions(), delegator.InitialTargetVersion)
	child, err := delegator.NewShardDelegator(
		ctx, params.CollectionID, params.ReplicaID, targetVChannel, params.Version,
		node.clusterManager, node.manager, node.loader, seekPosition.GetTimestamp(),
		node.queryHook, node.chunkManager, queryView, node.binlogSaver,
		// A child is a shard like any other: once its key range outgrows the
		// split threshold a second split fences it, and it must front that split
		// just as a watched delegator does, both before and after its adoption.
		delegator.WithChildSpawner(node),
	)
	if err != nil {
		return nil, merr.Wrap(err, "failed to create split child delegator")
	}
	// wire the fronting parent before the pipeline starts so no delete the child
	// consumes escapes forwarding to the source delegator.
	if params.Parent != nil {
		child.SetFrontingParent(params.Parent)
	}
	node.delegators.Insert(targetVChannel, child)
	defer func() {
		if !success {
			node.delegators.GetAndRemove(targetVChannel)
			child.Close()
		}
	}()

	pipeline, err := node.pipelineManager.Add(params.CollectionID, targetVChannel)
	if err != nil {
		return nil, merr.Wrap(err, "failed to create split child pipeline")
	}
	defer func() {
		if !success {
			node.pipelineManager.Remove(targetVChannel)
		}
	}()

	defer func() {
		if !success {
			node.manager.Segment.RemoveBy(ctx, segments.WithChannel(targetVChannel), segments.WithType(segments.SegmentTypeGrowing))
		}
	}()
	if err := node.loadSplitChildRecovery(ctx, child, params, channelInfo); err != nil {
		return nil, err
	}

	if err := pipeline.ConsumeMsgStream(ctx, seekPosition); err != nil {
		return nil, merr.Wrap(err, "failed to seek split child pipeline")
	}
	pipeline.Start()
	child.Start()
	success = true

	nodeIDStr := fmt.Sprint(node.GetNodeID())
	metrics.QueryNodeSplitChildSpawnedTotal.WithLabelValues(nodeIDStr).Inc()
	metrics.QueryNodeSplitChildNum.WithLabelValues(nodeIDStr).Inc()

	log.Info(ctx, "spawned an in-process child delegator for a split target",
		mlog.Uint64("seekTimestamp", seekPosition.GetTimestamp()))
	return child, nil
}

// loadSplitChildRecovery brings a freshly built split child to the state
// WatchDmChannels would give a delegator of the target, from the target's
// recovery view, before its pipeline seeks to the target's checkpoint:
//
//   - the target's L0 segments, the deletes on the target up to the checkpoint;
//   - its unflushed segments, loaded as growing (with those L0 deletes applied):
//     the synced part of its growing data and every flushed segment still
//     IsInvisible (at defaults, every target-flushed segment until it is sorted
//     after Done), which DataCoord reports as unflushed and the source's view
//     never takes in;
//   - the exclusions that keep the pipeline from re-creating what is loaded, or
//     what the source serves as sealed (the target's visible flushed segments,
//     attributed to the source).
//
// It then forwards every delete it now holds to the fronting parent. The child
// consumes only from the checkpoint, so without the L0 records the parent would
// never apply the target's deletes in (T_switch, checkpoint] to its own view.
//
// On a first spawn the target has flushed nothing and all of this is empty; it
// matters for a respawn after a QueryNode restart mid-window, and for a first
// spawn delayed past the target's first sync.
func (node *QueryNode) loadSplitChildRecovery(ctx context.Context, child delegator.ShardDelegator, params delegator.SpawnChildParams,
	channelInfo *datapb.VchannelInfo,
) error {
	// shaped like QueryCoord's watch request so the watch path's loaders apply.
	req := &querypb.WatchDmChannelsRequest{
		CollectionID: params.CollectionID,
		Infos:        []*datapb.VchannelInfo{channelInfo},
		Version:      params.Version,
	}
	segmentIDs := append(append([]int64(nil), channelInfo.GetUnflushedSegmentIds()...), channelInfo.GetLevelZeroSegmentIds()...)
	if len(segmentIDs) > 0 {
		infos, err := node.getSplitTargetSegmentInfos(ctx, segmentIDs)
		if err != nil {
			return err
		}
		req.SegmentInfos = infos
	}

	growingInfo := lo.SliceToMap(channelInfo.GetUnflushedSegmentIds(), func(id int64) (int64, uint64) {
		return id, req.GetSegmentInfos()[id].GetDmlPosition().GetTimestamp()
	})
	child.AddExcludedSegments(growingInfo)
	sealedInfo := lo.SliceToMap(append(append([]int64(nil), channelInfo.GetFlushedSegmentIds()...), channelInfo.GetDroppedSegmentIds()...),
		func(id int64) (int64, uint64) { return id, typeutil.MaxTimestamp })
	child.AddExcludedSegments(sealedInfo)

	if err := loadL0Segments(ctx, child, req); err != nil {
		return merr.Wrap(err, "failed to load split target L0 segments")
	}
	if err := loadGrowingSegments(ctx, child, req); err != nil {
		return merr.Wrap(err, "failed to load split target unflushed segments")
	}
	if err := child.ForwardKnownDeletesToParent(ctx); err != nil {
		return merr.Wrap(err, "failed to forward split target deletes to the source")
	}
	return nil
}

// getSplitTargetSegmentInfos fetches the full segment infos (binlogs included)
// of a split target's segments, the way QueryCoord fills a watch request.
func (node *QueryNode) getSplitTargetSegmentInfos(ctx context.Context, segmentIDs []int64) (map[int64]*datapb.SegmentInfo, error) {
	if node.mixCoord == nil {
		return nil, merr.WrapErrServiceInternalMsg("no coordinator handle to fetch split target segment infos")
	}
	mixCoord, err := node.mixCoord.GetWithContext(ctx)
	if err != nil {
		return nil, merr.Wrap(err, "failed to get coordinator client for split target segment infos")
	}
	infos := make(map[int64]*datapb.SegmentInfo, len(segmentIDs))
	for _, batch := range lo.Chunk(segmentIDs, splitTargetSegmentInfoBatch) {
		resp, err := mixCoord.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
			SegmentIDs:       batch,
			IncludeUnHealthy: true,
		})
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return nil, merr.Wrap(err, "failed to get split target segment infos")
		}
		if err := binlog.DecompressMultiBinLogs(resp.GetInfos()); err != nil {
			return nil, merr.Wrap(err, "failed to decompress split target segment binlogs")
		}
		for _, info := range resp.GetInfos() {
			infos[info.GetID()] = info
		}
	}
	return infos, nil
}

// splitTargetSegmentInfoBatch bounds one GetSegmentInfo call, as QueryCoord's
// broker does.
const splitTargetSegmentInfoBatch = 1000

// claimSplitTarget takes the target's channel claim, the one WatchDmChannels
// holds from before it registers a delegator until that delegator is started
// or, on failure, removed and closed.
func (node *QueryNode) claimSplitTarget(params delegator.SpawnChildParams) bool {
	return node.subscribingChannels.Insert(params.TargetVChannel)
}

func errSplitTargetBeingWatched(params delegator.SpawnChildParams) error {
	return merr.WrapErrServiceUnavailable("split target is being watched",
		fmt.Sprintf("target %s, source %s", params.TargetVChannel, params.SourceVChannel))
}

// reuseRegisteredUnderClaim decides about a delegator registered for the target
// that this source does not front, holding the target's channel claim.
// WatchDmChannels registers its delegator before it loads the target's L0 and
// growing segments and keeps the claim until it is done, so a delegator seen
// under the claim is complete: attaching one mid-watch would forward none of the
// L0 deletes it registers later, and a watch that then failed would close a
// delegator the source fronts. While the claim is held elsewhere the spawn
// yields with a retriable error.
func (node *QueryNode) reuseRegisteredUnderClaim(ctx context.Context, params delegator.SpawnChildParams) (delegator.ShardDelegator, error) {
	if !node.claimSplitTarget(params) {
		return nil, errSplitTargetBeingWatched(params)
	}
	defer node.subscribingChannels.Remove(params.TargetVChannel)
	existing, ok := node.delegators.Get(params.TargetVChannel)
	if !ok {
		// removed meanwhile (a failed watch, a release): the retry spawns anew.
		return nil, merr.WrapErrServiceUnavailable("split target delegator went away",
			fmt.Sprintf("target %s, source %s", params.TargetVChannel, params.SourceVChannel))
	}
	return reuseSplitChild(ctx, existing, params)
}

// reuseSplitChild returns a delegator already registered for a split target,
// fronted by the spawning source:
//
//   - the source's own child (a re-consume of the fence, or a recovery respawn
//     racing it) is returned as is;
//   - a delegator fronted by nobody -- typically one querycoord watched for the
//     target on its own after adoption, while this spawn was waiting -- is
//     attached: marked adopted (it is a shard querycoord manages, and must stay
//     visible to it), fronted by the source, and made to forward every delete
//     it already holds (its L0 and its buffer) to the source, since until now it
//     forwarded none. Refusing it instead would leave the target pending, and
//     every read through the source refused, until the source is released;
//   - a delegator fronted by another source is refused with
//     ErrChannelReduplicate: fronting it from here too would return its rows
//     twice. The source retries, so the refusal ends when that parent lets go.
func reuseSplitChild(ctx context.Context, existing delegator.ShardDelegator, params delegator.SpawnChildParams) (delegator.ShardDelegator, error) {
	targetVChannel := params.TargetVChannel
	switch parent := existing.FrontingParent(); {
	case params.Parent != nil && parent == params.Parent:
		mlog.Info(ctx, "split child delegator already registered, reuse it",
			mlog.String("sourceVChannel", params.SourceVChannel), mlog.String("targetVChannel", targetVChannel))
		return existing, nil
	case params.Parent != nil && parent == nil:
		// adopted first, so setting the parent never makes it an un-adopted
		// child that GetDataDistribution would hide from querycoord.
		existing.MarkAdopted()
		existing.SetFrontingParent(params.Parent)
		if err := existing.ForwardKnownDeletesToParent(ctx); err != nil {
			// detach again, so the retry re-attaches and forwards them all.
			existing.SetFrontingParent(nil)
			return nil, merr.Wrap(err, "failed to attach the delegator serving the split target to its source")
		}
		mlog.Info(ctx, "attached the delegator already serving a split target to its source",
			mlog.String("sourceVChannel", params.SourceVChannel), mlog.String("targetVChannel", targetVChannel))
		return existing, nil
	default:
		return nil, merr.WrapErrChannelReduplicate(targetVChannel,
			fmt.Sprintf("a delegator fronted by another source already serves the split target, not %s", params.SourceVChannel))
	}
}

// respawnSplitChildrenOnRecovery re-creates the in-process split children for a
// source vchannel that is mid-split, after the source delegator is (re)watched.
// On a restart the SplitShard fence may sit behind the channel checkpoint and
// never be re-consumed, so the children would otherwise be lost; instead the
// targets are re-derived from durable coordinator state (the collection's
// shard infos). It is a no-op unless this vchannel is itself a fenced split
// source, and ProcessSplitShard is idempotent so an already-spawned child is
// left untouched (the common, non-restart case). The source refuses reads until
// it returns (MarkSplitRecoveryPending, set by the watch); a failing describe is
// retried rather than given up on.
func (node *QueryNode) respawnSplitChildrenOnRecovery(ctx context.Context, source delegator.ShardDelegator, collectionID int64, sourceVChannel string) {
	log := mlog.With(mlog.Int64("collectionID", collectionID), mlog.String("sourceVChannel", sourceVChannel))
	// Deferred, so it runs after ProcessSplitShard below has made the re-derived
	// targets pending spawns: reads go from refused-while-recovering straight to
	// refused-while-spawning, never answered without the targets in between.
	defer source.FinishSplitRecovery()
	if node.mixCoord == nil {
		// This runs in a goroutine spawned by WatchDmChannels, so a nil
		// dereference here takes the whole node down rather than failing one
		// request. A node without a coordinator handle has nothing to recover
		// from anyway.
		log.Warn(ctx, "no coordinator handle, skip split child recovery")
		return
	}
	resp, ok := node.describeForSplitRecovery(ctx, source, collectionID)
	if !ok {
		return
	}

	vchannels := resp.GetVirtualChannelNames()
	shardInfos := resp.GetShardInfos()

	// A source the collection no longer lists was retired by an adoption: there
	// is no target left to re-derive, and it must never serve alone.
	if len(vchannels) > 0 && !lo.Contains(vchannels, sourceVChannel) {
		source.RefuseReadsAsRetiredSource(ctx)
		return
	}

	stateOf := func(vchannel string) schemapb.ShardState {
		for i, name := range vchannels {
			if name == vchannel && i < len(shardInfos) {
				return shardInfos[i].GetState()
			}
		}
		return schemapb.ShardState_ShardNormal
	}

	// only a fenced split source recovers children; a Normal vchannel does not.
	if stateOf(sourceVChannel) != schemapb.ShardState_ShardSplitting {
		return
	}

	// Re-front the not-yet-adopted (Creating) targets, but only when this source
	// is the collection's ONLY splitting one.
	//
	// A read fans out to every source, so a target fronted by two of them has its
	// post-fence rows returned twice. Which single source fronts which target is
	// the coordinator's choice, made when it built the fence messages; it is
	// provenance with the split task's lifetime and is not in the collection
	// meta, so this rebuild cannot reproduce it from a DescribeCollection alone.
	//
	// With one splitting source the choice is forced -- every Creating target is
	// fronted by it -- and the rebuild is exact. With several (a rehash, where
	// every target draws from every source) it is not derivable here, and
	// guessing would double-count rows. Today the trigger (DataCoord
	// shard_split_manager.go's detectOnce, "one split per collection at a
	// time") never plans a second active task for a collection that already has
	// one, so two of ITS shards being simultaneously Splitting cannot happen
	// regardless of dataCoord.shardSplit.maxConcurrentTasks (which only bounds
	// how many collections split concurrently, not shards within one) -- this
	// branch is unreached in practice and kept only as a defensive fallback
	// should that invariant ever change. Skipping the respawn instead (I-2)
	// does not refuse reads and is not visible as the vchannel "not serving": the
	// source is up and answers every read on its own, from its own (pre-fence)
	// view alone, exactly like frontingChildren() with an empty snapshot. What is
	// missing is the unfronted target's rows written after the fence -- silently,
	// not as an error -- until the target is adopted and the proxy starts routing
	// its key range there directly. Accepted as a known gap (see doc §11); not
	// fixed this round.
	splittingSources := 0
	for i := range vchannels {
		if i < len(shardInfos) && shardInfos[i].GetState() == schemapb.ShardState_ShardSplitting {
			splittingSources++
		}
	}
	if splittingSources > 1 {
		log.Warn(ctx, "several sources are splitting; the fronting assignment is not derivable from meta, skipping the child respawn",
			mlog.Int("splittingSources", splittingSources))
		return
	}

	var targets []string
	for i, vchannel := range vchannels {
		if i >= len(shardInfos) || shardInfos[i].GetState() != schemapb.ShardState_ShardCreating {
			continue
		}
		targets = append(targets, vchannel)
	}
	if len(targets) == 0 {
		return
	}
	if err := source.ProcessSplitShard(ctx, targets); err != nil {
		log.Warn(ctx, "failed to respawn split children on recovery", mlog.Err(err))
		return
	}
	log.Info(ctx, "respawned in-process split children on recovery", mlog.Int("targetCount", len(targets)))
}

// describeForSplitRecovery describes the collection for a split recovery,
// retrying until the coordinator answers. It gives up only when the recovery is
// moot: ctx ends (node shutdown) or the source is no longer serviceable
// (released). Meanwhile the source refuses every read (MarkSplitRecoveryPending):
// without the shard states it cannot tell whether it must front targets, and a
// source that must, answering alone, misses their rows and deletes silently.
func (node *QueryNode) describeForSplitRecovery(ctx context.Context, source delegator.ShardDelegator, collectionID int64) (*milvuspb.DescribeCollectionResponse, bool) {
	log := mlog.With(mlog.Int64("collectionID", collectionID))
	for attempt := 0; ; attempt++ {
		mixCoord, err := node.mixCoord.GetWithContext(ctx)
		if err != nil {
			log.Warn(ctx, "failed to get coordinator client for split child recovery", mlog.Err(err))
			return nil, false
		}
		// A restart re-watches every channel at once, and each watch runs this
		// recovery: the recoveries of one collection share one describe in
		// flight rather than each sending its own to rootcoord.
		resp, err, _ := node.splitRecoveryDescribes.Do(fmt.Sprint(collectionID), func() (*milvuspb.DescribeCollectionResponse, error) {
			resp, err := mixCoord.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
				// The Base is not optional: rootcoord's task Prepare reads its MsgType.
				Base:         commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection)),
				CollectionID: collectionID,
			})
			return resp, merr.CheckRPCCall(resp, err)
		})
		if err == nil {
			return resp, true
		}
		backoff := splitRecoveryRetryBackoff(attempt)
		log.RatedWarn(ctx, rate.Every(10*time.Second), "failed to describe collection for split child recovery, reads through the vchannel are refused until it answers",
			mlog.Int("attempt", attempt+1), mlog.Duration("retryIn", backoff), mlog.Err(err))
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(backoff):
		}
		if !source.Serviceable() {
			log.Info(ctx, "split child recovery given up, the source is released")
			return nil, false
		}
	}
}

// splitRecoveryRetryBackoff is how long a split recovery waits before
// describing the collection again: doubling from one second, capped at thirty.
func splitRecoveryRetryBackoff(attempt int) time.Duration {
	return min(time.Second<<min(attempt, 5), 30*time.Second)
}

// releaseSplitChildren handles the source delegator's in-process split children
// when the source channel is released. An un-adopted child (the split did not
// hand its target off) is torn down with the source: removed from the node, its
// pipeline stopped, growing segments dropped, and the collection ref the spawn
// took released. An already-adopted child is now an independent shard owned by
// querycoord, so it is kept alive and merely detached from the dying source
// (stops forwarding deletes). Safe to call on a source with no children.
func (node *QueryNode) releaseSplitChildren(ctx context.Context, source delegator.ShardDelegator, collectionID int64) {
	// stop any in-flight spawn from publishing a child onto this gone source.
	// Set before snapshotting children so a spawn that publishes after the
	// snapshot sees releasing and aborts itself instead of orphaning.
	source.MarkReleasing()

	nodeIDStr := fmt.Sprint(node.GetNodeID())
	for _, childVChannel := range source.SplitChildVChannels() {
		child, ok := node.delegators.Get(childVChannel)
		if !ok {
			continue
		}
		if !child.IsUnadoptedSplitChild() {
			// adopted: detach from the source but leave the live shard in place.
			// The fronted-child gauge was already decremented at adoption.
			source.DetachSplitChild(childVChannel)
			child.SetFrontingParent(nil)
			mlog.Info(ctx, "detached an adopted shard-split child from its released source",
				mlog.String("childVChannel", childVChannel))
			continue
		}
		// An un-adopted child that was split in turn fronts un-adopted children of
		// its own. Release those first, so none of them outlives it registered on
		// the node with a running pipeline and no parent.
		node.releaseSplitChildren(ctx, child, collectionID)
		node.delegators.GetAndRemove(childVChannel)
		node.pipelineManager.Remove(childVChannel)
		child.Close()
		node.manager.Segment.RemoveBy(ctx, segments.WithChannel(childVChannel), segments.WithType(segments.SegmentTypeGrowing))
		node.manager.Collection.Unref(collectionID, 1)
		metrics.QueryNodeSplitChildNum.WithLabelValues(nodeIDStr).Dec()
		mlog.Info(ctx, "released an un-adopted shard-split child delegator with its source",
			mlog.String("childVChannel", childVChannel))
	}
}

// AbortSplitChild tears down a child the spawner created but could not publish
// because the source was released or had stopped mid-spawn. It mirrors the
// un-adopted teardown in releaseSplitChildren: the child was never fronted, so
// it cannot have been adopted.
func (node *QueryNode) AbortSplitChild(ctx context.Context, child delegator.ShardDelegator, collectionID int64, vchannel string) {
	node.delegators.GetAndRemove(vchannel)
	node.pipelineManager.Remove(vchannel)
	child.Close()
	node.manager.Segment.RemoveBy(ctx, segments.WithChannel(vchannel), segments.WithType(segments.SegmentTypeGrowing))
	node.manager.Collection.Unref(collectionID, 1)
	metrics.QueryNodeSplitChildNum.WithLabelValues(fmt.Sprint(node.GetNodeID())).Dec()
	mlog.Info(ctx, "aborted an unpublished shard-split child after source release or stop",
		mlog.String("childVChannel", vchannel))
}

// waitSplitTargetRecovery polls the coordinator's recovery info until the split
// target vchannel appears with a seek position, and returns the target's whole
// recovery view (seek position, unflushed and L0 segment ids). It is bounded so a target that
// never materializes (e.g. the split aborted before creation) does not block
// forever; it is driven by the node lifetime context so node shutdown cancels it.
func (node *QueryNode) waitSplitTargetRecovery(collectionID int64, targetVChannel string) (*datapb.VchannelInfo, error) {
	if node.mixCoord == nil {
		// The spawn runs off the flow graph in its own goroutine, so a nil
		// dereference here would take the whole node down. Fail this spawn: the
		// target stays unfronted, which reads as a channel not yet serving.
		return nil, merr.WrapErrServiceInternalMsg("no coordinator handle to fetch split target %s recovery info", targetVChannel)
	}
	mixCoord, err := node.mixCoord.GetWithContext(node.ctx)
	if err != nil {
		return nil, merr.Wrap(err, "failed to get coordinator client for split child recovery")
	}

	var channelInfo *datapb.VchannelInfo
	err = retry.Do(node.ctx, func() error {
		resp, err := mixCoord.GetRecoveryInfoV2(node.ctx, &datapb.GetRecoveryInfoRequestV2{CollectionID: collectionID})
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		for _, channel := range resp.GetChannels() {
			if channel.GetChannelName() == targetVChannel {
				// Not merely non-nil: it has to be a position the dispatcher will
				// SEEK from. A vchannel created moments ago has no checkpoint yet,
				// so datacoord falls back to the earliest segment's DML position,
				// which on a target the rewrite has not written to carries neither
				// a message ID nor a WAL name. A dispatcher built on that one skips
				// the seek, and the delegator's streaming adaptor -- whose Seek is
				// what opens the WAL scanner -- panics the whole querynode the
				// first time it reads. Waiting costs a retry; not waiting costs the
				// process.
				if !msgdispatcher.SeekablePosition(channel.GetSeekPosition()) {
					return merr.WrapErrChannelNotFound(targetVChannel,
						"split target has no seekable position yet")
				}
				channelInfo = channel
				return nil
			}
		}
		return merr.WrapErrChannelNotFound(targetVChannel, "split target not yet in recovery info")
	}, retry.Attempts(120), retry.Sleep(time.Second), retry.MaxSleepTime(time.Second))
	if err != nil {
		return nil, merr.Wrapf(err, "split target %s recovery info not available", targetVChannel)
	}
	return channelInfo, nil
}
