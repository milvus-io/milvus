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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// make sure QueryNode implements the shard-split child spawner.
var _ delegator.ChildSpawner = (*QueryNode)(nil)

// SpawnSplitChild creates and starts an in-process child delegator for a
// shard-split target vchannel. The child is born as an un-adopted split child
// (frontingParent set, adopted=false), so GetDataDistribution skips it
// (IsUnadoptedSplitChild) and no proxy read reaches it until querycoord adopts
// it; the source delegator reaches it only through the returned in-process
// handle. It is started so it consumes its WAL and serves fronted reads.
//
// It is idempotent: a re-consume of the fence finds the child already
// registered and returns it.
func (node *QueryNode) SpawnSplitChild(ctx context.Context, params delegator.SpawnChildParams) (delegator.ShardDelegator, error) {
	targetVChannel := params.Target.GetVchannel()
	log := mlog.With(
		mlog.Int64("collectionID", params.CollectionID),
		mlog.String("sourceVChannel", params.SourceVChannel),
		mlog.String("targetVChannel", targetVChannel),
	)

	if existing, ok := node.delegators.Get(targetVChannel); ok {
		log.Info(ctx, "split child delegator already registered, reuse it")
		return existing, nil
	}

	collection := node.manager.Collection.Get(params.CollectionID)
	if collection == nil {
		return nil, merr.WrapErrCollectionNotFound(params.CollectionID, "source collection missing while spawning split child")
	}

	// The target vchannel is created just after the fence, so its recovery info
	// (channel list + channel-checkpoint seek) may not be visible yet; wait for
	// it, the same seek path any delegator uses.
	seekPosition, err := node.waitSplitTargetRecovery(params.CollectionID, targetVChannel)
	if err != nil {
		return nil, err
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
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create split child delegator")
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
		return nil, errors.Wrap(err, "failed to create split child pipeline")
	}
	defer func() {
		if !success {
			node.pipelineManager.Remove(targetVChannel)
		}
	}()

	if err := pipeline.ConsumeMsgStream(ctx, seekPosition); err != nil {
		return nil, errors.Wrap(err, "failed to seek split child pipeline")
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

// respawnSplitChildrenOnRecovery re-creates the in-process split children for a
// source vchannel that is mid-split, after the source delegator is (re)watched.
// On a restart the SplitShard fence may sit behind the channel checkpoint and
// never be re-consumed, so the children would otherwise be lost; instead the
// targets are re-derived from durable coordinator state (the collection's
// shard infos). It is a no-op unless this vchannel is itself a fenced split
// source, and ProcessSplitShard is idempotent so an already-spawned child is
// left untouched (the common, non-restart case).
func (node *QueryNode) respawnSplitChildrenOnRecovery(ctx context.Context, source delegator.ShardDelegator, collectionID int64, sourceVChannel string) {
	log := mlog.With(mlog.Int64("collectionID", collectionID), mlog.String("sourceVChannel", sourceVChannel))
	if node.mixCoord == nil {
		// This runs in a goroutine spawned by WatchDmChannels, so a nil
		// dereference here takes the whole node down rather than failing one
		// request. A node without a coordinator handle has nothing to recover
		// from anyway.
		log.Warn(ctx, "no coordinator handle, skip split child recovery")
		return
	}
	mixCoord, err := node.mixCoord.GetWithContext(ctx)
	if err != nil {
		log.Warn(ctx, "failed to get coordinator client for split child recovery", mlog.Err(err))
		return
	}
	resp, err := mixCoord.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		// The Base is not optional: rootcoord's task Prepare reads its MsgType.
		Base:         commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection)),
		CollectionID: collectionID,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn(ctx, "failed to describe collection for split child recovery", mlog.Err(err))
		return
	}

	vchannels := resp.GetVirtualChannelNames()
	shardInfos := resp.GetShardInfos()
	stateOf := func(vchannel string) schemapb.ShardState {
		for i, name := range vchannels {
			if name == vchannel && i < len(shardInfos) {
				return shardInfos[i].GetState()
			}
		}
		return schemapb.ShardState_ShardNormal
	}

	// only a fenced split source recovers children; a normal/dropped vchannel does not.
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
	// guessing would double-count rows. Refusing leaves those targets fronted by
	// nobody until they are adopted, which reads as a channel not yet serving
	// rather than as wrong results.
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

	var targets []*messagespb.SplitShardTarget
	for i, vchannel := range vchannels {
		if i >= len(shardInfos) || shardInfos[i].GetState() != schemapb.ShardState_ShardCreating {
			continue
		}
		targets = append(targets, &messagespb.SplitShardTarget{Vchannel: vchannel})
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
// because the source was released mid-spawn. It mirrors the un-adopted teardown
// in releaseSplitChildren: the child was never fronted, so it cannot have been
// adopted.
func (node *QueryNode) AbortSplitChild(ctx context.Context, child delegator.ShardDelegator, collectionID int64, vchannel string) {
	node.delegators.GetAndRemove(vchannel)
	node.pipelineManager.Remove(vchannel)
	child.Close()
	node.manager.Segment.RemoveBy(ctx, segments.WithChannel(vchannel), segments.WithType(segments.SegmentTypeGrowing))
	node.manager.Collection.Unref(collectionID, 1)
	metrics.QueryNodeSplitChildNum.WithLabelValues(fmt.Sprint(node.GetNodeID())).Dec()
	mlog.Info(ctx, "aborted an unpublished shard-split child after source release",
		mlog.String("childVChannel", vchannel))
}

// waitSplitTargetRecovery polls the coordinator's recovery info until the split
// target vchannel appears with a seek position. It is bounded so a target that
// never materializes (e.g. the split aborted before creation) does not block
// forever; it is driven by the node lifetime context so node shutdown cancels it.
func (node *QueryNode) waitSplitTargetRecovery(collectionID int64, targetVChannel string) (*msgpb.MsgPosition, error) {
	mixCoord, err := node.mixCoord.GetWithContext(node.ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get coordinator client for split child recovery")
	}

	var seekPosition *msgpb.MsgPosition
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
				seekPosition = channel.GetSeekPosition()
				return nil
			}
		}
		return merr.WrapErrChannelNotFound(targetVChannel, "split target not yet in recovery info")
	}, retry.Attempts(120), retry.Sleep(time.Second), retry.MaxSleepTime(time.Second))
	if err != nil {
		return nil, errors.Wrapf(err, "split target %s recovery info not available", targetVChannel)
	}
	return seekPosition, nil
}
