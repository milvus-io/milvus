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

package checkers

import (
	"context"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TODO(sunby): have too much similar codes with SegmentChecker
type ChannelChecker struct {
	*checkerActivation
	meta         *meta.Meta
	dist         *meta.DistributionManager
	targetMgr    meta.TargetManagerInterface
	nodeMgr      *session.NodeManager
	scheduler    task.Scheduler
	assignPolicy assign.AssignPolicy

	// splitState skips watching a collection's not-yet-adopted split targets
	// (ShardState_ShardCreating): they are fronted in-process by the source
	// delegator and must not be picked up by querycoord until adoption. May be nil.
	splitState *meta.ShardSplitStateCache

	// version cache for fast skip when nothing changed
	versionCache map[int64]*collectionVersionCache
}

func NewChannelChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr meta.TargetManagerInterface,
	nodeMgr *session.NodeManager,
	scheduler task.Scheduler,
	splitState *meta.ShardSplitStateCache,
) *ChannelChecker {
	// Create RoundRobin assign policy in constructor to maximize loading speed
	// Note: RoundRobin may break short-term balance but prioritizes loading speed
	assignPolicy := assign.GetGlobalAssignPolicyFactory().GetPolicy(assign.PolicyTypeRoundRobin)

	return &ChannelChecker{
		checkerActivation: newCheckerActivation(),
		meta:              meta,
		dist:              dist,
		targetMgr:         targetMgr,
		nodeMgr:           nodeMgr,
		scheduler:         scheduler,
		assignPolicy:      assignPolicy,
		splitState:        splitState,
		versionCache:      make(map[int64]*collectionVersionCache),
	}
}

func (c *ChannelChecker) ID() utils.CheckerType {
	return utils.ChannelChecker
}

func (c *ChannelChecker) Description() string {
	return "DmChannelChecker checks the lack of DmChannels, or some DmChannels are redundant"
}

func (c *ChannelChecker) readyToCheck(ctx context.Context, collectionID int64) bool {
	metaExist := (c.meta.GetCollection(ctx, collectionID) != nil)
	targetExist := c.targetMgr.IsNextTargetExist(ctx, collectionID) || c.targetMgr.IsCurrentTargetExist(ctx, collectionID, common.AllPartitionsID)

	return metaExist && targetExist
}

func (c *ChannelChecker) Check(ctx context.Context) []task.Task {
	if !c.IsActive() {
		return nil
	}

	collectionIDs := c.meta.GetAll(ctx)
	tasks := make([]task.Task, 0)
	for _, cid := range collectionIDs {
		if c.readyToCheck(ctx, cid) {
			// Fast path: skip if target and dist versions unchanged
			currentTargetVersion := c.targetMgr.GetCollectionTargetVersion(ctx, cid, meta.NextTarget)
			currentDistVersion := c.dist.ChannelDistManager.GetVersion()
			if c.isCollectionSynced(cid, currentTargetVersion, currentDistVersion) {
				continue
			}

			replicas := c.meta.GetByCollection(ctx, cid)
			hasTask := false
			for _, r := range replicas {
				replicaTasks := c.checkReplica(ctx, r)
				if len(replicaTasks) > 0 {
					hasTask = true
					tasks = append(tasks, replicaTasks...)
				}
			}

			// Only update version cache if no tasks were generated
			// If tasks were generated, we need to re-check next time
			if !hasTask {
				c.updateVersionCache(cid, currentTargetVersion, currentDistVersion)
			}
		}
	}

	// clean up version cache for released collections
	c.cleanVersionCache(collectionIDs)

	// clean channel which has been released
	channels := c.dist.ChannelDistManager.GetByFilter()
	released := utils.FilterReleased(channels, collectionIDs)
	releaseTasks := c.createChannelReduceTasks(ctx, released, meta.NilReplica)
	task.SetReason("collection released", releaseTasks...)
	tasks = append(tasks, releaseTasks...)

	// clean node which has been move out from replica
	for _, nodeInfo := range c.nodeMgr.GetAll() {
		nodeID := nodeInfo.ID()
		channelOnQN := c.dist.ChannelDistManager.GetByFilter(meta.WithNodeID2Channel(nodeID))
		collectionChannels := lo.GroupBy(channelOnQN, func(ch *meta.DmChannel) int64 { return ch.CollectionID })
		for collectionID, channels := range collectionChannels {
			replica := c.meta.GetByCollectionAndNode(ctx, collectionID, nodeID)
			if replica == nil {
				reduceTasks := c.createChannelReduceTasks(ctx, channels, meta.NilReplica)
				task.SetReason("dirty channel exists", reduceTasks...)
				tasks = append(tasks, reduceTasks...)
			}
		}
	}
	return tasks
}

// isCollectionSynced checks if target and dist versions are unchanged since last check
func (c *ChannelChecker) isCollectionSynced(collectionID int64, targetVersion, channelDistVersion int64) bool {
	cache, ok := c.versionCache[collectionID]
	if !ok {
		return false
	}
	return cache.targetVersion == targetVersion && cache.channelDistVersion == channelDistVersion
}

// updateVersionCache updates the version cache for a collection
func (c *ChannelChecker) updateVersionCache(collectionID int64, targetVersion, channelDistVersion int64) {
	c.versionCache[collectionID] = &collectionVersionCache{
		targetVersion:      targetVersion,
		channelDistVersion: channelDistVersion,
	}
}

// cleanVersionCache removes entries for collections that no longer exist.
// Only runs when cache has more entries than active collections, meaning stale entries exist.
func (c *ChannelChecker) cleanVersionCache(activeCollections []int64) {
	if len(c.versionCache) <= len(activeCollections) {
		return
	}
	activeSet := make(map[int64]struct{}, len(activeCollections))
	for _, cid := range activeCollections {
		activeSet[cid] = struct{}{}
	}
	for cid := range c.versionCache {
		if _, ok := activeSet[cid]; !ok {
			delete(c.versionCache, cid)
		}
	}
}

func (c *ChannelChecker) checkReplica(ctx context.Context, replica *meta.Replica) []task.Task {
	ret := make([]task.Task, 0)

	lacks, redundancies := c.getDmChannelDiff(ctx, replica.GetCollectionID(), replica.GetID())
	tasks := c.createChannelLoadTask(c.getTraceCtx(ctx, replica.GetCollectionID()), lacks, replica)
	task.SetReason("lacks of channel", tasks...)
	ret = append(ret, tasks...)

	tasks = c.createChannelReduceTasks(c.getTraceCtx(ctx, replica.GetCollectionID()), redundancies, replica)
	task.SetReason("collection released", tasks...)
	ret = append(ret, tasks...)

	repeated := c.findRepeatedChannels(ctx, replica.GetID())
	tasks = c.createChannelReduceTasks(c.getTraceCtx(ctx, replica.GetCollectionID()), repeated, replica)
	task.SetReason("redundancies of channel", tasks...)
	ret = append(ret, tasks...)

	// All channel related tasks should be with high priority
	task.SetPriority(task.TaskPriorityHigh, tasks...)
	return ret
}

// GetDmChannelDiff get channel diff between target and dist
func (c *ChannelChecker) getDmChannelDiff(ctx context.Context, collectionID int64,
	replicaID int64,
) (toLoad, toRelease []*meta.DmChannel) {
	replica := c.meta.Get(ctx, replicaID)
	if replica == nil {
		mlog.Info(ctx, "replica does not exist, skip it")
		return toLoad, toRelease
	}

	dist := c.dist.ChannelDistManager.GetByFilter(meta.WithReplica2Channel(replica))
	distMap := typeutil.NewSet[string]()
	for _, ch := range dist {
		distMap.Insert(ch.GetChannelName())
	}

	nextTargetMap := c.targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.NextTarget)
	currentTargetMap := c.targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.CurrentTarget)

	// get channels which exists on dist, but not exist on current and next
	for _, ch := range dist {
		_, existOnCurrent := currentTargetMap[ch.GetChannelName()]
		_, existOnNext := nextTargetMap[ch.GetChannelName()]
		if !existOnNext && !existOnCurrent {
			toRelease = append(toRelease, ch)
		}
	}

	// not-yet-adopted split targets are fronted in-process by the source
	// delegator; querycoord must not watch them until they leave the Creating
	// state at adoption, or it would build a fresh delegator and replay the WAL.
	creatingTargets := typeutil.NewSet[string]()
	if c.splitState != nil {
		creatingTargets.Insert(c.splitState.CreatingTargetChannels(ctx, collectionID)...)
	}

	// get channels which exists on next target, but not on dist
	for name, channel := range nextTargetMap {
		_, existOnDist := distMap[name]
		if !existOnDist && !creatingTargets.Contain(name) {
			toLoad = append(toLoad, channel)
		}
	}

	// release a fully-handed-off split source: after adoption datacoord marks the
	// source ShardDropped, but it lingers in dist and the target list. A dropped
	// source is released only once the targets that replaced it are serving, so it
	// fronts nothing by then and the split key range is never left unserved
	// (design defense 3). Each dropped source is gated on its OWN targets
	// (source_vchannel); when that mapping is absent (older meta) it falls back to
	// the collection-wide check.
	//
	// It is ALSO gated on the source having left the current target, and that
	// second gate is what keeps reads alive. GetShardLeaders enumerates the
	// current target, so a source released while it is still listed there leaves
	// a channel with no leader — and one such channel fails the whole call, not
	// just that shard, so every read of the collection errors with "no available
	// shard leaders" until the current target catches up. Measured at 44s in an
	// E2E run: the source was unsubscribed at 19:14:28 and was still being
	// enumerated at 19:15:09.
	//
	// The two conditions are not the same: the targets serve as soon as they are
	// loaded, while the current target advances only once EVERY next-target
	// channel has a delegator that is both synced and data-ready. Waiting for the
	// later one costs nothing — the source is only a safety net by then — and
	// holding it cannot stall the advance, because the advance looks at the next
	// target, which no longer contains it.
	if c.splitState != nil {
		droppedSources := c.splitState.DroppedSourceChannels(ctx, collectionID)
		if len(droppedSources) > 0 {
			droppedSet := typeutil.NewSet(droppedSources...)
			for _, src := range droppedSources {
				if _, stillRouted := currentTargetMap[src]; stillRouted {
					// reads still fan out here; releasing now would break them.
					continue
				}
				// Which targets came from THIS source is provenance, and lives in
				// the split task rather than the collection meta -- so the check
				// is the collection-wide one: every live target must be serving
				// before any retired source is released. That is strictly
				// stronger than "this source's own targets are serving", so it
				// can only delay a release, never allow an early one.
				if !liveTargetsServing(nextTargetMap, dist, droppedSet) {
					continue
				}
				for _, ch := range dist {
					if ch.GetChannelName() == src {
						toRelease = append(toRelease, ch)
					}
				}
			}
		}
	}

	return toLoad, toRelease
}

// serviceableSet collects the channels in dist that have a serviceable leader.
func serviceableSet(dist []*meta.DmChannel) typeutil.Set[string] {
	serviceable := typeutil.NewSet[string]()
	for _, ch := range dist {
		if ch.View != nil && ch.View.Status.GetServiceable() {
			serviceable.Insert(ch.GetChannelName())
		}
	}
	return serviceable
}

// allChannelsServing reports whether every one of the given channels has a
// serviceable leader in dist. Used to gate a dropped split source's release on
// its own targets serving.
func allChannelsServing(channels []string, dist []*meta.DmChannel) bool {
	serviceable := serviceableSet(dist)
	for _, ch := range channels {
		if !serviceable.Contain(ch) {
			return false
		}
	}
	return true
}

// liveTargetsServing reports whether every non-dropped target channel of the
// collection has a serviceable leader in dist. It is the collection-wide
// fallback used to release a dropped split source when its own source_vchannel
// mapping is unavailable.
func liveTargetsServing(nextTargetMap map[string]*meta.DmChannel, dist []*meta.DmChannel, droppedSources typeutil.Set[string]) bool {
	serviceable := serviceableSet(dist)
	for name := range nextTargetMap {
		if droppedSources.Contain(name) {
			continue
		}
		if !serviceable.Contain(name) {
			return false
		}
	}
	return true
}

func (c *ChannelChecker) findRepeatedChannels(ctx context.Context, replicaID int64) []*meta.DmChannel {
	replica := c.meta.Get(ctx, replicaID)
	dupChannels := make([]*meta.DmChannel, 0)

	if replica == nil {
		mlog.Info(ctx, "replica does not exist, skip it")
		return dupChannels
	}

	delegatorList := c.dist.ChannelDistManager.GetByFilter(meta.WithReplica2Channel(replica))
	for _, delegator := range delegatorList {
		leader := c.dist.ChannelDistManager.GetShardLeader(delegator.GetChannelName(), replica)
		if leader == nil {
			mlog.Warn(ctx, "channel leader does not exist, skip it", mlog.String("channel", delegator.GetChannelName()))
			continue
		}
		// if channel's version is smaller than shard leader's version, it means that the channel is not up to date
		if delegator.Version < leader.Version && delegator.Node != leader.Node {
			dupChannels = append(dupChannels, delegator)
		}
	}

	return dupChannels
}

func (c *ChannelChecker) createChannelLoadTask(ctx context.Context, channels []*meta.DmChannel, replica *meta.Replica) []task.Task {
	plans := make([]assign.ChannelAssignPlan, 0)
	for _, ch := range channels {
		var rwNodes []int64
		if streamingutil.IsStreamingServiceEnabled() {
			rwNodes = replica.GetRWSQNodes()
		} else {
			if rwNodes = replica.GetChannelRWNodes(ch.GetChannelName()); len(rwNodes) == 0 {
				rwNodes = replica.GetRWNodes()
			}
		}
		plan := c.assignPolicy.AssignChannel(ctx, replica.GetCollectionID(), []*meta.DmChannel{ch}, rwNodes, true)
		plans = append(plans, plan...)
	}

	for i := range plans {
		plans[i].Replica = replica
	}

	// TODO: same known limitation as SegmentChecker.createSegmentLoadTasks --
	// a channel whose real watch time (L0/growing backlog, seek distance)
	// consistently exceeds ChannelTaskTimeout never converges: killed and
	// rebuilt with the same budget every check tick, no backoff or retry cap.
	return balance.CreateChannelTasksFromPlans(ctx, c.ID(), Params.QueryCoordCfg.ChannelTaskTimeout.GetAsDuration(time.Millisecond), plans)
}

func (c *ChannelChecker) createChannelReduceTasks(ctx context.Context, channels []*meta.DmChannel, replica *meta.Replica) []task.Task {
	ret := make([]task.Task, 0, len(channels))
	for _, ch := range channels {
		action := task.NewChannelAction(ch.Node, task.ActionTypeReduce, ch.GetChannelName())
		task, err := task.NewChannelTask(ctx, Params.QueryCoordCfg.ChannelTaskTimeout.GetAsDuration(time.Millisecond), c.ID(), ch.GetCollectionID(), replica, action)
		if err != nil {
			mlog.Warn(ctx, "create channel reduce task failed",
				mlog.Int64("collection", ch.GetCollectionID()),
				mlog.Int64("replica", replica.GetID()),
				mlog.String("channel", ch.GetChannelName()),
				mlog.Int64("from", ch.Node),
				mlog.Err(err),
			)
			continue
		}
		ret = append(ret, task)
	}
	return ret
}

func (c *ChannelChecker) getTraceCtx(ctx context.Context, collectionID int64) context.Context {
	coll := c.meta.GetCollection(ctx, collectionID)
	if coll == nil || coll.LoadSpan == nil {
		return ctx
	}

	return trace.ContextWithSpan(ctx, coll.LoadSpan)
}
