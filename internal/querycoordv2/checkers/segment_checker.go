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
	"sort"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const initialTargetVersion = int64(0)

type collectionVersionCache struct {
	targetVersion      int64
	segmentDistVersion int64
	channelDistVersion int64
}

type SegmentChecker struct {
	*checkerActivation
	meta         *meta.Meta
	dist         *meta.DistributionManager
	targetMgr    meta.TargetManagerInterface
	nodeMgr      *session.NodeManager
	scheduler    task.Scheduler
	assignPolicy assign.AssignPolicy

	// version cache for fast skip when nothing changed
	versionCache map[int64]*collectionVersionCache

	// replicasWithRegularNodes holds the ID of every replica that has been
	// seen with a regular RW query node and whose resource group has not
	// given regular nodes up since. It is what tells a replica whose regular
	// node is merely away from one that has none to wait for (see
	// createSegmentLoadTasks); it is set on every check of a replica
	// (noteRegularNodes) and dropped at the top of every round for a replica
	// that is gone or whose group holds no regular node and asks for none
	// (forgetReleasedReplicas).
	replicasWithRegularNodes typeutil.UniqueSet
}

func NewSegmentChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr meta.TargetManagerInterface,
	nodeMgr *session.NodeManager,
	scheduler task.Scheduler,
) *SegmentChecker {
	// Create RoundRobin assign policy in constructor to maximize loading speed
	// Note: RoundRobin may break short-term balance but prioritizes loading speed
	assignPolicy := assign.GetGlobalAssignPolicyFactory().GetPolicy(assign.PolicyTypeRoundRobin)

	return &SegmentChecker{
		checkerActivation: newCheckerActivation(),
		meta:              meta,
		dist:              dist,
		targetMgr:         targetMgr,
		nodeMgr:           nodeMgr,
		scheduler:         scheduler,
		assignPolicy:      assignPolicy,
		versionCache:      make(map[int64]*collectionVersionCache),

		replicasWithRegularNodes: typeutil.NewUniqueSet(),
	}
}

func (c *SegmentChecker) ID() utils.CheckerType {
	return utils.SegmentChecker
}

func (c *SegmentChecker) Description() string {
	return "SegmentChecker checks the lack of segments, or some segments are redundant"
}

func (c *SegmentChecker) readyToCheck(ctx context.Context, collectionID int64) bool {
	metaExist := (c.meta.GetCollection(ctx, collectionID) != nil)
	targetExist := c.targetMgr.IsNextTargetExist(ctx, collectionID) || c.targetMgr.IsCurrentTargetExist(ctx, collectionID, common.AllPartitionsID)

	return metaExist && targetExist
}

func (c *SegmentChecker) Check(ctx context.Context) []task.Task {
	if !c.IsActive() {
		return nil
	}

	// Before any replica is checked, so that the regular-node record the
	// placement reads is as current as the round's own view of the
	// resource groups. Refreshed after the replicas instead, a round whose
	// record was stale would wait, produce no task, and mark the
	// collection synced. Dropping a record also drops the collection from
	// the version cache below: a group giving regular nodes up moves
	// neither the target nor the distribution version, so on a quiet
	// cluster the fast path would otherwise keep skipping the collection,
	// and the segment that is now free to be placed would wait for the
	// next target re-pull (NextTargetSurviveTime, minutes) instead of
	// this round.
	c.forgetReleasedReplicas(ctx)

	collectionIDs := c.meta.GetAll(ctx)
	for _, cid := range collectionIDs {
		if c.readyToCheck(ctx, cid) {
			// Fast path: skip if target and dist versions unchanged
			currentTargetVersion := c.targetMgr.GetCollectionTargetVersion(ctx, cid, meta.NextTarget)
			currentSegmentDistVersion := c.dist.SegmentDistManager.GetVersion()
			currentChannelDistVersion := c.dist.ChannelDistManager.GetVersion()
			if c.isCollectionSynced(cid, currentTargetVersion, currentSegmentDistVersion, currentChannelDistVersion) {
				continue
			}

			replicas := c.meta.GetByCollection(ctx, cid)
			hasTask := false
			for _, r := range replicas {
				tasks := c.checkReplica(ctx, r)
				// Add tasks immediately after checking each replica to reduce
				// the time window between task generation and addition.
				// This prevents duplicate segment loading when dist updates
				// and old tasks are removed during the window.
				for _, t := range tasks {
					hasTask = true
					if err := c.scheduler.Add(t); err != nil {
						t.Cancel(err)
					}
				}
			}

			// Only update version cache if no tasks were generated
			// If tasks were generated, we need to re-check next time
			if !hasTask {
				c.updateVersionCache(cid, currentTargetVersion, currentSegmentDistVersion, currentChannelDistVersion)
			}
		}
	}

	// clean up version cache for released collections
	c.cleanVersionCache(collectionIDs)

	// find already released segments which are not contained in target
	results := make([]task.Task, 0)
	segments := c.dist.SegmentDistManager.GetByFilter()
	released := utils.FilterReleased(segments, collectionIDs)
	reduceTasks := c.createSegmentReduceTasks(ctx, released, meta.NilReplica, querypb.DataScope_Historical)
	task.SetReason("collection released", reduceTasks...)
	task.SetPriority(task.TaskPriorityNormal, reduceTasks...)
	results = append(results, reduceTasks...)

	// clean node which has been move out from replica
	for _, nodeInfo := range c.nodeMgr.GetAll() {
		nodeID := nodeInfo.ID()
		segmentsOnQN := c.dist.SegmentDistManager.GetByFilter(meta.WithNodeID(nodeID))
		collectionSegments := lo.GroupBy(segmentsOnQN, func(segment *meta.Segment) int64 { return segment.GetCollectionID() })
		for collectionID, segments := range collectionSegments {
			replica := c.meta.GetByCollectionAndNode(ctx, collectionID, nodeID)
			if replica == nil {
				reduceTasks := c.createSegmentReduceTasks(ctx, segments, meta.NilReplica, querypb.DataScope_Historical)
				task.SetReason("dirty segment exists", reduceTasks...)
				task.SetPriority(task.TaskPriorityNormal, reduceTasks...)
				results = append(results, reduceTasks...)
			}
		}
	}

	return results
}

// isCollectionSynced checks if target and dist versions are unchanged since last check
func (c *SegmentChecker) isCollectionSynced(collectionID int64, targetVersion, segmentDistVersion, channelDistVersion int64) bool {
	cache, ok := c.versionCache[collectionID]
	if !ok {
		return false
	}
	return cache.targetVersion == targetVersion &&
		cache.segmentDistVersion == segmentDistVersion &&
		cache.channelDistVersion == channelDistVersion
}

// updateVersionCache updates the version cache for a collection
func (c *SegmentChecker) updateVersionCache(collectionID int64, targetVersion, segmentDistVersion, channelDistVersion int64) {
	c.versionCache[collectionID] = &collectionVersionCache{
		targetVersion:      targetVersion,
		segmentDistVersion: segmentDistVersion,
		channelDistVersion: channelDistVersion,
	}
}

// cleanVersionCache removes entries for collections that no longer exist.
// Only runs when cache has more entries than active collections, meaning stale entries exist.
func (c *SegmentChecker) cleanVersionCache(activeCollections []int64) {
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

func (c *SegmentChecker) checkReplica(ctx context.Context, replica *meta.Replica) []task.Task {
	ret := make([]task.Task, 0)
	c.noteRegularNodes(replica)

	replicaSegmentDist := c.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithReplica(replica))
	delegatorList := c.dist.ChannelDistManager.GetByFilter(meta.WithReplica2Channel(replica))
	ch2DelegatorList := lo.GroupBy(delegatorList, func(d *meta.DmChannel) string {
		return d.View.Channel
	})

	// compare with targets to find the lack and redundancy of segments
	lacks, loadPriorities, redundancies, toUpdate := c.getSealedSegmentDiff(ctx, replica.GetCollectionID(), replica, replicaSegmentDist)
	tasks := c.createSegmentLoadTasks(c.getTraceCtx(ctx, replica.GetCollectionID()), lacks, loadPriorities, replica)
	task.SetReason("lacks of segment", tasks...)
	task.SetPriority(task.TaskPriorityNormal, tasks...)
	ret = append(ret, tasks...)

	tasks = c.createSegmentReopenTasks(c.getTraceCtx(ctx, replica.GetCollectionID()), toUpdate, replica)
	task.SetReason("segment updated", tasks...)
	task.SetPriority(task.TaskPriorityNormal, tasks...)
	ret = append(ret, tasks...)

	// sealed segments resident on a streaming node's query node while the
	// replica has a regular one: moved, like a balancer's move, at its
	// priority (CreateSegmentTasksFromPlans sets Low for a move)
	tasks = c.createMisplacedSegmentMoveTasks(c.getTraceCtx(ctx, replica.GetCollectionID()), replica, replicaSegmentDist)
	task.SetReason("sealed segment misplaced on a streaming query node", tasks...)
	ret = append(ret, tasks...)

	redundancies = c.filterOutSegmentInUse(ctx, replica, redundancies, ch2DelegatorList)
	tasks = c.createSegmentReduceTasks(c.getTraceCtx(ctx, replica.GetCollectionID()), redundancies, replica, querypb.DataScope_Historical)
	task.SetReason("segment not exists in target", tasks...)
	task.SetPriority(task.TaskPriorityNormal, tasks...)
	ret = append(ret, tasks...)

	// compare inner dists to find repeated loaded segments
	redundancies = c.findRepeatedSealedSegments(ctx, replica, replicaSegmentDist)
	redundancies = c.filterOutExistedOnLeader(replica, redundancies, ch2DelegatorList)
	tasks = c.createSegmentReduceTasks(c.getTraceCtx(ctx, replica.GetCollectionID()), redundancies, replica, querypb.DataScope_Historical)
	task.SetReason("redundancies of segment", tasks...)
	// set deduplicate task priority to low, to avoid deduplicate task cancel balance task
	task.SetPriority(task.TaskPriorityLow, tasks...)
	ret = append(ret, tasks...)

	// compare with target to find the lack and redundancy of segments
	_, redundancies = c.getGrowingSegmentDiff(ctx, replica.GetCollectionID(), replica, delegatorList)
	tasks = c.createSegmentReduceTasks(c.getTraceCtx(ctx, replica.GetCollectionID()), redundancies, replica, querypb.DataScope_Streaming)
	task.SetReason("streaming segment not exists in target", tasks...)
	task.SetPriority(task.TaskPriorityNormal, tasks...)
	ret = append(ret, tasks...)

	return ret
}

// GetGrowingSegmentDiff get streaming segment diff between leader view and target
func (c *SegmentChecker) getGrowingSegmentDiff(ctx context.Context, collectionID int64,
	replica *meta.Replica,
	delegatorList []*meta.DmChannel,
) (toLoad []*datapb.SegmentInfo, toRelease []*meta.Segment) {
	if len(delegatorList) == 0 {
		return toLoad, toRelease
	}

	log := mlog.With(
		mlog.FieldCollectionID(collectionID),
		mlog.Int64("replicaID", replica.GetID()))

	// Hoisted out of the loop: all five depend only on collectionID. The two
	// GetGrowingSegmentsByCollection calls rebuild a UniqueSet over every DM
	// channel of the target, so an N-shard collection paid that N times per
	// replica per check round. Trade-off: all five now run even when every
	// delegator fails the version gate below, where the per-iteration form ran
	// only the first.
	targetVersion := c.targetMgr.GetCollectionTargetVersion(ctx, collectionID, meta.CurrentTarget)
	nextTargetExist := c.targetMgr.IsNextTargetExist(ctx, collectionID)
	nextTargetSegmentIDs := c.targetMgr.GetGrowingSegmentsByCollection(ctx, collectionID, meta.NextTarget)
	currentTargetSegmentIDs := c.targetMgr.GetGrowingSegmentsByCollection(ctx, collectionID, meta.CurrentTarget)
	currentTargetChannelMap := c.targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.CurrentTarget)

	for _, d := range delegatorList {
		view := d.View
		if view.TargetVersion != targetVersion {
			// before shard delegator update it's readable version, skip release segment
			log.RatedInfo(ctx, rate.Limit(20), "before shard delegator update it's readable version, skip release segment",
				mlog.String("channelName", view.Channel),
				mlog.FieldNodeID(view.ID),
				mlog.Int64("leaderVersion", view.TargetVersion),
				mlog.Int64("currentVersion", targetVersion),
			)
			continue
		}

		// get segment which exist on leader view, but not on current target and next target
		for _, segment := range view.GrowingSegments {
			if !currentTargetSegmentIDs.Contain(segment.GetID()) && nextTargetExist && !nextTargetSegmentIDs.Contain(segment.GetID()) {
				if channel, ok := currentTargetChannelMap[segment.InsertChannel]; ok {
					timestampInSegment := segment.GetStartPosition().GetTimestamp()
					timestampInTarget := channel.GetSeekPosition().GetTimestamp()
					// release growing segment if in dropped segment list
					if funcutil.SliceContain(channel.GetDroppedSegmentIds(), segment.GetID()) {
						log.Info(ctx, "growing segment exists in dropped segment list, release it", mlog.FieldSegmentID(segment.GetID()))
						toRelease = append(toRelease, segment)
						continue
					}
					// filter toRelease which seekPosition is newer than next target dmChannel
					if timestampInSegment < timestampInTarget {
						log.Info(ctx, "growing segment not exist in target, so release it",
							mlog.FieldSegmentID(segment.GetID()),
						)
						toRelease = append(toRelease, segment)
					}
				}
			}
		}
	}

	return toLoad, toRelease
}

// GetSealedSegmentDiff get historical segment diff between target and dist
func (c *SegmentChecker) getSealedSegmentDiff(
	ctx context.Context,
	collectionID int64,
	replica *meta.Replica,
	dist []*meta.Segment,
) (toLoad []*datapb.SegmentInfo, loadPriorities []commonpb.LoadPriority, toRelease []*meta.Segment, toUpdate []*meta.Segment) {
	sort.Slice(dist, func(i, j int) bool {
		return dist[i].Version < dist[j].Version
	})
	distMap := make(map[int64]*meta.Segment)
	for _, s := range dist {
		distMap[s.GetID()] = s
	}

	isSegmentLack := func(segment *datapb.SegmentInfo) bool {
		_, existInDist := distMap[segment.ID]
		return !existInDist
	}
	isSegmentUpdate := func(segment *datapb.SegmentInfo) bool {
		segInDist, existInDist := distMap[segment.ID]
		if !existInDist {
			return false
		}
		// Trigger reopen when storage v2 data version is behind the target.
		// DataVersion bumps on storage v2 binlog changes that don't necessarily
		// move the manifest version.
		// Skip when the QueryNode did not report DataVersion (nil pointer from
		// proto3 optional): during a mixed-version rollout an old QueryNode has
		// no way to advance DataVersion, so triggering Reopen would loop forever.
		if segInDist.DataVersion != nil && *segInDist.DataVersion < segment.GetDataVersion() {
			return true
		}
		// Trigger reopen when dist manifest is older than target manifest.
		// If dist manifest is same or newer (e.g., loaded after L0 compaction updated DataCoord),
		// the data is already up-to-date and no reopen is needed.
		cmp, err := packed.CompareManifestPath(segInDist.ManifestPath, segment.GetManifestPath())
		if err != nil {
			mlog.RatedWarn(ctx, rate.Limit(10), "manifest path not comparable, skip reopen",
				mlog.FieldSegmentID(segment.GetID()),
				mlog.String("distManifest", segInDist.ManifestPath),
				mlog.String("targetManifest", segment.GetManifestPath()),
				mlog.Err(err))
			return false
		}
		return cmp < 0
	}

	nextTargetExist := c.targetMgr.IsNextTargetExist(ctx, collectionID)
	nextTargetMap := c.targetMgr.GetSealedSegmentsByCollection(ctx, collectionID, meta.NextTarget)
	currentTargetExist := c.targetMgr.IsCurrentTargetExist(ctx, collectionID, common.AllPartitionsID)
	currentTargetMap := c.targetMgr.GetSealedSegmentsByCollection(ctx, collectionID, meta.CurrentTarget)

	// Hoisted out of the loop below, where it was resolved once per segment on
	// the refresh/import path and each call read-locks the collection manager's
	// coordinator-wide RWMutex. The pointer only: IsRefreshed() still reads live
	// state under the collection's own lock, so a refresh landing mid-loop is
	// still observed.
	collection := c.meta.GetCollection(ctx, collectionID)

	// Segment which exist on next target, but not on dist
	for _, segment := range nextTargetMap {
		if isSegmentLack(segment) {
			if currentTargetExist {
				_, existOnCurrent := currentTargetMap[segment.GetID()]
				if existOnCurrent {
					// Segment exists in current target but missing in dist -> Recovery scenario (HIGH priority)
					loadPriorities = append(loadPriorities, commonpb.LoadPriority_HIGH)
				} else {
					// Segment not in current target -> check if refresh in progress
					if collection != nil && !collection.IsRefreshed() {
						// Refresh scenario (import) -> Use user's configured priority
						loadPriorities = append(loadPriorities, replica.LoadPriority())
					} else {
						// Handoff scenario (growing -> sealed flush) -> LOW priority
						loadPriorities = append(loadPriorities, commonpb.LoadPriority_LOW)
					}
				}
			} else {
				// Initial Load -> Use user's configured priority
				loadPriorities = append(loadPriorities, replica.LoadPriority())
			}
			toLoad = append(toLoad, segment)
		}
		if isSegmentUpdate(segment) {
			toUpdate = append(toUpdate, distMap[segment.GetID()])
		}
	}

	// get segment which exist on dist, but not on current target and next target
	for _, segment := range dist {
		_, existOnCurrent := currentTargetMap[segment.GetID()]
		_, existOnNext := nextTargetMap[segment.GetID()]

		// l0 segment should be release with channel together
		if !existOnNext && nextTargetExist && !existOnCurrent {
			toRelease = append(toRelease, segment)
		}
	}

	return toLoad, loadPriorities, toRelease, toUpdate
}

func (c *SegmentChecker) findRepeatedSealedSegments(ctx context.Context, replica *meta.Replica, dist []*meta.Segment) []*meta.Segment {
	segments := make([]*meta.Segment, 0)
	versions := make(map[int64]*meta.Segment)
	for _, s := range dist {
		maxVer, ok := versions[s.GetID()]
		if !ok {
			versions[s.GetID()] = s
			continue
		}
		if maxVer.Version <= s.Version {
			segments = append(segments, maxVer)
			versions[s.GetID()] = s
		} else {
			segments = append(segments, s)
		}
	}

	return segments
}

// for duplicated segment, we should release the one which is not serving on leader
func (c *SegmentChecker) filterOutExistedOnLeader(replica *meta.Replica, segments []*meta.Segment, ch2DelegatorList map[string][]*meta.DmChannel) []*meta.Segment {
	notServing := make([]*meta.Segment, 0, len(segments))
	for _, s := range segments {
		delegatorList := ch2DelegatorList[s.GetInsertChannel()]
		if len(delegatorList) == 0 {
			continue
		}

		servingOnLeader := false
		for _, delegator := range delegatorList {
			segInView, ok := delegator.View.Segments[s.GetID()]
			if ok && segInView.NodeID == s.Node {
				servingOnLeader = true
				break
			}
		}

		if !servingOnLeader {
			notServing = append(notServing, s)
		}
	}
	return notServing
}

// for sealed segment which doesn't exist in target, we should release it after delegator has updated to latest readable version
func (c *SegmentChecker) filterOutSegmentInUse(ctx context.Context, replica *meta.Replica, segments []*meta.Segment, ch2DelegatorList map[string][]*meta.DmChannel) []*meta.Segment {
	notUsed := make([]*meta.Segment, 0, len(segments))
	for _, s := range segments {
		currentTargetVersion := c.targetMgr.GetCollectionTargetVersion(ctx, s.CollectionID, meta.CurrentTarget)
		partition := c.meta.GetPartition(ctx, s.PartitionID)

		delegatorList := ch2DelegatorList[s.GetInsertChannel()]
		if len(delegatorList) == 0 {
			continue
		}

		stillInUseByDelegator := false
		// if delegator has valid target version, and before it update to latest readable version, skip release it's sealed segment
		for _, delegator := range delegatorList {
			// Notice: if syncTargetVersion stuck, segment on delegator won't be released
			readableVersionNotUpdate := delegator.View.TargetVersion != initialTargetVersion && delegator.View.TargetVersion < currentTargetVersion
			if partition != nil && readableVersionNotUpdate {
				// leader view version hasn't been updated, segment maybe still in use
				stillInUseByDelegator = true
				break
			}
		}

		if !stillInUseByDelegator {
			notUsed = append(notUsed, s)
		}
	}
	return notUsed
}

func (c *SegmentChecker) createSegmentLoadTasks(ctx context.Context, segments []*datapb.SegmentInfo, loadPriorities []commonpb.LoadPriority, replica *meta.Replica) []task.Task {
	logger := mlog.With(
		mlog.FieldCollectionID(replica.GetCollectionID()),
		mlog.Int64("replicaID", replica.GetID()),
	)
	if len(segments) == 0 {
		return nil
	}
	priorityMap := make(map[int64]commonpb.LoadPriority)
	for i, s := range segments {
		priorityMap[s.GetID()] = loadPriorities[i]
	}

	shardSegments := lo.GroupBy(segments, func(s *datapb.SegmentInfo) string {
		return s.GetInsertChannel()
	})

	plans := make([]assign.SegmentAssignPlan, 0)
	for shard, segments := range shardSegments {
		// if channel is not subscribed yet, skip load segments
		leader := c.dist.ChannelDistManager.GetShardLeader(shard, replica)
		if leader == nil {
			logger.RatedInfo(ctx, rate.Limit(10), "no shard leader for replica to load segment",
				mlog.String("shard", shard))
			continue
		}

		rwNodes := replica.GetChannelRWNodes(shard)
		if len(rwNodes) == 0 {
			rwNodes = replica.GetRWNodes()
		}
		// A sealed segment belongs on a regular query node, and the split
		// between those and the streaming query nodes that carry delegators
		// stays exactly as it was: this only runs when the replica has NO
		// regular node at all, and none to wait for as far as this
		// coordinator can see (replicasWithRegularNodes, see
		// noteRegularNodes and forgetReleasedReplicas).
		//
		// That is not a broken replica. A resource group whose only compute
		// is a streaming node has none by construction - milvus keeps the
		// query node embedded in a streaming node out of the resource
		// manager, and `run streamingnode` enables that query node precisely
		// so it can serve. Without this the candidate set is empty, no plan
		// is produced, and the segment is never loaded: the delegator's
		// readable target version therefore never advances, the load sits at
		// partial progress until it times out, and nothing reports why,
		// because no task was ever created to fail.
		//
		// The "none to wait for" half is what keeps a MIXED group - a regular
		// query node next to the streaming node - off this path while its
		// regular node restarts. The node leaves the resource group
		// (handleNodeStopping/handleNodeDown unassign it) and the replica
		// (the replica observer flips it rw->ro and removes it) at once, so
		// for the length of the restart the replica reads exactly like a
		// streaming-only one, and so does its group's node set: neither the
		// RW set nor that set can tell the two apart, and the group's
		// configured node count cannot on its own either, since a form's
		// running query cluster asks for N regular nodes while its compute
		// is streaming nodes alone. What can is memory: a replica seen with
		// a regular node keeps its sealed segments for one, and they wait
		// while it is away, as on master.
		//
		// That memory reflects membership, not history alone. A regular
		// node can leave for good: the operator hands it to another group
		// (TransferNode), scales the group down to none, or moves the
		// replica itself into a group whose only compute is a streaming
		// node (TransferReplica keeps the replica's ID). Kept on history,
		// the memory would then gate every placement path of the replica
		// until a coordinator restart - empty RW set, no fallback, no task,
		// nothing logged, the delegator never reaching the new target. So
		// the record is dropped once the replica's group holds no regular
		// node AND asks for none (forgetReleasedReplicas): a group that
		// still asks for one is owed it, and the resource manager pulls one
		// in (recoverMissingNodeRG), so the replica keeps waiting; a group
		// that asks for none has given regular nodes up for good. A restart
		// in a group that asks for one or more keeps the record, since the
		// group's request outlives the node.
		//
		// A group that asks for no regular node yet holds one - the default
		// group, whose requests are 0 - keeps the record for as long as it
		// holds one; once it holds none the coordinator cannot tell a
		// restart from a scale-down, and the record goes. Its regular
		// node's restart is therefore the same bounded window as this
		// coordinator's own restart, whose memory is empty, as it is for a
		// replica just spawned by an update of the load config (job_update
		// puts it into meta before RecoverReplicaOfCollection gives it
		// nodes). A mixed group whose regular node happens to be away at
		// that moment, or such a fresh replica checked before it received
		// its node, gets ONE placement on the streaming node's query node;
		// the move pass in checkReplica (createMisplacedSegmentMoveTasks)
		// brings those segments back onto the regular node once it returns,
		// so the case is bounded to that one window and repairs itself.
		//
		// Only an installed form places segments there, the same gate the
		// admission in utils.AssignReplica keys on: a stock deployment never
		// admits a replica whose only compute is a streaming node, and if one
		// did lose its last regular node its segments would stay on the
		// streaming node's query node for good, since the balancers only walk
		// GetRWNodes. A stock binary keeps the empty candidate set it always
		// had.
		if len(rwNodes) == 0 && extension.FormInstalled() && streamingutil.IsStreamingServiceEnabled() &&
			!c.replicasWithRegularNodes.Contain(replica.GetID()) {
			rwNodes = replica.GetRWSQNodes()
		}

		segmentInfos := lo.Map(segments, func(s *datapb.SegmentInfo, _ int) *meta.Segment {
			return &meta.Segment{
				SegmentInfo: s,
			}
		})
		shardPlans := c.assignPolicy.AssignSegment(ctx, replica.GetCollectionID(), segmentInfos, rwNodes, true)
		for i := range shardPlans {
			shardPlans[i].Replica = replica
			shardPlans[i].LoadPriority = priorityMap[shardPlans[i].Segment.GetID()]
		}
		plans = append(plans, shardPlans...)
	}

	// TODO: this assumes a single segment always finishes loading within
	// SegmentTaskTimeout (5min default). If a segment's real load time is
	// consistently longer (large disk-index segment, throttled cold storage),
	// the task is killed by its deadline every round and rebuilt here with
	// the same budget on the next check tick -- it never converges. Needs
	// either backoff/a retry cap on repeated DeadlineExceeded rebuilds, or a
	// no-progress timeout instead of a flat per-task wall-clock budget.
	return balance.CreateSegmentTasksFromPlans(ctx, c.ID(), Params.QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond), plans)
}

// noteRegularNodes records that the replica has a regular RW query node, if
// it has one now. The record outlives the node: a replica whose regular node
// is away keeps it for as long as its group is owed one, which is what
// createSegmentLoadTasks reads to keep the replica's sealed segments waiting
// for that node rather than placing them on a streaming node's query node.
// It is called on every check of a replica, so the record is as fresh as the
// checker's own view.
func (c *SegmentChecker) noteRegularNodes(replica *meta.Replica) {
	if replica.RWNodesCount() > 0 {
		c.replicasWithRegularNodes.Insert(replica.GetID())
	}
}

// forgetReleasedReplicas drops the regular-node record of every replica that
// has released its claim to one: a replica that no longer exists, so the
// record stays bounded by the replicas there are, and a replica whose
// resource group has given regular nodes up (groupGaveRegularNodesUp), so
// the record reflects membership rather than history. It runs once per
// check round, before any replica is checked: one in-memory lookup per
// recorded replica, and one snapshot per resource group the recorded
// replicas are in.
//
// A live replica whose record is dropped has a placement that was gated a
// moment ago and is not any more, with nothing else having moved: its
// collection is taken out of the version cache so this round looks at it
// rather than skipping it as unchanged.
func (c *SegmentChecker) forgetReleasedReplicas(ctx context.Context) {
	released := make([]int64, 0)
	gaveUp := make(map[string]bool)
	c.replicasWithRegularNodes.Range(func(replicaID int64) bool {
		replica := c.meta.Get(ctx, replicaID)
		if replica == nil {
			released = append(released, replicaID)
			return true
		}
		groupName := replica.GetResourceGroup()
		answer, asked := gaveUp[groupName]
		if !asked {
			answer = c.groupGaveRegularNodesUp(ctx, groupName)
			gaveUp[groupName] = answer
		}
		if answer {
			released = append(released, replicaID)
			delete(c.versionCache, replica.GetCollectionID())
		}
		return true
	})
	c.replicasWithRegularNodes.Remove(released...)
}

// groupGaveRegularNodesUp answers whether the resource group holds no
// regular query node and asks for none: NodeNum is the regular nodes
// assigned to it now, MissingNumOfNodes the ones it is owed on top. A group
// that is owed one is merely waiting for it, and the resource manager pulls
// one in; a group that holds none and is owed none has given them up - the
// operator transferred its node away, scaled it to none, or the replica was
// moved into a group that never had any. A group the resource manager does
// not know is not judged: the replica keeps waiting, as on master.
func (c *SegmentChecker) groupGaveRegularNodesUp(ctx context.Context, groupName string) bool {
	group := c.meta.GetResourceGroup(ctx, groupName)
	if group == nil {
		return false
	}
	return group.NodeNum() == 0 && group.MissingNumOfNodes() == 0
}

// createMisplacedSegmentMoveTasks moves the sealed segments of the replica
// that sit on a streaming node's embedded query node onto a regular query
// node, once the replica has one.
//
// Such a segment was placed there by createSegmentLoadTasks while the group
// had no regular node, and nothing on master ever moves it back: the
// balancers walk GetRWNodes/GetRONodes only, and the redundancy pass here
// measures the distribution with meta.WithReplica, whose Contains includes
// the streaming query nodes, so the segment is neither lacking nor redundant
// as far as the target is concerned. It is misplaced, and this is the one
// pass that knows it. The move is one task - load on the regular node, then
// release from the streaming node - so the replica keeps serving the segment
// throughout, exactly as a balancer's move does; a bare release would leave a
// gap until the next round loaded it again.
//
// Only segments in the target are moved: one that has left the target is the
// redundancy pass's to release, and loading it onto a regular node first
// would be wasted work. Growing segments are not this checker's to place and
// live in the delegator's leader view, not in the segment distribution, so
// they are untouched by construction; so are the delegators themselves.
//
// Only an installed form runs this, as only an installed form places a
// sealed segment on a streaming node's query node in the first place. A
// stock binary keeps its checker round exactly as it was.
func (c *SegmentChecker) createMisplacedSegmentMoveTasks(ctx context.Context, replica *meta.Replica, dist []*meta.Segment) []task.Task {
	if !extension.FormInstalled() || replica.RWSQNodesCount()+replica.ROSQNodesCount() == 0 {
		return nil
	}
	misplaced := lo.Filter(dist, func(s *meta.Segment, _ int) bool {
		return replica.ContainSQNode(s.Node)
	})
	if len(misplaced) == 0 {
		return nil
	}
	targets := c.targetMgr.GetSealedSegmentsByCollection(ctx, replica.GetCollectionID(), meta.NextTargetFirst)
	misplaced = lo.Filter(misplaced, func(s *meta.Segment, _ int) bool {
		_, inTarget := targets[s.GetID()]
		return inTarget
	})

	logger := mlog.With(
		mlog.FieldCollectionID(replica.GetCollectionID()),
		mlog.Int64("replicaID", replica.GetID()),
	)
	plans := make([]assign.SegmentAssignPlan, 0)
	for shard, segments := range lo.GroupBy(misplaced, func(s *meta.Segment) string { return s.GetInsertChannel() }) {
		// A move loads the segment through the shard's delegator, as a load
		// does; without one the segment waits where it is.
		if c.dist.ChannelDistManager.GetShardLeader(shard, replica) == nil {
			logger.RatedInfo(ctx, rate.Limit(10), "no shard leader for replica to move a misplaced segment",
				mlog.String("shard", shard))
			continue
		}
		rwNodes := replica.GetChannelRWNodes(shard)
		if len(rwNodes) == 0 {
			rwNodes = replica.GetRWNodes()
		}
		if len(rwNodes) == 0 {
			// Still no regular node: the streaming node's query node is
			// where the segment belongs for now.
			continue
		}
		residentOn := lo.SliceToMap(segments, func(s *meta.Segment) (int64, int64) { return s.GetID(), s.Node })
		shardPlans := c.assignPolicy.AssignSegment(ctx, replica.GetCollectionID(), segments, rwNodes, true)
		for i := range shardPlans {
			shardPlans[i].From = residentOn[shardPlans[i].Segment.GetID()]
			shardPlans[i].Replica = replica
			shardPlans[i].LoadPriority = replica.LoadPriority()
		}
		plans = append(plans, shardPlans...)
	}
	if len(plans) == 0 {
		return nil
	}
	logger.Info(ctx, "moving sealed segments off a streaming node's query node onto a regular query node",
		mlog.Int64s("segmentIDs", lo.Map(plans, func(p assign.SegmentAssignPlan, _ int) int64 { return p.Segment.GetID() })))
	return balance.CreateSegmentTasksFromPlans(ctx, c.ID(), Params.QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond), plans)
}

func (c *SegmentChecker) createSegmentReopenTasks(ctx context.Context, segments []*meta.Segment, replica *meta.Replica) []task.Task {
	ret := make([]task.Task, 0, len(segments))
	for _, s := range segments {
		action := task.NewSegmentAction(s.Node, task.ActionTypeReopen, s.GetInsertChannel(), s.GetID())
		task, err := task.NewSegmentTask(
			ctx,
			Params.QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond),
			c.ID(),
			s.GetCollectionID(),
			replica,
			replica.LoadPriority(),
			action,
		)
		if err != nil {
			mlog.Warn(ctx, "create segment reopen task failed",
				mlog.Int64("collection", s.GetCollectionID()),
				mlog.Int64("replica", replica.GetID()),
				mlog.String("channel", s.GetInsertChannel()),
				mlog.Int64("from", s.Node),
				mlog.Err(err),
			)
			continue
		}

		ret = append(ret, task)
	}
	return ret
}

func (c *SegmentChecker) createSegmentReduceTasks(ctx context.Context, segments []*meta.Segment, replica *meta.Replica, scope querypb.DataScope) []task.Task {
	ret := make([]task.Task, 0, len(segments))
	for _, s := range segments {
		action := task.NewSegmentActionWithScope(s.Node, task.ActionTypeReduce, s.GetInsertChannel(), s.GetID(), scope, int(s.GetNumOfRows()))
		task, err := task.NewSegmentTask(
			ctx,
			Params.QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond),
			c.ID(),
			s.GetCollectionID(),
			replica,
			replica.LoadPriority(),
			action,
		)
		if err != nil {
			mlog.Warn(ctx, "create segment reduce task failed",
				mlog.Int64("collection", s.GetCollectionID()),
				mlog.Int64("replica", replica.GetID()),
				mlog.String("channel", s.GetInsertChannel()),
				mlog.Int64("from", s.Node),
				mlog.Err(err),
			)
			continue
		}

		ret = append(ret, task)
	}
	return ret
}

func (c *SegmentChecker) getTraceCtx(ctx context.Context, collectionID int64) context.Context {
	coll := c.meta.GetCollection(ctx, collectionID)
	if coll == nil || coll.LoadSpan == nil {
		return ctx
	}

	return trace.ContextWithSpan(ctx, coll.LoadSpan)
}
