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
	"fmt"
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
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
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

	redundancies = c.filterOutSegmentInUse(ctx, replica, redundancies, ch2DelegatorList)
	forced, redundancies := c.forceReleaseCandidates(ctx, replica, redundancies, lacks, replicaSegmentDist)
	tasks = c.createSegmentReduceTasks(c.getTraceCtx(ctx, replica.GetCollectionID()), redundancies, replica, querypb.DataScope_Historical)
	task.SetReason("segment not exists in target", tasks...)
	task.SetPriority(task.TaskPriorityNormal, tasks...)
	ret = append(ret, tasks...)

	tasks = c.createSegmentReduceTasks(c.getTraceCtx(ctx, replica.GetCollectionID()), forced, replica, querypb.DataScope_Historical)
	for _, t := range tasks {
		t.(*task.SegmentTask).SetForceRelease()
	}
	task.SetReason("delegator refused to release the segment for too long while the replica cannot load", tasks...)
	// high priority so that it replaces the ordinary release task the delegator keeps refusing
	task.SetPriority(task.TaskPriorityHigh, tasks...)
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

// forceReleaseCandidates splits the sealed segments to release into the ones to release by force
// and the rest. The delegator refuses to release a sealed segment it still serves, and the refusal
// clears on its own once the delegator is synced to a newer target -- which needs the replacement
// segments loaded. When the node is out of memory and the refused segments are what holds it, that
// never happens: the replica is stuck, and each next-target refresh adds to what it has to load.
// This is the way out, taken only when all of the following hold:
//
//   - the delegator has refused the segment for at least forceReleaseRefusedSegmentAfter,
//     counted from the first refusal under the current target (a target change restarts it);
//   - the replica has made no progress since: no segment loaded, current target unchanged;
//   - segments are waiting to be loaded, and every RW node of the replica is marked resource
//     exhausted -- the segment checker assigns loads round-robin and bypasses the exhaustion
//     filter, so a replica that cannot load anywhere has every node marked within a few rounds.
//     A load failing for another reason (a corrupt file, object storage down) marks nothing and
//     the fallback does not fire; freeing memory would not help there.
//
// When one segment of a channel qualifies, every segment of that channel in the list is forced
// with it: the delegator is unserviceable after the first one anyway, and this frees the most at
// once. The list is what the diff computed this round -- segments in neither the current nor the
// next target -- so the victims are revalidated at force time. Refusals of segments that left the
// distribution some other way are dropped here as well.
func (c *SegmentChecker) forceReleaseCandidates(ctx context.Context, replica *meta.Replica, toRelease []*meta.Segment,
	lacks []*datapb.SegmentInfo, dist []*meta.Segment,
) (forced, rest []*meta.Segment) {
	tracker := c.dist.ReleaseRefusals
	if tracker == nil {
		return nil, toRelease
	}
	distSegments := typeutil.NewUniqueSet(lo.Map(dist, func(s *meta.Segment, _ int) int64 { return s.GetID() })...)
	tracker.Prune(replica.GetID(), func(segmentID int64) bool { return distSegments.Contain(segmentID) })
	c.reportReleaseRefusals(replica.GetCollectionID())

	after := Params.QueryCoordCfg.ForceReleaseRefusedSegmentAfter.GetAsDuration(time.Second)
	if after <= 0 || len(toRelease) == 0 {
		return nil, toRelease
	}

	log := mlog.With(
		mlog.FieldCollectionID(replica.GetCollectionID()),
		mlog.Int64("replicaID", replica.GetID()))
	now := time.Now()
	currentVersion := c.targetMgr.GetCollectionTargetVersion(ctx, replica.GetCollectionID(), meta.CurrentTarget)
	lastLoad, _ := tracker.LastLoadSucceeded(replica.GetID())
	expiredChannels := typeutil.NewSet[string]()
	for _, s := range toRelease {
		refusal, ok := tracker.Get(replica.GetID(), s.GetID())
		if !ok || refusal.TargetVersion != currentVersion || now.Sub(refusal.Since) < after || lastLoad.After(refusal.Since) {
			continue
		}
		expiredChannels.Insert(s.GetInsertChannel())
	}
	if expiredChannels.Len() == 0 {
		return nil, toRelease
	}
	if len(lacks) == 0 {
		log.RatedInfo(ctx, rate.Limit(1.0/60.0), "release refused for too long but no segment waits to be loaded, not forcing",
			mlog.Strings("channels", expiredChannels.Collect()))
		return nil, toRelease
	}
	rwNodes := replica.GetRWNodes()
	if len(rwNodes) == 0 || !lo.EveryBy(rwNodes, c.nodeMgr.IsResourceExhausted) {
		log.RatedInfo(ctx, rate.Limit(1.0/60.0), "release refused for too long but some node of the replica can still load, not forcing",
			mlog.Strings("channels", expiredChannels.Collect()),
			mlog.Int64s("rwNodes", rwNodes))
		return nil, toRelease
	}

	for _, s := range toRelease {
		if expiredChannels.Contain(s.GetInsertChannel()) {
			forced = append(forced, s)
		} else {
			rest = append(rest, s)
		}
	}
	log.Warn(ctx, "forcing the release of segments the delegator refuses to release: the replica cannot load and every node is out of resources",
		mlog.Strings("channels", expiredChannels.Collect()),
		mlog.Int64s("segments", lo.Map(forced, func(s *meta.Segment, _ int) int64 { return s.GetID() })),
		mlog.Int64s("nodes", lo.Uniq(lo.Map(forced, func(s *meta.Segment, _ int) int64 { return s.Node }))),
		mlog.Int("waitingToLoad", len(lacks)),
		mlog.Duration("refusedAtLeast", after))
	return forced, rest
}

// reportReleaseRefusals publishes how many sealed segments of the collection the delegators refuse to
// release and for how long the longest refusal has lasted.
func (c *SegmentChecker) reportReleaseRefusals(collectionID int64) {
	refusals := c.dist.ReleaseRefusals.ListByCollection(collectionID)
	label := fmt.Sprint(collectionID)
	if len(refusals) == 0 {
		metrics.QueryCoordReleaseRefusedSegmentNum.DeleteLabelValues(label)
		metrics.QueryCoordReleaseRefusedMaxSeconds.DeleteLabelValues(label)
		return
	}
	now := time.Now()
	longest := time.Duration(0)
	for _, refusal := range refusals {
		if d := now.Sub(refusal.Since); d > longest {
			longest = d
		}
	}
	metrics.QueryCoordReleaseRefusedSegmentNum.WithLabelValues(label).Set(float64(len(refusals)))
	metrics.QueryCoordReleaseRefusedMaxSeconds.WithLabelValues(label).Set(longest.Seconds())
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
