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

package utils

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/blang/semver/v4"
	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func CheckNodeAvailable(nodeID int64, info *session.NodeInfo) error {
	if info == nil {
		return merr.WrapErrNodeOffline(nodeID)
	}
	return nil
}

// In a replica, a shard is available, if and only if:
// 1. The leader is online
// 2. All QueryNodes in the distribution are online
// 3. The last heartbeat response time is within HeartbeatAvailableInterval for all QueryNodes(include leader) in the distribution
// 4. All segments of the shard in target should be in the distribution
// 5. The delegator has caught up with streaming data
func CheckDelegatorDataReady(nodeMgr *session.NodeManager, targetMgr meta.TargetManagerInterface, leader *meta.LeaderView, scope int32) error {
	// Check whether leader is online
	info := nodeMgr.Get(leader.ID)
	if info == nil {
		err := merr.WrapErrNodeOffline(leader.ID)
		mlog.Info(context.TODO(), "leader is not available", mlog.Err(err))
		return merr.Wrap(err, "leader not available")
	}

	// Check if delegator is still catching up with streaming data
	if leader.Status != nil && leader.Status.GetCatchingUpStreamingData() {
		mlog.RatedInfo(context.TODO(), rate.Limit(10), "leader is not available due to still catching up streaming data",
			mlog.String("channel", leader.Channel))
		return merr.WrapErrChannelNotAvailable(leader.Channel, "still catching up streaming data")
	}

	segmentDist := targetMgr.GetSealedSegmentsByChannel(context.TODO(), leader.CollectionID, leader.Channel, scope)
	// Check whether segments are fully loaded
	for segmentID := range segmentDist {
		version, exist := leader.Segments[segmentID]
		if !exist {
			mlog.RatedInfo(context.TODO(), rate.Limit(10), "leader is not available due to lack of segment", mlog.Int64("segmentID", segmentID))
			return merr.WrapErrSegmentLack(segmentID)
		}

		// Check whether segment's worker node is online
		info := nodeMgr.Get(version.GetNodeID())
		if info == nil {
			err := merr.WrapErrNodeOffline(leader.ID)
			mlog.Info(context.TODO(), "leader is not available due to QueryNode unavailable",
				mlog.Int64("segmentID", segmentID),
				mlog.Err(err))
			return err
		}
	}
	return nil
}

func CheckSegmentDataReady(ctx context.Context, collectionID int64, distManager *meta.DistributionManager, targetMgr meta.TargetManagerInterface, scope int32) error {
	// Check whether segments are fully loaded
	segmentDist := targetMgr.GetSealedSegmentsByCollection(ctx, collectionID, scope)
	distSegments := distManager.SegmentDistManager.GetByFilter(meta.WithCollectionID(collectionID))
	distBySegmentID := make(map[int64][]*meta.Segment, len(distSegments))
	for _, segment := range distSegments {
		distBySegmentID[segment.GetID()] = append(distBySegmentID[segment.GetID()], segment)
	}

	for segmentID, segmentInfo := range segmentDist {
		segments := distBySegmentID[segmentID]
		if len(segments) == 0 {
			mlog.RatedInfo(context.TODO(), rate.Limit(10), "segment is not available", mlog.Int64("segmentID", segmentID))
			return merr.WrapErrSegmentLack(segmentID)
		}

		for _, segment := range segments {
			cmp, err := packed.CompareManifestPath(segment.ManifestPath, segmentInfo.GetManifestPath())
			if err != nil {
				mlog.RatedWarn(context.TODO(), rate.Limit(10), "segment manifest path not comparable",
					mlog.Int64("segmentID", segmentID),
					mlog.String("distManifest", segment.ManifestPath),
					mlog.String("targetManifest", segmentInfo.GetManifestPath()),
					mlog.Err(err))
				return err
			}
			if cmp < 0 {
				// dist manifest is older than target, segment data is not ready yet
				mlog.RatedInfo(context.TODO(), rate.Limit(10), "segment manifest is outdated",
					mlog.Int64("segmentID", segmentID),
					mlog.String("distManifest", segment.ManifestPath),
					mlog.String("targetManifest", segmentInfo.GetManifestPath()))
				return merr.WrapErrSegmentNotLoaded(segmentID)
			}
			// cmp >= 0: dist manifest is same or newer than target.
			// Still check DataVersion for storage v2 binlog changes that don't move the manifest.
			// Skip when the QueryNode did not report DataVersion (old node in mixed-version rollout).
			if segment.DataVersion != nil && *segment.DataVersion < segmentInfo.GetDataVersion() {
				mlog.RatedInfo(context.TODO(), rate.Limit(10), "segment data version is outdated",
					mlog.Int64("segmentID", segmentID),
					mlog.Int32("distDataVersion", *segment.DataVersion),
					mlog.Int32("targetDataVersion", segmentInfo.GetDataVersion()))
				return merr.WrapErrSegmentNotLoaded(segmentID)
			}
		}
	}
	return nil
}

func checkLoadStatus(ctx context.Context, m *meta.Meta, collectionID int64, withUnserviceableShards bool) error {
	percentage := m.CalculateLoadPercentage(ctx, collectionID)
	if percentage < 0 {
		err := merr.WrapErrCollectionNotLoaded(collectionID)
		mlog.Warn(ctx, "failed to GetShardLeaders", mlog.Err(err))
		return err
	}
	// When the caller accepts unserviceable shards (e.g. proxy refreshing its
	// shard-leader cache during replica reconfig), skip the full-load gate so
	// the caller can route by the per-leader Serviceable flag instead.
	if withUnserviceableShards {
		return nil
	}
	collection := m.GetCollection(ctx, collectionID)
	if collection != nil && collection.GetStatus() == querypb.LoadStatus_Loaded {
		// when collection is loaded, regard collection as readable, set percentage == 100
		percentage = 100
	}

	if percentage < 100 {
		err := merr.WrapErrCollectionNotFullyLoaded(collectionID)
		msg := fmt.Sprintf("collection %v is not fully loaded", collectionID)
		mlog.Warn(ctx, msg)
		return err
	}
	return nil
}

func GetShardLeadersWithChannels(
	ctx context.Context,
	m *meta.Meta,
	dist *meta.DistributionManager,
	nodeMgr *session.NodeManager,
	collectionID int64,
	channels map[string]*meta.DmChannel,
	withUnserviceableShards bool,
) ([]*querypb.ShardLeadersList, error) {
	return GetShardLeadersWithChannelsAndReplicaFilter(ctx, m, dist, nodeMgr, collectionID, channels, withUnserviceableShards, nil)
}

func GetShardLeadersWithChannelsAndReplicaFilter(
	ctx context.Context,
	m *meta.Meta,
	dist *meta.DistributionManager,
	nodeMgr *session.NodeManager,
	collectionID int64,
	channels map[string]*meta.DmChannel,
	withUnserviceableShards bool,
	replicaFilter func(*meta.Replica) bool,
) ([]*querypb.ShardLeadersList, error) {
	replicas := m.GetByCollection(ctx, collectionID)
	if replicaFilter != nil {
		replicas = lo.Filter(replicas, func(replica *meta.Replica, _ int) bool {
			return replicaFilter(replica)
		})
	}
	return buildShardLeadersFromReplicas(ctx, dist, nodeMgr, channels, withUnserviceableShards, replicas)
}

// buildShardLeadersFromReplicas is the leader-assembly half of
// GetShardLeadersWithChannelsAndReplicaFilter, split out so a caller that has
// already selected its replicas can hand that exact slice over instead of
// having the walk re-read the replica set.
//
// That matters wherever a caller decided something about the replicas before
// calling: two unsynchronized reads of the same concurrent map are two
// different snapshots, and TransferReplica / MoveReplica rewrite a replica's
// resource group by replacing the object, so a commit landing between them
// changes which replicas the second read selects. GetShardLeadersByResourceGroup
// runs its scoped full-load stand-in over its selection and would otherwise
// have the walk re-derive a different one, turning ordinary in-flight
// reconfiguration into a non-retriable ChannelNotAvailable.
func buildShardLeadersFromReplicas(
	ctx context.Context,
	dist *meta.DistributionManager,
	nodeMgr *session.NodeManager,
	channels map[string]*meta.DmChannel,
	withUnserviceableShards bool,
	replicas []*meta.Replica,
) ([]*querypb.ShardLeadersList, error) {
	ret := make([]*querypb.ShardLeadersList, 0)

	for _, channel := range channels {
		ids := make([]int64, 0, len(replicas))
		addrs := make([]string, 0, len(replicas))
		serviceable := make([]bool, 0, len(replicas))
		for _, replica := range replicas {
			leader := dist.ChannelDistManager.GetShardLeader(channel.GetChannelName(), replica)
			if leader == nil || (!withUnserviceableShards && !leader.IsServiceable()) {
				mlog.RatedWarn(ctx, rate.Limit(1.0/60.0), "leader is not available in replica",
					mlog.String("channel", channel.GetChannelName()), mlog.Int64("replicaID", replica.GetID()))
				continue
			}
			info := nodeMgr.Get(leader.Node)
			if info != nil {
				ids = append(ids, info.ID())
				addrs = append(addrs, info.Addr())
				serviceable = append(serviceable, leader.IsServiceable())
			}
		}

		if len(ids) == 0 && !withUnserviceableShards {
			err := merr.WrapErrChannelNotAvailable(channel.GetChannelName())
			msg := fmt.Sprintf("channel %s is not available in any replica", channel.GetChannelName())
			mlog.Warn(ctx, msg, mlog.Err(err))
			return nil, err
		}

		ret = append(ret, &querypb.ShardLeadersList{
			ChannelName: channel.GetChannelName(),
			NodeIds:     ids,
			NodeAddrs:   addrs,
			Serviceable: serviceable,
		})
	}

	return ret, nil
}

func GetShardLeaders(ctx context.Context,
	m *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	dist *meta.DistributionManager,
	nodeMgr *session.NodeManager,
	collectionID int64,
	withUnserviceableShards bool,
) ([]*querypb.ShardLeadersList, error) {
	return GetShardLeadersWithReplicaFilter(ctx, m, targetMgr, dist, nodeMgr, collectionID, withUnserviceableShards, nil)
}

func GetShardLeadersWithReplicaFilter(ctx context.Context,
	m *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	dist *meta.DistributionManager,
	nodeMgr *session.NodeManager,
	collectionID int64,
	withUnserviceableShards bool,
	replicaFilter func(*meta.Replica) bool,
) ([]*querypb.ShardLeadersList, error) {
	if err := checkLoadStatus(ctx, m, collectionID, withUnserviceableShards); err != nil {
		return nil, err
	}

	channels := targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.CurrentTarget)
	if len(channels) == 0 {
		msg := "loaded collection do not found any channel in target, may be in recovery"
		err := merr.WrapErrCollectionOnRecovering(collectionID, msg)
		mlog.Warn(ctx, "failed to get channels", mlog.Err(err))
		return nil, err
	}
	return GetShardLeadersWithChannelsAndReplicaFilter(ctx, m, dist, nodeMgr, collectionID, channels, withUnserviceableShards, replicaFilter)
}

// GetShardLeadersByResourceGroup answers GetShardLeaders restricted to the
// replicas whose own resource group is resourceGroup. It is a separate entry
// point rather than a replicaFilter over GetShardLeadersWithReplicaFilter
// because the scoped question needs a DIFFERENT load gate, not just a
// different filter:
//
//   - The registered-at-all half of checkLoadStatus runs FIRST, before any
//     resource-group reasoning, so a collection that is not loaded at all
//     answers ErrCollectionNotLoaded for the scoped shape exactly as for the
//     unscoped one. The proxy's retry policy branches on that code
//     (lb_policy.go), so the two shapes must not answer different families
//     for the same state.
//   - The full-load half of checkLoadStatus does not run at all: it reads the
//     collection-wide percentage, which a sibling resource group still
//     loading keeps below 100 -- the exact state the scope exists to see
//     through. shouldUpdateCurrentTarget pools ready delegators across
//     replicas, so the leading group's delegators alone promote the current
//     target while the collection-wide figure lags; gating on that figure
//     would refuse the leading group until the laggard finishes, inverting
//     the scope's purpose and contradicting
//     ShardLeaderReadinessByResourceGroup, which deliberately bypasses the
//     same gate. In its place the strict form runs the SCOPED equivalent of
//     that half -- every current-target channel must have a serviceable,
//     query-visible leader inside this group -- and refuses with the same
//     retriable ErrCollectionNotFullyLoaded the unscoped gate uses.
//
// The strict form (withUnserviceableShards == false) therefore has four
// refusals, and the retry semantics of each are part of the contract because
// merr.Status copies the sentinel's retriable bit onto the wire and the
// generic gRPC wrapper re-issues the call only when it is set. Two of them --
// the FIRST and the THIRD below -- run before the withUnserviceableShards
// branch, so a loose caller can reach those two as well; what it never gets
// is the second and the fourth, which are the resource-group-specific pair.
//
//   - ErrCollectionNotLoaded (101, non-retriable): the collection is not
//     registered as loaded at all. Same family, same code as the unscoped
//     shape for the same state.
//   - ErrReplicaNotFound (400, non-retriable): the collection is loaded, but
//     this resource group holds no replica of it. Terminal by construction --
//     the answer cannot change until someone loads the collection into this
//     group -- and refused up front, by name. Falling through to the channel
//     walk would blame the channel instead, which misleads twice over: the
//     channel is fine (a sibling group may be serving it right now), and it
//     invites a retry that will never succeed. This is the shard-leader
//     counterpart of LoadPercentageByResourceGroup's -1.
//   - ErrCollectionOnRecovering (106, retriable): the collection is
//     registered as loaded but has no channel in the current target, which is
//     what a collection under recovery looks like. Not resource-group
//     specific -- the unscoped path answers it for the same state -- and, like
//     the not-loaded refusal above, it precedes the withUnserviceableShards
//     branch, so a loose caller gets it too.
//   - ErrCollectionNotFullyLoaded (103, retriable): this group holds a
//     replica, but not every shard has a serviceable leader in it yet.
//     Waiting is exactly the right response, so this must NOT be the
//     non-retriable per-channel ErrChannelNotAvailable (503) the unscoped
//     path raises after its full-load gate has already passed: there, a
//     missing leader on a fully loaded collection really is channel-level
//     unavailability, while here it is ordinary load progress. Reserving 103
//     for it also keeps the scoped and unscoped shapes on one story -- both
//     answer 103 while the collection is still coming up, and they differ
//     only in whose progress they measure.
//
// A caller accepting unserviceable shards (the proxy refreshing its cache)
// gets neither the name-refusal nor the coverage refusal -- for a group that
// holds nothing, or holds a replica that cannot serve yet, it wants the empty
// answer instead.
func GetShardLeadersByResourceGroup(ctx context.Context,
	m *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	dist *meta.DistributionManager,
	nodeMgr *session.NodeManager,
	collectionID int64,
	resourceGroup string,
	withUnserviceableShards bool,
) ([]*querypb.ShardLeadersList, error) {
	// An empty resource group is the absence of a filter, not a filter that
	// matches nothing. That is what the proto field documents and what both
	// sibling surfaces implement (LoadPercentageByResourceGroup,
	// ShardLeaderReadinessByResourceGroup); comparing it literally here would
	// make this the one function of the three where an unset field means "no
	// replica matches". There is no scoped question to answer, and the scoped
	// gate above is only justified by a named group, so hand the request back
	// to the unscoped path whole -- which also keeps the unscoped answer
	// byte-identical to what it was before this field existed.
	if resourceGroup == "" {
		return GetShardLeadersWithReplicaFilter(ctx, m, targetMgr, dist, nodeMgr, collectionID, withUnserviceableShards,
			func(replica *meta.Replica) bool {
				return replica.IsQueryVisible()
			})
	}

	// withUnserviceableShards=true here is "registered-at-all only": see the
	// gate rationale above.
	if err := checkLoadStatus(ctx, m, collectionID, true); err != nil {
		return nil, err
	}

	// holds and scoped answer two different questions and must be counted
	// separately: a query-invisible replica (load-config spawns replicas
	// invisible until every one of them is serviceable) still means the
	// collection lives in this group, so it keeps the group out of the
	// terminal ReplicaNotFound bucket, but no query can be routed to its
	// leader, so it cannot make a shard count as covered. This is the same
	// split ShardLeaderReadinessByResourceGroup makes between inRG and its
	// replica list.
	// One read of the replica set feeds everything below: the holds verdict,
	// the coverage gate, and the leader assembly. See the note at the
	// buildShardLeadersFromReplicas call for why re-reading would be a bug.
	holds := false
	scoped := make([]*meta.Replica, 0)
	for _, replica := range m.GetByCollection(ctx, collectionID) {
		if replica.GetResourceGroup() != resourceGroup {
			continue
		}
		holds = true
		if replica.IsQueryVisible() {
			scoped = append(scoped, replica)
		}
	}
	if !withUnserviceableShards && !holds {
		// merr.Wrapf rather than WrapErrReplicaNotFound: the latter stamps
		// its argument as a replica id, and the only id in hand here is
		// the collection's.
		err := merr.Wrapf(merr.ErrReplicaNotFound,
			"collection %d has no replica in resource group %q", collectionID, resourceGroup)
		mlog.Warn(ctx, "failed to get shard leaders", mlog.Err(err))
		return nil, err
	}

	channels := targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.CurrentTarget)
	if len(channels) == 0 {
		msg := "loaded collection do not found any channel in target, may be in recovery"
		err := merr.WrapErrCollectionOnRecovering(collectionID, msg)
		mlog.Warn(ctx, "failed to get channels", mlog.Err(err))
		return nil, err
	}

	// The scoped stand-in for the full-load gate. It uses the same three
	// per-leader conditions the assembly below applies, over the same replica
	// snapshot, so a group that clears this walk clears that one too: the
	// remaining way the call below can answer ChannelNotAvailable is a leader
	// ceasing to be serviceable between the two reads of the DISTRIBUTION,
	// which is genuine channel-level unavailability and is what that error
	// means. The replica set cannot shift underneath, which is the point of
	// passing scoped down rather than letting the walk re-read it.
	if !withUnserviceableShards {
		uncovered := make([]string, 0, len(channels))
		for _, channel := range channels {
			if !hasServiceableLeaderInReplicas(dist, nodeMgr, channel.GetChannelName(), scoped) {
				uncovered = append(uncovered, channel.GetChannelName())
			}
		}
		if len(uncovered) > 0 {
			// channels arrives as a map, so its iteration order is random;
			// sort so that one state always prints one line.
			sort.Strings(uncovered)
			err := merr.WrapErrCollectionNotFullyLoaded(collectionID,
				fmt.Sprintf("resource group %q has no serviceable leader yet for shard(s) %v",
					resourceGroup, uncovered))
			mlog.Warn(ctx, "failed to get shard leaders", mlog.Err(err))
			return nil, err
		}
	}

	// A request that names a resource group is asking which leaders THAT group
	// can serve from, and the answer is not derivable from the unscoped one:
	// the response flattens every replica into one list per channel, and a
	// replica may borrow nodes from another resource group, so node-set
	// membership is not replica membership. The replica is only in hand here.
	//
	// scoped is handed over rather than re-derived by the walk so that the
	// coverage gate above and the assembly below see ONE snapshot of the
	// replica set. Re-reading would take a second, unsynchronized snapshot,
	// and TransferReplica / MoveReplica rewrite a replica's resource group by
	// replacing the object: a transfer out of this group committing between
	// the two reads would leave the walk with no replica to serve from and
	// answer the non-retriable ChannelNotAvailable -- for ordinary in-flight
	// reconfiguration, and with the exact code the contract above argues must
	// not be used for a group in transition.
	return buildShardLeadersFromReplicas(ctx, dist, nodeMgr, channels, withUnserviceableShards, scoped)
}

// CheckCollectionsQueryable check all channels are watched and all segments are loaded for this collection
func CheckCollectionsQueryable(ctx context.Context, m *meta.Meta, targetMgr meta.TargetManagerInterface, dist *meta.DistributionManager, nodeMgr *session.NodeManager) error {
	maxInterval := paramtable.Get().QueryCoordCfg.UpdateCollectionLoadStatusInterval.GetAsDuration(time.Minute)
	for _, coll := range m.GetAllCollections(ctx) {
		err := checkCollectionQueryable(ctx, m, targetMgr, dist, nodeMgr, coll)
		// the collection is not queryable, if meet following conditions:
		// 1. Some segments are not loaded
		// 2. Collection is not starting to release
		// 3. The load percentage has not been updated in the last 5 minutes.
		if err != nil && m.Exist(ctx, coll.CollectionID) && time.Since(coll.UpdatedAt) >= maxInterval {
			mlog.Warn(ctx, "collection not querable",
				mlog.Int64("collectionID", coll.CollectionID),
				mlog.Time("lastUpdated", coll.UpdatedAt),
				mlog.Duration("maxInterval", maxInterval),
				mlog.Err(err))
			return err
		}
	}
	return nil
}

// checkCollectionQueryable check all channels are watched and all segments are loaded for this collection
func checkCollectionQueryable(ctx context.Context, m *meta.Meta, targetMgr meta.TargetManagerInterface, dist *meta.DistributionManager, nodeMgr *session.NodeManager, coll *meta.Collection) error {
	collectionID := coll.GetCollectionID()
	if err := checkLoadStatus(ctx, m, collectionID, false); err != nil {
		return err
	}

	channels := targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.CurrentTarget)
	if len(channels) == 0 {
		msg := "loaded collection do not found any channel in target, may be in recovery"
		err := merr.WrapErrCollectionOnRecovering(collectionID, msg)
		mlog.Warn(ctx, "failed to get channels", mlog.Err(err))
		return err
	}

	shardList, err := GetShardLeadersWithChannels(ctx, m, dist, nodeMgr, collectionID, channels, false)
	if err != nil {
		return err
	}

	if len(channels) != len(shardList) {
		return merr.WrapErrCollectionNotFullyLoaded(collectionID, "still have unwatched channels or loaded segments")
	}

	return nil
}

// GetChannelRWAndRONodesFor260 gets the RW and RO nodes of the channel.
func GetChannelRWAndRONodesFor260(replica *meta.Replica, nodeManager *session.NodeManager) ([]int64, []int64) {
	rwNodes, roNodes := replica.GetRWSQNodes(), replica.GetROSQNodes()
	if rwQueryNodesLessThan260 := filterNodeLessThan260(replica.GetRWNodes(), nodeManager); len(rwQueryNodesLessThan260) > 0 {
		// Add rwNodes to roNodes to balance channels from querynode to streamingnode forcely.
		roNodes = append(roNodes, rwQueryNodesLessThan260...)
		mlog.Debug(context.TODO(), "find querynode need to balance channel to streamingnode", mlog.Int64s("rwQueryNodesLessThan260", rwQueryNodesLessThan260))
	}
	roNodes = append(roNodes, replica.GetRONodes()...)
	return rwNodes, roNodes
}

// filterNodeLessThan260 filter the query nodes that version is less than 2.6.0
func filterNodeLessThan260(nodes []int64, nodeManager *session.NodeManager) []int64 {
	checker := semver.MustParseRange(">=2.6.0-dev")
	filteredNodes := make([]int64, 0)
	for _, nodeID := range nodes {
		if session := nodeManager.Get(nodeID); session != nil && checker(session.Version()) {
			continue
		}
		filteredNodes = append(filteredNodes, nodeID)
	}
	return filteredNodes
}
