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

package balance

import (
	"context"
	"math"
	"sort"

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

const shardDiskBalanceEpsilon = 1e-6

type shardDiskTaskViewer interface {
	GetTasks(filters ...task.TaskFilter) []task.Task
}

type shardDiskMoveCandidate struct {
	shard  string
	target int64
}

func (b *ScoreBasedBalancer) balanceNamespacePartitionShardDisk(ctx context.Context, br *balanceReport, replica *meta.Replica) ([]assign.SegmentAssignPlan, []assign.ChannelAssignPlan) {
	rwNodes := replica.GetRWNodes()
	if len(rwNodes) < 2 {
		br.AddRecord(StrRecord("no enough rwNodes to balance namespace-partition shards"))
		return nil, nil
	}
	if b.nodeManager == nil || b.dist == nil || b.targetMgr == nil {
		br.AddRecord(StrRecord("missing dependencies to balance namespace-partition shards"))
		return nil, nil
	}

	leadersByShard, segmentsByShard, collectionShards := b.collectShardDistributions(replica)
	nodeDiskBytes, shardNodeDiskBytes := b.collectShardDiskStats(rwNodes, collectionShards)
	if len(shardNodeDiskBytes) == 0 {
		br.AddRecord(StrRecord("no shard disk stats found for namespace-partition collection"))
		return nil, nil
	}

	busyShards := make(map[string]struct{})
	b.applyInflightShardDiskProjection(replica, nodeDiskBytes, shardNodeDiskBytes, busyShards)

	candidate, ok := findShardDiskMoveCandidate(rwNodes, nodeDiskBytes, shardNodeDiskBytes, busyShards)
	if !ok {
		br.AddRecord(StrRecord("no namespace-partition shard disk balance candidate found"))
		return nil, nil
	}

	leader := leadersByShard[candidate.shard]
	if leader == nil {
		br.AddRecord(StrRecordf("skip shard %s because no query delegator is available", candidate.shard))
		return nil, nil
	}
	if leader.Node != candidate.target {
		return nil, []assign.ChannelAssignPlan{
			{
				Channel: leader,
				Replica: replica,
				From:    leader.Node,
				To:      candidate.target,
			},
		}
	}

	segmentPlans := b.genShardSegmentPlans(ctx, replica, candidate.target, segmentsByShard[candidate.shard])
	if len(segmentPlans) == 0 {
		br.AddRecord(StrRecordf("shard %s delegator is already on target %d and has no movable sealed segment", candidate.shard, candidate.target))
		return nil, nil
	}
	return segmentPlans, nil
}

func (b *ScoreBasedBalancer) collectShardDistributions(replica *meta.Replica) (map[string]*meta.DmChannel, map[string][]*meta.Segment, map[string]struct{}) {
	leadersByShard := make(map[string]*meta.DmChannel)
	segmentsByShard := make(map[string][]*meta.Segment)
	collectionShards := make(map[string]struct{})

	channels := b.dist.ChannelDistManager.GetByFilter(
		meta.WithReplica2Channel(replica),
		meta.WithCollectionID2Channel(replica.GetCollectionID()),
	)
	for _, channel := range channels {
		shard := channel.GetChannelName()
		if shard == "" {
			continue
		}
		collectionShards[shard] = struct{}{}
		leadersByShard[shard] = b.dist.ChannelDistManager.GetShardLeader(shard, replica)
	}

	segments := b.dist.SegmentDistManager.GetByFilter(
		meta.WithReplica(replica),
		meta.WithCollectionID(replica.GetCollectionID()),
	)
	for _, segment := range segments {
		shard := segment.GetInsertChannel()
		if shard == "" {
			continue
		}
		collectionShards[shard] = struct{}{}
		segmentsByShard[shard] = append(segmentsByShard[shard], segment)
	}
	for _, segments := range segmentsByShard {
		sort.Slice(segments, func(i, j int) bool {
			return segments[i].GetID() < segments[j].GetID()
		})
	}

	return leadersByShard, segmentsByShard, collectionShards
}

func (b *ScoreBasedBalancer) collectShardDiskStats(rwNodes []int64, collectionShards map[string]struct{}) (map[int64]float64, map[string]map[int64]float64) {
	nodeDiskBytes := make(map[int64]float64, len(rwNodes))
	shardNodeDiskBytes := make(map[string]map[int64]float64)

	for _, nodeID := range rwNodes {
		nodeDiskBytes[nodeID] = 0
		node := b.nodeManager.Get(nodeID)
		if node == nil {
			continue
		}
		for _, stat := range node.CacheShardDiskUsageStats() {
			if stat == nil || stat.GetShard() == "" || stat.GetDiskBytes() <= 0 {
				continue
			}
			if _, ok := collectionShards[stat.GetShard()]; !ok {
				continue
			}
			if _, ok := shardNodeDiskBytes[stat.GetShard()]; !ok {
				shardNodeDiskBytes[stat.GetShard()] = make(map[int64]float64)
			}
			shardNodeDiskBytes[stat.GetShard()][nodeID] += stat.GetDiskBytes()
			nodeDiskBytes[nodeID] += stat.GetDiskBytes()
		}
	}

	return nodeDiskBytes, shardNodeDiskBytes
}

func (b *ScoreBasedBalancer) applyInflightShardDiskProjection(replica *meta.Replica, nodeDiskBytes map[int64]float64, shardNodeDiskBytes map[string]map[int64]float64, busyShards map[string]struct{}) {
	viewer, ok := b.scheduler.(shardDiskTaskViewer)
	if !ok {
		return
	}

	projectedShards := make(map[string]struct{})
	tasks := viewer.GetTasks(
		task.WithCollectionID2TaskFilter(replica.GetCollectionID()),
		task.WithReplicaID2TaskFilter(replica.GetID()),
	)
	for _, activeTask := range tasks {
		shard := activeTask.Shard()
		if shard == "" {
			continue
		}
		busyShards[shard] = struct{}{}
		if task.GetTaskType(activeTask) != task.TaskTypeMove {
			continue
		}
		if _, ok := projectedShards[shard]; ok {
			continue
		}
		target, ok := moveTaskTarget(activeTask)
		if !ok {
			continue
		}
		if _, ok := nodeDiskBytes[target]; !ok {
			continue
		}
		projectShardDiskPlacement(nodeDiskBytes, shardNodeDiskBytes, shard, target)
		projectedShards[shard] = struct{}{}
	}
}

func moveTaskTarget(activeTask task.Task) (int64, bool) {
	for _, action := range activeTask.Actions() {
		if action.Type() == task.ActionTypeGrow {
			return action.Node(), true
		}
	}
	return 0, false
}

func findShardDiskMoveCandidate(rwNodes []int64, nodeDiskBytes map[int64]float64, shardNodeDiskBytes map[string]map[int64]float64, busyShards map[string]struct{}) (shardDiskMoveCandidate, bool) {
	beforeRange := shardDiskRange(rwNodes, nodeDiskBytes)
	if beforeRange <= shardDiskBalanceEpsilon {
		return shardDiskMoveCandidate{}, false
	}

	sources := append([]int64(nil), rwNodes...)
	sort.Slice(sources, func(i, j int) bool {
		if math.Abs(nodeDiskBytes[sources[i]]-nodeDiskBytes[sources[j]]) <= shardDiskBalanceEpsilon {
			return sources[i] < sources[j]
		}
		return nodeDiskBytes[sources[i]] > nodeDiskBytes[sources[j]]
	})

	targets := append([]int64(nil), rwNodes...)
	sort.Slice(targets, func(i, j int) bool {
		if math.Abs(nodeDiskBytes[targets[i]]-nodeDiskBytes[targets[j]]) <= shardDiskBalanceEpsilon {
			return targets[i] < targets[j]
		}
		return nodeDiskBytes[targets[i]] < nodeDiskBytes[targets[j]]
	})

	for _, source := range sources {
		sourceShards := sourceShardDiskWeights(source, shardNodeDiskBytes, busyShards)
		if len(sourceShards) == 0 {
			continue
		}
		for _, shard := range sourceShards {
			for _, target := range targets {
				if source == target || nodeDiskBytes[source] <= nodeDiskBytes[target]+shardDiskBalanceEpsilon {
					continue
				}
				after := cloneNodeDiskBytes(nodeDiskBytes)
				projectShardDiskPlacement(after, shardNodeDiskBytes, shard.name, target)
				if shardDiskRange(rwNodes, after)+shardDiskBalanceEpsilon < beforeRange {
					return shardDiskMoveCandidate{
						shard:  shard.name,
						target: target,
					}, true
				}
			}
		}
	}

	return shardDiskMoveCandidate{}, false
}

type shardDiskWeight struct {
	name   string
	weight float64
}

func sourceShardDiskWeights(source int64, shardNodeDiskBytes map[string]map[int64]float64, busyShards map[string]struct{}) []shardDiskWeight {
	shards := make([]shardDiskWeight, 0)
	for shard, nodeWeights := range shardNodeDiskBytes {
		if _, busy := busyShards[shard]; busy {
			continue
		}
		weight := nodeWeights[source]
		if weight <= 0 {
			continue
		}
		shards = append(shards, shardDiskWeight{name: shard, weight: weight})
	}
	sort.Slice(shards, func(i, j int) bool {
		if math.Abs(shards[i].weight-shards[j].weight) <= shardDiskBalanceEpsilon {
			return shards[i].name < shards[j].name
		}
		return shards[i].weight > shards[j].weight
	})
	return shards
}

func projectShardDiskPlacement(nodeDiskBytes map[int64]float64, shardNodeDiskBytes map[string]map[int64]float64, shard string, target int64) {
	var total float64
	for nodeID, diskBytes := range shardNodeDiskBytes[shard] {
		total += diskBytes
		nodeDiskBytes[nodeID] -= diskBytes
		if nodeDiskBytes[nodeID] < shardDiskBalanceEpsilon {
			nodeDiskBytes[nodeID] = 0
		}
	}
	nodeDiskBytes[target] += total
}

func shardDiskRange(rwNodes []int64, nodeDiskBytes map[int64]float64) float64 {
	minDisk := math.Inf(1)
	maxDisk := math.Inf(-1)
	for _, nodeID := range rwNodes {
		diskBytes := nodeDiskBytes[nodeID]
		if diskBytes < minDisk {
			minDisk = diskBytes
		}
		if diskBytes > maxDisk {
			maxDisk = diskBytes
		}
	}
	if math.IsInf(minDisk, 0) || math.IsInf(maxDisk, 0) {
		return 0
	}
	return maxDisk - minDisk
}

func cloneNodeDiskBytes(nodeDiskBytes map[int64]float64) map[int64]float64 {
	cloned := make(map[int64]float64, len(nodeDiskBytes))
	for nodeID, diskBytes := range nodeDiskBytes {
		cloned[nodeID] = diskBytes
	}
	return cloned
}

func (b *ScoreBasedBalancer) genShardSegmentPlans(ctx context.Context, replica *meta.Replica, target int64, segments []*meta.Segment) []assign.SegmentAssignPlan {
	segmentRefCount := make(map[int64]int, len(segments))
	for _, segment := range segments {
		segmentRefCount[segment.GetID()]++
	}

	plans := make([]assign.SegmentAssignPlan, 0, len(segments))
	for _, segment := range segments {
		if segment.Node == target {
			continue
		}
		if segmentRefCount[segment.GetID()] != 1 {
			continue
		}
		if !b.targetMgr.CanSegmentBeMoved(ctx, segment.GetCollectionID(), segment.GetID()) {
			continue
		}
		plans = append(plans, assign.SegmentAssignPlan{
			Segment: segment,
			Replica: replica,
			From:    segment.Node,
			To:      target,
		})
	}
	return plans
}
