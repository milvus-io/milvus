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

package querycoord

import (
	"context"
	"sort"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

func getCompareMapFromSlice(sliceData []int64) map[int64]struct{} {
	compareMap := make(map[int64]struct{})
	for _, data := range sliceData {
		compareMap[data] = struct{}{}
	}

	return compareMap
}

func estimateSegmentSize(segmentLoadInfo *querypb.SegmentLoadInfo) int64 {
	segmentSize := int64(0)

	vecFieldID2IndexInfo := make(map[int64]*querypb.FieldIndexInfo)
	for _, fieldIndexInfo := range segmentLoadInfo.IndexInfos {
		if fieldIndexInfo.EnableIndex {
			fieldID := fieldIndexInfo.FieldID
			vecFieldID2IndexInfo[fieldID] = fieldIndexInfo
		}
	}

	for _, fieldBinlog := range segmentLoadInfo.BinlogPaths {
		fieldID := fieldBinlog.FieldID
		if FieldIndexInfo, ok := vecFieldID2IndexInfo[fieldID]; ok {
			segmentSize += FieldIndexInfo.IndexSize
		} else {
			segmentSize += getFieldSizeFromFieldBinlog(fieldBinlog)
		}
	}

	// get size of state data
	for _, fieldBinlog := range segmentLoadInfo.Statslogs {
		segmentSize += getFieldSizeFromFieldBinlog(fieldBinlog)
	}

	// get size of delete data
	for _, fieldBinlog := range segmentLoadInfo.Deltalogs {
		segmentSize += getFieldSizeFromFieldBinlog(fieldBinlog)
	}

	return segmentSize
}

func getFieldSizeFromFieldBinlog(fieldBinlog *datapb.FieldBinlog) int64 {
	fieldSize := int64(0)
	for _, binlog := range fieldBinlog.Binlogs {
		fieldSize += binlog.LogSize
	}

	return fieldSize

}

func getDstNodeIDByTask(t task) int64 {
	var nodeID int64
	switch t.msgType() {
	case commonpb.MsgType_LoadSegments:
		loadSegment := t.(*loadSegmentTask)
		nodeID = loadSegment.DstNodeID
	case commonpb.MsgType_WatchDmChannels:
		watchDmChannel := t.(*watchDmChannelTask)
		nodeID = watchDmChannel.NodeID
	case commonpb.MsgType_WatchDeltaChannels:
		watchDeltaChannel := t.(*watchDeltaChannelTask)
		nodeID = watchDeltaChannel.NodeID
	case commonpb.MsgType_ReleaseCollection:
		releaseCollection := t.(*releaseCollectionTask)
		nodeID = releaseCollection.NodeID
	case commonpb.MsgType_ReleasePartitions:
		releasePartition := t.(*releasePartitionTask)
		nodeID = releasePartition.NodeID
	case commonpb.MsgType_ReleaseSegments:
		releaseSegment := t.(*releaseSegmentTask)
		nodeID = releaseSegment.NodeID
	default:
		//TODO::
	}

	return nodeID
}

// syncReplicaSegments syncs the segments distribution of replica to shard leaders
// only syncs the segments in shards if not nil
func syncReplicaSegments(ctx context.Context, meta Meta, cluster Cluster, replicaID UniqueID, shards ...string) error {
	replica, err := meta.getReplicaByID(replicaID)
	if err != nil {
		return err
	}

	collectionSegments := make(map[UniqueID]*querypb.SegmentInfo)
	for _, segment := range meta.showSegmentInfos(replica.CollectionID, nil) {
		collectionSegments[segment.SegmentID] = segment
	}

	shardSegments := make(map[string][]*querypb.SegmentInfo) // DMC -> []SegmentInfo
	for _, segment := range collectionSegments {
		// Group segments by shard
		segments, ok := shardSegments[segment.DmChannel]
		if !ok {
			segments = make([]*querypb.SegmentInfo, 0)
		}

		segments = append(segments, segment)
		shardSegments[segment.DmChannel] = segments
	}

	for _, shard := range replica.ShardReplicas {
		if len(shards) > 0 && !isInShards(shard.DmChannelName, shards) {
			continue
		}

		segments := shardSegments[shard.DmChannelName]
		req := querypb.SyncReplicaSegmentsRequest{
			VchannelName:    shard.DmChannelName,
			ReplicaSegments: make([]*querypb.ReplicaSegmentsInfo, 0, len(segments)),
		}

		sort.Slice(segments, func(i, j int) bool {
			inode := getNodeInReplica(replica, segments[i].NodeIds)
			jnode := getNodeInReplica(replica, segments[j].NodeIds)

			return inode < jnode ||
				inode == jnode && segments[i].PartitionID < segments[j].PartitionID
		})

		for i, j := 0, 0; i < len(segments); i = j {
			node := getNodeInReplica(replica, segments[i].NodeIds)
			partition := segments[i].PartitionID

			j++
			for j < len(segments) &&
				getNodeInReplica(replica, segments[j].NodeIds) == node &&
				segments[j].PartitionID == partition {
				j++
			}

			segmentIds := make([]UniqueID, 0, len(segments[i:j]))
			for _, segment := range segments[i:j] {
				segmentIds = append(segmentIds, segment.SegmentID)
			}

			req.ReplicaSegments = append(req.ReplicaSegments, &querypb.ReplicaSegmentsInfo{
				NodeId:      node,
				PartitionId: partition,
				SegmentIds:  segmentIds,
			})
		}

		log.Debug("sync replica segments",
			zap.Int64("replicaID", replicaID),
			zap.Int64("leaderID", shard.LeaderID),
			zap.Any("req", req))
		err := cluster.SyncReplicaSegments(ctx, shard.LeaderID, &req)
		if err != nil {
			return err
		}
	}

	return nil
}

func isInShards(shard string, shards []string) bool {
	for _, item := range shards {
		if shard == item {
			return true
		}
	}

	return false
}

// getNodeInReplica gets the node which is in the replica
func getNodeInReplica(replica *milvuspb.ReplicaInfo, nodes []UniqueID) UniqueID {
	for _, node := range nodes {
		if nodeIncluded(node, replica.NodeIds) {
			return node
		}
	}

	return 0
}

func removeFromSlice(origin []UniqueID, del ...UniqueID) []UniqueID {
	set := make(typeutil.UniqueSet, len(origin))
	set.Insert(origin...)
	set.Remove(del...)

	return set.Collect()
}

func uniqueSlice(origin []UniqueID) []UniqueID {
	set := make(typeutil.UniqueSet, len(origin))
	set.Insert(origin...)
	return set.Collect()
}

// diffSlice returns a slice containing items in src but not in diff
func diffSlice(src []UniqueID, diff ...UniqueID) []UniqueID {
	set := make(typeutil.UniqueSet, len(src))
	set.Insert(src...)
	set.Remove(diff...)

	return set.Collect()
}

func getReplicaAvailableMemory(cluster Cluster, replica *milvuspb.ReplicaInfo) uint64 {
	availableMemory := uint64(0)
	nodes := getNodeInfos(cluster, replica.NodeIds)
	for _, node := range nodes {
		availableMemory += node.totalMem - node.memUsage
	}

	return availableMemory
}

func mergeWatchDeltaChannelInfo(infos []*datapb.VchannelInfo) []*datapb.VchannelInfo {
	minPositions := make(map[string]int)
	for index, info := range infos {
		_, ok := minPositions[info.ChannelName]
		if !ok {
			minPositions[info.ChannelName] = index
		}
		minTimeStampIndex := minPositions[info.ChannelName]
		if info.SeekPosition.GetTimestamp() < infos[minTimeStampIndex].SeekPosition.GetTimestamp() {
			minPositions[info.ChannelName] = index
		}
	}
	var result []*datapb.VchannelInfo
	for _, index := range minPositions {
		result = append(result, infos[index])
	}
	return result
}

func mergeDmChannelInfo(infos []*datapb.VchannelInfo) map[string]*datapb.VchannelInfo {
	minPositions := make(map[string]*datapb.VchannelInfo)
	for _, info := range infos {
		if _, ok := minPositions[info.ChannelName]; !ok {
			minPositions[info.ChannelName] = info
			continue
		}
		minPositionInfo := minPositions[info.ChannelName]
		if info.SeekPosition.GetTimestamp() < minPositionInfo.SeekPosition.GetTimestamp() {
			minPositionInfo.SeekPosition = info.SeekPosition
		}
		minPositionInfo.DroppedSegmentIds = append(minPositionInfo.DroppedSegmentIds, info.DroppedSegmentIds...)
		minPositionInfo.UnflushedSegmentIds = append(minPositionInfo.UnflushedSegmentIds, info.UnflushedSegmentIds...)
		minPositionInfo.FlushedSegmentIds = append(minPositionInfo.FlushedSegmentIds, info.FlushedSegmentIds...)
	}

	return minPositions
}
