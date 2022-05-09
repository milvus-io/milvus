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

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
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
	case commonpb.MsgType_WatchQueryChannels:
		watchQueryChannel := t.(*watchQueryChannelTask)
		nodeID = watchQueryChannel.NodeID
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

func syncReplicaSegments(ctx context.Context, cluster Cluster, childTasks []task) error {
	type SegmentIndex struct {
		NodeID      UniqueID
		PartitionID UniqueID
		ReplicaID   UniqueID
	}

	type ShardLeader struct {
		ReplicaID UniqueID
		LeaderID  UniqueID
	}

	shardSegments := make(map[string]map[SegmentIndex]typeutil.UniqueSet) // DMC -> set[Segment]
	shardLeaders := make(map[string][]*ShardLeader)                       // DMC -> leader
	for _, childTask := range childTasks {
		switch task := childTask.(type) {
		case *loadSegmentTask:
			nodeID := getDstNodeIDByTask(task)
			for _, segment := range task.Infos {
				segments, ok := shardSegments[segment.InsertChannel]
				if !ok {
					segments = make(map[SegmentIndex]typeutil.UniqueSet)
				}

				index := SegmentIndex{
					NodeID:      nodeID,
					PartitionID: segment.PartitionID,
					ReplicaID:   task.ReplicaID,
				}

				_, ok = segments[index]
				if !ok {
					segments[index] = make(typeutil.UniqueSet)
				}
				segments[index].Insert(segment.SegmentID)

				shardSegments[segment.InsertChannel] = segments
			}

		case *watchDmChannelTask:
			leaderID := getDstNodeIDByTask(task)
			leader := &ShardLeader{
				ReplicaID: task.ReplicaID,
				LeaderID:  leaderID,
			}

			for _, dmc := range task.Infos {
				leaders, ok := shardLeaders[dmc.ChannelName]
				if !ok {
					leaders = make([]*ShardLeader, 0)
				}

				leaders = append(leaders, leader)

				shardLeaders[dmc.ChannelName] = leaders
			}
		}
	}

	for dmc, leaders := range shardLeaders {
		segments, ok := shardSegments[dmc]
		if !ok {
			continue
		}

		for _, leader := range leaders {
			req := querypb.SyncReplicaSegmentsRequest{
				VchannelName:    dmc,
				ReplicaSegments: make([]*querypb.ReplicaSegmentsInfo, 0, len(segments)),
			}

			for index, segmentSet := range segments {
				if index.ReplicaID == leader.ReplicaID {
					req.ReplicaSegments = append(req.ReplicaSegments,
						&querypb.ReplicaSegmentsInfo{
							NodeId:      index.NodeID,
							PartitionId: index.PartitionID,
							SegmentIds:  segmentSet.Collect(),
						})
				}
			}

			err := cluster.syncReplicaSegments(ctx, leader.LeaderID, &req)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func removeFromSlice(origin []UniqueID, del ...UniqueID) []UniqueID {
	set := make(typeutil.UniqueSet, len(origin))
	set.Insert(origin...)
	set.Remove(del...)

	return set.Collect()
}
