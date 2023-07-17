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

package task

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func Wait(ctx context.Context, timeout time.Duration, tasks ...Task) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var err error
	go func() {
		for _, task := range tasks {
			err = task.Wait()
			if err != nil {
				cancel()
				break
			}
		}
		cancel()
	}()
	<-ctx.Done()

	return err
}

func SetPriority(priority Priority, tasks ...Task) {
	for i := range tasks {
		tasks[i].SetPriority(priority)
	}
}

func SetReason(reason string, tasks ...Task) {
	for i := range tasks {
		tasks[i].SetReason(reason)
	}
}

// GetTaskType returns the task's type,
// for now, only 3 types;
// - only 1 grow action -> Grow
// - only 1 reduce action -> Reduce
// - 1 grow action, and ends with 1 reduce action -> Move
func GetTaskType(task Task) Type {
	if len(task.Actions()) > 1 {
		return TaskTypeMove
	} else if task.Actions()[0].Type() == ActionTypeGrow {
		return TaskTypeGrow
	} else {
		return TaskTypeReduce
	}
}

func packLoadSegmentRequest(
	task *SegmentTask,
	action Action,
	schema *schemapb.CollectionSchema,
	loadMeta *querypb.LoadMetaInfo,
	loadInfo *querypb.SegmentLoadInfo,
	indexInfo []*indexpb.IndexInfo,
) *querypb.LoadSegmentsRequest {
	return &querypb.LoadSegmentsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_LoadSegments),
			commonpbutil.WithMsgID(task.ID()),
		),
		Infos:          []*querypb.SegmentLoadInfo{loadInfo},
		Schema:         schema,   // assign it for compatibility of rolling upgrade from 2.2.x to 2.3
		LoadMeta:       loadMeta, // assign it for compatibility of rolling upgrade from 2.2.x to 2.3
		CollectionID:   task.CollectionID(),
		ReplicaID:      task.ReplicaID(),
		DeltaPositions: []*msgpb.MsgPosition{loadInfo.GetDeltaPosition()}, // assign it for compatibility of rolling upgrade from 2.2.x to 2.3
		DstNodeID:      action.Node(),
		Version:        time.Now().UnixNano(),
		NeedTransfer:   true,
		IndexInfoList:  indexInfo,
	}
}

func packReleaseSegmentRequest(task *SegmentTask, action *SegmentAction) *querypb.ReleaseSegmentsRequest {
	return &querypb.ReleaseSegmentsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ReleaseSegments),
			commonpbutil.WithMsgID(task.ID()),
		),

		NodeID:       action.Node(),
		CollectionID: task.CollectionID(),
		SegmentIDs:   []int64{task.SegmentID()},
		Scope:        action.Scope(),
		Shard:        action.Shard(),
		NeedTransfer: false,
	}
}

func packLoadMeta(loadType querypb.LoadType, metricType string, collectionID int64, partitions ...int64) *querypb.LoadMetaInfo {
	return &querypb.LoadMetaInfo{
		LoadType:     loadType,
		CollectionID: collectionID,
		PartitionIDs: partitions,
		MetricType:   metricType,
	}
}

func packSubChannelRequest(
	task *ChannelTask,
	action Action,
	schema *schemapb.CollectionSchema,
	loadMeta *querypb.LoadMetaInfo,
	channel *meta.DmChannel,
	indexInfo []*indexpb.IndexInfo,
) *querypb.WatchDmChannelsRequest {
	return &querypb.WatchDmChannelsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_WatchDmChannels),
			commonpbutil.WithMsgID(task.ID()),
		),
		NodeID:        action.Node(),
		CollectionID:  task.CollectionID(),
		Infos:         []*datapb.VchannelInfo{channel.VchannelInfo},
		Schema:        schema,   // assign it for compatibility of rolling upgrade from 2.2.x to 2.3
		LoadMeta:      loadMeta, // assign it for compatibility of rolling upgrade from 2.2.x to 2.3
		ReplicaID:     task.ReplicaID(),
		Version:       time.Now().UnixNano(),
		IndexInfoList: indexInfo,
	}
}

func fillSubChannelRequest(
	ctx context.Context,
	req *querypb.WatchDmChannelsRequest,
	broker meta.Broker,
) error {
	segmentIDs := typeutil.NewUniqueSet()
	for _, vchannel := range req.GetInfos() {
		segmentIDs.Insert(vchannel.GetFlushedSegmentIds()...)
		segmentIDs.Insert(vchannel.GetUnflushedSegmentIds()...)
	}

	if segmentIDs.Len() == 0 {
		return nil
	}

	resp, err := broker.GetSegmentInfo(ctx, segmentIDs.Collect()...)
	if err != nil {
		return err
	}
	segmentInfos := make(map[int64]*datapb.SegmentInfo)
	for _, info := range resp.GetInfos() {
		segmentInfos[info.GetID()] = info
	}
	req.SegmentInfos = segmentInfos
	return nil
}

func packUnsubDmChannelRequest(task *ChannelTask, action Action) *querypb.UnsubDmChannelRequest {
	return &querypb.UnsubDmChannelRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_UnsubDmChannel),
			commonpbutil.WithMsgID(task.ID()),
		),
		NodeID:       action.Node(),
		CollectionID: task.CollectionID(),
		ChannelName:  task.Channel(),
	}
}

func getShardLeader(replicaMgr *meta.ReplicaManager, distMgr *meta.DistributionManager, collectionID, nodeID int64, channel string) (int64, bool) {
	replica := replicaMgr.GetByCollectionAndNode(collectionID, nodeID)
	if replica == nil {
		return 0, false
	}
	return distMgr.GetShardLeader(replica, channel)
}

func getMetricType(indexInfos []*indexpb.IndexInfo, schema *schemapb.CollectionSchema) (string, error) {
	vecField, err := typeutil.GetVectorFieldSchema(schema)
	if err != nil {
		return "", err
	}
	indexInfo, ok := lo.Find(indexInfos, func(info *indexpb.IndexInfo) bool {
		return info.GetFieldID() == vecField.GetFieldID()
	})
	if !ok || indexInfo == nil {
		err = fmt.Errorf("cannot find index info for %s field", vecField.GetName())
		return "", err
	}
	metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.MetricTypeKey, indexInfo.GetIndexParams())
	if err != nil {
		return "", err
	}
	return metricType, nil
}
