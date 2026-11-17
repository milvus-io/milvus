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
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/collectionutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// idSource helper type for using id as task source
type idSource int64

func (s idSource) String() string {
	return fmt.Sprintf("ID-%d", s)
}

func WrapIDSource(id int64) Source {
	return idSource(id)
}

// Wait blocks until every task in tasks has finished (succeeded, failed, or
// been canceled) or ctx is done, whichever comes first. It does not impose
// any timeout of its own; callers that need to bound how long they'll wait
// should set a deadline on ctx themselves.
//
// The wait happens on the caller's goroutine on purpose. Handing it off to a
// helper goroutine and racing it against ctx would return to the caller on
// time but leave the helper parked on a task that has no lifetime guarantee
// of its own: a task's execution deadline is only armed on first dispatch (see
// Task.ActivateDeadline), so a task repeatedly bounced by the executor's
// admission cap never arms a deadline, and taskScheduler.checkStale has no
// time-based staleness check, so on a healthy cluster with a saturated
// executor the helper would stay parked indefinitely.
func Wait(ctx context.Context, tasks ...Task) error {
	for _, task := range tasks {
		if err := task.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
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
	switch {
	case len(task.Actions()) > 1:
		return TaskTypeMove
	case task.Actions()[0].Type() == ActionTypeGrow:
		return TaskTypeGrow
	case task.Actions()[0].Type() == ActionTypeReduce:
		return TaskTypeReduce
	case task.Actions()[0].Type() == ActionTypeUpdate || task.Actions()[0].Type() == ActionTypeReopen:
		return TaskTypeUpdate
	case task.Actions()[0].Type() == ActionTypeStatsUpdate:
		return TaskTypeStatsUpdate
	case task.Actions()[0].Type() == ActionTypeDropIndex:
		return TaskTypeDropIndex
	}
	return 0
}

func packLoadSegmentRequest(
	task *SegmentTask,
	action Action,
	schema *schemapb.CollectionSchema,
	collectionProperties []*commonpb.KeyValuePair,
	loadMeta *querypb.LoadMetaInfo,
	loadInfo *querypb.SegmentLoadInfo,
	indexInfo []*indexpb.IndexInfo,
) *querypb.LoadSegmentsRequest {
	loadScope := querypb.LoadScope_Full
	if action.Type() == ActionTypeUpdate {
		loadScope = querypb.LoadScope_Index
	}

	if action.Type() == ActionTypeStatsUpdate {
		loadScope = querypb.LoadScope_Stats
	}

	if action.Type() == ActionTypeReopen {
		loadScope = querypb.LoadScope_Reopen
	}

	if task.Source() == utils.LeaderChecker {
		loadScope = querypb.LoadScope_Delta
	}

	finalSchema := applyCollectionSettings(schema, collectionProperties)
	applyIndexWarmupSetting(loadInfo, finalSchema, collectionProperties)

	return &querypb.LoadSegmentsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_LoadSegments),
			commonpbutil.WithMsgID(task.ID()),
		),
		Infos:          []*querypb.SegmentLoadInfo{loadInfo},
		Schema:         finalSchema, // assign it for compatibility of rolling upgrade from 2.2.x to 2.3
		LoadMeta:       loadMeta,    // assign it for compatibility of rolling upgrade from 2.2.x to 2.3
		CollectionID:   task.CollectionID(),
		ReplicaID:      task.ReplicaID(),
		DeltaPositions: []*msgpb.MsgPosition{loadInfo.GetDeltaPosition()}, // assign it for compatibility of rolling upgrade from 2.2.x to 2.3
		DstNodeID:      action.Node(),
		Version:        time.Now().UnixNano(),
		NeedTransfer:   true,
		IndexInfoList:  indexInfo,
		LoadScope:      loadScope,
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
		Scope:        action.GetScope(),
		Shard:        action.GetShard(),
		NeedTransfer: false,
	}
}

func packLoadMeta(loadType querypb.LoadType, collectionInfo *milvuspb.DescribeCollectionResponse, resourceGroup string, loadFields []int64, partitions ...int64) *querypb.LoadMetaInfo {
	return &querypb.LoadMetaInfo{
		LoadType:      loadType,
		CollectionID:  collectionInfo.GetCollectionID(),
		PartitionIDs:  partitions,
		DbName:        collectionInfo.GetDbName(),
		ResourceGroup: resourceGroup,
		LoadFields:    loadFields,
		SchemaVersion: collectionInfo.GetUpdateTimestamp(),
	}
}

func packSubChannelRequest(
	task *ChannelTask,
	action Action,
	schema *schemapb.CollectionSchema,
	collectionProperties []*commonpb.KeyValuePair,
	loadMeta *querypb.LoadMetaInfo,
	channel *meta.DmChannel,
	indexInfo []*indexpb.IndexInfo,
	partitions []int64,
	targetVersion int64,
) *querypb.WatchDmChannelsRequest {
	finalSchema := applyCollectionSettings(schema, collectionProperties)
	return &querypb.WatchDmChannelsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_WatchDmChannels),
			commonpbutil.WithMsgID(task.ID()),
		),
		NodeID:        action.Node(),
		CollectionID:  task.CollectionID(),
		PartitionIDs:  partitions,
		Infos:         []*datapb.VchannelInfo{channel.VchannelInfo},
		Schema:        finalSchema, // assign it for compatibility of rolling upgrade from 2.2.x to 2.3
		LoadMeta:      loadMeta,    // assign it for compatibility of rolling upgrade from 2.2.x to 2.3
		ReplicaID:     task.ReplicaID(),
		Version:       time.Now().UnixNano(),
		IndexInfoList: indexInfo,
		TargetVersion: targetVersion,
	}
}

func fillSubChannelRequest(
	ctx context.Context,
	req *querypb.WatchDmChannelsRequest,
	broker meta.Broker,
	includeFlushed bool,
) error {
	segmentIDs := typeutil.NewUniqueSet()
	for _, vchannel := range req.GetInfos() {
		if includeFlushed {
			segmentIDs.Insert(vchannel.GetFlushedSegmentIds()...)
		}
		segmentIDs.Insert(vchannel.GetUnflushedSegmentIds()...)
		segmentIDs.Insert(vchannel.GetLevelZeroSegmentIds()...)
	}

	if segmentIDs.Len() == 0 {
		return nil
	}

	segmentInfos, err := broker.GetSegmentInfo(ctx, segmentIDs.Collect()...)
	if err != nil {
		return err
	}

	req.SegmentInfos = lo.SliceToMap(segmentInfos, func(info *datapb.SegmentInfo) (int64, *datapb.SegmentInfo) {
		return info.GetID(), info
	})

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

// applyCollectionSettings produces the same effective schema as the QueryNode
// WAL and SyncDistribution paths.
func applyCollectionSettings(schema *schemapb.CollectionSchema, collectionProperties []*commonpb.KeyValuePair) *schemapb.CollectionSchema {
	return collectionutil.ApplySettings(schema, collectionProperties)
}

// ApplyCollectionMmapSetting applies mmap defaults to an owned schema.
func ApplyCollectionMmapSetting(schema *schemapb.CollectionSchema, collectionProperties []*commonpb.KeyValuePair) *schemapb.CollectionSchema {
	return collectionutil.ApplyMmapSetting(schema, collectionProperties)
}

func autoWarmupForNonPKIsolationCollection(collectionProperties []*commonpb.KeyValuePair) bool {
	return collectionutil.AutoWarmupEnabled(collectionProperties)
}

func applyCollectionWarmupSetting(schema *schemapb.CollectionSchema, collectionProperties []*commonpb.KeyValuePair) *schemapb.CollectionSchema {
	return collectionutil.ApplyWarmupSetting(schema, collectionProperties)
}

// applyIndexWarmupSetting applies collection-level index warmup setting to segment index params
// Index params warmup setting has higher priority than collection-level
// Priority: index-level > collection-level > autoWarmupForNonPKIsolationCollection (all indexes)
// Collection-level granular keys: warmup.scalarIndex, warmup.vectorIndex
func applyIndexWarmupSetting(loadInfo *querypb.SegmentLoadInfo, schema *schemapb.CollectionSchema, collectionProperties []*commonpb.KeyValuePair) {
	// Get collection-level granular warmup policies for indexes
	scalarIndexWarmup, scalarIndexExist := common.GetWarmupPolicyByKey(common.WarmupScalarIndexKey, collectionProperties...)
	vectorIndexWarmup, vectorIndexExist := common.GetWarmupPolicyByKey(common.WarmupVectorIndexKey, collectionProperties...)
	autoWarmup := autoWarmupForNonPKIsolationCollection(collectionProperties)

	if !scalarIndexExist && !vectorIndexExist && !autoWarmup {
		return
	}

	// Build fieldID to field schema map for quick lookup
	fieldMap := make(map[int64]*schemapb.FieldSchema)
	for _, field := range schema.GetFields() {
		fieldMap[field.GetFieldID()] = field
	}
	// Include nested fields from struct arrays (struct array fields themselves don't support indexes)
	for _, structField := range schema.GetStructArrayFields() {
		for _, field := range structField.GetFields() {
			fieldMap[field.GetFieldID()] = field
		}
	}

	for _, indexInfo := range loadInfo.GetIndexInfos() {
		// Check if index params already has warmup setting
		_, exist := common.GetWarmupPolicy(indexInfo.IndexParams...)
		if exist {
			continue
		}

		field, ok := fieldMap[indexInfo.GetFieldID()]
		if !ok {
			continue
		}

		isVector := typeutil.IsVectorType(field.GetDataType())
		if isVector && vectorIndexExist {
			indexInfo.IndexParams = append(indexInfo.IndexParams, &commonpb.KeyValuePair{
				Key:   common.WarmupKey,
				Value: vectorIndexWarmup,
			})
		} else if !isVector && scalarIndexExist {
			indexInfo.IndexParams = append(indexInfo.IndexParams, &commonpb.KeyValuePair{
				Key:   common.WarmupKey,
				Value: scalarIndexWarmup,
			})
		} else if autoWarmup {
			// autoWarmupForNonPKIsolationCollection fallback: force sync warmup for ALL indexes (scalar and vector)
			// here vector indexes are included in autowarmup bcz they are critical for search performance
			indexInfo.IndexParams = append(indexInfo.IndexParams, &commonpb.KeyValuePair{
				Key:   common.WarmupKey,
				Value: common.WarmupSync,
			})
		}
	}
}
