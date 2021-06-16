// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package queryservice

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
)

type task interface {
	TraceCtx() context.Context
	ID() UniqueID // return ReqId
	SetID(id UniqueID)
	Type() commonpb.MsgType
	Timestamp() Timestamp
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
	TaskPriority() querypb.TriggerCondition
	GetParentTask() task
	GetChildTask() []task
	AddChildTask(t task)
}

type BaseTask struct {
	Condition
	ctx    context.Context
	cancel context.CancelFunc
	result *commonpb.Status

	taskID           UniqueID
	triggerCondition querypb.TriggerCondition
	parentTask       task
	childTasks       []task
}

func (bt *BaseTask) ID() UniqueID {
	return bt.taskID
}

func (bt *BaseTask) SetID(id UniqueID) {
	bt.taskID = id
}

func (bt *BaseTask) TraceCtx() context.Context {
	return bt.ctx
}

func (bt *BaseTask) TaskPriority() querypb.TriggerCondition {
	return bt.triggerCondition
}

func (bt *BaseTask) GetParentTask() task {
	return bt.parentTask
}

func (bt *BaseTask) GetChildTask() []task {
	return bt.childTasks
}

func (bt *BaseTask) AddChildTask(t task) {
	bt.childTasks = append(bt.childTasks, t)
}

//************************grpcTask***************************//
type LoadCollectionTask struct {
	BaseTask
	*querypb.LoadCollectionRequest
	masterService   types.MasterService
	dataService     types.DataService
	cluster         *queryNodeCluster
	meta            *meta
	toWatchPosition map[string]*internalpb.MsgPosition
	excludeSegment  map[string][]UniqueID
	watchNeeded     bool
}

func (lct *LoadCollectionTask) Type() commonpb.MsgType {
	return lct.Base.MsgType
}

func (lct *LoadCollectionTask) Timestamp() Timestamp {
	return lct.Base.Timestamp
}

func (lct *LoadCollectionTask) PreExecute(ctx context.Context) error {
	collectionID := lct.CollectionID
	schema := lct.Schema
	log.Debug("start do LoadCollectionTask",
		zap.Int64("msgID", lct.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Stringer("schema", schema))
	return nil
}

func (lct *LoadCollectionTask) Execute(ctx context.Context) error {
	collectionID := lct.CollectionID
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	showPartitionRequest := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ShowPartitions,
		},
		CollectionID: collectionID,
	}
	showPartitionResponse, err := lct.masterService.ShowPartitions(ctx, showPartitionRequest)
	if err != nil {
		status.Reason = err.Error()
		lct.result = status
		return err
	}

	log.Debug("loadCollectionTask: get recovery info", zap.Int64s("partitionIDs", showPartitionResponse.PartitionIDs))
	segment2Binlog := make(map[UniqueID]*querypb.SegmentLoadInfo)
	watchRequests := make(map[string]*querypb.WatchDmChannelsRequest)
	channelsToWatch := make([]string, 0)
	segmentsToLoad := make([]UniqueID, 0)
	partitionIDs := showPartitionResponse.PartitionIDs
	log.Debug("partitionIDs", zap.Int64s("partitionIDs", partitionIDs))
	for _, partitionID := range partitionIDs {
		getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequest{
			Base:         lct.Base,
			CollectionID: collectionID,
			PartitionID:  partitionID,
		}
		//recoveryInfo, err := mockGetRecoveryInfoFromDataService(lct.ctx, lct.masterService, lct.dataService, getRecoveryInfoRequest)
		recoveryInfo, err := lct.dataService.GetRecoveryInfo(lct.ctx, getRecoveryInfoRequest)
		if err != nil {
			status.Reason = err.Error()
			lct.result = status
			return err
		}

		for _, segmentBingLog := range recoveryInfo.Binlogs {
			segmentID := segmentBingLog.SegmentID
			segmentLoadInfo := &querypb.SegmentLoadInfo{
				SegmentID:    segmentBingLog.SegmentID,
				PartitionID:  partitionID,
				CollectionID: collectionID,
				BinlogPaths:  make([]*datapb.FieldBinlog, 0),
			}
			segmentLoadInfo.BinlogPaths = append(segmentLoadInfo.BinlogPaths, segmentBingLog.FieldBinlogs...)
			segmentsToLoad = append(segmentsToLoad, segmentID)
			segment2Binlog[segmentID] = segmentLoadInfo
		}

		for _, info := range recoveryInfo.Channels {
			channel := info.ChannelName
			if _, ok := watchRequests[channel]; !ok {
				watchRequest := &querypb.WatchDmChannelsRequest{
					Base:         lct.Base,
					CollectionID: collectionID,
					Infos:        []*datapb.VchannelInfo{info},
					Schema:       lct.Schema,
				}
				watchRequests[channel] = watchRequest
				channelsToWatch = append(channelsToWatch, channel)
				continue
			}
			oldInfo := watchRequests[channel].Infos[0]
			newInfo := mergeVChannelInfo(oldInfo, info)
			watchRequests[channel].Infos = []*datapb.VchannelInfo{newInfo}
		}
	}

	log.Debug("loadCollectionTask: segments and channels are ready to load or watch")
	segment2Nodes := shuffleSegmentsToQueryNode(segmentsToLoad, lct.cluster)
	watchRequest2Nodes := shuffleChannelsToQueryNode(channelsToWatch, lct.cluster)

	watchQueryChannelInfo := make(map[int64]bool)
	node2Segments := make(map[int64][]*querypb.SegmentLoadInfo)
	for segmentID, nodeID := range segment2Nodes {
		if _, ok := node2Segments[nodeID]; !ok {
			node2Segments[nodeID] = make([]*querypb.SegmentLoadInfo, 0)
		}
		node2Segments[nodeID] = append(node2Segments[nodeID], segment2Binlog[segmentID])
		if lct.cluster.hasWatchedQueryChannel(lct.ctx, nodeID, collectionID) {
			watchQueryChannelInfo[nodeID] = true
			continue
		}
		watchQueryChannelInfo[nodeID] = false
	}
	for _, nodeID := range watchRequest2Nodes {
		if lct.cluster.hasWatchedQueryChannel(lct.ctx, nodeID, collectionID) {
			watchQueryChannelInfo[nodeID] = true
			continue
		}
		watchQueryChannelInfo[nodeID] = false
	}

	for nodeID, segmentInfos := range node2Segments {
		loadSegmentsRequest := &querypb.LoadSegmentsRequest{
			Base:          lct.Base,
			NodeID:        nodeID,
			Infos:         segmentInfos,
			Schema:        lct.Schema,
			LoadCondition: querypb.TriggerCondition_grpcRequest,
		}

		loadSegmentTask := &LoadSegmentTask{
			BaseTask: BaseTask{
				ctx:              lct.ctx,
				Condition:        NewTaskCondition(lct.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},

			LoadSegmentsRequest: loadSegmentsRequest,
			cluster:             lct.cluster,
		}
		lct.AddChildTask(loadSegmentTask)
		log.Debug("add a loadSegmentTask to loadCollectionTask's childTask", zap.Any("load segment Task", loadSegmentTask))
	}

	for channel, nodeID := range watchRequest2Nodes {
		watchRequests[channel].NodeID = nodeID
		watchDmChannelTask := &WatchDmChannelTask{
			BaseTask: BaseTask{
				ctx:       lct.ctx,
				Condition: NewTaskCondition(lct.ctx),

				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			WatchDmChannelsRequest: watchRequests[channel],
			cluster:                lct.cluster,
		}
		//TODO::open when queryNode watchDmChannel work
		lct.AddChildTask(watchDmChannelTask)
		log.Debug("add a watchDmChannelTask to loadCollectionTask's childTask", zap.Any("task", watchDmChannelTask))
	}

	for nodeID, watched := range watchQueryChannelInfo {
		if !watched {
			queryChannel, queryResultChannel := lct.meta.GetQueryChannel(collectionID)

			addQueryChannelRequest := &querypb.AddQueryChannelRequest{
				Base:             lct.Base,
				NodeID:           nodeID,
				CollectionID:     collectionID,
				RequestChannelID: queryChannel,
				ResultChannelID:  queryResultChannel,
			}
			watchQueryChannelTask := &WatchQueryChannelTask{
				BaseTask: BaseTask{
					ctx:       lct.ctx,
					Condition: NewTaskCondition(lct.ctx),

					triggerCondition: querypb.TriggerCondition_grpcRequest,
				},

				AddQueryChannelRequest: addQueryChannelRequest,
				cluster:                lct.cluster,
			}
			lct.AddChildTask(watchQueryChannelTask)
			log.Debug("add a watchQueryChannelTask to loadCollectionTask's childTask")
		}
	}

	log.Debug("LoadCollection execute done",
		zap.Int64("msgID", lct.ID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (lct *LoadCollectionTask) PostExecute(ctx context.Context) error {
	collectionID := lct.CollectionID
	log.Debug("LoadCollectionTask postExecute done",
		zap.Int64("msgID", lct.ID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

type ReleaseCollectionTask struct {
	BaseTask
	*querypb.ReleaseCollectionRequest
	nodeID  int64
	cluster *queryNodeCluster
}

func (rct *ReleaseCollectionTask) Type() commonpb.MsgType {
	return rct.Base.MsgType
}

func (rct *ReleaseCollectionTask) Timestamp() Timestamp {
	return rct.Base.Timestamp
}

func (rct *ReleaseCollectionTask) PreExecute(ctx context.Context) error {
	collectionID := rct.CollectionID
	log.Debug("start do ReleaseCollectionTask",
		zap.Int64("msgID", rct.ID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (rct *ReleaseCollectionTask) Execute(ctx context.Context) error {
	collectionID := rct.CollectionID
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if rct.nodeID <= 0 {
		for nodeID := range rct.cluster.nodes {
			releaseCollectionTask := &ReleaseCollectionTask{
				BaseTask: BaseTask{
					ctx:              rct.ctx,
					Condition:        NewTaskCondition(rct.ctx),
					triggerCondition: querypb.TriggerCondition_grpcRequest,
				},

				ReleaseCollectionRequest: rct.ReleaseCollectionRequest,
				nodeID:                   nodeID,
				cluster:                  rct.cluster,
			}
			rct.AddChildTask(releaseCollectionTask)
			log.Debug("add a releaseCollectionTask to releaseCollectionTask's childTask")
		}
	} else {
		_, err := rct.cluster.releaseCollection(ctx, rct.nodeID, rct.ReleaseCollectionRequest)
		if err != nil {
			log.Error("release collection end, node occur error", zap.String("nodeID", fmt.Sprintln(rct.nodeID)))
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
			rct.result = status
			return err
		}
	}

	rct.result = status
	log.Debug("ReleaseCollectionTask Execute done",
		zap.Int64("msgID", rct.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64("nodeID", rct.nodeID))
	return nil
}

func (rct *ReleaseCollectionTask) PostExecute(ctx context.Context) error {
	collectionID := rct.CollectionID

	log.Debug("ReleaseCollectionTask postExecute done",
		zap.Int64("msgID", rct.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64("nodeID", rct.nodeID))
	return nil
}

type LoadPartitionTask struct {
	BaseTask
	*querypb.LoadPartitionsRequest
	masterService types.MasterService
	dataService   types.DataService
	cluster       *queryNodeCluster
	meta          *meta
}

func (lpt *LoadPartitionTask) Type() commonpb.MsgType {
	return lpt.Base.MsgType
}

func (lpt *LoadPartitionTask) Timestamp() Timestamp {
	return lpt.Base.Timestamp
}

func (lpt *LoadPartitionTask) PreExecute(ctx context.Context) error {
	collectionID := lpt.CollectionID
	log.Debug("start do LoadPartitionTask",
		zap.Int64("msgID", lpt.ID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (lpt *LoadPartitionTask) Execute(ctx context.Context) error {
	collectionID := lpt.CollectionID
	partitionIDs := lpt.PartitionIDs

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	segmentsToLoad := make([]UniqueID, 0)
	segment2BingLog := make(map[UniqueID]*querypb.SegmentLoadInfo)
	channelsToWatch := make([]string, 0)
	watchRequests := make([]*querypb.WatchDmChannelsRequest, 0)
	for _, partitionID := range partitionIDs {
		getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequest{
			Base:         lpt.Base,
			CollectionID: collectionID,
			PartitionID:  partitionID,
		}
		//recoveryInfo, err := mockGetRecoveryInfoFromDataService(lpt.ctx, lpt.masterService, lpt.dataService, getRecoveryInfoRequest)
		recoveryInfo, err := lpt.dataService.GetRecoveryInfo(lpt.ctx, getRecoveryInfoRequest)
		if err != nil {
			status.Reason = err.Error()
			lpt.result = status
			return err
		}

		for _, segmentBingLog := range recoveryInfo.Binlogs {
			segmentID := segmentBingLog.SegmentID
			segmentLoadInfo := &querypb.SegmentLoadInfo{
				SegmentID:    segmentID,
				PartitionID:  partitionID,
				CollectionID: collectionID,
				BinlogPaths:  make([]*datapb.FieldBinlog, 0),
			}
			segmentLoadInfo.BinlogPaths = append(segmentLoadInfo.BinlogPaths, segmentBingLog.FieldBinlogs...)
			segmentsToLoad = append(segmentsToLoad, segmentID)
			segment2BingLog[segmentID] = segmentLoadInfo
		}

		for _, info := range recoveryInfo.Channels {
			channel := info.ChannelName
			watchRequest := &querypb.WatchDmChannelsRequest{
				Base:         lpt.Base,
				CollectionID: collectionID,
				PartitionID:  partitionID,
				Infos:        []*datapb.VchannelInfo{info},
				Schema:       lpt.Schema,
			}
			channelsToWatch = append(channelsToWatch, channel)
			watchRequests = append(watchRequests, watchRequest)
			log.Debug("set watchDmChannelsRequests", zap.Any("request", watchRequest), zap.Int64("collectionID", collectionID))
		}
	}

	segment2Nodes := shuffleSegmentsToQueryNode(segmentsToLoad, lpt.cluster)
	watchRequest2Nodes := shuffleChannelsToQueryNode(channelsToWatch, lpt.cluster)
	log.Debug("watch request to node", zap.Any("request map", watchRequest2Nodes), zap.Int64("collectionID", collectionID))

	watchQueryChannelInfo := make(map[int64]bool)
	node2Segments := make(map[int64][]*querypb.SegmentLoadInfo)
	for segmentID, nodeID := range segment2Nodes {
		if _, ok := node2Segments[nodeID]; !ok {
			node2Segments[nodeID] = make([]*querypb.SegmentLoadInfo, 0)
		}
		node2Segments[nodeID] = append(node2Segments[nodeID], segment2BingLog[segmentID])
		if lpt.cluster.hasWatchedQueryChannel(lpt.ctx, nodeID, collectionID) {
			watchQueryChannelInfo[nodeID] = true
			continue
		}
		watchQueryChannelInfo[nodeID] = false
	}
	for _, nodeID := range watchRequest2Nodes {
		if lpt.cluster.hasWatchedQueryChannel(lpt.ctx, nodeID, collectionID) {
			watchQueryChannelInfo[nodeID] = true
			continue
		}
		watchQueryChannelInfo[nodeID] = false
	}

	for nodeID, segmentInfos := range node2Segments {
		loadSegmentsRequest := &querypb.LoadSegmentsRequest{
			Base:          lpt.Base,
			NodeID:        nodeID,
			Infos:         segmentInfos,
			Schema:        lpt.Schema,
			LoadCondition: querypb.TriggerCondition_grpcRequest,
		}

		loadSegmentTask := &LoadSegmentTask{
			BaseTask: BaseTask{
				ctx:              lpt.ctx,
				Condition:        NewTaskCondition(lpt.ctx),
				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},

			LoadSegmentsRequest: loadSegmentsRequest,
			cluster:             lpt.cluster,
		}
		lpt.AddChildTask(loadSegmentTask)
		log.Debug("add a loadSegmentTask to loadPartitionTask's childTask")
	}

	index := 0
	for _, nodeID := range watchRequest2Nodes {
		watchRequests[index].NodeID = nodeID
		watchDmChannelTask := &WatchDmChannelTask{
			BaseTask: BaseTask{
				ctx:       lpt.ctx,
				Condition: NewTaskCondition(lpt.ctx),

				triggerCondition: querypb.TriggerCondition_grpcRequest,
			},
			WatchDmChannelsRequest: watchRequests[index],
			cluster:                lpt.cluster,
		}
		lpt.AddChildTask(watchDmChannelTask)
		log.Debug("add a watchDmChannelTask to loadPartitionTask's childTask")
		index++
	}

	for nodeID, watched := range watchQueryChannelInfo {
		if !watched {
			queryChannel, queryResultChannel := lpt.meta.GetQueryChannel(collectionID)

			addQueryChannelRequest := &querypb.AddQueryChannelRequest{
				Base:             lpt.Base,
				NodeID:           nodeID,
				CollectionID:     collectionID,
				RequestChannelID: queryChannel,
				ResultChannelID:  queryResultChannel,
			}
			watchQueryChannelTask := &WatchQueryChannelTask{
				BaseTask: BaseTask{
					ctx:       lpt.ctx,
					Condition: NewTaskCondition(lpt.ctx),

					triggerCondition: querypb.TriggerCondition_grpcRequest,
				},

				AddQueryChannelRequest: addQueryChannelRequest,
				cluster:                lpt.cluster,
			}
			lpt.AddChildTask(watchQueryChannelTask)
			log.Debug("add a watchQueryChannelTask to loadPartitionTask's childTask")
		}
	}

	log.Debug("LoadPartitionTask Execute done",
		zap.Int64("msgID", lpt.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))
	status.ErrorCode = commonpb.ErrorCode_Success
	lpt.result = status
	return nil
}

func (lpt *LoadPartitionTask) PostExecute(ctx context.Context) error {
	collectionID := lpt.CollectionID
	partitionIDs := lpt.PartitionIDs
	log.Debug("LoadPartitionTask postExecute done",
		zap.Int64("msgID", lpt.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))
	//lpt.cancel()
	return nil
}

type ReleasePartitionTask struct {
	BaseTask
	*querypb.ReleasePartitionsRequest
	nodeID  int64
	cluster *queryNodeCluster
}

func (rpt *ReleasePartitionTask) Type() commonpb.MsgType {
	return rpt.Base.MsgType
}

func (rpt *ReleasePartitionTask) Timestamp() Timestamp {
	return rpt.Base.Timestamp
}

func (rpt *ReleasePartitionTask) PreExecute(ctx context.Context) error {
	collectionID := rpt.CollectionID
	log.Debug("start do releasePartitionTask",
		zap.Int64("msgID", rpt.ID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (rpt *ReleasePartitionTask) Execute(ctx context.Context) error {
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if rpt.nodeID <= 0 {
		for nodeID := range rpt.cluster.nodes {
			releasePartitionTask := &ReleasePartitionTask{
				BaseTask: BaseTask{
					ctx:              rpt.ctx,
					Condition:        rpt.Condition,
					triggerCondition: querypb.TriggerCondition_grpcRequest,
				},

				ReleasePartitionsRequest: rpt.ReleasePartitionsRequest,
				nodeID:                   nodeID,
				cluster:                  rpt.cluster,
			}
			rpt.AddChildTask(releasePartitionTask)
			log.Debug("add a releasePartitionTask to releasePartitionTask's childTask")
		}
	} else {
		status, err := rpt.cluster.releasePartitions(ctx, rpt.nodeID, rpt.ReleasePartitionsRequest)
		if err != nil {
			rpt.result = status
			return err
		}
	}

	rpt.result = status
	log.Debug("ReleasePartitionTask Execute done",
		zap.Int64("msgID", rpt.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("nodeID", rpt.nodeID))
	return nil
}

func (rpt *ReleasePartitionTask) PostExecute(ctx context.Context) error {
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs

	log.Debug("ReleasePartitionTask postExecute done",
		zap.Int64("msgID", rpt.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int64("nodeID", rpt.nodeID))
	return nil
}

//****************************internal task*******************************//
type LoadSegmentTask struct {
	BaseTask
	*querypb.LoadSegmentsRequest
	cluster *queryNodeCluster
}

func (lst *LoadSegmentTask) Type() commonpb.MsgType {
	return lst.Base.MsgType
}
func (lst *LoadSegmentTask) Timestamp() Timestamp {
	return lst.Base.Timestamp
}
func (lst *LoadSegmentTask) PreExecute(ctx context.Context) error {
	segmentIDs := make([]UniqueID, 0)
	for _, info := range lst.Infos {
		segmentIDs = append(segmentIDs, info.SegmentID)
	}
	log.Debug("start do loadSegmentTask",
		zap.Int64s("segmentIDs", segmentIDs))
	return nil
}
func (lst *LoadSegmentTask) Execute(ctx context.Context) error {
	status, err := lst.cluster.LoadSegments(lst.ctx, lst.NodeID, lst.LoadSegmentsRequest)
	if err != nil {
		lst.result = status
		return err
	}

	lst.result = status
	log.Debug("loadSegmentTask Execute done")
	return nil
}
func (lst *LoadSegmentTask) PostExecute(ctx context.Context) error {
	log.Debug("loadSegmentTask postExecute done")
	return nil
}

type ReleaseSegmentTask struct {
	BaseTask
	*querypb.ReleaseSegmentsRequest
	cluster *queryNodeCluster
}

func (rst *ReleaseSegmentTask) Type() commonpb.MsgType {
	return rst.Base.MsgType
}
func (rst *ReleaseSegmentTask) Timestamp() Timestamp {
	return rst.Base.Timestamp
}
func (rst *ReleaseSegmentTask) PreExecute(ctx context.Context) error {
	segmentIDs := rst.SegmentIDs
	log.Debug("start do releaseSegmentTask",
		zap.Int64s("segmentIDs", segmentIDs))
	return nil
}
func (rst *ReleaseSegmentTask) Execute(ctx context.Context) error {
	status, err := rst.cluster.ReleaseSegments(rst.ctx, rst.NodeID, rst.ReleaseSegmentsRequest)
	if err != nil {
		rst.result = status
		return err
	}

	rst.result = status
	log.Debug("releaseSegmentTask Execute done",
		zap.Int64s("segmentIDs", rst.SegmentIDs))
	return nil
}
func (rst *ReleaseSegmentTask) PostExecute(ctx context.Context) error {
	segmentIDs := rst.SegmentIDs
	log.Debug("releaseSegmentTask postExecute done",
		zap.Int64s("segmentIDs", segmentIDs))
	return nil
}

type WatchDmChannelTask struct {
	BaseTask
	*querypb.WatchDmChannelsRequest
	cluster *queryNodeCluster
}

func (wdt *WatchDmChannelTask) Type() commonpb.MsgType {
	return wdt.Base.MsgType
}
func (wdt *WatchDmChannelTask) Timestamp() Timestamp {
	return wdt.Base.Timestamp
}
func (wdt *WatchDmChannelTask) PreExecute(ctx context.Context) error {
	channelInfos := wdt.Infos
	channels := make([]string, 0)
	for _, info := range channelInfos {
		channels = append(channels, info.ChannelName)
	}
	log.Debug("start do watchDmChannelTask",
		zap.Strings("dmChannels", channels))
	return nil
}
func (wdt *WatchDmChannelTask) Execute(ctx context.Context) error {
	status, err := wdt.cluster.WatchDmChannels(wdt.ctx, wdt.NodeID, wdt.WatchDmChannelsRequest)
	if err != nil {
		wdt.result = status
		return err
	}

	wdt.result = status
	log.Debug("watchDmChannelsTask Execute done")
	return nil
}
func (wdt *WatchDmChannelTask) PostExecute(ctx context.Context) error {
	log.Debug("watchDmChannelTask postExecute done")
	return nil
}

type WatchQueryChannelTask struct {
	BaseTask
	*querypb.AddQueryChannelRequest
	cluster *queryNodeCluster
}

func (aqt *WatchQueryChannelTask) Type() commonpb.MsgType {
	return aqt.Base.MsgType
}
func (aqt *WatchQueryChannelTask) Timestamp() Timestamp {
	return aqt.Base.Timestamp
}
func (aqt *WatchQueryChannelTask) PreExecute(ctx context.Context) error {
	log.Debug("start do WatchQueryChannelTask",
		zap.Int64("collectionID", aqt.CollectionID),
		zap.String("queryChannel", aqt.RequestChannelID),
		zap.String("queryResultChannel", aqt.ResultChannelID))
	return nil
}
func (aqt *WatchQueryChannelTask) Execute(ctx context.Context) error {
	status, err := aqt.cluster.AddQueryChannel(aqt.ctx, aqt.NodeID, aqt.AddQueryChannelRequest)
	if err != nil {
		aqt.result = status
		return err
	}

	aqt.result = status
	log.Debug("watchQueryChannelTask Execute done",
		zap.Int64("collectionID", aqt.CollectionID),
		zap.String("queryChannel", aqt.RequestChannelID),
		zap.String("queryResultChannel", aqt.ResultChannelID))
	return nil
}

func (aqt *WatchQueryChannelTask) PostExecute(ctx context.Context) error {
	log.Debug("WatchQueryChannelTask postExecute done",
		zap.Int64("collectionID", aqt.CollectionID),
		zap.String("queryChannel", aqt.RequestChannelID),
		zap.String("queryResultChannel", aqt.ResultChannelID))
	return nil
}

func mockGetRecoveryInfoFromDataService(ctx context.Context,
	master types.MasterService,
	dataService types.DataService,
	req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	segmentIDs := make([]UniqueID, 0)
	showSegmentRequest := &milvuspb.ShowSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ShowSegments,
		},
		CollectionID: req.CollectionID,
		PartitionID:  req.PartitionID,
	}

	showSegmentsResponse, err := master.ShowSegments(ctx, showSegmentRequest)
	if err != nil {
		return nil, err
	}
	segmentIDs = append(segmentIDs, showSegmentsResponse.SegmentIDs...)
	getSegmentStatesResponse, err := dataService.GetSegmentStates(ctx, &datapb.GetSegmentStatesRequest{
		SegmentIDs: segmentIDs,
	})
	if err != nil {
		return nil, err
	}
	log.Debug("get segmentStates from dataService", zap.Any("states", getSegmentStatesResponse))
	segmentStates := make(map[UniqueID]*datapb.SegmentStateInfo)
	channel2Segments := make(map[string][]UniqueID)
	for _, state := range getSegmentStatesResponse.States {
		segmentID := state.SegmentID
		segmentStates[segmentID] = state
		if state.StartPosition == nil || len(state.StartPosition.ChannelName) == 0 {
			continue
		}
		channelName := state.StartPosition.ChannelName
		if _, ok := channel2Segments[channelName]; !ok {
			segments := make([]UniqueID, 0)
			segments = append(segments, segmentID)
			channel2Segments[channelName] = segments
		} else {
			channel2Segments[channelName] = append(channel2Segments[channelName], segmentID)
		}
	}
	channelInfos := make([]*datapb.VchannelInfo, 0)
	segmentBinlogs := make([]*datapb.SegmentBinlogs, 0)
	for channel, segmentIDs := range channel2Segments {
		channelInfo := &datapb.VchannelInfo{
			CollectionID: req.CollectionID,
			ChannelName:  channel,
			SeekPosition: &internalpb.MsgPosition{
				ChannelName: channel,
			},
			UnflushedSegments: make([]*datapb.SegmentInfo, 0),
			FlushedSegments:   make([]UniqueID, 0),
		}
		sort.Slice(segmentIDs, func(i, j int) bool {
			return segmentIDs[i] < segmentIDs[j]
		})
		for _, id := range segmentIDs {
			if segmentStates[id].State == commonpb.SegmentState_Flushed {
				channelInfo.FlushedSegments = append(channelInfo.FlushedSegments, id)
				channelInfo.SeekPosition = segmentStates[id].EndPosition
				continue
			}
			if segmentStates[id].StartPosition != nil {
				checkpoint := &datapb.SegmentInfo{
					ID:          id,
					DmlPosition: segmentStates[id].StartPosition,
				}
				channelInfo.UnflushedSegments = append(channelInfo.UnflushedSegments, checkpoint)
				if checkpoint.DmlPosition.Timestamp < channelInfo.SeekPosition.Timestamp || channelInfo.SeekPosition.Timestamp == 0 {
					channelInfo.SeekPosition = checkpoint.DmlPosition
				}
			}
		}
		channelInfos = append(channelInfos, channelInfo)

		for _, id := range channelInfo.FlushedSegments {
			segmentBinlog := &datapb.SegmentBinlogs{
				SegmentID:    id,
				FieldBinlogs: make([]*datapb.FieldBinlog, 0),
			}
			insertBinlogPathRequest := &datapb.GetInsertBinlogPathsRequest{
				SegmentID: id,
			}

			pathResponse, err := dataService.GetInsertBinlogPaths(ctx, insertBinlogPathRequest)
			if err != nil || pathResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
				return nil, err
			}
			if len(pathResponse.FieldIDs) != len(pathResponse.Paths) || len(pathResponse.FieldIDs) <= 0 {
				return nil, errors.New("illegal GetInsertBinlogPathsResponse")
			}

			for index, fieldID := range pathResponse.FieldIDs {
				if len(pathResponse.Paths[index].Values) == 0 {
					continue
				}
				fieldBingLog := &datapb.FieldBinlog{
					FieldID: fieldID,
					Binlogs: pathResponse.Paths[index].Values,
				}
				segmentBinlog.FieldBinlogs = append(segmentBinlog.FieldBinlogs, fieldBingLog)
				log.Debug("get binlog path from data service",
					zap.Int64("collectionID", req.CollectionID),
					zap.Int64("partitionID", req.PartitionID),
					zap.Int64("segmentID", id),
					zap.Int64("fieldID", fieldID),
					zap.Strings("binlogPath", pathResponse.Paths[index].Values))
			}
			segmentBinlogs = append(segmentBinlogs, segmentBinlog)
		}
	}
	if len(channelInfos) == 0 {
		// get physical channels
		desColReq := &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_DescribeCollection,
			},
			CollectionID: req.CollectionID,
		}
		desColRsp, err := master.DescribeCollection(ctx, desColReq)
		if err != nil {
			log.Error("get physical channels failed, err = " + err.Error())
			return nil, err
		}
		for _, channel := range desColRsp.VirtualChannelNames {
			channelInfo := &datapb.VchannelInfo{
				CollectionID: req.CollectionID,
				ChannelName:  channel,
			}
			channelInfos = append(channelInfos, channelInfo)
		}
	}

	return &datapb.GetRecoveryInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Channels: channelInfos,
		Binlogs:  segmentBinlogs,
	}, nil
}

func shuffleChannelsToQueryNode(dmChannels []string, cluster *queryNodeCluster) map[string]int64 {
	maxNumChannels := 0
	for nodeID := range cluster.nodes {
		numChannels, _ := cluster.getNumDmChannels(nodeID)
		if numChannels > maxNumChannels {
			maxNumChannels = numChannels
		}
	}
	res := make(map[string]int64)
	if len(dmChannels) == 0 {
		return res
	}

	offset := 0
	loopAll := false
	for {
		lastOffset := offset
		if !loopAll {
			for id := range cluster.nodes {
				numSegments, _ := cluster.getNumSegments(id)
				if numSegments >= maxNumChannels {
					continue
				}
				res[dmChannels[offset]] = id
				offset++
				if offset == len(dmChannels) {
					return res
				}
			}
		} else {
			for id := range cluster.nodes {
				res[dmChannels[offset]] = id
				offset++
				if offset == len(dmChannels) {
					return res
				}
			}
		}
		if lastOffset == offset {
			loopAll = true
		}
	}
}

func shuffleSegmentsToQueryNode(segmentIDs []UniqueID, cluster *queryNodeCluster) map[UniqueID]int64 {
	maxNumSegments := 0
	for nodeID := range cluster.nodes {
		numSegments, _ := cluster.getNumSegments(nodeID)
		if numSegments > maxNumSegments {
			maxNumSegments = numSegments
		}
	}
	res := make(map[UniqueID]int64)

	if len(segmentIDs) == 0 {
		return res
	}

	offset := 0
	loopAll := false
	for {
		lastOffset := offset
		if !loopAll {
			for id := range cluster.nodes {
				numSegments, _ := cluster.getNumSegments(id)
				if numSegments >= maxNumSegments {
					continue
				}
				res[segmentIDs[offset]] = id
				offset++
				if offset == len(segmentIDs) {
					return res
				}
			}
		} else {
			for id := range cluster.nodes {
				res[segmentIDs[offset]] = id
				offset++
				if offset == len(segmentIDs) {
					return res
				}
			}
		}
		if lastOffset == offset {
			loopAll = true
		}
	}
}

func mergeVChannelInfo(info1 *datapb.VchannelInfo, info2 *datapb.VchannelInfo) *datapb.VchannelInfo {
	collectionID := info1.CollectionID
	channelName := info1.ChannelName
	var seekPosition *internalpb.MsgPosition
	if info1.SeekPosition == nil || info2.SeekPosition == nil {
		seekPosition = &internalpb.MsgPosition{
			ChannelName: channelName,
		}
	} else {
		seekPosition = info1.SeekPosition
		if info1.SeekPosition.Timestamp > info2.SeekPosition.Timestamp {
			seekPosition = info2.SeekPosition
		}
	}

	checkPoints := make([]*datapb.SegmentInfo, 0)
	checkPoints = append(checkPoints, info1.UnflushedSegments...)
	checkPoints = append(checkPoints, info2.UnflushedSegments...)

	flushedSegments := make([]UniqueID, 0)
	flushedSegments = append(flushedSegments, info1.FlushedSegments...)
	flushedSegments = append(flushedSegments, info2.FlushedSegments...)

	return &datapb.VchannelInfo{
		CollectionID:      collectionID,
		ChannelName:       channelName,
		SeekPosition:      seekPosition,
		UnflushedSegments: checkPoints,
		FlushedSegments:   flushedSegments,
	}
}

//****************************handoff task********************************//
type HandoffTask struct {
}

//*********************** ***load balance task*** ************************//
type loadBalanceTask struct {
}

//func watchDmChannels(ctx context.Context,
//	dataService types.DataService,
//	cluster *queryNodeCluster,
//	meta *meta,
//	dbID UniqueID,
//	collectionID UniqueID,
//	msgBase *commonpb.MsgBase,
//	toWatchPosition map[string]*internalpb.MsgPosition,
//	excludeSegment map[string][]UniqueID) error {
//	col, err := meta.getCollectionByID(0, collectionID)
//	if err != nil {
//		return err
//	}
//	channelRequest := datapb.GetInsertChannelsRequest{
//		DbID:         dbID,
//		CollectionID: collectionID,
//	}
//	resp, err := dataService.GetInsertChannels(ctx, &channelRequest)
//	if err != nil {
//		return err
//	}
//	if len(resp.Values) == 0 {
//		err = errors.New("haven't assign dm channel to collection")
//		return err
//	}
//
//	dmChannels := resp.Values
//	channelsWithoutPos := make([]string, 0)
//	for _, channel := range dmChannels {
//		findChannel := false
//		ChannelsWithPos := col.dmChannels
//		for _, ch := range ChannelsWithPos {
//			if channel == ch {
//				findChannel = true
//				break
//			}
//		}
//		if !findChannel {
//			channelsWithoutPos = append(channelsWithoutPos, channel)
//		}
//	}
//
//	err = meta.addDmChannels(dbID, collectionID, channelsWithoutPos)
//	if err != nil {
//		return err
//	}
//	//for _, ch := range channelsWithoutPos {
//	//	pos := &internalpb.MsgPosition{
//	//		ChannelName: ch,
//	//	}
//	//	err = meta.addDmChannels(dbID, collectionID, chs)
//	//	if err != nil {
//	//		return err
//	//	}
//	//}
//
//	nodeID2Channels := shuffleChannelsToQueryNode(dbID, dmChannels, cluster)
//	for nodeID, channels := range nodeID2Channels {
//		//node := queryNodes[nodeID]
//		watchDmChannelsInfo := make([]*querypb.WatchDmChannelInfo, 0)
//		for _, ch := range channels {
//			info := &querypb.WatchDmChannelInfo{
//				ChannelID: ch,
//				//Pos:              col.dmChannels2Pos[ch],
//				//ExcludedSegments: col.excludeSegmentIds,
//			}
//			if _, ok := toWatchPosition[ch]; ok {
//				info.Pos = toWatchPosition[ch]
//				info.ExcludedSegments = excludeSegment[ch]
//			}
//			watchDmChannelsInfo = append(watchDmChannelsInfo, info)
//		}
//		request := &querypb.WatchDmChannelsRequest{
//			Base:         msgBase,
//			CollectionID: collectionID,
//			ChannelIDs:   channels,
//			Infos:        watchDmChannelsInfo,
//		}
//		_, err := cluster.WatchDmChannels(ctx, nodeID, request)
//		if err != nil {
//			return err
//		}
//		cluster.AddDmChannels(dbID, nodeID, channels, collectionID)
//		log.Debug("query node ", zap.String("nodeID", strconv.FormatInt(nodeID, 10)), zap.String("watch channels", fmt.Sprintln(channels)))
//	}
//
//	return nil
//}
