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
	"math/rand"
	"sort"
	"strconv"

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
}

type BaseTask struct {
	Condition
	ctx              context.Context
	cancel           context.CancelFunc
	result           *commonpb.Status

	taskID UniqueID
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

func (lct *LoadCollectionTask) ID() UniqueID {
	return lct.Base.MsgID
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
	dbID := lct.DbID
	collectionID := lct.CollectionID
	schema := lct.LoadCollectionRequest.Schema
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	// get partitionIDs
	showPartitionRequest := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ShowPartitions,
			MsgID:   lct.Base.MsgID,
		},
		CollectionID: collectionID,
	}

	showPartitionResponse, err := lct.masterService.ShowPartitions(ctx, showPartitionRequest)
	if err != nil {
		status.Reason = err.Error()
		lct.result = status
		return fmt.Errorf("call master ShowPartitions: %s", err)
	}
	log.Debug("ShowPartitions returned from Master", zap.String("role", Params.RoleName), zap.Int64("msgID", showPartitionRequest.Base.MsgID))
	if showPartitionResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
		lct.result = showPartitionResponse.Status
		return err
	}
	partitionIDs := showPartitionResponse.PartitionIDs

	partitionIDsToLoad := make([]UniqueID, 0)
	partitionsInReplica, err := lct.meta.getPartitions(dbID, collectionID)
	if err != nil {
		status.Reason = err.Error()
		lct.result = status
		return err
	}
	for _, id := range partitionIDs {
		cached := false
		for _, partition := range partitionsInReplica {
			if id == partition.id {
				cached = true
				break
			}
		}
		if !cached {
			partitionIDsToLoad = append(partitionIDsToLoad, id)
		}
	}

	if len(partitionIDsToLoad) == 0 {
		log.Debug("load collection done", zap.String("role", Params.RoleName), zap.Int64("msgID", lct.ID()), zap.String("collectionID", fmt.Sprintln(collectionID)))
		status.ErrorCode = commonpb.ErrorCode_Success
		status.Reason = "Partitions has been already loaded!"
		lct.result = status
		return nil
	}

	loadPartitionsRequest := &querypb.LoadPartitionsRequest{
		Base:         lct.Base,
		DbID:         dbID,
		CollectionID: collectionID,
		PartitionIDs: partitionIDsToLoad,
		Schema:       schema,
	}

	status, _, err = LoadPartitionMetaCheck(lct.meta, loadPartitionsRequest)
	if err != nil {
		lct.result = status
		return err
	}

	loadPartitionTask := &LoadPartitionTask{
		BaseTask: BaseTask{
			ctx:       ctx,
			Condition: NewTaskCondition(ctx),
		},
		LoadPartitionsRequest: loadPartitionsRequest,
		masterService:         lct.masterService,
		dataService:           lct.dataService,
		cluster:               lct.cluster,
		meta:                  lct.meta,
		toWatchPosition:       lct.toWatchPosition,
		excludeSegment:        lct.excludeSegment,
		watchNeeded:           false,
	}

	err = loadPartitionTask.PreExecute(ctx)
	if err != nil {
		status.Reason = err.Error()
		lct.result = status
		return err
	}
	err = loadPartitionTask.Execute(ctx)
	if err != nil {
		status.Reason = err.Error()
		lct.result = status
		return err
	}
	log.Debug("LoadCollection execute done",
		zap.Int64("msgID", lct.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Stringer("schema", schema))

	return nil
}

func (lct *LoadCollectionTask) PostExecute(ctx context.Context) error {
	dbID := lct.DbID
	collectionID := lct.CollectionID
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	if lct.watchNeeded {
		err := watchDmChannels(ctx, lct.dataService, lct.cluster, lct.meta, dbID, collectionID, lct.Base, lct.toWatchPosition, lct.excludeSegment)
		if err != nil {
			log.Debug("watchDmChannels failed", zap.Int64("msgID", lct.ID()), zap.Int64("collectionID", collectionID), zap.Error(err))
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
			lct.result = status
			return err
		}
	}

	log.Debug("LoadCollectionTask postExecute done",
		zap.Int64("msgID", lct.ID()),
		zap.Int64("collectionID", collectionID))
	lct.result = status
	//lct.cancel()
	return nil
}

type ReleaseCollectionTask struct {
	BaseTask
	*querypb.ReleaseCollectionRequest
	cluster *queryNodeCluster
	meta    Replica
}

func (rct *ReleaseCollectionTask) ID() UniqueID {
	return rct.Base.MsgID
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
	for nodeID := range rct.cluster.nodes {
		_, err := rct.cluster.ReleaseCollection(ctx, nodeID, rct.ReleaseCollectionRequest)
		if err != nil {
			log.Error("release collection end, node occur error", zap.String("nodeID", fmt.Sprintln(nodeID)))
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
			rct.result = status
			return err
		}
	}

	rct.result = status
	log.Debug("ReleaseCollectionTask Execute done",
		zap.Int64("msgID", rct.ID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

func (rct *ReleaseCollectionTask) PostExecute(ctx context.Context) error {
	dbID := rct.DbID
	collectionID := rct.CollectionID
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	err := rct.meta.releaseCollection(dbID, collectionID)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		rct.result = status
		return err
	}

	rct.result = status
	log.Debug("ReleaseCollectionTask postExecute done",
		zap.Int64("msgID", rct.ID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

type LoadPartitionTask struct {
	BaseTask
	*querypb.LoadPartitionsRequest
	masterService   types.MasterService
	dataService     types.DataService
	cluster         *queryNodeCluster
	meta            *meta
	toWatchPosition map[string]*internalpb.MsgPosition
	excludeSegment  map[string][]UniqueID
	watchNeeded     bool
}

func (lpt *LoadPartitionTask) ID() UniqueID {
	return lpt.Base.MsgID
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
	//TODO::suggest different partitions have different dm channel
	dbID := lpt.DbID
	collectionID := lpt.CollectionID
	partitionIDs := lpt.PartitionIDs
	schema := lpt.Schema

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	for _, partitionID := range partitionIDs {
		showSegmentRequest := &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_ShowSegments,
			},
			CollectionID: collectionID,
			PartitionID:  partitionID,
		}
		showSegmentResponse, err := lpt.masterService.ShowSegments(ctx, showSegmentRequest)
		if err != nil {
			status.Reason = err.Error()
			lpt.result = status
			return err
		}
		segmentIDs := showSegmentResponse.SegmentIDs
		if len(segmentIDs) == 0 {
			loadSegmentRequest := &querypb.LoadSegmentsRequest{
				// TODO: use unique id allocator to assign reqID
				Base: &commonpb.MsgBase{
					Timestamp: lpt.Base.Timestamp,
					MsgID:     rand.Int63n(10000000000),
				},
				DbID:         dbID,
				CollectionID: collectionID,
				PartitionID:  partitionID,
				Schema:       schema,
			}
			for nodeID := range lpt.cluster.nodes {
				_, err := lpt.cluster.LoadSegments(ctx, nodeID, loadSegmentRequest)
				if err != nil {
					status.Reason = err.Error()
					lpt.result = status
					return err
				}
			}
		}

		lpt.meta.updatePartitionState(dbID, collectionID, partitionID, querypb.PartitionState_PartialInMemory)

		segmentStates := make(map[UniqueID]*datapb.SegmentStateInfo)
		channel2segs := make(map[string][]UniqueID)
		resp, err := lpt.dataService.GetSegmentStates(ctx, &datapb.GetSegmentStatesRequest{
			SegmentIDs: segmentIDs,
		})
		if err != nil {
			status.Reason = err.Error()
			lpt.result = status
			return err
		}
		log.Debug("getSegmentStates result ", zap.Any("segment states", resp.States), zap.Any("result status", resp.Status))
		for _, state := range resp.States {
			log.Debug("segment ", zap.String("state.SegmentID", fmt.Sprintln(state.SegmentID)), zap.String("state", fmt.Sprintln(state.StartPosition)))
			segmentID := state.SegmentID
			segmentStates[segmentID] = state
			channelName := state.StartPosition.ChannelName
			if _, ok := channel2segs[channelName]; !ok {
				segments := make([]UniqueID, 0)
				segments = append(segments, segmentID)
				channel2segs[channelName] = segments
			} else {
				channel2segs[channelName] = append(channel2segs[channelName], segmentID)
			}
		}

		excludeSegment := make([]UniqueID, 0)
		for id, state := range segmentStates {
			if state.State > commonpb.SegmentState_Growing {
				excludeSegment = append(excludeSegment, id)
			}
		}
		for channel, segmentIDs := range channel2segs {
			sort.Slice(segmentIDs, func(i, j int) bool {
				return segmentStates[segmentIDs[i]].StartPosition.Timestamp < segmentStates[segmentIDs[j]].StartPosition.Timestamp
			})
			toLoadSegmentIDs := make([]UniqueID, 0)
			var watchedStartPos *internalpb.MsgPosition = nil
			var startPosition *internalpb.MsgPosition = nil
			for index, id := range segmentIDs {
				if segmentStates[id].State <= commonpb.SegmentState_Growing {
					if index > 0 {
						pos := segmentStates[id].StartPosition
						if len(pos.MsgID) == 0 {
							watchedStartPos = startPosition
							break
						}
					}
					watchedStartPos = segmentStates[id].StartPosition
					break
				}
				toLoadSegmentIDs = append(toLoadSegmentIDs, id)
				watchedStartPos = segmentStates[id].EndPosition
				startPosition = segmentStates[id].StartPosition
			}
			if watchedStartPos == nil {
				watchedStartPos = &internalpb.MsgPosition{
					ChannelName: channel,
				}
			}

			err = lpt.meta.addDmChannels(dbID, collectionID, []string{channel})
			if err != nil {
				status.Reason = err.Error()
				lpt.result = status
				return err
			}
			lpt.toWatchPosition[channel] = watchedStartPos
			lpt.excludeSegment[channel] = toLoadSegmentIDs
			//err = lpt.meta.addExcludeSegmentIDs(dbID, collectionID, toLoadSegmentIDs)
			//if err != nil {
			//	status.Reason = err.Error()
			//	lpt.result = status
			//	return err
			//}

			segment2Node := shuffleSegmentsToQueryNode(toLoadSegmentIDs, lpt.cluster)
			for nodeID, assignedSegmentIDs := range segment2Node {
				loadSegmentRequest := &querypb.LoadSegmentsRequest{
					// TODO: use unique id allocator to assign reqID
					Base: &commonpb.MsgBase{
						Timestamp: lpt.Base.Timestamp,
						MsgID:     rand.Int63n(10000000000),
					},
					DbID:         dbID,
					CollectionID: collectionID,
					PartitionID:  partitionID,
					SegmentIDs:   assignedSegmentIDs,
					Schema:       schema,
				}

				//node := lpt.cluster.nodes[nodeID]
				status, err := lpt.cluster.LoadSegments(ctx, nodeID, loadSegmentRequest)
				if err != nil {
					lpt.result = status
					return err
				}
				lpt.cluster.AddSegments(assignedSegmentIDs, nodeID, collectionID)
			}
		}

		lpt.meta.updatePartitionState(dbID, collectionID, partitionID, querypb.PartitionState_InMemory)
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
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	dbID := lpt.DbID
	collectionID := lpt.CollectionID
	partitionIDs := lpt.PartitionIDs
	if lpt.watchNeeded {
		err := watchDmChannels(ctx, lpt.dataService, lpt.cluster, lpt.meta, dbID, collectionID, lpt.Base, lpt.toWatchPosition, lpt.excludeSegment)
		if err != nil {
			log.Debug("watchDmChannels failed", zap.Int64("msgID", lpt.ID()), zap.Int64s("partitionIDs", partitionIDs), zap.Error(err))
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
			lpt.result = status
			return err
		}
	}
	log.Debug("watchDmChannels completed", zap.Int64("msgID", lpt.ID()), zap.Int64s("partitionIDs", partitionIDs))
	lpt.result = status
	log.Debug("LoadPartitionTask postExecute done",
		zap.Int64("msgID", lpt.ID()),
		zap.Int64("collectionID", collectionID))
	//lpt.cancel()
	return nil
}

type ReleasePartitionTask struct {
	BaseTask
	*querypb.ReleasePartitionsRequest
	cluster *queryNodeCluster
	meta    *meta
}

func (rpt *ReleasePartitionTask) ID() UniqueID {
	return rpt.Base.MsgID
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
	for nodeID := range rpt.cluster.nodes {
		status, err := rpt.cluster.ReleasePartitions(ctx, nodeID, rpt.ReleasePartitionsRequest)
		if err != nil {
			rpt.result = status
			return err
		}
	}

	rpt.result = status
	log.Debug("ReleasePartitionTask Execute done",
		zap.Int64("msgID", rpt.ID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs))
	return nil
}

func (rpt *ReleasePartitionTask) PostExecute(ctx context.Context) error {
	dbID := rpt.DbID
	collectionID := rpt.CollectionID
	partitionIDs := rpt.PartitionIDs
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	for _, partitionID := range partitionIDs {
		err := rpt.meta.releasePartition(dbID, collectionID, partitionID)
		if err != nil {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
			rpt.result = status
			return err
		}
	}
	rpt.result = status
	log.Debug("ReleasePartitionTask postExecute done",
		zap.Int64("msgID", rpt.ID()),
		zap.Int64("collectionID", collectionID))
	return nil
}

//****************************handoff task********************************//
type HandoffTask struct {
}

//**************************queryNodeCluster down task***************************//
type queryNodeDownTask struct {
}

//*********************** ***load balance task*** ************************//
type loadBalanceTask struct {
}

//****************************internal task*******************************//

type LoadSegmentTask struct {
	BaseTask
	sourceNode int64
	dstNode    int64
	*querypb.LoadSegmentsRequest
}

type ReleaseSegmentTask struct {
}

type WatchDmChannelTask struct {
}

type AddQueryChannelTask struct {
}

func watchDmChannels(ctx context.Context,
	dataService types.DataService,
	cluster *queryNodeCluster,
	meta *meta,
	dbID UniqueID,
	collectionID UniqueID,
	msgBase *commonpb.MsgBase,
	toWatchPosition map[string]*internalpb.MsgPosition,
	excludeSegment map[string][]UniqueID) error {
	col, err := meta.getCollectionByID(0, collectionID)
	if err != nil {
		return err
	}
	channelRequest := datapb.GetInsertChannelsRequest{
		DbID:         dbID,
		CollectionID: collectionID,
	}
	resp, err := dataService.GetInsertChannels(ctx, &channelRequest)
	if err != nil {
		return err
	}
	if len(resp.Values) == 0 {
		err = errors.New("haven't assign dm channel to collection")
		return err
	}

	dmChannels := resp.Values
	channelsWithoutPos := make([]string, 0)
	for _, channel := range dmChannels {
		findChannel := false
		ChannelsWithPos := col.dmChannels
		for _, ch := range ChannelsWithPos {
			if channel == ch {
				findChannel = true
				break
			}
		}
		if !findChannel {
			channelsWithoutPos = append(channelsWithoutPos, channel)
		}
	}

	err = meta.addDmChannels(dbID, collectionID, channelsWithoutPos)
	if err != nil {
		return err
	}
	//for _, ch := range channelsWithoutPos {
	//	pos := &internalpb.MsgPosition{
	//		ChannelName: ch,
	//	}
	//	err = meta.addDmChannels(dbID, collectionID, chs)
	//	if err != nil {
	//		return err
	//	}
	//}

	nodeID2Channels := shuffleChannelsToQueryNode(dbID, dmChannels, cluster)
	for nodeID, channels := range nodeID2Channels {
		//node := queryNodes[nodeID]
		watchDmChannelsInfo := make([]*querypb.WatchDmChannelInfo, 0)
		for _, ch := range channels {
			info := &querypb.WatchDmChannelInfo{
				ChannelID: ch,
				//Pos:              col.dmChannels2Pos[ch],
				//ExcludedSegments: col.excludeSegmentIds,
			}
			if _, ok := toWatchPosition[ch]; ok {
				info.Pos = toWatchPosition[ch]
				info.ExcludedSegments = excludeSegment[ch]
			}
			watchDmChannelsInfo = append(watchDmChannelsInfo, info)
		}
		request := &querypb.WatchDmChannelsRequest{
			Base:         msgBase,
			CollectionID: collectionID,
			ChannelIDs:   channels,
			Infos:        watchDmChannelsInfo,
		}
		_, err := cluster.WatchDmChannels(ctx, nodeID, request)
		if err != nil {
			return err
		}
		cluster.AddDmChannels(dbID, nodeID, channels, collectionID)
		log.Debug("query node ", zap.String("nodeID", strconv.FormatInt(nodeID, 10)), zap.String("watch channels", fmt.Sprintln(channels)))
	}

	return nil
}

func shuffleChannelsToQueryNode(dbID UniqueID, dmChannels []string, cluster *queryNodeCluster) map[int64][]string {
	maxNumChannels := 0
	for nodeID := range cluster.nodes {
		//TODO::issue error
		numChannels, _ := cluster.getNumChannels(dbID, nodeID)
		if numChannels > maxNumChannels {
			maxNumChannels = numChannels
		}
	}
	res := make(map[int64][]string)
	offset := 0
	loopAll := false
	for {
		lastOffset := offset
		if !loopAll {
			for id := range cluster.nodes {
				if cluster.getSegmentsLength(id) >= maxNumChannels {
					continue
				}
				if _, ok := res[id]; !ok {
					res[id] = make([]string, 0)
				}
				res[id] = append(res[id], dmChannels[offset])
				offset++
				if offset == len(dmChannels) {
					return res
				}
			}
		} else {
			for id := range cluster.nodes {
				if _, ok := res[id]; !ok {
					res[id] = make([]string, 0)
				}
				res[id] = append(res[id], dmChannels[offset])
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

func shuffleSegmentsToQueryNode(segmentIDs []UniqueID, cluster *queryNodeCluster) map[int64][]UniqueID {
	maxNumSegments := 0
	for nodeID := range cluster.nodes {
		numSegments := cluster.getNumSegments(nodeID)
		if numSegments > maxNumSegments {
			maxNumSegments = numSegments
		}
	}
	res := make(map[int64][]UniqueID)
	for nodeID := range cluster.nodes {
		segments := make([]UniqueID, 0)
		res[nodeID] = segments
	}

	if len(segmentIDs) == 0 {
		return res
	}

	offset := 0
	loopAll := false
	for {
		lastOffset := offset
		if !loopAll {
			for id := range cluster.nodes {
				if cluster.getSegmentsLength(id) >= maxNumSegments {
					continue
				}
				if _, ok := res[id]; !ok {
					res[id] = make([]UniqueID, 0)
				}
				res[id] = append(res[id], segmentIDs[offset])
				offset++
				if offset == len(segmentIDs) {
					return res
				}
			}
		} else {
			for id := range cluster.nodes {
				if _, ok := res[id]; !ok {
					res[id] = make([]UniqueID, 0)
				}
				res[id] = append(res[id], segmentIDs[offset])
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
