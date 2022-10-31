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

package querynode

import (
	"context"
	"errors"
	"fmt"
	"math"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	queryPb "github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

type watchDmChannelsTask struct {
	baseTask
	req  *queryPb.WatchDmChannelsRequest
	node *QueryNode
}

// watchDmChannelsTask
func (w *watchDmChannelsTask) Execute(ctx context.Context) (err error) {
	collectionID := w.req.CollectionID
	partitionIDs := w.req.GetPartitionIDs()

	lType := w.req.GetLoadMeta().GetLoadType()
	if lType == queryPb.LoadType_UnKnownType {
		// if no partitionID is specified, load type is load collection
		if len(partitionIDs) != 0 {
			lType = queryPb.LoadType_LoadPartition
		} else {
			lType = queryPb.LoadType_LoadCollection
		}
	}

	// get all vChannels
	var vChannels []Channel
	VPChannels := make(map[string]string) // map[vChannel]pChannel
	for _, info := range w.req.Infos {
		v := info.ChannelName
		p := funcutil.ToPhysicalChannel(info.ChannelName)
		vChannels = append(vChannels, v)
		VPChannels[v] = p
	}

	if len(VPChannels) != len(vChannels) {
		return errors.New("get physical channels failed, illegal channel length, collectionID = " + fmt.Sprintln(collectionID))
	}

	log.Info("Starting WatchDmChannels ...",
		zap.String("collectionName", w.req.Schema.Name),
		zap.Int64("collectionID", collectionID),
		zap.Int64("replicaID", w.req.GetReplicaID()),
		zap.String("load type", lType.String()),
		zap.Strings("vChannels", vChannels),
	)

	// init collection meta
	coll := w.node.metaReplica.addCollection(collectionID, w.req.Schema)

	// filter out the already exist channels
	vChannels = coll.AddChannels(vChannels, VPChannels)
	defer func() {
		if err != nil {
			for _, vChannel := range vChannels {
				coll.removeVChannel(vChannel)
			}
		}
	}()

	if len(vChannels) == 0 {
		log.Warn("all channels has be added before, ignore watch dml requests")
		return nil
	}

	//add shard cluster
	for _, vchannel := range vChannels {
		w.node.ShardClusterService.addShardCluster(w.req.GetCollectionID(), w.req.GetReplicaID(), vchannel)
	}

	defer func() {
		if err != nil {
			for _, vchannel := range vChannels {
				w.node.ShardClusterService.releaseShardCluster(vchannel)
			}
		}
	}()

	unFlushedSegmentIDs, err := w.LoadGrowingSegments(ctx, collectionID)

	// remove growing segment if watch dmChannels failed
	defer func() {
		if err != nil {
			for _, segmentID := range unFlushedSegmentIDs {
				w.node.metaReplica.removeSegment(segmentID, segmentTypeGrowing)
			}
		}
	}()

	channel2FlowGraph, err := w.initFlowGraph(ctx, collectionID, vChannels, VPChannels)
	if err != nil {
		return err
	}

	coll.setLoadType(lType)

	log.Info("watchDMChannel, init replica done", zap.Int64("collectionID", collectionID), zap.Strings("vChannels", vChannels))

	// create tSafe
	for _, channel := range vChannels {
		w.node.tSafeReplica.addTSafe(channel)
	}

	// add tsafe watch in query shard if exists
	for _, dmlChannel := range vChannels {
		w.node.queryShardService.addQueryShard(collectionID, dmlChannel, w.req.GetReplicaID())
	}

	// start flow graphs
	for _, fg := range channel2FlowGraph {
		fg.flowGraph.Start()
	}

	log.Info("WatchDmChannels done", zap.Int64("collectionID", collectionID), zap.Strings("vChannels", vChannels))
	return nil
}

func (w *watchDmChannelsTask) LoadGrowingSegments(ctx context.Context, collectionID UniqueID) ([]UniqueID, error) {
	// load growing segments
	unFlushedSegments := make([]*queryPb.SegmentLoadInfo, 0)
	unFlushedSegmentIDs := make([]UniqueID, 0)
	for _, info := range w.req.Infos {
		for _, ufInfoID := range info.GetUnflushedSegmentIds() {
			// unFlushed segment may not have binLogs, skip loading
			ufInfo := w.req.GetSegmentInfos()[ufInfoID]
			if ufInfo == nil {
				log.Warn("an unflushed segment is not found in segment infos", zap.Int64("segment ID", ufInfoID))
				continue
			}
			if len(ufInfo.GetBinlogs()) > 0 {
				unFlushedSegments = append(unFlushedSegments, &queryPb.SegmentLoadInfo{
					SegmentID:     ufInfo.ID,
					PartitionID:   ufInfo.PartitionID,
					CollectionID:  ufInfo.CollectionID,
					BinlogPaths:   ufInfo.Binlogs,
					NumOfRows:     ufInfo.NumOfRows,
					Statslogs:     ufInfo.Statslogs,
					Deltalogs:     ufInfo.Deltalogs,
					InsertChannel: ufInfo.InsertChannel,
				})
				unFlushedSegmentIDs = append(unFlushedSegmentIDs, ufInfo.GetID())
			} else {
				log.Info("skip segment which binlog is empty", zap.Int64("segmentID", ufInfo.ID))
			}
		}
	}
	req := &queryPb.LoadSegmentsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_LoadSegments),
			commonpbutil.WithMsgID(w.req.Base.MsgID), // use parent task's msgID
		),
		Infos:        unFlushedSegments,
		CollectionID: collectionID,
		Schema:       w.req.GetSchema(),
		LoadMeta:     w.req.GetLoadMeta(),
	}

	// update partition info from unFlushedSegments and loadMeta
	for _, info := range req.Infos {
		err := w.node.metaReplica.addPartition(collectionID, info.PartitionID)
		if err != nil {
			return nil, err
		}
	}
	for _, partitionID := range req.GetLoadMeta().GetPartitionIDs() {
		err := w.node.metaReplica.addPartition(collectionID, partitionID)
		if err != nil {
			return nil, err
		}
	}

	log.Info("loading growing segments in WatchDmChannels...",
		zap.Int64("collectionID", collectionID),
		zap.Int64s("unFlushedSegmentIDs", unFlushedSegmentIDs),
	)
	err := w.node.loader.LoadSegment(w.ctx, req, segmentTypeGrowing)
	if err != nil {
		log.Warn("failed to load segment", zap.Int64("collection", collectionID), zap.Error(err))
		return nil, err
	}
	log.Info("successfully load growing segments done in WatchDmChannels",
		zap.Int64("collectionID", collectionID),
		zap.Int64s("unFlushedSegmentIDs", unFlushedSegmentIDs),
	)
	return unFlushedSegmentIDs, nil
}

func (w *watchDmChannelsTask) initFlowGraph(ctx context.Context, collectionID UniqueID, vChannels []Channel, VPChannels map[string]string) (map[string]*queryNodeFlowGraph, error) {
	// So far, we don't support to enable each node with two different channel
	consumeSubName := funcutil.GenChannelSubName(Params.CommonCfg.QueryNodeSubName, collectionID, Params.QueryNodeCfg.GetNodeID())

	// group channels by to seeking or consuming
	channel2SeekPosition := make(map[string]*internalpb.MsgPosition)

	// for channel with no position
	channel2AsConsumerPosition := make(map[string]*internalpb.MsgPosition)
	for _, info := range w.req.Infos {
		if info.SeekPosition == nil || len(info.SeekPosition.MsgID) == 0 {
			channel2AsConsumerPosition[info.ChannelName] = info.SeekPosition
			continue
		}
		info.SeekPosition.MsgGroup = consumeSubName
		channel2SeekPosition[info.ChannelName] = info.SeekPosition
	}
	log.Info("watchDMChannel, group channels done", zap.Int64("collectionID", collectionID))

	// add excluded segments for unFlushed segments,
	// unFlushed segments before check point should be filtered out.
	unFlushedCheckPointInfos := make([]*datapb.SegmentInfo, 0)
	for _, info := range w.req.Infos {
		for _, ufsID := range info.GetUnflushedSegmentIds() {
			unFlushedCheckPointInfos = append(unFlushedCheckPointInfos, w.req.SegmentInfos[ufsID])
		}
	}
	w.node.metaReplica.addExcludedSegments(collectionID, unFlushedCheckPointInfos)
	unflushedSegmentIDs := make([]UniqueID, len(unFlushedCheckPointInfos))
	for i, segInfo := range unFlushedCheckPointInfos {
		unflushedSegmentIDs[i] = segInfo.GetID()
	}
	log.Info("watchDMChannel, add check points info for unflushed segments done",
		zap.Int64("collectionID", collectionID),
		zap.Any("unflushedSegmentIDs", unflushedSegmentIDs),
	)

	// add excluded segments for flushed segments,
	// flushed segments with later check point than seekPosition should be filtered out.
	flushedCheckPointInfos := make([]*datapb.SegmentInfo, 0)
	for _, info := range w.req.Infos {
		for _, flushedSegmentID := range info.GetFlushedSegmentIds() {
			flushedSegment := w.req.SegmentInfos[flushedSegmentID]
			for _, position := range channel2SeekPosition {
				if flushedSegment.DmlPosition != nil &&
					flushedSegment.DmlPosition.ChannelName == position.ChannelName &&
					flushedSegment.DmlPosition.Timestamp > position.Timestamp {
					flushedCheckPointInfos = append(flushedCheckPointInfos, flushedSegment)
				}
			}
		}
	}
	w.node.metaReplica.addExcludedSegments(collectionID, flushedCheckPointInfos)
	flushedSegmentIDs := make([]UniqueID, len(flushedCheckPointInfos))
	for i, segInfo := range flushedCheckPointInfos {
		flushedSegmentIDs[i] = segInfo.GetID()
	}
	log.Info("watchDMChannel, add check points info for flushed segments done",
		zap.Int64("collectionID", collectionID),
		zap.Any("flushedSegmentIDs", flushedSegmentIDs),
	)

	// add excluded segments for dropped segments,
	// exclude all msgs with dropped segment id
	// DO NOT refer to dropped segment info, see issue https://github.com/milvus-io/milvus/issues/19704
	var droppedCheckPointInfos []*datapb.SegmentInfo
	for _, info := range w.req.Infos {
		for _, droppedSegmentID := range info.GetDroppedSegmentIds() {
			droppedCheckPointInfos = append(droppedCheckPointInfos, &datapb.SegmentInfo{
				ID:            droppedSegmentID,
				CollectionID:  collectionID,
				InsertChannel: info.GetChannelName(),
				DmlPosition: &internalpb.MsgPosition{
					ChannelName: info.GetChannelName(),
					Timestamp:   math.MaxUint64,
				},
			})
		}
	}
	w.node.metaReplica.addExcludedSegments(collectionID, droppedCheckPointInfos)
	droppedSegmentIDs := make([]UniqueID, len(droppedCheckPointInfos))
	for i, segInfo := range droppedCheckPointInfos {
		droppedSegmentIDs[i] = segInfo.GetID()
	}
	log.Info("watchDMChannel, add check points info for dropped segments done",
		zap.Int64("collectionID", collectionID),
		zap.Any("droppedSegmentIDs", droppedSegmentIDs),
	)

	// add flow graph
	channel2FlowGraph, err := w.node.dataSyncService.addFlowGraphsForDMLChannels(collectionID, vChannels)
	if err != nil {
		log.Warn("watchDMChannel, add flowGraph for dmChannels failed", zap.Int64("collectionID", collectionID), zap.Strings("vChannels", vChannels), zap.Error(err))
		return nil, err
	}
	log.Info("Query node add DML flow graphs", zap.Int64("collectionID", collectionID), zap.Any("channels", vChannels))

	// channels as consumer
	for channel, fg := range channel2FlowGraph {
		if _, ok := channel2AsConsumerPosition[channel]; ok {
			// use pChannel to consume
			err = fg.consumeFlowGraph(VPChannels[channel], consumeSubName)
			if err != nil {
				log.Error("msgStream as consumer failed for dmChannels", zap.Int64("collectionID", collectionID), zap.String("vChannel", channel))
				break
			}
		}

		if pos, ok := channel2SeekPosition[channel]; ok {
			pos.MsgGroup = consumeSubName
			// use pChannel to seek
			pos.ChannelName = VPChannels[channel]
			err = fg.consumeFlowGraphFromPosition(pos)
			if err != nil {
				log.Error("msgStream seek failed for dmChannels", zap.Int64("collectionID", collectionID), zap.String("vChannel", channel))
				break
			}
		}
	}

	if err != nil {
		log.Warn("watchDMChannel, add flowGraph for dmChannels failed", zap.Int64("collectionID", collectionID), zap.Strings("vChannels", vChannels), zap.Error(err))
		for _, fg := range channel2FlowGraph {
			fg.flowGraph.Close()
		}
		gcChannels := make([]Channel, 0)
		for channel := range channel2FlowGraph {
			gcChannels = append(gcChannels, channel)
		}
		w.node.dataSyncService.removeFlowGraphsByDMLChannels(gcChannels)
		return nil, err
	}

	log.Info("watchDMChannel, add flowGraph for dmChannels success", zap.Int64("collectionID", collectionID), zap.Strings("vChannels", vChannels))
	return channel2FlowGraph, nil
}
