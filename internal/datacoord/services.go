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

package datacoord

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/trace"
)

// checks whether server in Healthy State
func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.isServing) != ServerStateHealthy
}

// GetTimeTickChannel legacy API, returns time tick channel name
func (s *Server) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: Params.CommonCfg.DataCoordTimeTick,
	}, nil
}

// GetStatisticsChannel legacy API, returns statistics channel name
func (s *Server) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "no statistics channel",
		},
	}, nil
}

// Flush notify segment to flush
// this api only guarantees all the segments requested is sealed
// these segments will be flushed only after the Flush policy is fulfilled
func (s *Server) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	log.Info("receive flush request", zap.Int64("dbID", req.GetDbID()), zap.Int64("collectionID", req.GetCollectionID()))
	sp, ctx := trace.StartSpanFromContextWithOperationName(ctx, "DataCoord-Flush")
	defer sp.Finish()
	resp := &datapb.FlushResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "",
		},
		DbID:         0,
		CollectionID: 0,
		SegmentIDs:   []int64{},
	}
	if s.isClosed() {
		resp.Status.Reason = serverNotServingErrMsg
		return resp, nil
	}
	sealedSegments, err := s.segmentManager.SealAllSegments(ctx, req.GetCollectionID(), req.GetSegmentIDs())
	if err != nil {
		resp.Status.Reason = fmt.Sprintf("failed to flush %d, %s", req.CollectionID, err)
		return resp, nil
	}
	log.Info("flush response with segments",
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Any("segments", sealedSegments))
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.DbID = req.GetDbID()
	resp.CollectionID = req.GetCollectionID()
	resp.SegmentIDs = sealedSegments
	return resp, nil
}

// AssignSegmentID applies for segment ids and make allocation for records.
func (s *Server) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	if s.isClosed() {
		return &datapb.AssignSegmentIDResponse{
			Status: &commonpb.Status{
				Reason:    serverNotServingErrMsg,
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, nil
	}

	assigns := make([]*datapb.SegmentIDAssignment, 0, len(req.SegmentIDRequests))

	for _, r := range req.SegmentIDRequests {
		log.Info("handle assign segment request",
			zap.Int64("collectionID", r.GetCollectionID()),
			zap.Int64("partitionID", r.GetPartitionID()),
			zap.String("channelName", r.GetChannelName()),
			zap.Uint32("count", r.GetCount()),
			zap.Bool("isImport", r.GetIsImport()))

		// Load the collection info from Root Coordinator, if it is not found in server meta.
		if s.meta.GetCollection(r.GetCollectionID()) == nil {
			err := s.loadCollectionFromRootCoord(ctx, r.GetCollectionID())
			if err != nil {
				log.Warn("failed to load collection in alloc segment", zap.Any("request", r), zap.Error(err))
				continue
			}
		}

		// Add the channel to cluster for watching.
		s.cluster.Watch(r.ChannelName, r.CollectionID)

		segmentAllocations := make([]*Allocation, 0)
		if r.GetIsImport() {
			// Have segment manager allocate and return the segment allocation info.
			segAlloc, err := s.segmentManager.AllocSegmentForImport(ctx,
				r.CollectionID, r.PartitionID, r.ChannelName, int64(r.Count))
			if err != nil {
				log.Warn("failed to alloc segment for import", zap.Any("request", r), zap.Error(err))
				continue
			}
			segmentAllocations = append(segmentAllocations, segAlloc)
		} else {
			// Have segment manager allocate and return the segment allocation info.
			segAlloc, err := s.segmentManager.AllocSegment(ctx,
				r.CollectionID, r.PartitionID, r.ChannelName, int64(r.Count))
			if err != nil {
				log.Warn("failed to alloc segment", zap.Any("request", r), zap.Error(err))
				continue
			}
			segmentAllocations = append(segmentAllocations, segAlloc...)
		}

		log.Info("success to assign segments", zap.Int64("collectionID", r.GetCollectionID()), zap.Any("assignments", segmentAllocations))

		for _, allocation := range segmentAllocations {
			result := &datapb.SegmentIDAssignment{
				SegID:        allocation.SegmentID,
				ChannelName:  r.ChannelName,
				Count:        uint32(allocation.NumOfRows),
				CollectionID: r.CollectionID,
				PartitionID:  r.PartitionID,
				ExpireTime:   allocation.ExpireTime,
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
					Reason:    "",
				},
			}
			assigns = append(assigns, result)
		}
	}
	return &datapb.AssignSegmentIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		SegIDAssignments: assigns,
	}, nil
}

// GetSegmentStates returns segments state
func (s *Server) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	resp := &datapb.GetSegmentStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = serverNotServingErrMsg
		return resp, nil
	}

	for _, segmentID := range req.SegmentIDs {
		state := &datapb.SegmentStateInfo{
			SegmentID: segmentID,
		}
		segmentInfo := s.meta.GetSegment(segmentID)
		if segmentInfo == nil {
			state.State = commonpb.SegmentState_NotExist
		} else {
			state.State = segmentInfo.GetState()
			state.StartPosition = segmentInfo.GetStartPosition()
		}
		resp.States = append(resp.States, state)
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_Success

	return resp, nil
}

// GetInsertBinlogPaths returns binlog paths info for requested segments
func (s *Server) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	resp := &datapb.GetInsertBinlogPathsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = serverNotServingErrMsg
		return resp, nil
	}
	segment := s.meta.GetSegment(req.GetSegmentID())
	if segment == nil {
		resp.Status.Reason = "segment not found"
		return resp, nil
	}
	binlogs := segment.GetBinlogs()
	fids := make([]UniqueID, 0, len(binlogs))
	paths := make([]*internalpb.StringList, 0, len(binlogs))
	for _, field := range binlogs {
		fids = append(fids, field.GetFieldID())
		binlogs := field.GetBinlogs()
		p := make([]string, 0, len(binlogs))
		for _, log := range binlogs {
			p = append(p, log.GetLogPath())
		}
		paths = append(paths, &internalpb.StringList{Values: p})
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.FieldIDs = fids
	resp.Paths = paths
	return resp, nil
}

// GetCollectionStatistics returns statistics for collection
// for now only row count is returned
func (s *Server) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
	ctx = logutil.WithModule(ctx, moduleName)
	logutil.Logger(ctx).Debug("received request to get collection statistics")
	resp := &datapb.GetCollectionStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = serverNotServingErrMsg
		return resp, nil
	}
	nums := s.meta.GetNumRowsOfCollection(req.CollectionID)
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.Stats = append(resp.Stats, &commonpb.KeyValuePair{Key: "row_count", Value: strconv.FormatInt(nums, 10)})
	logutil.Logger(ctx).Debug("success to get collection statistics", zap.Any("response", resp))
	return resp, nil
}

// GetPartitionStatistics returns statistics for parition
// for now only row count is returned
func (s *Server) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	resp := &datapb.GetPartitionStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = serverNotServingErrMsg
		return resp, nil
	}
	nums := s.meta.GetNumRowsOfPartition(req.CollectionID, req.PartitionID)
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.Stats = append(resp.Stats, &commonpb.KeyValuePair{Key: "row_count", Value: strconv.FormatInt(nums, 10)})
	return resp, nil
}

// GetSegmentInfoChannel legacy API, returns segment info statistics channel
func (s *Server) GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: Params.CommonCfg.DataCoordSegmentInfo,
	}, nil
}

// GetSegmentInfo returns segment info requested, status, row count, etc included
func (s *Server) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	resp := &datapb.GetSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = serverNotServingErrMsg
		return resp, nil
	}
	infos := make([]*datapb.SegmentInfo, 0, len(req.SegmentIDs))
	for _, id := range req.SegmentIDs {
		var info *SegmentInfo
		if req.IncludeUnHealthy {
			info = s.meta.GetAllSegment(id)
			if info != nil {
				infos = append(infos, info.SegmentInfo)
			}
		} else {
			info = s.meta.GetSegment(id)
			if info == nil {
				resp.Status.Reason = fmt.Sprintf("failed to get segment %d", id)
				return resp, nil
			}
			infos = append(infos, info.SegmentInfo)
		}
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.Infos = infos
	return resp, nil
}

// SaveBinlogPaths updates segment related binlog path
// works for Checkpoints and Flush
func (s *Server) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	resp := &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}

	if s.isClosed() {
		resp.Reason = serverNotServingErrMsg
		return resp, nil
	}

	log.Info("receive SaveBinlogPaths request",
		zap.Int64("nodeID", req.GetBase().GetSourceID()),
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64("segmentID", req.GetSegmentID()),
		zap.Bool("isFlush", req.GetFlushed()),
		zap.Bool("isDropped", req.GetDropped()),
		zap.Any("startPositions", req.GetStartPositions()),
		zap.Any("checkpoints", req.GetCheckPoints()))

	// validate
	nodeID := req.GetBase().GetSourceID()
	segmentID := req.GetSegmentID()
	segment := s.meta.GetSegment(segmentID)

	if segment == nil {
		log.Error("failed to get segment", zap.Int64("segmentID", segmentID))
		failResponseWithCode(resp, commonpb.ErrorCode_SegmentNotFound, fmt.Sprintf("failed to get segment %d", segmentID))
		return resp, nil
	}

	// No need to check import channel--node matching in data import case.
	if !req.GetImporting() {
		channel := segment.GetInsertChannel()
		if !s.channelManager.Match(nodeID, channel) {
			failResponse(resp, fmt.Sprintf("channel %s is not watched on node %d", channel, nodeID))
			resp.ErrorCode = commonpb.ErrorCode_MetaFailed
			log.Warn("node is not matched with channel", zap.String("channel", channel), zap.Int64("nodeID", nodeID))
			return resp, nil
		}
	}

	if req.GetDropped() {
		s.segmentManager.DropSegment(ctx, segment.GetID())
	}

	// Set segment to SegmentState_Flushing. Also save binlogs and checkpoints.
	err := s.meta.UpdateFlushSegmentsInfo(
		req.GetSegmentID(),
		req.GetFlushed(),
		req.GetDropped(),
		req.GetImporting(),
		req.GetField2BinlogPaths(),
		req.GetField2StatslogPaths(),
		req.GetDeltalogs(),
		req.GetCheckPoints(),
		req.GetStartPositions())
	if err != nil {
		log.Error("save binlog and checkpoints failed",
			zap.Int64("segmentID", req.GetSegmentID()),
			zap.Error(err))
		resp.Reason = err.Error()
		return resp, nil
	}

	log.Info("flush segment with meta", zap.Int64("segment id", req.SegmentID),
		zap.Any("meta", req.GetField2BinlogPaths()))

	if req.GetFlushed() {
		s.segmentManager.DropSegment(ctx, req.SegmentID)
		s.flushCh <- req.SegmentID

		if Params.DataCoordCfg.EnableCompaction {
			cctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
			defer cancel()

			ct, err := getCompactTime(cctx, s.allocator)
			if err == nil {
				err = s.compactionTrigger.triggerSingleCompaction(segment.GetCollectionID(),
					segment.GetPartitionID(), segmentID, segment.GetInsertChannel(), ct)
				if err != nil {
					log.Warn("failed to trigger single compaction", zap.Int64("segment ID", segmentID))
				} else {
					log.Info("compaction triggered for segment", zap.Int64("segment ID", segmentID))
				}
			} else {
				log.Warn("failed to get time travel reverse time")
			}
		}
	}
	resp.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// DropVirtualChannel notifies vchannel dropped
// And contains the remaining data log & checkpoint to update
func (s *Server) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	resp := &datapb.DropVirtualChannelResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = serverNotServingErrMsg
		return resp, nil
	}

	channel := req.GetChannelName()
	log.Info("receive DropVirtualChannel request",
		zap.String("channel name", channel))

	// validate
	nodeID := req.GetBase().GetSourceID()
	if !s.channelManager.Match(nodeID, channel) {
		failResponse(resp.Status, fmt.Sprintf("channel %s is not watched on node %d", channel, nodeID))
		resp.Status.ErrorCode = commonpb.ErrorCode_MetaFailed
		log.Warn("node is not matched with channel", zap.String("channel", channel), zap.Int64("nodeID", nodeID))
		return resp, nil
	}

	segments := make([]*SegmentInfo, 0, len(req.GetSegments()))
	for _, seg2Drop := range req.GetSegments() {
		info := &datapb.SegmentInfo{
			ID:            seg2Drop.GetSegmentID(),
			CollectionID:  seg2Drop.GetCollectionID(),
			InsertChannel: channel,
			Binlogs:       seg2Drop.GetField2BinlogPaths(),
			Statslogs:     seg2Drop.GetField2StatslogPaths(),
			Deltalogs:     seg2Drop.GetDeltalogs(),
			StartPosition: seg2Drop.GetStartPosition(),
			DmlPosition:   seg2Drop.GetCheckPoint(),
			NumOfRows:     seg2Drop.GetNumOfRows(),
		}
		segment := NewSegmentInfo(info)
		segments = append(segments, segment)
	}

	err := s.meta.UpdateDropChannelSegmentInfo(channel, segments)
	if err != nil {
		log.Error("Update Drop Channel segment info failed", zap.String("channel", channel), zap.Error(err))
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	log.Info("DropVChannel plan to remove", zap.String("channel", channel))
	err = s.channelManager.Release(nodeID, channel)
	if err != nil {
		log.Warn("DropVChannel failed to ReleaseAndRemove", zap.String("channel", channel), zap.Error(err))
	}
	s.segmentManager.DropSegmentsOfChannel(ctx, channel)

	// no compaction triggerred in Drop procedure
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// SetSegmentState reset the state of the given segment.
func (s *Server) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest) (*datapb.SetSegmentStateResponse, error) {
	if s.isClosed() {
		return &datapb.SetSegmentStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    serverNotServingErrMsg,
			},
		}, nil
	}
	err := s.meta.SetState(req.GetSegmentId(), req.GetNewState())
	if err != nil {
		log.Error("failed to updated segment state in dataCoord meta",
			zap.Int64("segment ID", req.SegmentId),
			zap.String("to state", req.GetNewState().String()))
		return &datapb.SetSegmentStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}
	return &datapb.SetSegmentStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

// GetComponentStates returns DataCoord's current state
func (s *Server) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	nodeID := common.NotRegisteredID
	if s.session != nil && s.session.Registered() {
		nodeID = s.session.ServerID // or Params.NodeID
	}

	resp := &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			// NodeID:    Params.NodeID, // will race with Server.Register()
			NodeID:    nodeID,
			Role:      "datacoord",
			StateCode: 0,
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}
	state := atomic.LoadInt64(&s.isServing)
	switch state {
	case ServerStateInitializing:
		resp.State.StateCode = internalpb.StateCode_Initializing
	case ServerStateHealthy:
		resp.State.StateCode = internalpb.StateCode_Healthy
	default:
		resp.State.StateCode = internalpb.StateCode_Abnormal
	}
	return resp, nil
}

// GetRecoveryInfo get recovery info for segment
func (s *Server) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	collectionID := req.GetCollectionID()
	partitionID := req.GetPartitionID()
	log.Info("receive get recovery info request",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID))
	resp := &datapb.GetRecoveryInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = serverNotServingErrMsg
		return resp, nil
	}
	segmentIDs := s.meta.GetSegmentsIDOfPartition(collectionID, partitionID)
	segment2Binlogs := make(map[UniqueID][]*datapb.FieldBinlog)
	segment2StatsBinlogs := make(map[UniqueID][]*datapb.FieldBinlog)
	segment2DeltaBinlogs := make(map[UniqueID][]*datapb.FieldBinlog)
	segment2InsertChannel := make(map[UniqueID]string)
	segmentsNumOfRows := make(map[UniqueID]int64)

	flushedIDs := make(map[int64]struct{})
	for _, id := range segmentIDs {
		segment := s.meta.GetSegment(id)
		if segment == nil {
			errMsg := fmt.Sprintf("failed to get segment %d", id)
			log.Error(errMsg)
			resp.Status.Reason = errMsg
			return resp, nil
		}
		if segment.State != commonpb.SegmentState_Flushed && segment.State != commonpb.SegmentState_Flushing {
			continue
		}
		segment2InsertChannel[segment.ID] = segment.InsertChannel
		binlogs := segment.GetBinlogs()

		if len(binlogs) == 0 {
			continue
		}

		_, ok := flushedIDs[id]
		if !ok {
			flushedIDs[id] = struct{}{}
		}

		field2Binlog := make(map[UniqueID][]*datapb.Binlog)
		for _, field := range binlogs {
			field2Binlog[field.GetFieldID()] = append(field2Binlog[field.GetFieldID()], field.GetBinlogs()...)
		}

		for f, paths := range field2Binlog {
			fieldBinlogs := &datapb.FieldBinlog{
				FieldID: f,
				Binlogs: paths,
			}
			segment2Binlogs[id] = append(segment2Binlogs[id], fieldBinlogs)
		}

		segmentsNumOfRows[id] = segment.NumOfRows

		statsBinlogs := segment.GetStatslogs()
		field2StatsBinlog := make(map[UniqueID][]*datapb.Binlog)
		for _, field := range statsBinlogs {
			field2StatsBinlog[field.GetFieldID()] = append(field2StatsBinlog[field.GetFieldID()], field.GetBinlogs()...)
		}

		for f, paths := range field2StatsBinlog {
			fieldBinlogs := &datapb.FieldBinlog{
				FieldID: f,
				Binlogs: paths,
			}
			segment2StatsBinlogs[id] = append(segment2StatsBinlogs[id], fieldBinlogs)
		}

		if len(segment.GetDeltalogs()) > 0 {
			segment2DeltaBinlogs[id] = append(segment2DeltaBinlogs[id], segment.GetDeltalogs()...)
		}
	}

	binlogs := make([]*datapb.SegmentBinlogs, 0, len(segment2Binlogs))
	for segmentID := range flushedIDs {
		sbl := &datapb.SegmentBinlogs{
			SegmentID:     segmentID,
			NumOfRows:     segmentsNumOfRows[segmentID],
			FieldBinlogs:  segment2Binlogs[segmentID],
			Statslogs:     segment2StatsBinlogs[segmentID],
			Deltalogs:     segment2DeltaBinlogs[segmentID],
			InsertChannel: segment2InsertChannel[segmentID],
		}
		binlogs = append(binlogs, sbl)
	}

	dresp, err := s.rootCoordClient.DescribeCollection(s.ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_DescribeCollection,
			SourceID: Params.DataCoordCfg.GetNodeID(),
		},
		CollectionID: collectionID,
	})
	if err = VerifyResponse(dresp, err); err != nil {
		log.Error("get collection info from master failed",
			zap.Int64("collectionID", collectionID),
			zap.Error(err))

		resp.Status.Reason = err.Error()
		return resp, nil
	}

	channels := dresp.GetVirtualChannelNames()
	channelInfos := make([]*datapb.VchannelInfo, 0, len(channels))
	for _, c := range channels {
		channelInfo := s.handler.GetVChanPositions(c, collectionID, partitionID)
		channelInfos = append(channelInfos, channelInfo)
		log.Debug("datacoord append channelInfo in GetRecoveryInfo",
			zap.Any("collectionID", collectionID),
			zap.Any("channelInfo", channelInfo),
		)
	}

	resp.Binlogs = binlogs
	resp.Channels = channelInfos
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// GetFlushedSegments returns all segment matches provided criterion and in State Flushed
// If requested partition id < 0, ignores the partition id filter
func (s *Server) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	resp := &datapb.GetFlushedSegmentsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	collectionID := req.GetCollectionID()
	partitionID := req.GetPartitionID()
	log.Debug("received get flushed segments request",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID))
	if s.isClosed() {
		resp.Status.Reason = serverNotServingErrMsg
		return resp, nil
	}
	var segmentIDs []UniqueID
	if partitionID < 0 {
		segmentIDs = s.meta.GetSegmentsIDOfCollection(collectionID)
	} else {
		segmentIDs = s.meta.GetSegmentsIDOfPartition(collectionID, partitionID)
	}
	ret := make([]UniqueID, 0, len(segmentIDs))
	for _, id := range segmentIDs {
		segment := s.meta.GetSegment(id)
		if segment != nil && segment.GetState() != commonpb.SegmentState_Flushed {
			continue
		}

		// if this segment == nil, we assume this segment has been compacted and flushed
		ret = append(ret, id)
	}

	resp.Segments = ret
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// GetMetrics returns DataCoord metrics info
// it may include SystemMetrics, Topology metrics, etc.
func (s *Server) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log.Debug("received get metrics request",
		zap.Int64("nodeID", Params.DataCoordCfg.GetNodeID()),
		zap.String("request", req.Request))

	if s.isClosed() {
		log.Warn("DataCoord.GetMetrics failed",
			zap.Int64("node_id", Params.DataCoordCfg.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(errDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID())))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID()),
			},
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("DataCoord.GetMetrics failed to parse metric type",
			zap.Int64("node_id", Params.DataCoordCfg.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response: "",
		}, nil
	}

	log.Debug("DataCoord.GetMetrics",
		zap.String("metric_type", metricType))

	if metricType == metricsinfo.SystemInfoMetrics {
		ret, err := s.metricsCacheManager.GetSystemInfoMetrics()
		if err == nil && ret != nil {
			return ret, nil
		}
		log.Debug("failed to get system info metrics from cache, recompute instead",
			zap.Error(err))

		metrics, err := s.getSystemInfoMetrics(ctx, req)

		log.Debug("DataCoord.GetMetrics",
			zap.Int64("node_id", Params.DataCoordCfg.GetNodeID()),
			zap.String("req", req.Request),
			zap.String("metric_type", metricType),
			zap.Any("metrics", metrics), // TODO(dragondriver): necessary? may be very large
			zap.Error(err))

		s.metricsCacheManager.UpdateSystemInfoMetrics(metrics)

		return metrics, nil
	}

	log.RatedWarn(60.0, "DataCoord.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("node_id", Params.DataCoordCfg.GetNodeID()),
		zap.String("req", req.Request),
		zap.String("metric_type", metricType))

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    metricsinfo.MsgUnimplementedMetric,
		},
		Response: "",
	}, nil
}

// ManualCompaction triggers a compaction for a collection
func (s *Server) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	log.Info("received manual compaction", zap.Int64("collectionID", req.GetCollectionID()))

	resp := &milvuspb.ManualCompactionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	if s.isClosed() {
		log.Warn("failed to execute manual compaction", zap.Int64("collectionID", req.GetCollectionID()),
			zap.Error(errDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID())))
		resp.Status.Reason = msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID())
		return resp, nil
	}

	if !Params.DataCoordCfg.EnableCompaction {
		resp.Status.Reason = "compaction disabled"
		return resp, nil
	}

	ct, err := getCompactTime(ctx, s.allocator)
	if err != nil {
		log.Warn("failed to get compact time", zap.Int64("collectionID", req.GetCollectionID()), zap.Error(err))
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	id, err := s.compactionTrigger.forceTriggerCompaction(req.CollectionID, ct)
	if err != nil {
		log.Error("failed to trigger manual compaction", zap.Int64("collectionID", req.GetCollectionID()), zap.Error(err))
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	log.Info("success to trigger manual compaction", zap.Int64("collectionID", req.GetCollectionID()), zap.Int64("compactionID", id))
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.CompactionID = id
	return resp, nil
}

// GetCompactionState gets the state of a compaction
func (s *Server) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	log.Info("received get compaction state request", zap.Int64("compactionID", req.GetCompactionID()))
	resp := &milvuspb.GetCompactionStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	if s.isClosed() {
		log.Warn("failed to get compaction state", zap.Int64("compactionID", req.GetCompactionID()),
			zap.Error(errDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID())))
		resp.Status.Reason = msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID())
		return resp, nil
	}

	if !Params.DataCoordCfg.EnableCompaction {
		resp.Status.Reason = "compaction disabled"
		return resp, nil
	}

	tasks := s.compactionHandler.getCompactionTasksBySignalID(req.GetCompactionID())
	state, executingCnt, completedCnt, failedCnt, timeoutCnt := getCompactionState(tasks)

	resp.State = state
	resp.ExecutingPlanNo = int64(executingCnt)
	resp.CompletedPlanNo = int64(completedCnt)
	resp.TimeoutPlanNo = int64(timeoutCnt)
	resp.FailedPlanNo = int64(failedCnt)
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	log.Info("success to get compaction state", zap.Any("state", state), zap.Int("executing", executingCnt),
		zap.Int("completed", completedCnt), zap.Int("failed", failedCnt), zap.Int("timeout", timeoutCnt))
	return resp, nil
}

// GetCompactionStateWithPlans returns the compaction state of given plan
func (s *Server) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	log.Info("received the request to get compaction state with plans", zap.Int64("compactionID", req.GetCompactionID()))

	resp := &milvuspb.GetCompactionPlansResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
	}

	if s.isClosed() {
		log.Warn("failed to get compaction state with plans", zap.Int64("compactionID", req.GetCompactionID()), zap.Error(errDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID())))
		resp.Status.Reason = msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID())
		return resp, nil
	}

	if !Params.DataCoordCfg.EnableCompaction {
		resp.Status.Reason = "compaction disabled"
		return resp, nil
	}

	tasks := s.compactionHandler.getCompactionTasksBySignalID(req.GetCompactionID())
	for _, task := range tasks {
		resp.MergeInfos = append(resp.MergeInfos, getCompactionMergeInfo(task))
	}

	state, _, _, _, _ := getCompactionState(tasks)

	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.State = state
	log.Info("success to get state with plans", zap.Any("state", state), zap.Any("merge infos", resp.MergeInfos))
	return resp, nil
}

func getCompactionMergeInfo(task *compactionTask) *milvuspb.CompactionMergeInfo {
	segments := task.plan.GetSegmentBinlogs()
	var sources []int64
	for _, s := range segments {
		sources = append(sources, s.GetSegmentID())
	}

	var target int64 = -1
	if task.result != nil {
		target = task.result.GetSegmentID()
	}

	return &milvuspb.CompactionMergeInfo{
		Sources: sources,
		Target:  target,
	}
}

func getCompactionState(tasks []*compactionTask) (state commonpb.CompactionState, executingCnt, completedCnt, failedCnt, timeoutCnt int) {
	if len(tasks) == 0 {
		state = commonpb.CompactionState_Executing
		return
	}
	for _, t := range tasks {
		switch t.state {
		case executing:
			executingCnt++
		case completed:
			completedCnt++
		case failed:
			failedCnt++
		case timeout:
			timeoutCnt++
		}
	}
	if executingCnt != 0 {
		state = commonpb.CompactionState_Executing
	} else {
		state = commonpb.CompactionState_Completed
	}
	return
}

// WatchChannels notifies DataCoord to watch vchannels of a collection.
func (s *Server) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
	log.Info("receive watch channels request", zap.Any("channels", req.GetChannelNames()))
	resp := &datapb.WatchChannelsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	if s.isClosed() {
		log.Warn("failed to watch channels request", zap.Any("channels", req.GetChannelNames()),
			zap.Error(errDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID())))
		resp.Status.Reason = msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID())
		return resp, nil
	}
	for _, channelName := range req.GetChannelNames() {
		ch := &channel{
			Name:         channelName,
			CollectionID: req.GetCollectionID(),
		}
		err := s.channelManager.Watch(ch)
		if err != nil {
			log.Warn("fail to watch channelName", zap.String("channelName", channelName), zap.Error(err))
			resp.Status.Reason = err.Error()
			return resp, nil
		}
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_Success

	return resp, nil
}

// GetFlushState gets the flush state of multiple segments
func (s *Server) GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	log.Info("DataCoord receive get flush state request", zap.Int64s("segmentIDs", req.GetSegmentIDs()), zap.Int("len", len(req.GetSegmentIDs())))

	resp := &milvuspb.GetFlushStateResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}
	if s.isClosed() {
		log.Warn("failed to get flush state because of closed server",
			zap.Int64s("segmentIDs", req.GetSegmentIDs()), zap.Int("len", len(req.GetSegmentIDs())))
		resp.Status.Reason = msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID())
		return resp, nil
	}

	var unflushed []UniqueID
	for _, sid := range req.GetSegmentIDs() {
		segment := s.meta.GetSegment(sid)
		// segment is nil if it was compacted or it's a empty segment and is set to dropped
		if segment == nil || segment.GetState() == commonpb.SegmentState_Flushing ||
			segment.GetState() == commonpb.SegmentState_Flushed {
			continue
		}

		unflushed = append(unflushed, sid)
	}

	if len(unflushed) != 0 {
		log.Info("[flush state] unflushed segment ids", zap.Int64s("segmentIDs", unflushed), zap.Int("len", len(unflushed)))
		resp.Flushed = false
	} else {
		log.Info("[flush state] all segment is flushed", zap.Int64s("segment ids", req.GetSegmentIDs()))
		resp.Flushed = true
	}

	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// Import distributes the import tasks to dataNodes.
// It returns a failed status if no dataNode is available or if any error occurs.
func (s *Server) Import(ctx context.Context, itr *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
	log.Info("DataCoord receives import request", zap.Any("import task request", itr))
	resp := &datapb.ImportTaskResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	if s.isClosed() {
		log.Error("failed to import for closed DataCoord service")
		resp.Status.Reason = msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID())
		return resp, nil
	}

	nodes := s.channelManager.store.GetNodes()
	if len(nodes) == 0 {
		log.Error("import failed as all DataNodes are offline")
		return resp, nil
	}

	avaNodes := getDiff(nodes, itr.GetWorkingNodes())
	if len(avaNodes) > 0 {
		// If there exists available DataNodes, pick one at random.
		resp.DatanodeId = avaNodes[rand.Intn(len(avaNodes))]
		log.Info("picking a free dataNode",
			zap.Any("all dataNodes", nodes),
			zap.Int64("picking free dataNode with ID", resp.GetDatanodeId()))
		s.cluster.Import(s.ctx, resp.GetDatanodeId(), itr)
	} else {
		// No dataNode is available, reject the import request.
		msg := "all DataNodes are busy working on data import, the task has been rejected and wait for idle datanode"
		log.Info(msg, zap.Int64("task ID", itr.GetImportTask().GetTaskId()))
		resp.Status.Reason = msg
		return resp, nil
	}

	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// UpdateSegmentStatistics updates a segment's stats.
func (s *Server) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) (*commonpb.Status, error) {
	resp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "",
	}
	if s.isClosed() {
		log.Warn("failed to update segment stat for closed server")
		resp.Reason = msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID())
		return resp, nil
	}
	s.updateSegmentStatistics(req.GetStats())
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// getDiff returns the difference of base and remove. i.e. all items that are in `base` but not in `remove`.
func getDiff(base, remove []int64) []int64 {
	mb := make(map[int64]struct{}, len(remove))
	for _, x := range remove {
		mb[x] = struct{}{}
	}
	var diff []int64
	for _, x := range base {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}

// AcquireSegmentLock acquire the reference lock of the segments.
func (s *Server) AcquireSegmentLock(ctx context.Context, req *datapb.AcquireSegmentLockRequest) (*commonpb.Status, error) {
	resp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	if s.isClosed() {
		log.Warn("failed to acquire segments reference lock for closed server")
		resp.Reason = msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID())
		return resp, nil
	}

	hasSegments, err := s.meta.HasSegments(req.SegmentIDs)
	if !hasSegments || err != nil {
		log.Error("AcquireSegmentLock failed", zap.Error(err))
		resp.Reason = err.Error()
		return resp, nil
	}

	err = s.segReferManager.AddSegmentsLock(req.TaskID, req.SegmentIDs, req.NodeID)
	if err != nil {
		log.Warn("Add reference lock on segments failed", zap.Int64s("segIDs", req.SegmentIDs), zap.Error(err))
		resp.Reason = err.Error()
		return resp, nil
	}
	hasSegments, err = s.meta.HasSegments(req.SegmentIDs)
	if !hasSegments || err != nil {
		log.Error("AcquireSegmentLock failed, try to release reference lock", zap.Error(err))
		if err2 := retry.Do(ctx, func() error {
			return s.segReferManager.ReleaseSegmentsLock(req.TaskID, req.NodeID)
		}, retry.Attempts(100)); err2 != nil {
			panic(err)
		}
		resp.Reason = err.Error()
		return resp, nil
	}
	resp.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// ReleaseSegmentLock release the reference lock of the segments.
func (s *Server) ReleaseSegmentLock(ctx context.Context, req *datapb.ReleaseSegmentLockRequest) (*commonpb.Status, error) {
	resp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}

	if s.isClosed() {
		log.Warn("failed to release segments reference lock for closed server")
		resp.Reason = msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID())
		return resp, nil
	}

	err := s.segReferManager.ReleaseSegmentsLock(req.TaskID, req.NodeID)
	if err != nil {
		log.Error("DataCoord ReleaseSegmentLock failed", zap.Int64("taskID", req.TaskID), zap.Int64("nodeID", req.NodeID),
			zap.Error(err))
		resp.Reason = err.Error()
		return resp, nil
	}
	resp.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

func (s *Server) AddSegment(ctx context.Context, req *datapb.AddSegmentRequest) (*commonpb.Status, error) {
	log.Info("DataCoord putting segment to the right DataNode",
		zap.Int64("segment ID", req.GetSegmentId()),
		zap.Int64("collection ID", req.GetCollectionId()),
		zap.Int64("partition ID", req.GetPartitionId()),
		zap.String("channel name", req.GetChannelName()),
		zap.Int64("# of rows", req.GetRowNum()))
	errResp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "",
	}
	if s.isClosed() {
		log.Warn("failed to add segment for closed server")
		errResp.Reason = msgDataCoordIsUnhealthy(Params.DataCoordCfg.GetNodeID())
		return errResp, nil
	}
	ok, nodeID := s.channelManager.getNodeIDByChannelName(req.GetChannelName())
	if !ok {
		log.Error("no DataNode found for channel", zap.String("channel name", req.GetChannelName()))
		errResp.Reason = fmt.Sprint("no DataNode found for channel ", req.GetChannelName())
		return errResp, nil
	}
	s.cluster.AddSegment(s.ctx, nodeID, req)
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}
