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
	"sync"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/errorutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// checks whether server in Healthy State
func (s *Server) isClosed() bool {
	return s.stateCode.Load() != commonpb.StateCode_Healthy
}

// GetTimeTickChannel legacy API, returns time tick channel name
func (s *Server) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: Params.CommonCfg.DataCoordTimeTick.GetValue(),
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
	log := log.Ctx(ctx)
	log.Info("receive flush request",
		zap.Int64("dbID", req.GetDbID()),
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Bool("isImporting", req.GetIsImport()))
	ctx, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "DataCoord-Flush")
	defer sp.End()
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

	// generate a timestamp timeOfSeal, all data before timeOfSeal is guaranteed to be sealed or flushed
	ts, err := s.allocator.allocTimestamp(ctx)
	if err != nil {
		log.Warn("unable to alloc timestamp", zap.Error(err))
	}
	timeOfSeal, _ := tsoutil.ParseTS(ts)

	sealedSegmentIDs, err := s.segmentManager.SealAllSegments(ctx, req.GetCollectionID(), req.GetSegmentIDs(), req.GetIsImport())
	if err != nil {
		resp.Status.Reason = fmt.Sprintf("failed to flush %d, %s", req.CollectionID, err)
		return resp, nil
	}

	sealedSegmentsIDDict := make(map[UniqueID]bool)
	for _, sealedSegmentID := range sealedSegmentIDs {
		sealedSegmentsIDDict[sealedSegmentID] = true
	}

	segments := s.meta.GetSegmentsOfCollection(req.GetCollectionID())
	flushSegmentIDs := make([]UniqueID, 0, len(segments))
	for _, segment := range segments {
		if segment != nil &&
			(segment.GetState() == commonpb.SegmentState_Flushed ||
				segment.GetState() == commonpb.SegmentState_Flushing) &&
			!sealedSegmentsIDDict[segment.GetID()] {
			flushSegmentIDs = append(flushSegmentIDs, segment.GetID())
		}
	}

	log.Info("flush response with segments",
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("sealSegments", sealedSegmentIDs),
		zap.Int64s("flushSegments", flushSegmentIDs),
		zap.Time("timeOfSeal", timeOfSeal))
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.DbID = req.GetDbID()
	resp.CollectionID = req.GetCollectionID()
	resp.SegmentIDs = sealedSegmentIDs
	resp.TimeOfSeal = timeOfSeal.Unix()
	resp.FlushSegmentIDs = flushSegmentIDs
	return resp, nil
}

// AssignSegmentID applies for segment ids and make allocation for records.
func (s *Server) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	log := log.Ctx(ctx)
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
			zap.Bool("isImport", r.GetIsImport()),
			zap.Int64("import task ID", r.GetImportTaskID()))

		// Load the collection info from Root Coordinator, if it is not found in server meta.
		// Note: this request wouldn't be received if collection didn't exist.
		_, err := s.handler.GetCollection(ctx, r.GetCollectionID())
		if err != nil {
			log.Warn("cannot get collection schema", zap.Error(err))
		}

		// Add the channel to cluster for watching.
		s.cluster.Watch(r.ChannelName, r.CollectionID)

		segmentAllocations := make([]*Allocation, 0)
		if r.GetIsImport() {
			// Have segment manager allocate and return the segment allocation info.
			segAlloc, err := s.segmentManager.allocSegmentForImport(ctx,
				r.GetCollectionID(), r.GetPartitionID(), r.GetChannelName(), int64(r.GetCount()), r.GetImportTaskID())
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
		segmentInfo := s.meta.GetHealthySegment(segmentID)
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
	segment := s.meta.GetHealthySegment(req.GetSegmentID())
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
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)
	log.Info("received request to get collection statistics")
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
	log.Info("success to get collection statistics", zap.Any("response", resp))
	return resp, nil
}

// GetPartitionStatistics returns statistics for partition
// if partID is empty, return statistics for all partitions of the collection
// for now only row count is returned
func (s *Server) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("partitionIDs", req.GetPartitionIDs()),
	)
	resp := &datapb.GetPartitionStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = serverNotServingErrMsg
		return resp, nil
	}
	nums := int64(0)
	if len(req.GetPartitionIDs()) == 0 {
		nums = s.meta.GetNumRowsOfCollection(req.CollectionID)
	}
	for _, partID := range req.GetPartitionIDs() {
		num := s.meta.GetNumRowsOfPartition(req.CollectionID, partID)
		nums += num
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.Stats = append(resp.Stats, &commonpb.KeyValuePair{Key: "row_count", Value: strconv.FormatInt(nums, 10)})
	log.Info("success to get partition statistics", zap.Any("response", resp))
	return resp, nil
}

// GetSegmentInfoChannel legacy API, returns segment info statistics channel
func (s *Server) GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: Params.CommonCfg.DataCoordSegmentInfo.GetValue(),
	}, nil
}

// GetSegmentInfo returns segment info requested, status, row count, etc included
// Called by: QueryCoord, DataNode, IndexCoord, Proxy.
func (s *Server) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	log := log.Ctx(ctx)
	resp := &datapb.GetSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = serverNotServingErrMsg
		return resp, nil
	}
	infos := make([]*datapb.SegmentInfo, 0, len(req.GetSegmentIDs()))
	channelCPs := make(map[string]*msgpb.MsgPosition)
	for _, id := range req.SegmentIDs {
		var info *SegmentInfo
		if req.IncludeUnHealthy {
			info = s.meta.GetSegment(id)

			if info == nil {
				log.Warn("failed to get segment, this may have been cleaned", zap.Int64("segmentID", id))
				resp.Status.Reason = msgSegmentNotFound(id)
				return resp, nil
			}

			child := s.meta.GetCompactionTo(id)
			clonedInfo := info.Clone()
			if child != nil {
				clonedInfo.Deltalogs = append(clonedInfo.Deltalogs, child.GetDeltalogs()...)
				clonedInfo.DmlPosition = child.GetDmlPosition()
			}
			segmentutil.ReCalcRowCount(info.SegmentInfo, clonedInfo.SegmentInfo)
			infos = append(infos, clonedInfo.SegmentInfo)
		} else {
			info = s.meta.GetHealthySegment(id)
			if info == nil {
				resp.Status.Reason = msgSegmentNotFound(id)
				return resp, nil
			}
			clonedInfo := info.Clone()
			segmentutil.ReCalcRowCount(info.SegmentInfo, clonedInfo.SegmentInfo)
			infos = append(infos, clonedInfo.SegmentInfo)
		}
		vchannel := info.InsertChannel
		if _, ok := channelCPs[vchannel]; vchannel != "" && !ok {
			channelCPs[vchannel] = s.meta.GetChannelCheckpoint(vchannel)
		}
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.Infos = infos
	resp.ChannelCheckpoint = channelCPs
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

	log := log.Ctx(ctx).With(
		zap.Int64("nodeID", req.GetBase().GetSourceID()),
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64("segmentID", req.GetSegmentID()),
	)

	log.Info("receive SaveBinlogPaths request",
		zap.Bool("isFlush", req.GetFlushed()),
		zap.Bool("isDropped", req.GetDropped()),
		zap.Any("startPositions", req.GetStartPositions()),
		zap.Any("checkpoints", req.GetCheckPoints()))

	nodeID := req.GetBase().GetSourceID()
	// virtual channel name
	channelName := req.Channel
	// for compatibility issue , if len(channelName) not exist, skip the check
	// No need to check import channel--node matching in data import case.
	// Also avoid to handle segment not found error if not the owner of shard
	if !req.GetImporting() && len(channelName) != 0 {
		if !s.channelManager.Match(nodeID, channelName) {
			failResponse(resp, fmt.Sprintf("channel %s is not watched on node %d", channelName, nodeID))
			resp.ErrorCode = commonpb.ErrorCode_MetaFailed
			log.Warn("node is not matched with channel", zap.String("channel", channelName))
			return resp, nil
		}
	}

	// validate
	segmentID := req.GetSegmentID()
	segment := s.meta.GetSegment(segmentID)

	if segment == nil {
		log.Error("failed to get segment")
		failResponseWithCode(resp, commonpb.ErrorCode_SegmentNotFound, fmt.Sprintf("failed to get segment %d", segmentID))
		return resp, nil
	}

	if segment.State == commonpb.SegmentState_Dropped {
		log.Info("save to dropped segment, ignore this request")
		resp.ErrorCode = commonpb.ErrorCode_Success
		return resp, nil
	} else if !isSegmentHealthy(segment) {
		log.Error("failed to get segment")
		failResponseWithCode(resp, commonpb.ErrorCode_SegmentNotFound, fmt.Sprintf("failed to get segment %d", segmentID))
		return resp, nil
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
		log.Error("save binlog and checkpoints failed", zap.Error(err))
		resp.Reason = err.Error()
		return resp, nil
	}

	log.Info("flush segment with meta", zap.Any("meta", req.GetField2BinlogPaths()))

	if req.GetFlushed() {
		s.segmentManager.DropSegment(ctx, req.SegmentID)
		s.flushCh <- req.SegmentID

		if !req.Importing && Params.DataCoordCfg.EnableCompaction.GetAsBool() {
			err = s.compactionTrigger.triggerSingleCompaction(segment.GetCollectionID(), segment.GetPartitionID(),
				segmentID, segment.GetInsertChannel())
			if err != nil {
				log.Warn("failed to trigger single compaction")
			} else {
				log.Info("compaction triggered for segment")
			}
		}
	}
	resp.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// DropVirtualChannel notifies vchannel dropped
// And contains the remaining data log & checkpoint to update
func (s *Server) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	log := log.Ctx(ctx)
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
		zap.String("channelName", channel))

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

	// no compaction triggered in Drop procedure
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// SetSegmentState reset the state of the given segment.
func (s *Server) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest) (*datapb.SetSegmentStateResponse, error) {
	log := log.Ctx(ctx)
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
			zap.Int64("segmentID", req.SegmentId),
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

func (s *Server) GetStateCode() commonpb.StateCode {
	code := s.stateCode.Load()
	if code == nil {
		return commonpb.StateCode_Abnormal
	}
	return code.(commonpb.StateCode)
}

// GetComponentStates returns DataCoord's current state
func (s *Server) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	code := s.GetStateCode()
	nodeID := common.NotRegisteredID
	if s.session != nil && s.session.Registered() {
		nodeID = s.session.ServerID // or Params.NodeID
	}
	resp := &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			// NodeID:    Params.NodeID, // will race with Server.Register()
			NodeID:    nodeID,
			Role:      "datacoord",
			StateCode: code,
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}
	return resp, nil
}

// GetRecoveryInfo get recovery info for segment.
// Called by: QueryCoord.
func (s *Server) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	log := log.Ctx(ctx)
	collectionID := req.GetCollectionID()
	partitionID := req.GetPartitionID()
	log = log.With(
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
	)
	log.Info("get recovery info request received")
	resp := &datapb.GetRecoveryInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = serverNotServingErrMsg
		return resp, nil
	}

	dresp, err := s.broker.DescribeCollectionInternal(s.ctx, collectionID)
	if err != nil {
		log.Error("get collection info from rootcoord failed",
			zap.Error(err))
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	channels := dresp.GetVirtualChannelNames()
	channelInfos := make([]*datapb.VchannelInfo, 0, len(channels))
	flushedIDs := make(typeutil.UniqueSet)
	for _, c := range channels {
		channelInfo := s.handler.GetQueryVChanPositions(&channel{Name: c, CollectionID: collectionID}, partitionID)
		channelInfos = append(channelInfos, channelInfo)
		log.Info("datacoord append channelInfo in GetRecoveryInfo",
			zap.Any("channelInfo", channelInfo),
		)
		flushedIDs.Insert(channelInfo.GetFlushedSegmentIds()...)
	}

	segment2Binlogs := make(map[UniqueID][]*datapb.FieldBinlog)
	segment2StatsBinlogs := make(map[UniqueID][]*datapb.FieldBinlog)
	segment2DeltaBinlogs := make(map[UniqueID][]*datapb.FieldBinlog)
	segment2InsertChannel := make(map[UniqueID]string)
	segmentsNumOfRows := make(map[UniqueID]int64)
	for id := range flushedIDs {
		segment := s.meta.GetSegment(id)
		if segment == nil {
			errMsg := fmt.Sprintf("failed to get segment %d", id)
			log.Error(errMsg)
			resp.Status.Reason = errMsg
			return resp, nil
		}
		// Skip non-flushing, non-flushed and dropped segments.
		if segment.State != commonpb.SegmentState_Flushed && segment.State != commonpb.SegmentState_Flushing && segment.State != commonpb.SegmentState_Dropped {
			continue
		}
		// Also skip bulk insert & fake segments.
		if segment.GetIsImporting() || segment.GetIsFake() {
			continue
		}
		segment2InsertChannel[segment.ID] = segment.InsertChannel
		binlogs := segment.GetBinlogs()

		if len(binlogs) == 0 {
			flushedIDs.Remove(id)
			continue
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

		if newCount := segmentutil.CalcRowCountFromBinLog(segment.SegmentInfo); newCount != segment.NumOfRows {
			log.Warn("segment row number meta inconsistent with bin log row count and will be corrected",
				zap.Int64("segmentID", segment.GetID()),
				zap.Int64("segment meta row count (wrong)", segment.GetNumOfRows()),
				zap.Int64("segment bin log row count (correct)", newCount))
			segmentsNumOfRows[id] = newCount
		} else {
			segmentsNumOfRows[id] = segment.NumOfRows
		}

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

	resp.Channels = channelInfos
	resp.Binlogs = binlogs
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// GetRecoveryInfoV2 get recovery info for segment
// Called by: QueryCoord.
func (s *Server) GetRecoveryInfoV2(ctx context.Context, req *datapb.GetRecoveryInfoRequestV2) (*datapb.GetRecoveryInfoResponseV2, error) {
	log := log.Ctx(ctx)
	collectionID := req.GetCollectionID()
	partitionIDs := req.GetPartitionIDs()
	log = log.With(
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
	)
	log.Info("get recovery info request received")
	resp := &datapb.GetRecoveryInfoResponseV2{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = serverNotServingErrMsg
		return resp, nil
	}

	dresp, err := s.broker.DescribeCollectionInternal(s.ctx, collectionID)
	if err != nil {
		log.Error("get collection info from rootcoord failed",
			zap.Error(err))

		resp.Status.Reason = err.Error()
		return resp, nil
	}
	channels := dresp.GetVirtualChannelNames()
	channelInfos := make([]*datapb.VchannelInfo, 0, len(channels))
	flushedIDs := make(typeutil.UniqueSet)
	for _, c := range channels {
		channelInfo := s.handler.GetQueryVChanPositions(&channel{Name: c, CollectionID: collectionID}, partitionIDs...)
		channelInfos = append(channelInfos, channelInfo)
		log.Info("datacoord append channelInfo in GetRecoveryInfo",
			zap.Any("channelInfo", channelInfo),
		)
		flushedIDs.Insert(channelInfo.GetFlushedSegmentIds()...)
	}

	segmentInfos := make([]*datapb.SegmentInfo, 0)
	for id := range flushedIDs {
		segment := s.meta.GetSegment(id)
		if segment == nil {
			errMsg := fmt.Sprintf("failed to get segment %d", id)
			log.Error(errMsg)
			resp.Status.Reason = errMsg
			return resp, nil
		}
		// Skip non-flushing, non-flushed and dropped segments.
		if segment.State != commonpb.SegmentState_Flushed && segment.State != commonpb.SegmentState_Flushing && segment.State != commonpb.SegmentState_Dropped {
			continue
		}
		// Also skip bulk insert segments.
		if segment.GetIsImporting() {
			continue
		}

		binlogs := segment.GetBinlogs()
		if len(binlogs) == 0 {
			continue
		}
		rowCount := segmentutil.CalcRowCountFromBinLog(segment.SegmentInfo)
		if rowCount != segment.NumOfRows && rowCount > 0 {
			log.Warn("segment row number meta inconsistent with bin log row count and will be corrected",
				zap.Int64("segmentID", segment.GetID()),
				zap.Int64("segment meta row count (wrong)", segment.GetNumOfRows()),
				zap.Int64("segment bin log row count (correct)", rowCount))
		} else {
			rowCount = segment.NumOfRows
		}

		segmentInfos = append(segmentInfos, &datapb.SegmentInfo{
			ID:            segment.ID,
			PartitionID:   segment.PartitionID,
			CollectionID:  segment.CollectionID,
			InsertChannel: segment.InsertChannel,
			NumOfRows:     rowCount,
			Binlogs:       segment.Binlogs,
			Statslogs:     segment.Statslogs,
			Deltalogs:     segment.Deltalogs,
		})
	}

	resp.Channels = channelInfos
	resp.Segments = segmentInfos
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// GetFlushedSegments returns all segment matches provided criterion and in state Flushed or Dropped (compacted but not GCed yet)
// If requested partition id < 0, ignores the partition id filter
func (s *Server) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	log := log.Ctx(ctx)
	resp := &datapb.GetFlushedSegmentsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	collectionID := req.GetCollectionID()
	partitionID := req.GetPartitionID()
	log.Info("received get flushed segments request",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
	)
	if s.isClosed() {
		resp.Status.Reason = serverNotServingErrMsg
		return resp, nil
	}
	var segmentIDs []UniqueID
	if partitionID < 0 {
		segmentIDs = s.meta.GetSegmentsIDOfCollectionWithDropped(collectionID)
	} else {
		segmentIDs = s.meta.GetSegmentsIDOfPartitionWithDropped(collectionID, partitionID)
	}
	ret := make([]UniqueID, 0, len(segmentIDs))
	for _, id := range segmentIDs {
		segment := s.meta.GetSegment(id)
		// if this segment == nil, we assume this segment has been gc
		if segment == nil ||
			(segment.GetState() != commonpb.SegmentState_Dropped &&
				segment.GetState() != commonpb.SegmentState_Flushed &&
				segment.GetState() != commonpb.SegmentState_Flushing) {
			continue
		}
		if !req.GetIncludeUnhealthy() && segment.GetState() == commonpb.SegmentState_Dropped {
			continue
		}
		ret = append(ret, id)
	}

	resp.Segments = ret
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// GetSegmentsByStates returns all segment matches provided criterion and States
// If requested partition id < 0, ignores the partition id filter
func (s *Server) GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest) (*datapb.GetSegmentsByStatesResponse, error) {
	log := log.Ctx(ctx)
	resp := &datapb.GetSegmentsByStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	collectionID := req.GetCollectionID()
	partitionID := req.GetPartitionID()
	states := req.GetStates()
	log.Info("received get segments by states request",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Any("states", states))
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

	statesDict := make(map[commonpb.SegmentState]bool)
	for _, state := range states {
		statesDict[state] = true
	}
	for _, id := range segmentIDs {
		segment := s.meta.GetHealthySegment(id)
		if segment != nil && statesDict[segment.GetState()] {
			ret = append(ret, id)
		}
	}

	resp.Segments = ret
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// ShowConfigurations returns the configurations of DataCoord matching req.Pattern
func (s *Server) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	log := log.Ctx(ctx)
	if s.isClosed() {
		log.Warn("DataCoord.ShowConfigurations failed",
			zap.Int64("nodeId", paramtable.GetNodeID()),
			zap.String("req", req.Pattern),
			zap.Error(errDataCoordIsUnhealthy(paramtable.GetNodeID())))

		return &internalpb.ShowConfigurationsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgDataCoordIsUnhealthy(paramtable.GetNodeID()),
			},
			Configuations: nil,
		}, nil
	}

	configList := make([]*commonpb.KeyValuePair, 0)
	for key, value := range Params.GetComponentConfigurations("datacoord", req.Pattern) {
		configList = append(configList,
			&commonpb.KeyValuePair{
				Key:   key,
				Value: value,
			})
	}

	return &internalpb.ShowConfigurationsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Configuations: configList,
	}, nil
}

// GetMetrics returns DataCoord metrics info
// it may include SystemMetrics, Topology metrics, etc.
func (s *Server) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log := log.Ctx(ctx)
	if s.isClosed() {
		log.Warn("DataCoord.GetMetrics failed",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(errDataCoordIsUnhealthy(paramtable.GetNodeID())))

		return &milvuspb.GetMetricsResponse{
			ComponentName: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, paramtable.GetNodeID()),
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgDataCoordIsUnhealthy(paramtable.GetNodeID()),
			},
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("DataCoord.GetMetrics failed to parse metric type",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			ComponentName: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, paramtable.GetNodeID()),
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response: "",
		}, nil
	}

	if metricType == metricsinfo.SystemInfoMetrics {
		metrics, err := s.getSystemInfoMetrics(ctx, req)
		if err != nil {
			log.Warn("DataCoord GetMetrics failed", zap.Int64("nodeID", paramtable.GetNodeID()), zap.Error(err))
			return &milvuspb.GetMetricsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    err.Error(),
				},
			}, nil
		}

		log.RatedDebug(60, "DataCoord.GetMetrics",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.String("metricType", metricType),
			zap.Any("metrics", metrics), // TODO(dragondriver): necessary? may be very large
			zap.Error(err))

		return metrics, nil
	}

	log.RatedWarn(60.0, "DataCoord.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("nodeID", paramtable.GetNodeID()),
		zap.String("req", req.Request),
		zap.String("metricType", metricType))

	return &milvuspb.GetMetricsResponse{
		ComponentName: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, paramtable.GetNodeID()),
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    metricsinfo.MsgUnimplementedMetric,
		},
		Response: "",
	}, nil
}

// ManualCompaction triggers a compaction for a collection
func (s *Server) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)
	log.Info("received manual compaction")

	resp := &milvuspb.ManualCompactionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	if s.isClosed() {
		log.Warn("failed to execute manual compaction", zap.Error(errDataCoordIsUnhealthy(paramtable.GetNodeID())))
		resp.Status.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return resp, nil
	}

	if !Params.DataCoordCfg.EnableCompaction.GetAsBool() {
		resp.Status.Reason = "compaction disabled"
		return resp, nil
	}

	id, err := s.compactionTrigger.forceTriggerCompaction(req.CollectionID)
	if err != nil {
		log.Error("failed to trigger manual compaction", zap.Error(err))
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	log.Info("success to trigger manual compaction", zap.Int64("compactionID", id))
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.CompactionID = id
	return resp, nil
}

// GetCompactionState gets the state of a compaction
func (s *Server) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("compactionID", req.GetCompactionID()),
	)
	log.Info("received get compaction state request")
	resp := &milvuspb.GetCompactionStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	if s.isClosed() {
		log.Warn("failed to get compaction state", zap.Error(errDataCoordIsUnhealthy(paramtable.GetNodeID())))
		resp.Status.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return resp, nil
	}

	if !Params.DataCoordCfg.EnableCompaction.GetAsBool() {
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
		zap.Int("completed", completedCnt), zap.Int("failed", failedCnt), zap.Int("timeout", timeoutCnt),
		zap.Int64s("plans", lo.Map(tasks, func(t *compactionTask, _ int) int64 {
			if t.plan == nil {
				return -1
			}
			return t.plan.PlanID
		})))
	return resp, nil
}

// GetCompactionStateWithPlans returns the compaction state of given plan
func (s *Server) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("compactionID", req.GetCompactionID()),
	)
	log.Info("received the request to get compaction state with plans")

	resp := &milvuspb.GetCompactionPlansResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError},
	}

	if s.isClosed() {
		log.Warn("failed to get compaction state with plans", zap.Error(errDataCoordIsUnhealthy(paramtable.GetNodeID())))
		resp.Status.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return resp, nil
	}

	if !Params.DataCoordCfg.EnableCompaction.GetAsBool() {
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
	log.Info("success to get state with plans", zap.Any("state", state), zap.Any("merge infos", resp.MergeInfos),
		zap.Int64s("plans", lo.Map(tasks, func(t *compactionTask, _ int) int64 {
			if t.plan == nil {
				return -1
			}
			return t.plan.PlanID
		})))
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
	for _, t := range tasks {
		switch t.state {
		case pipelining:
			executingCnt++
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
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Strings("channels", req.GetChannelNames()),
	)
	log.Info("receive watch channels request")
	resp := &datapb.WatchChannelsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	if s.isClosed() {
		log.Warn("failed to watch channels request", zap.Error(errDataCoordIsUnhealthy(paramtable.GetNodeID())))
		resp.Status.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return resp, nil
	}
	for _, channelName := range req.GetChannelNames() {
		ch := &channel{
			Name:           channelName,
			CollectionID:   req.GetCollectionID(),
			StartPositions: req.GetStartPositions(),
			Schema:         req.GetSchema(),
		}
		err := s.channelManager.Watch(ch)
		if err != nil {
			log.Warn("fail to watch channelName", zap.Error(err))
			resp.Status.Reason = err.Error()
			return resp, nil
		}
		if err := s.meta.catalog.MarkChannelAdded(ctx, ch.Name); err != nil {
			// TODO: add background task to periodically cleanup the orphaned channel add marks.
			log.Error("failed to mark channel added", zap.Error(err))
			resp.Status.Reason = err.Error()
			return resp, nil
		}
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_Success

	return resp, nil
}

// GetFlushState gets the flush state of multiple segments
func (s *Server) GetFlushState(ctx context.Context, req *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	log := log.Ctx(ctx).WithRateGroup("dc.GetFlushState", 1, 60)
	resp := &milvuspb.GetFlushStateResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}
	if s.isClosed() {
		log.Warn("DataCoord receive GetFlushState request, server closed",
			zap.Int64s("segmentIDs", req.GetSegmentIDs()), zap.Int("len", len(req.GetSegmentIDs())))
		resp.Status.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return resp, nil
	}

	var unflushed []UniqueID
	for _, sid := range req.GetSegmentIDs() {
		segment := s.meta.GetHealthySegment(sid)
		// segment is nil if it was compacted or it's a empty segment and is set to dropped
		if segment == nil || segment.GetState() == commonpb.SegmentState_Flushing ||
			segment.GetState() == commonpb.SegmentState_Flushed {
			continue
		}
		unflushed = append(unflushed, sid)
	}

	if len(unflushed) != 0 {
		log.RatedInfo(10, "DataCoord receive GetFlushState request, Flushed is false", zap.Int64s("unflushed", unflushed), zap.Int("len", len(unflushed)))
		resp.Flushed = false
	} else {
		log.Info("DataCoord receive GetFlushState request, Flushed is true", zap.Int64s("segmentIDs", req.GetSegmentIDs()), zap.Int("len", len(req.GetSegmentIDs())))
		resp.Flushed = true
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// GetFlushAllState checks if all DML messages before `FlushAllTs` have been flushed.
func (s *Server) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error) {
	log := log.Ctx(ctx)
	resp := &milvuspb.GetFlushAllStateResponse{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}}
	if s.isClosed() {
		log.Warn("DataCoord receive GetFlushAllState request, server closed")
		resp.Status.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return resp, nil
	}

	dbsRsp, err := s.broker.ListDatabases(ctx)
	if err != nil {
		log.Warn("failed to ListDatabases", zap.Error(err))
		resp.Status.Reason = err.Error()
		return resp, nil
	}

	for _, dbName := range dbsRsp.DbNames {
		showColRsp, err := s.broker.ShowCollections(ctx, dbName)
		if err != nil {
			log.Warn("failed to ShowCollections", zap.Error(err))
			resp.Status.Reason = err.Error()
			return resp, nil
		}

		for _, collection := range showColRsp.GetCollectionIds() {
			describeColRsp, err := s.broker.DescribeCollectionInternal(ctx, collection)
			if err != nil {
				log.Warn("failed to DescribeCollectionInternal", zap.Error(err))
				resp.Status.Reason = err.Error()
				return resp, nil
			}
			for _, channel := range describeColRsp.GetVirtualChannelNames() {
				channelCP := s.meta.GetChannelCheckpoint(channel)
				if channelCP == nil || channelCP.GetTimestamp() < req.GetFlushAllTs() {
					resp.Flushed = false
					resp.Status.ErrorCode = commonpb.ErrorCode_Success
					return resp, nil
				}
			}
		}
	}
	resp.Flushed = true
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// Import distributes the import tasks to dataNodes.
// It returns a failed status if no dataNode is available or if any error occurs.
func (s *Server) Import(ctx context.Context, itr *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
	log := log.Ctx(ctx)
	log.Info("DataCoord receives import request", zap.Any("import task request", itr))
	resp := &datapb.ImportTaskResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}

	if s.isClosed() {
		log.Error("failed to import for closed DataCoord service")
		resp.Status.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return resp, nil
	}

	nodes := s.sessionManager.getLiveNodeIDs()
	if len(nodes) == 0 {
		log.Error("import failed as all DataNodes are offline")
		resp.Status.Reason = "no data node available"
		return resp, nil
	}
	log.Info("available DataNodes are", zap.Int64s("node ID", nodes))

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
	log := log.Ctx(ctx)
	resp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "",
	}
	if s.isClosed() {
		log.Warn("failed to update segment stat for closed server")
		resp.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return resp, nil
	}
	s.updateSegmentStatistics(req.GetStats())
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// UpdateChannelCheckpoint updates channel checkpoint in dataCoord.
func (s *Server) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)
	resp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}
	if s.isClosed() {
		log.Warn("failed to update channel position for closed server")
		resp.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return resp, nil
	}

	err := s.meta.UpdateChannelCheckpoint(req.GetVChannel(), req.GetPosition())
	if err != nil {
		log.Warn("failed to UpdateChannelCheckpoint", zap.String("vChannel", req.GetVChannel()), zap.Error(err))
		resp.Reason = err.Error()
		return resp, nil
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// ReportDataNodeTtMsgs send datenode timetick messages to dataCoord.
func (s *Server) ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)
	if s.isClosed() {
		log.Warn("failed to report dataNode ttMsgs on closed server")
		return merr.Status(merr.WrapErrServiceUnavailable(msgDataCoordIsUnhealthy(s.session.ServerID))), nil
	}

	for _, ttMsg := range req.GetMsgs() {
		sub := tsoutil.SubByNow(ttMsg.GetTimestamp())
		metrics.DataCoordConsumeDataNodeTimeTickLag.
			WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), ttMsg.GetChannelName()).
			Set(float64(sub))
		err := s.handleRPCTimetickMessage(ctx, ttMsg)
		if err != nil {
			log.Error("fail to handle Datanode Timetick Msg",
				zap.Int64("sourceID", ttMsg.GetBase().GetSourceID()),
				zap.String("channelName", ttMsg.GetChannelName()),
				zap.Error(err))
			return merr.Status(merr.WrapErrServiceInternal("fail to handle Datanode Timetick Msg")), nil
		}
	}

	return merr.Status(nil), nil
}

func (s *Server) handleRPCTimetickMessage(ctx context.Context, ttMsg *msgpb.DataNodeTtMsg) error {
	log := log.Ctx(ctx)
	ch := ttMsg.GetChannelName()
	ts := ttMsg.GetTimestamp()

	// ignore to handle RPC Timetick message since it's no longer the leader
	if !s.cluster.channelManager.Match(ttMsg.GetBase().GetSourceID(), ch) {
		log.Warn("node is not matched with channel",
			zap.String("channelName", ch),
			zap.Int64("nodeID", ttMsg.GetBase().GetSourceID()),
		)
		return nil
	}

	s.updateSegmentStatistics(ttMsg.GetSegmentsStats())

	if err := s.segmentManager.ExpireAllocations(ch, ts); err != nil {
		return fmt.Errorf("expire allocations: %w", err)
	}

	flushableIDs, err := s.segmentManager.GetFlushableSegments(ctx, ch, ts)
	if err != nil {
		return fmt.Errorf("get flushable segments: %w", err)
	}
	flushableSegments := s.getFlushableSegmentsInfo(flushableIDs)

	if len(flushableSegments) == 0 {
		return nil
	}

	log.Info("start flushing segments",
		zap.Int64s("segment IDs", flushableIDs))
	// update segment last update triggered time
	// it's ok to fail flushing, since next timetick after duration will re-trigger
	s.setLastFlushTime(flushableSegments)

	finfo := make([]*datapb.SegmentInfo, 0, len(flushableSegments))
	for _, info := range flushableSegments {
		finfo = append(finfo, info.SegmentInfo)
	}
	err = s.cluster.Flush(s.ctx, ttMsg.GetBase().GetSourceID(), ch, finfo)
	if err != nil {
		log.Warn("failed to handle flush", zap.Any("source", ttMsg.GetBase().GetSourceID()), zap.Error(err))
		return err
	}

	return nil
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

// SaveImportSegment saves the segment binlog paths and puts this segment to its belonging DataNode as a flushed segment.
func (s *Server) SaveImportSegment(ctx context.Context, req *datapb.SaveImportSegmentRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionId()),
	)
	log.Info("DataCoord putting segment to the right DataNode and saving binlog path",
		zap.Int64("segmentID", req.GetSegmentId()),
		zap.Int64("partitionID", req.GetPartitionId()),
		zap.String("channelName", req.GetChannelName()),
		zap.Int64("# of rows", req.GetRowNum()))
	errResp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "",
	}
	if s.isClosed() {
		log.Warn("failed to add segment for closed server")
		errResp.ErrorCode = commonpb.ErrorCode_DataCoordNA
		errResp.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return errResp, nil
	}
	// Look for the DataNode that watches the channel.
	ok, nodeID := s.channelManager.getNodeIDByChannelName(req.GetChannelName())
	if !ok {
		log.Error("no DataNode found for channel", zap.String("channelName", req.GetChannelName()))
		errResp.Reason = fmt.Sprint("no DataNode found for channel ", req.GetChannelName())
		return errResp, nil
	}
	// Call DataNode to add the new segment to its own flow graph.
	cli, err := s.sessionManager.getClient(ctx, nodeID)
	if err != nil {
		log.Error("failed to get DataNode client for SaveImportSegment",
			zap.Int64("DataNode ID", nodeID),
			zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}
	resp, err := cli.AddImportSegment(ctx,
		&datapb.AddImportSegmentRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithTimeStamp(req.GetBase().GetTimestamp()),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			SegmentId:    req.GetSegmentId(),
			ChannelName:  req.GetChannelName(),
			CollectionId: req.GetCollectionId(),
			PartitionId:  req.GetPartitionId(),
			RowNum:       req.GetRowNum(),
			StatsLog:     req.GetSaveBinlogPathReq().GetField2StatslogPaths(),
		})
	if err := VerifyResponse(resp.GetStatus(), err); err != nil {
		log.Error("failed to add segment", zap.Int64("DataNode ID", nodeID), zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}
	log.Info("succeed to add segment", zap.Int64("DataNode ID", nodeID), zap.Any("add segment req", req))
	// Fill in start position message ID.
	req.SaveBinlogPathReq.StartPositions[0].StartPosition.MsgID = resp.GetChannelPos()

	// Start saving bin log paths.
	rsp, err := s.SaveBinlogPaths(context.Background(), req.GetSaveBinlogPathReq())
	if err := VerifyResponse(rsp, err); err != nil {
		log.Error("failed to SaveBinlogPaths", zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// UnsetIsImportingState unsets the isImporting states of the given segments.
// An error status will be returned and error will be logged, if we failed to update *all* segments.
func (s *Server) UnsetIsImportingState(ctx context.Context, req *datapb.UnsetIsImportingStateRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)
	log.Info("unsetting isImport state of segments",
		zap.Int64s("segments", req.GetSegmentIds()))
	var reportErr error
	for _, segID := range req.GetSegmentIds() {
		if err := s.meta.UnsetIsImporting(segID); err != nil {
			// Fail-open.
			log.Error("failed to unset segment is importing state", zap.Int64("segmentID", segID))
			reportErr = err
		}
	}
	if reportErr != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    reportErr.Error(),
		}, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// MarkSegmentsDropped marks the given segments as `Dropped`.
// An error status will be returned and error will be logged, if we failed to mark *all* segments.
// Deprecated, do not use it
func (s *Server) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest) (*commonpb.Status, error) {
	log.Info("marking segments dropped", zap.Int64s("segments", req.GetSegmentIds()))
	failure := false
	for _, segID := range req.GetSegmentIds() {
		if err := s.meta.SetState(segID, commonpb.SegmentState_Dropped); err != nil {
			// Fail-open.
			log.Error("failed to set segment state as dropped", zap.Int64("segmentID", segID))
			failure = true
		}
	}
	if failure {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		}, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (s *Server) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)
	errResp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "",
	}

	if s.isClosed() {
		log.Warn("failed to broadcast collection information for closed server")
		errResp.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return errResp, nil
	}

	// get collection info from cache
	clonedColl := s.meta.GetClonedCollectionInfo(req.CollectionID)

	properties := make(map[string]string)
	for _, pair := range req.Properties {
		properties[pair.GetKey()] = pair.GetValue()
	}

	// cache miss and update cache
	if clonedColl == nil {
		collInfo := &collectionInfo{
			ID:             req.GetCollectionID(),
			Schema:         req.GetSchema(),
			Partitions:     req.GetPartitionIDs(),
			StartPositions: req.GetStartPositions(),
			Properties:     properties,
		}
		s.meta.AddCollection(collInfo)
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}

	clonedColl.Properties = properties
	s.meta.AddCollection(clonedColl)
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (s *Server) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	if s.isClosed() {
		reason := errorutil.UnHealthReason("datacoord", paramtable.GetNodeID(), "datacoord is closed")
		return &milvuspb.CheckHealthResponse{IsHealthy: false, Reasons: []string{reason}}, nil
	}

	mu := &sync.Mutex{}
	group, ctx := errgroup.WithContext(ctx)
	nodes := s.sessionManager.getLiveNodeIDs()
	errReasons := make([]string, 0, len(nodes))

	for _, nodeID := range nodes {
		nodeID := nodeID
		group.Go(func() error {
			cli, err := s.sessionManager.getClient(ctx, nodeID)
			if err != nil {
				mu.Lock()
				defer mu.Unlock()
				errReasons = append(errReasons, errorutil.UnHealthReason("datanode", nodeID, err.Error()))
				return err
			}

			sta, err := cli.GetComponentStates(ctx)
			isHealthy, reason := errorutil.UnHealthReasonWithComponentStatesOrErr("datanode", nodeID, sta, err)
			if !isHealthy {
				mu.Lock()
				defer mu.Unlock()
				errReasons = append(errReasons, reason)
			}
			return err
		})
	}

	err := group.Wait()
	if err != nil || len(errReasons) != 0 {
		return &milvuspb.CheckHealthResponse{IsHealthy: false, Reasons: errReasons}, nil
	}

	return &milvuspb.CheckHealthResponse{IsHealthy: true, Reasons: errReasons}, nil
}

func (s *Server) GcConfirm(ctx context.Context, request *datapb.GcConfirmRequest) (*datapb.GcConfirmResponse, error) {
	resp := &datapb.GcConfirmResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
		GcFinished: false,
	}

	if s.isClosed() {
		resp.Status.Reason = msgDataCoordIsUnhealthy(paramtable.GetNodeID())
		return resp, nil
	}

	resp.GcFinished = s.meta.GcConfirm(ctx, request.GetCollectionId(), request.GetPartitionId())
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}
