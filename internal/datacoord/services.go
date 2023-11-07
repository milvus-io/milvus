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

	"github.com/cockroachdb/errors"
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
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// GetTimeTickChannel legacy API, returns time tick channel name
func (s *Server) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
		Value:  Params.CommonCfg.DataCoordTimeTick.GetValue(),
	}, nil
}

// GetStatisticsChannel legacy API, returns statistics channel name
func (s *Server) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Status(merr.WrapErrChannelNotFound("no statistics channel")),
	}, nil
}

// Flush notify segment to flush
// this api only guarantees all the segments requested is sealed
// these segments will be flushed only after the Flush policy is fulfilled
func (s *Server) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("dbID", req.GetDbID()),
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Bool("isImporting", req.GetIsImport()))
	log.Info("receive flush request")
	ctx, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "DataCoord-Flush")
	defer sp.End()

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.FlushResponse{
			Status: merr.Status(err),
		}, nil
	}

	// generate a timestamp timeOfSeal, all data before timeOfSeal is guaranteed to be sealed or flushed
	ts, err := s.allocator.allocTimestamp(ctx)
	if err != nil {
		log.Warn("unable to alloc timestamp", zap.Error(err))
		return &datapb.FlushResponse{
			Status: merr.Status(err),
		}, nil
	}
	timeOfSeal, _ := tsoutil.ParseTS(ts)

	sealedSegmentIDs, err := s.segmentManager.SealAllSegments(ctx, req.GetCollectionID(), req.GetSegmentIDs(), req.GetIsImport())
	if err != nil {
		return &datapb.FlushResponse{
			Status: merr.Status(errors.Wrapf(err, "failed to flush collection %d",
				req.GetCollectionID())),
		}, nil
	}

	sealedSegmentsIDDict := make(map[UniqueID]bool)
	for _, sealedSegmentID := range sealedSegmentIDs {
		sealedSegmentsIDDict[sealedSegmentID] = true
	}

	segments := s.meta.GetSegmentsOfCollection(req.GetCollectionID())
	flushSegmentIDs := make([]UniqueID, 0, len(segments))
	for _, segment := range segments {
		if segment != nil &&
			(isFlushState(segment.GetState())) &&
			!sealedSegmentsIDDict[segment.GetID()] {
			flushSegmentIDs = append(flushSegmentIDs, segment.GetID())
		}
	}

	var isUnimplemented bool
	err = retry.Do(ctx, func() error {
		for _, channelInfo := range s.channelManager.GetAssignedChannels() {
			nodeID := channelInfo.NodeID
			channels := lo.Filter(channelInfo.Channels, func(channel *channel, _ int) bool {
				return channel.CollectionID == req.GetCollectionID()
			})
			channelNames := lo.Map(channels, func(channel *channel, _ int) string {
				return channel.Name
			})
			err = s.cluster.FlushChannels(ctx, nodeID, ts, channelNames)
			if err != nil && errors.Is(err, merr.ErrServiceUnimplemented) {
				isUnimplemented = true
				return nil
			}
			if err != nil {
				return err
			}
		}
		return nil
	}, retry.Attempts(60)) // about 3min
	if err != nil {
		return &datapb.FlushResponse{
			Status: merr.Status(err),
		}, nil
	}

	if isUnimplemented {
		// For compatible with rolling upgrade from version 2.2.x,
		// fall back to the flush logic of version 2.2.x;
		log.Warn("DataNode FlushChannels unimplemented", zap.Error(err))
		ts = 0
	}

	log.Info("flush response with segments",
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("sealSegments", sealedSegmentIDs),
		zap.Int64s("flushSegments", flushSegmentIDs),
		zap.Time("timeOfSeal", timeOfSeal),
		zap.Time("flushTs", tsoutil.PhysicalTime(ts)))

	return &datapb.FlushResponse{
		Status:          merr.Success(),
		DbID:            req.GetDbID(),
		CollectionID:    req.GetCollectionID(),
		SegmentIDs:      sealedSegmentIDs,
		TimeOfSeal:      timeOfSeal.Unix(),
		FlushSegmentIDs: flushSegmentIDs,
		FlushTs:         ts,
	}, nil
}

// AssignSegmentID applies for segment ids and make allocation for records.
func (s *Server) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	log := log.Ctx(ctx)
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.AssignSegmentIDResponse{
			Status: merr.Status(err),
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
		s.cluster.Watch(ctx, r.ChannelName, r.CollectionID)

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
				Status:       merr.Success(),
			}
			assigns = append(assigns, result)
		}
	}
	return &datapb.AssignSegmentIDResponse{
		Status:           merr.Success(),
		SegIDAssignments: assigns,
	}, nil
}

// GetSegmentStates returns segments state
func (s *Server) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.GetSegmentStatesResponse{
			Status: merr.Status(err),
		}, nil
	}

	resp := &datapb.GetSegmentStatesResponse{
		Status: merr.Success(),
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

	return resp, nil
}

// GetInsertBinlogPaths returns binlog paths info for requested segments
func (s *Server) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.GetInsertBinlogPathsResponse{
			Status: merr.Status(err),
		}, nil
	}
	segment := s.meta.GetHealthySegment(req.GetSegmentID())
	if segment == nil {
		return &datapb.GetInsertBinlogPathsResponse{
			Status: merr.Status(merr.WrapErrSegmentNotFound(req.GetSegmentID())),
		}, nil
	}

	resp := &datapb.GetInsertBinlogPathsResponse{
		Status: merr.Success(),
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
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.GetCollectionStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}

	resp := &datapb.GetCollectionStatisticsResponse{
		Status: merr.Success(),
	}
	nums := s.meta.GetNumRowsOfCollection(req.CollectionID)
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
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.GetPartitionStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}
	nums := int64(0)
	if len(req.GetPartitionIDs()) == 0 {
		nums = s.meta.GetNumRowsOfCollection(req.CollectionID)
	}
	for _, partID := range req.GetPartitionIDs() {
		num := s.meta.GetNumRowsOfPartition(req.CollectionID, partID)
		nums += num
	}
	resp.Stats = append(resp.Stats, &commonpb.KeyValuePair{Key: "row_count", Value: strconv.FormatInt(nums, 10)})
	log.Info("success to get partition statistics", zap.Any("response", resp))
	return resp, nil
}

// GetSegmentInfoChannel legacy API, returns segment info statistics channel
func (s *Server) GetSegmentInfoChannel(ctx context.Context, req *datapb.GetSegmentInfoChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
		Value:  Params.CommonCfg.DataCoordSegmentInfo.GetValue(),
	}, nil
}

// GetSegmentInfo returns segment info requested, status, row count, etc included
// Called by: QueryCoord, DataNode, IndexCoord, Proxy.
func (s *Server) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	log := log.Ctx(ctx)
	resp := &datapb.GetSegmentInfoResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.GetSegmentInfoResponse{
			Status: merr.Status(err),
		}, nil
	}
	infos := make([]*datapb.SegmentInfo, 0, len(req.GetSegmentIDs()))
	channelCPs := make(map[string]*msgpb.MsgPosition)
	for _, id := range req.SegmentIDs {
		var info *SegmentInfo
		if req.IncludeUnHealthy {
			info = s.meta.GetSegment(id)

			if info == nil {
				log.Warn("failed to get segment, this may have been cleaned", zap.Int64("segmentID", id))
				err := merr.WrapErrSegmentNotFound(id)
				resp.Status = merr.Status(err)
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
				err := merr.WrapErrSegmentNotFound(id)
				resp.Status = merr.Status(err)
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
	resp.Infos = infos
	resp.ChannelCheckpoint = channelCPs
	return resp, nil
}

// SaveBinlogPaths updates segment related binlog path
// works for Checkpoints and Flush
func (s *Server) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return merr.Status(err), nil
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
			err := merr.WrapErrChannelNotFound(channelName, fmt.Sprintf("for node %d", nodeID))
			log.Warn("node is not matched with channel", zap.String("channel", channelName), zap.Error(err))
			return merr.Status(err), nil
		}
	}

	// validate
	segmentID := req.GetSegmentID()
	segment := s.meta.GetSegment(segmentID)

	if segment == nil {
		err := merr.WrapErrSegmentNotFound(segmentID)
		log.Warn("failed to get segment", zap.Error(err))
		return merr.Status(err), nil
	}

	if segment.State == commonpb.SegmentState_Dropped {
		log.Info("save to dropped segment, ignore this request")
		return merr.Success(), nil
	} else if !isSegmentHealthy(segment) {
		err := merr.WrapErrSegmentNotFound(segmentID)
		log.Warn("failed to get segment, the segment not healthy", zap.Error(err))
		return merr.Status(err), nil
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
		return merr.Status(err), nil
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
	return merr.Success(), nil
}

// DropVirtualChannel notifies vchannel dropped
// And contains the remaining data log & checkpoint to update
func (s *Server) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	log := log.Ctx(ctx)
	resp := &datapb.DropVirtualChannelResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.DropVirtualChannelResponse{
			Status: merr.Status(err),
		}, nil
	}

	channel := req.GetChannelName()
	log.Info("receive DropVirtualChannel request",
		zap.String("channelName", channel))

	// validate
	nodeID := req.GetBase().GetSourceID()
	if !s.channelManager.Match(nodeID, channel) {
		err := merr.WrapErrChannelNotFound(channel, fmt.Sprintf("for node %d", nodeID))
		resp.Status = merr.Status(err)
		log.Warn("node is not matched with channel", zap.String("channel", channel), zap.Int64("nodeID", nodeID))
		return resp, nil
	}

	var collectionID int64
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
		collectionID = seg2Drop.GetCollectionID()
	}

	err := s.meta.UpdateDropChannelSegmentInfo(channel, segments)
	if err != nil {
		log.Error("Update Drop Channel segment info failed", zap.String("channel", channel), zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	log.Info("DropVChannel plan to remove", zap.String("channel", channel))
	err = s.channelManager.Release(nodeID, channel)
	if err != nil {
		log.Warn("DropVChannel failed to ReleaseAndRemove", zap.String("channel", channel), zap.Error(err))
	}
	s.segmentManager.DropSegmentsOfChannel(ctx, channel)

	metrics.CleanupDataCoordNumStoredRows(collectionID)

	// no compaction triggered in Drop procedure
	return resp, nil
}

// SetSegmentState reset the state of the given segment.
func (s *Server) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest) (*datapb.SetSegmentStateResponse, error) {
	log := log.Ctx(ctx)
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.SetSegmentStateResponse{
			Status: merr.Status(err),
		}, nil
	}
	err := s.meta.SetState(req.GetSegmentId(), req.GetNewState())
	if err != nil {
		log.Error("failed to updated segment state in dataCoord meta",
			zap.Int64("segmentID", req.SegmentId),
			zap.String("newState", req.GetNewState().String()))
		return &datapb.SetSegmentStateResponse{
			Status: merr.Status(err),
		}, nil
	}
	return &datapb.SetSegmentStateResponse{
		Status: merr.Success(),
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
func (s *Server) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	code := s.GetStateCode()
	nodeID := common.NotRegisteredID
	if s.session != nil && s.session.Registered() {
		nodeID = s.session.GetServerID() // or Params.NodeID
	}
	resp := &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			// NodeID:    Params.NodeID, // will race with Server.Register()
			NodeID:    nodeID,
			Role:      "datacoord",
			StateCode: code,
		},
		Status: merr.Success(),
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
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.GetRecoveryInfoResponse{
			Status: merr.Status(err),
		}, nil
	}

	dresp, err := s.broker.DescribeCollectionInternal(s.ctx, collectionID)
	if err != nil {
		log.Error("get collection info from rootcoord failed",
			zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	channels := dresp.GetVirtualChannelNames()
	channelInfos := make([]*datapb.VchannelInfo, 0, len(channels))
	flushedIDs := make(typeutil.UniqueSet)
	for _, c := range channels {
		channelInfo := s.handler.GetQueryVChanPositions(&channel{Name: c, CollectionID: collectionID}, partitionID)
		channelInfos = append(channelInfos, channelInfo)
		log.Info("datacoord append channelInfo in GetRecoveryInfo",
			zap.String("channel", channelInfo.GetChannelName()),
			zap.Int("# of unflushed segments", len(channelInfo.GetUnflushedSegmentIds())),
			zap.Int("# of flushed segments", len(channelInfo.GetFlushedSegmentIds())),
			zap.Int("# of dropped segments", len(channelInfo.GetDroppedSegmentIds())),
			zap.Int("# of indexed segments", len(channelInfo.GetIndexedSegmentIds())),
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
			err := merr.WrapErrSegmentNotFound(id)
			log.Warn("failed to get segment", zap.Int64("segmentID", id))
			resp.Status = merr.Status(err)
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

		if newCount := segmentutil.CalcRowCountFromBinLog(segment.SegmentInfo); newCount != segment.NumOfRows && newCount > 0 {
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
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.GetRecoveryInfoResponseV2{
			Status: merr.Status(err),
		}, nil
	}
	channels := s.channelManager.GetChannelsByCollectionID(collectionID)
	channelInfos := make([]*datapb.VchannelInfo, 0, len(channels))
	flushedIDs := make(typeutil.UniqueSet)
	for _, ch := range channels {
		channelInfo := s.handler.GetQueryVChanPositions(ch, partitionIDs...)
		channelInfos = append(channelInfos, channelInfo)
		log.Info("datacoord append channelInfo in GetRecoveryInfo",
			zap.String("channel", channelInfo.GetChannelName()),
			zap.Int("# of unflushed segments", len(channelInfo.GetUnflushedSegmentIds())),
			zap.Int("# of flushed segments", len(channelInfo.GetFlushedSegmentIds())),
			zap.Int("# of dropped segments", len(channelInfo.GetDroppedSegmentIds())),
			zap.Int("# of indexed segments", len(channelInfo.GetIndexedSegmentIds())),
		)
		flushedIDs.Insert(channelInfo.GetFlushedSegmentIds()...)
	}

	segmentInfos := make([]*datapb.SegmentInfo, 0)
	for id := range flushedIDs {
		segment := s.meta.GetSegment(id)
		if segment == nil {
			err := merr.WrapErrSegmentNotFound(id)
			log.Warn("failed to get segment", zap.Int64("segmentID", id))
			resp.Status = merr.Status(err)
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
		})
	}

	resp.Channels = channelInfos
	resp.Segments = segmentInfos
	return resp, nil
}

// GetFlushedSegments returns all segment matches provided criterion and in state Flushed or Dropped (compacted but not GCed yet)
// If requested partition id < 0, ignores the partition id filter
func (s *Server) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	log := log.Ctx(ctx)
	resp := &datapb.GetFlushedSegmentsResponse{
		Status: merr.Success(),
	}
	collectionID := req.GetCollectionID()
	partitionID := req.GetPartitionID()
	log.Info("received get flushed segments request",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
	)
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.GetFlushedSegmentsResponse{
			Status: merr.Status(err),
		}, nil
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
	return resp, nil
}

// GetSegmentsByStates returns all segment matches provided criterion and States
// If requested partition id < 0, ignores the partition id filter
func (s *Server) GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest) (*datapb.GetSegmentsByStatesResponse, error) {
	log := log.Ctx(ctx)
	resp := &datapb.GetSegmentsByStatesResponse{
		Status: merr.Success(),
	}
	collectionID := req.GetCollectionID()
	partitionID := req.GetPartitionID()
	states := req.GetStates()
	log.Info("received get segments by states request",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Any("states", states))
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.GetSegmentsByStatesResponse{
			Status: merr.Status(err),
		}, nil
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
	return resp, nil
}

// ShowConfigurations returns the configurations of DataCoord matching req.Pattern
func (s *Server) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &internalpb.ShowConfigurationsResponse{
			Status: merr.Status(err),
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
		Status:        merr.Success(),
		Configuations: configList,
	}, nil
}

// GetMetrics returns DataCoord metrics info
// it may include SystemMetrics, Topology metrics, etc.
func (s *Server) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log := log.Ctx(ctx)
	if err := merr.CheckHealthyStandby(s.GetStateCode()); err != nil {
		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(err),
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("DataCoord.GetMetrics failed to parse metric type",
			zap.Int64("nodeID", paramtable.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(err),
		)

		return &milvuspb.GetMetricsResponse{
			ComponentName: metricsinfo.ConstructComponentName(typeutil.DataCoordRole, paramtable.GetNodeID()),
			Status:        merr.Status(err),
		}, nil
	}

	if metricType == metricsinfo.SystemInfoMetrics {
		metrics, err := s.getSystemInfoMetrics(ctx, req)
		if err != nil {
			log.Warn("DataCoord GetMetrics failed", zap.Int64("nodeID", paramtable.GetNodeID()), zap.Error(err))
			return &milvuspb.GetMetricsResponse{
				Status: merr.Status(err),
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
		Status:        merr.Status(merr.WrapErrMetricNotFound(metricType)),
	}, nil
}

// ManualCompaction triggers a compaction for a collection
func (s *Server) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
	)
	log.Info("received manual compaction")

	resp := &milvuspb.ManualCompactionResponse{
		Status: merr.Success(),
	}

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &milvuspb.ManualCompactionResponse{
			Status: merr.Status(err),
		}, nil
	}

	if !Params.DataCoordCfg.EnableCompaction.GetAsBool() {
		resp.Status = merr.Status(merr.WrapErrServiceUnavailable("compaction disabled"))
		return resp, nil
	}

	id, err := s.compactionTrigger.forceTriggerCompaction(req.CollectionID)
	if err != nil {
		log.Error("failed to trigger manual compaction", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	log.Info("success to trigger manual compaction", zap.Int64("compactionID", id))
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
		Status: merr.Success(),
	}

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &milvuspb.GetCompactionStateResponse{
			Status: merr.Status(err),
		}, nil
	}

	if !Params.DataCoordCfg.EnableCompaction.GetAsBool() {
		resp.Status = merr.Status(merr.WrapErrServiceUnavailable("compaction disabled"))
		return resp, nil
	}

	tasks := s.compactionHandler.getCompactionTasksBySignalID(req.GetCompactionID())
	state, executingCnt, completedCnt, failedCnt, timeoutCnt := getCompactionState(tasks)

	resp.State = state
	resp.ExecutingPlanNo = int64(executingCnt)
	resp.CompletedPlanNo = int64(completedCnt)
	resp.TimeoutPlanNo = int64(timeoutCnt)
	resp.FailedPlanNo = int64(failedCnt)
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

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &milvuspb.GetCompactionPlansResponse{
			Status: merr.Status(err),
		}, nil
	}

	resp := &milvuspb.GetCompactionPlansResponse{
		Status: merr.Success(),
	}
	if !Params.DataCoordCfg.EnableCompaction.GetAsBool() {
		resp.Status = merr.Status(merr.WrapErrServiceUnavailable("compaction disabled"))
		return resp, nil
	}

	tasks := s.compactionHandler.getCompactionTasksBySignalID(req.GetCompactionID())
	for _, task := range tasks {
		resp.MergeInfos = append(resp.MergeInfos, getCompactionMergeInfo(task))
	}

	state, _, _, _, _ := getCompactionState(tasks)

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
		Status: merr.Success(),
	}

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.WatchChannelsResponse{
			Status: merr.Status(err),
		}, nil
	}
	for _, channelName := range req.GetChannelNames() {
		ch := &channel{
			Name:            channelName,
			CollectionID:    req.GetCollectionID(),
			StartPositions:  req.GetStartPositions(),
			Schema:          req.GetSchema(),
			CreateTimestamp: req.GetCreateTimestamp(),
		}
		err := s.channelManager.Watch(ctx, ch)
		if err != nil {
			log.Warn("fail to watch channelName", zap.Error(err))
			resp.Status = merr.Status(err)
			return resp, nil
		}
		if err := s.meta.catalog.MarkChannelAdded(ctx, ch.Name); err != nil {
			// TODO: add background task to periodically cleanup the orphaned channel add marks.
			log.Error("failed to mark channel added", zap.Error(err))
			resp.Status = merr.Status(err)
			return resp, nil
		}
	}

	return resp, nil
}

// GetFlushState gets the flush state of the collection based on the provided flush ts and segment IDs.
func (s *Server) GetFlushState(ctx context.Context, req *datapb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	log := log.Ctx(ctx).With(zap.Int64("collection", req.GetCollectionID()),
		zap.Time("flushTs", tsoutil.PhysicalTime(req.GetFlushTs()))).
		WithRateGroup("dc.GetFlushState", 1, 60)
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &milvuspb.GetFlushStateResponse{
			Status: merr.Status(err),
		}, nil
	}

	resp := &milvuspb.GetFlushStateResponse{Status: merr.Success()}
	if len(req.GetSegmentIDs()) > 0 {
		var unflushed []UniqueID
		for _, sid := range req.GetSegmentIDs() {
			segment := s.meta.GetHealthySegment(sid)
			// segment is nil if it was compacted, or it's an empty segment and is set to dropped
			if segment == nil || isFlushState(segment.GetState()) {
				continue
			}
			unflushed = append(unflushed, sid)
		}
		if len(unflushed) != 0 {
			log.RatedInfo(10, "DataCoord receive GetFlushState request, Flushed is false", zap.Int64s("unflushed", unflushed), zap.Int("len", len(unflushed)))
			resp.Flushed = false

			return resp, nil
		}
	}

	channels := make([]string, 0)
	for _, channelInfo := range s.channelManager.GetAssignedChannels() {
		filtered := lo.Filter(channelInfo.Channels, func(channel *channel, _ int) bool {
			return channel.CollectionID == req.GetCollectionID()
		})
		channelNames := lo.Map(filtered, func(channel *channel, _ int) string {
			return channel.Name
		})
		channels = append(channels, channelNames...)
	}

	if len(channels) == 0 { // For compatibility with old client
		resp.Flushed = true

		log.Info("GetFlushState all flushed without checking flush ts")
		return resp, nil
	}

	for _, channel := range channels {
		cp := s.meta.GetChannelCheckpoint(channel)
		if cp == nil || cp.GetTimestamp() < req.GetFlushTs() {
			resp.Flushed = false

			log.RatedInfo(10, "GetFlushState failed, channel unflushed", zap.String("channel", channel),
				zap.Time("CP", tsoutil.PhysicalTime(cp.GetTimestamp())),
				zap.Duration("lag", tsoutil.PhysicalTime(req.GetFlushTs()).Sub(tsoutil.PhysicalTime(cp.GetTimestamp()))))
			return resp, nil
		}
	}

	resp.Flushed = true
	log.Info("GetFlushState all flushed")

	return resp, nil
}

// GetFlushAllState checks if all DML messages before `FlushAllTs` have been flushed.
func (s *Server) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error) {
	log := log.Ctx(ctx)
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &milvuspb.GetFlushAllStateResponse{
			Status: merr.Status(err),
		}, nil
	}

	resp := &milvuspb.GetFlushAllStateResponse{Status: merr.Success()}

	dbsRsp, err := s.broker.ListDatabases(ctx)
	if err != nil {
		log.Warn("failed to ListDatabases", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	dbNames := dbsRsp.DbNames
	if req.GetDbName() != "" {
		dbNames = lo.Filter(dbNames, func(dbName string, _ int) bool {
			return dbName == req.GetDbName()
		})
		if len(dbNames) == 0 {
			resp.Status = merr.Status(merr.WrapErrDatabaseNotFound(req.GetDbName()))
			return resp, nil
		}
	}

	for _, dbName := range dbsRsp.DbNames {
		showColRsp, err := s.broker.ShowCollections(ctx, dbName)
		if err != nil {
			log.Warn("failed to ShowCollections", zap.Error(err))
			resp.Status = merr.Status(err)
			return resp, nil
		}

		for _, collection := range showColRsp.GetCollectionIds() {
			describeColRsp, err := s.broker.DescribeCollectionInternal(ctx, collection)
			if err != nil {
				log.Warn("failed to DescribeCollectionInternal", zap.Error(err))
				resp.Status = merr.Status(err)
				return resp, nil
			}
			for _, channel := range describeColRsp.GetVirtualChannelNames() {
				channelCP := s.meta.GetChannelCheckpoint(channel)
				if channelCP == nil || channelCP.GetTimestamp() < req.GetFlushAllTs() {
					resp.Flushed = false

					return resp, nil
				}
			}
		}
	}
	resp.Flushed = true
	return resp, nil
}

// Import distributes the import tasks to DataNodes.
// It returns a failed status if no DataNode is available or if any error occurs.
func (s *Server) Import(ctx context.Context, req *datapb.ImportTaskRequest) (*datapb.ImportTaskResponse, error) {
	log := log.Ctx(ctx)
	log.Info("DataCoord receives import request", zap.Any("req", req))
	resp := &datapb.ImportTaskResponse{
		Status: merr.Success(),
	}

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.ImportTaskResponse{
			Status: merr.Status(err),
		}, nil
	}

	nodes := s.sessionManager.getLiveNodeIDs()
	if len(nodes) == 0 {
		log.Warn("import failed as all DataNodes are offline")
		resp.Status = merr.Status(merr.WrapErrNodeLackAny("no live DataNode"))
		return resp, nil
	}
	log.Info("available DataNodes are", zap.Int64s("nodeIDs", nodes))

	avaNodes := getDiff(nodes, req.GetWorkingNodes())
	if len(avaNodes) > 0 {
		// If there exists available DataNodes, pick one at random.
		resp.DatanodeId = avaNodes[rand.Intn(len(avaNodes))]
		log.Info("picking a free DataNode",
			zap.Any("all DataNodes", nodes),
			zap.Int64("picking free DataNode with ID", resp.GetDatanodeId()))
		s.cluster.Import(s.ctx, resp.GetDatanodeId(), req)
	} else {
		// No DataNode is available, reject the import request.
		msg := "all DataNodes are busy working on data import, the task has been rejected and wait for idle DataNode"
		log.Info(msg, zap.Int64("taskID", req.GetImportTask().GetTaskId()))
		resp.Status = merr.Status(merr.WrapErrNodeLackAny("no available DataNode"))
		return resp, nil
	}

	return resp, nil
}

// UpdateSegmentStatistics updates a segment's stats.
func (s *Server) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	s.updateSegmentStatistics(req.GetStats())
	return merr.Success(), nil
}

// UpdateChannelCheckpoint updates channel checkpoint in dataCoord.
func (s *Server) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	err := s.meta.UpdateChannelCheckpoint(req.GetVChannel(), req.GetPosition())
	if err != nil {
		log.Warn("failed to UpdateChannelCheckpoint", zap.String("vChannel", req.GetVChannel()), zap.Error(err))
		return merr.Status(err), nil
	}

	return merr.Success(), nil
}

// ReportDataNodeTtMsgs send datenode timetick messages to dataCoord.
func (s *Server) ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return merr.Status(err), nil
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

	return merr.Success(), nil
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
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	// Look for the DataNode that watches the channel.
	ok, nodeID := s.channelManager.getNodeIDByChannelName(req.GetChannelName())
	if !ok {
		err := merr.WrapErrChannelNotFound(req.GetChannelName(), "no DataNode watches this channel")
		log.Error("no DataNode found for channel", zap.String("channelName", req.GetChannelName()), zap.Error(err))
		return merr.Status(err), nil
	}
	// Call DataNode to add the new segment to its own flow graph.
	cli, err := s.sessionManager.getClient(ctx, nodeID)
	if err != nil {
		log.Error("failed to get DataNode client for SaveImportSegment",
			zap.Int64("DataNode ID", nodeID),
			zap.Error(err))
		return merr.Status(err), nil
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
		return merr.Status(err), nil
	}
	log.Info("succeed to add segment", zap.Int64("DataNode ID", nodeID), zap.Any("add segment req", req))
	// Fill in start position message ID.
	req.SaveBinlogPathReq.StartPositions[0].StartPosition.MsgID = resp.GetChannelPos()

	// Start saving bin log paths.
	rsp, err := s.SaveBinlogPaths(context.Background(), req.GetSaveBinlogPathReq())
	if err := VerifyResponse(rsp, err); err != nil {
		log.Error("failed to SaveBinlogPaths", zap.Error(err))
		return merr.Status(err), nil
	}
	return merr.Success(), nil
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
			log.Error("failed to unset segment is importing state",
				zap.Int64("segmentID", segID),
			)
			reportErr = err
		}
	}

	return merr.Status(reportErr), nil
}

// MarkSegmentsDropped marks the given segments as `Dropped`.
// An error status will be returned and error will be logged, if we failed to mark *all* segments.
// Deprecated, do not use it
func (s *Server) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest) (*commonpb.Status, error) {
	log.Info("marking segments dropped", zap.Int64s("segments", req.GetSegmentIds()))
	var err error
	for _, segID := range req.GetSegmentIds() {
		if err = s.meta.SetState(segID, commonpb.SegmentState_Dropped); err != nil {
			// Fail-open.
			log.Error("failed to set segment state as dropped", zap.Int64("segmentID", segID))
			break
		}
	}
	return merr.Status(err), nil
}

func (s *Server) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return merr.Status(err), nil
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
		return merr.Success(), nil
	}

	clonedColl.Properties = properties
	s.meta.AddCollection(clonedColl)
	return merr.Success(), nil
}

func (s *Server) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &milvuspb.CheckHealthResponse{
			Status:  merr.Status(err),
			Reasons: []string{err.Error()},
		}, nil
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
				errReasons = append(errReasons, fmt.Sprintf("failed to get DataNode %d: %v", nodeID, err))
				return err
			}

			sta, err := cli.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
			if err != nil {
				return err
			}
			err = merr.AnalyzeState("DataNode", nodeID, sta)
			if err != nil {
				mu.Lock()
				defer mu.Unlock()
				errReasons = append(errReasons, err.Error())
			}
			return nil
		})
	}

	err := group.Wait()
	if err != nil || len(errReasons) != 0 {
		return &milvuspb.CheckHealthResponse{Status: merr.Success(), IsHealthy: false, Reasons: errReasons}, nil
	}

	return &milvuspb.CheckHealthResponse{Status: merr.Success(), IsHealthy: true, Reasons: errReasons}, nil
}

func (s *Server) GcConfirm(ctx context.Context, request *datapb.GcConfirmRequest) (*datapb.GcConfirmResponse, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.GcConfirmResponse{
			Status: merr.Status(err),
		}, nil
	}

	resp := &datapb.GcConfirmResponse{
		Status: merr.Success(),
	}
	resp.GcFinished = s.meta.GcConfirm(ctx, request.GetCollectionId(), request.GetPartitionId())
	return resp, nil
}
