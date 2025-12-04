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
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
		zap.Int64("collectionID", req.GetCollectionID()))
	log.Info("receive flush request")
	ctx, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "DataCoord-Flush")
	defer sp.End()

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.FlushResponse{
			Status: merr.Status(err),
		}, nil
	}

	// generate a timestamp timeOfSeal, all data before timeOfSeal is guaranteed to be sealed or flushed
	ts, err := s.allocator.AllocTimestamp(ctx)
	if err != nil {
		log.Warn("unable to alloc timestamp", zap.Error(err))
		return nil, err
	}
	flushResult, err := s.flushCollection(ctx, req.GetCollectionID(), ts, req.GetSegmentIDs())
	if err != nil {
		return &datapb.FlushResponse{
			Status: merr.Status(err),
		}, nil
	}

	return &datapb.FlushResponse{
		Status:          merr.Success(),
		DbID:            req.GetDbID(),
		CollectionID:    req.GetCollectionID(),
		SegmentIDs:      flushResult.GetSegmentIDs(),
		TimeOfSeal:      flushResult.GetTimeOfSeal(),
		FlushSegmentIDs: flushResult.GetFlushSegmentIDs(),
		FlushTs:         flushResult.GetFlushTs(),
		ChannelCps:      flushResult.GetChannelCps(),
	}, nil
}

func (s *Server) flushCollection(ctx context.Context, collectionID UniqueID, flushTs uint64, toFlushSegments []UniqueID) (*datapb.FlushResult, error) {
	channelCPs := make(map[string]*msgpb.MsgPosition, 0)
	coll, err := s.handler.GetCollection(ctx, collectionID)
	if err != nil {
		log.Warn("fail to get collection", zap.Error(err))
		return nil, err
	}
	if coll == nil {
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}
	// channel checkpoints must be gotten before sealSegment, make sure checkpoints is earlier than segment's endts
	for _, vchannel := range coll.VChannelNames {
		cp := s.meta.GetChannelCheckpoint(vchannel)
		channelCPs[vchannel] = cp
	}

	timeOfSeal, _ := tsoutil.ParseTS(flushTs)
	sealedSegmentsIDDict := make(map[UniqueID]bool)

	if !streamingutil.IsStreamingServiceEnabled() {
		for _, channel := range coll.VChannelNames {
			sealedSegmentIDs, err := s.segmentManager.SealAllSegments(ctx, channel, toFlushSegments)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to flush collection %d", collectionID)
			}
			for _, sealedSegmentID := range sealedSegmentIDs {
				sealedSegmentsIDDict[sealedSegmentID] = true
			}
		}
	}

	segments := s.meta.GetSegmentsOfCollection(ctx, collectionID)
	flushSegmentIDs := make([]UniqueID, 0, len(segments))
	for _, segment := range segments {
		if segment != nil &&
			isFlushState(segment.GetState()) &&
			segment.GetLevel() != datapb.SegmentLevel_L0 && // SegmentLevel_Legacy, SegmentLevel_L1, SegmentLevel_L2
			!sealedSegmentsIDDict[segment.GetID()] {
			flushSegmentIDs = append(flushSegmentIDs, segment.GetID())
		}
	}

	log.Info("flush response with segments",
		zap.Int64("collectionID", collectionID),
		zap.Int64s("sealSegments", lo.Keys(sealedSegmentsIDDict)),
		zap.Int("flushedSegmentsCount", len(flushSegmentIDs)),
		zap.Time("timeOfSeal", timeOfSeal),
		zap.Uint64("flushTs", flushTs),
		zap.Time("flushTs in time", tsoutil.PhysicalTime(flushTs)))

	return &datapb.FlushResult{
		CollectionID:    collectionID,
		SegmentIDs:      lo.Keys(sealedSegmentsIDDict),
		TimeOfSeal:      timeOfSeal.Unix(),
		FlushSegmentIDs: flushSegmentIDs,
		FlushTs:         flushTs,
		ChannelCps:      channelCPs,
		DbName:          coll.DatabaseName,
		CollectionName:  coll.Schema.GetName(),
	}, nil
}

func resolveCollectionsToFlush(ctx context.Context, s *Server, req *datapb.FlushAllRequest) ([]int64, error) {
	collectionsToFlush := make([]int64, 0)
	if len(req.GetFlushTargets()) > 0 {
		// Use flush_targets from request
		for _, target := range req.GetFlushTargets() {
			collectionsToFlush = append(collectionsToFlush, target.GetCollectionIds()...)
		}
	} else if req.GetDbName() != "" {
		// Backward compatibility: use deprecated db_name field
		showColRsp, err := s.broker.ShowCollectionIDs(ctx, req.GetDbName())
		if err != nil {
			log.Warn("failed to ShowCollectionIDs", zap.String("db", req.GetDbName()), zap.Error(err))
			return nil, err
		}
		for _, dbCollection := range showColRsp.GetDbCollections() {
			collectionsToFlush = append(collectionsToFlush, dbCollection.GetCollectionIDs()...)
		}
	} else {
		// Flush all databases
		dbsResp, err := s.broker.ListDatabases(ctx)
		if err != nil {
			return nil, err
		}
		for _, dbName := range dbsResp.GetDbNames() {
			showColRsp, err := s.broker.ShowCollectionIDs(ctx, dbName)
			if err != nil {
				log.Warn("failed to ShowCollectionIDs", zap.String("db", dbName), zap.Error(err))
				return nil, err
			}
			for _, dbCollection := range showColRsp.GetDbCollections() {
				collectionsToFlush = append(collectionsToFlush, dbCollection.GetCollectionIDs()...)
			}
		}
	}

	return collectionsToFlush, nil
}

func (s *Server) FlushAll(ctx context.Context, req *datapb.FlushAllRequest) (*datapb.FlushAllResponse, error) {
	log := log.Ctx(ctx)
	log.Info("receive flushAll request")
	ctx, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "DataCoord-Flush")
	defer sp.End()

	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		log.Info("server is not healthy", zap.Error(err), zap.Any("stateCode", s.GetStateCode()))
		return &datapb.FlushAllResponse{
			Status: merr.Status(err),
		}, nil
	}

	// generate a timestamp timeOfSeal, all data before timeOfSeal is guaranteed to be sealed or flushed
	ts, err := s.allocator.AllocTimestamp(ctx)
	if err != nil {
		log.Warn("unable to alloc timestamp", zap.Error(err))
		return nil, err
	}

	// resolve collections to flush
	collectionsToFlush, err := resolveCollectionsToFlush(ctx, s, req)
	if err != nil {
		return &datapb.FlushAllResponse{
			Status: merr.Status(err),
		}, nil
	}

	var mu sync.Mutex
	flushInfos := make([]*datapb.FlushResult, 0)
	wg := errgroup.Group{}
	// limit goroutine number to 100
	wg.SetLimit(100)
	for _, cid := range collectionsToFlush {
		wg.Go(func() error {
			flushResult, err := s.flushCollection(ctx, cid, ts, nil)
			if err != nil {
				log.Warn("failed to flush collection", zap.Int64("collectionID", cid), zap.Error(err))
				return err
			}
			mu.Lock()
			flushInfos = append(flushInfos, flushResult)
			mu.Unlock()
			return nil
		})
	}

	err = wg.Wait()
	if err != nil {
		return &datapb.FlushAllResponse{
			Status: merr.Status(err),
		}, nil
	}

	return &datapb.FlushAllResponse{
		Status:       merr.Success(),
		FlushTs:      ts,
		FlushResults: flushInfos,
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
		)

		// Load the collection info from Root Coordinator, if it is not found in server meta.
		// Note: this request wouldn't be received if collection didn't exist.
		_, err := s.handler.GetCollection(ctx, r.GetCollectionID())
		if err != nil {
			log.Warn("cannot get collection schema", zap.Error(err))
		}

		// Have segment manager allocate and return the segment allocation info.
		segmentAllocations, err := s.segmentManager.AllocSegment(ctx,
			r.CollectionID, r.PartitionID, r.ChannelName, int64(r.Count), r.GetStorageVersion())
		if err != nil {
			log.Warn("failed to alloc segment", zap.Any("request", r), zap.Error(err))
			assigns = append(assigns, &datapb.SegmentIDAssignment{
				ChannelName:  r.ChannelName,
				CollectionID: r.CollectionID,
				PartitionID:  r.PartitionID,
				Status:       merr.Status(err),
			})
			continue
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

// AllocSegment alloc a new growing segment, add it into segment meta.
// Only used by Streamingnode, should be deprecated in the future after growing segment fully managed by streaming node.
func (s *Server) AllocSegment(ctx context.Context, req *datapb.AllocSegmentRequest) (*datapb.AllocSegmentResponse, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.AllocSegmentResponse{Status: merr.Status(err)}, nil
	}
	// !!! SegmentId must be allocated from rootCoord id allocation.
	if req.GetCollectionId() == 0 || req.GetPartitionId() == 0 || req.GetVchannel() == "" || req.GetSegmentId() == 0 {
		return &datapb.AllocSegmentResponse{Status: merr.Status(merr.ErrParameterInvalid)}, nil
	}

	// Alloc new growing segment and return the segment info.
	segmentInfo, err := s.segmentManager.AllocNewGrowingSegment(
		ctx,
		AllocNewGrowingSegmentRequest{
			CollectionID:         req.GetCollectionId(),
			PartitionID:          req.GetPartitionId(),
			SegmentID:            req.GetSegmentId(),
			ChannelName:          req.GetVchannel(),
			StorageVersion:       req.GetStorageVersion(),
			IsCreatedByStreaming: req.GetIsCreatedByStreaming(),
		},
	)
	if err != nil {
		return &datapb.AllocSegmentResponse{Status: merr.Status(err)}, nil
	}
	clonedSegmentInfo := segmentInfo.Clone()
	return &datapb.AllocSegmentResponse{
		SegmentInfo: clonedSegmentInfo.SegmentInfo,
		Status:      merr.Success(),
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
		segmentInfo := s.meta.GetHealthySegment(ctx, segmentID)
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

	segment := s.meta.GetHealthySegment(ctx, req.GetSegmentID())
	if segment == nil {
		return &datapb.GetInsertBinlogPathsResponse{
			Status: merr.Status(merr.WrapErrSegmentNotFound(req.GetSegmentID())),
		}, nil
	}

	segment = segment.Clone()

	err := binlog.DecompressBinLog(storage.InsertBinlog, segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(), segment.GetBinlogs())
	if err != nil {
		return &datapb.GetInsertBinlogPathsResponse{
			Status: merr.Status(err),
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
	nums := s.meta.GetNumRowsOfCollection(ctx, req.CollectionID)
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
		nums = s.meta.GetNumRowsOfCollection(ctx, req.CollectionID)
	}
	for _, partID := range req.GetPartitionIDs() {
		num := s.meta.GetNumRowsOfPartition(ctx, req.CollectionID, partID)
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

	var getChildrenDelta func(id UniqueID) ([]*datapb.FieldBinlog, error)
	getChildrenDelta = func(id UniqueID) ([]*datapb.FieldBinlog, error) {
		children, ok := s.meta.GetCompactionTo(id)
		// double-check the segment, maybe the segment is being dropped concurrently.
		if !ok {
			log.Warn("failed to get segment, this may have been cleaned", zap.Int64("segmentID", id))
			err := merr.WrapErrSegmentNotFound(id)
			return nil, err
		}
		allDeltaLogs := make([]*datapb.FieldBinlog, 0)
		for _, child := range children {
			clonedChild := child.Clone()
			// child segment should decompress binlog path
			binlog.DecompressBinLog(storage.DeleteBinlog, clonedChild.GetCollectionID(), clonedChild.GetPartitionID(), clonedChild.GetID(), clonedChild.GetDeltalogs())
			allDeltaLogs = append(allDeltaLogs, clonedChild.GetDeltalogs()...)
			allChildrenDeltas, err := getChildrenDelta(child.GetID())
			if err != nil {
				return nil, err
			}
			allDeltaLogs = append(allDeltaLogs, allChildrenDeltas...)
		}

		return allDeltaLogs, nil
	}

	for _, id := range req.SegmentIDs {
		var info *SegmentInfo
		if req.IncludeUnHealthy {
			info = s.meta.GetSegment(ctx, id)
			// info may be not-nil, but ok is false when the segment is being dropped concurrently.
			if info == nil {
				log.Warn("failed to get segment, this may have been cleaned", zap.Int64("segmentID", id))
				err := merr.WrapErrSegmentNotFound(id)
				resp.Status = merr.Status(err)
				return resp, nil
			}

			clonedInfo := info.Clone()
			// We should retrieve the deltalog of all child segments,
			// but due to the compaction constraint based on indexed segment, there will be at most two generations.
			allChildrenDeltalogs, err := getChildrenDelta(id)
			if err != nil {
				resp.Status = merr.Status(err)
				return resp, nil
			}
			clonedInfo.Deltalogs = append(clonedInfo.Deltalogs, allChildrenDeltalogs...)
			segmentutil.ReCalcRowCount(info.SegmentInfo, clonedInfo.SegmentInfo)
			infos = append(infos, clonedInfo.SegmentInfo)
		} else {
			info = s.meta.GetHealthySegment(ctx, id)
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

	var (
		nodeID      = req.GetBase().GetSourceID()
		channelName = req.GetChannel()
	)

	log := log.Ctx(ctx).With(
		zap.Int64("nodeID", nodeID),
		zap.String("channel", channelName),
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64("segmentID", req.GetSegmentID()),
		zap.String("level", req.GetSegLevel().String()),
		zap.Bool("withFullBinlogs", req.GetWithFullBinlogs()),
	)

	log.Info("receive SaveBinlogPaths request",
		zap.Bool("isFlush", req.GetFlushed()),
		zap.Bool("isDropped", req.GetDropped()),
		zap.Any("checkpoints", req.GetCheckPoints()))

	// for compatibility issue , if len(channelName) not exist, skip the check
	// Also avoid to handle segment not found error if not the owner of shard
	if len(channelName) != 0 {
		// TODO: Current checker implementation is node id based, it cannot strictly promise when ABA channel assignment happens.
		// Meanwhile the match operation is not protected with the global segment meta with the same lock,
		// So the new recovery can happen with the old match operation concurrently, so it not safe enough to avoid double flush.
		// Moreover, the Match operation may be called if the flusher is ready to work, but the channel manager on coord don't see the assignment success.
		// So the match operation may be rejected and wait for retry.
		// TODO: We need to make an idempotent operation to avoid the double flush strictly.
		targetID, err := snmanager.StaticStreamingNodeManager.GetLatestWALLocated(ctx, channelName)
		if err != nil || targetID != nodeID {
			err := merr.WrapErrChannelNotFound(channelName, fmt.Sprintf("for node %d", nodeID))
			log.Warn("failed to get latest wal allocated", zap.Int64("nodeID", nodeID), zap.Int64("channel nodeID", targetID), zap.Error(err))
			return merr.Status(err), nil
		}
	}
	// for compatibility issue, before 2.3.4, SaveBinlogPaths has only logpath
	// try to parse path and fill logid
	err := binlog.CompressSaveBinlogPaths(req)
	if err != nil {
		log.Warn("fail to CompressSaveBinlogPaths", zap.String("channel", channelName), zap.Error(err))
		return merr.Status(err), nil
	}

	operators := []UpdateOperator{}

	if req.GetSegLevel() == datapb.SegmentLevel_L0 {
		operators = append(operators, CreateL0Operator(req.GetCollectionID(), req.GetPartitionID(), req.GetSegmentID(), req.GetChannel()))
	} else {
		segment := s.meta.GetSegment(ctx, req.GetSegmentID())
		// validate level one segment
		if segment == nil {
			err := merr.WrapErrSegmentNotFound(req.GetSegmentID())
			log.Warn("failed to get segment", zap.Error(err))
			return merr.Status(err), nil
		}

		if segment.State == commonpb.SegmentState_Dropped {
			log.Info("save to dropped segment, ignore this request")
			return merr.Success(), nil
		}

		if !isSegmentHealthy(segment) {
			err := merr.WrapErrSegmentNotFound(req.GetSegmentID())
			log.Warn("failed to get segment, the segment not healthy", zap.Error(err))
			return merr.Status(err), nil
		}

		// Set storage version
		operators = append(operators, SetStorageVersion(req.GetSegmentID(), req.GetStorageVersion()))

		// Set segment state
		if req.GetDropped() {
			// segmentManager manages growing segments
			s.segmentManager.DropSegment(ctx, req.GetChannel(), req.GetSegmentID())
			operators = append(operators, UpdateStatusOperator(req.GetSegmentID(), commonpb.SegmentState_Dropped))
		} else if req.GetFlushed() {
			s.segmentManager.DropSegment(ctx, req.GetChannel(), req.GetSegmentID())
			if enableSortCompaction() && req.GetSegLevel() != datapb.SegmentLevel_L0 {
				operators = append(operators, SetSegmentIsInvisible(req.GetSegmentID(), true))
			}
			// set segment to SegmentState_Flushed
			operators = append(operators, UpdateStatusOperator(req.GetSegmentID(), commonpb.SegmentState_Flushed))
		}
	}

	if req.GetWithFullBinlogs() {
		// check checkpoint will be executed at updateSegmentPack validation to ignore the illegal checkpoint update.
		operators = append(operators, UpdateBinlogsFromSaveBinlogPathsOperator(
			req.GetSegmentID(),
			req.GetField2BinlogPaths(),
			req.GetField2StatslogPaths(),
			req.GetDeltalogs(),
			req.GetField2Bm25LogPaths(),
		), UpdateCheckPointOperator(req.GetSegmentID(), req.GetCheckPoints(), true))
	} else {
		operators = append(operators, AddBinlogsOperator(req.GetSegmentID(), req.GetField2BinlogPaths(), req.GetField2StatslogPaths(), req.GetDeltalogs(), req.GetField2Bm25LogPaths()),
			UpdateCheckPointOperator(req.GetSegmentID(), req.GetCheckPoints()))
	}

	// save manifest, start positions and checkpoints
	operators = append(operators,
		UpdateManifest(req.GetSegmentID(), req.GetManifestPath()),
		UpdateStartPosition(req.GetStartPositions()),
		UpdateAsDroppedIfEmptyWhenFlushing(req.GetSegmentID()),
	)

	// Update segment info in memory and meta.
	if err := s.meta.UpdateSegmentsInfo(ctx, operators...); err != nil {
		if !errors.Is(err, ErrIgnoredSegmentMetaOperation) {
			log.Error("save binlog and checkpoints failed", zap.Error(err))
			return merr.Status(err), nil
		}
		log.Info("save binlog and checkpoints failed with ignorable error", zap.Error(err))
	}

	s.meta.SetLastWrittenTime(req.GetSegmentID())
	log.Info("SaveBinlogPaths sync segment with meta",
		zap.Any("checkpoints", req.GetCheckPoints()),
		zap.Strings("binlogs", stringifyBinlogs(req.GetField2BinlogPaths())),
		zap.Strings("deltalogs", stringifyBinlogs(req.GetDeltalogs())),
		zap.Strings("statslogs", stringifyBinlogs(req.GetField2StatslogPaths())),
		zap.Strings("bm25logs", stringifyBinlogs(req.GetField2Bm25LogPaths())),
	)

	if req.GetSegLevel() == datapb.SegmentLevel_L0 {
		metrics.DataCoordSizeStoredL0Segment.WithLabelValues(fmt.Sprint(req.GetCollectionID())).Observe(calculateL0SegmentSize(req.GetField2StatslogPaths()))

		s.compactionTriggerManager.OnCollectionUpdate(req.GetCollectionID())
		return merr.Success(), nil
	}

	// notify building index and compaction for "flushing/flushed" level one segment
	if req.GetFlushed() {
		// notify building index
		s.flushCh <- req.SegmentID

		// notify compaction
		_, err := s.compactionTrigger.TriggerCompaction(ctx,
			NewCompactionSignal().
				WithWaitResult(false).
				WithCollectionID(req.GetCollectionID()).
				WithPartitionID(req.GetPartitionID()).
				WithChannel(req.GetChannel()))
		if err != nil {
			log.Warn("failed to trigger single compaction")
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

	err := s.meta.UpdateDropChannelSegmentInfo(ctx, channel, segments)
	if err != nil {
		log.Error("Update Drop Channel segment info failed", zap.String("channel", channel), zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	s.segmentManager.DropSegmentsOfChannel(ctx, channel)
	s.compactionInspector.removeTasksByChannel(channel)
	metrics.DataCoordCheckpointUnixSeconds.DeleteLabelValues(fmt.Sprint(paramtable.GetNodeID()), channel)
	s.meta.MarkChannelCheckpointDropped(ctx, channel)

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
	err := s.meta.SetState(ctx, req.GetSegmentId(), req.GetNewState())
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

// UpdateStateCode update state code
func (s *Server) UpdateStateCode(code commonpb.StateCode) {
	s.stateCode.Store(code)
	log.Ctx(s.ctx).Info("update datacoord state", zap.String("state", code.String()))
}

// GetComponentStates returns DataCoord's current state
func (s *Server) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	code := s.GetStateCode()
	log.Ctx(ctx).Debug("DataCoord current state", zap.String("StateCode", code.String()))
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
		channelInfo := s.handler.GetQueryVChanPositions(&channelMeta{Name: c, CollectionID: collectionID}, partitionID)
		channelInfos = append(channelInfos, channelInfo)
		log.Info("datacoord append channelInfo in GetRecoveryInfo",
			zap.String("channel", channelInfo.GetChannelName()),
			zap.Int("# of unflushed segments", len(channelInfo.GetUnflushedSegmentIds())),
			zap.Int("# of flushed segments", len(channelInfo.GetFlushedSegmentIds())),
			zap.Int("# of dropped segments", len(channelInfo.GetDroppedSegmentIds())),
			zap.Int("# of indexed segments", len(channelInfo.GetIndexedSegmentIds())),
			zap.Int("# of l0 segments", len(channelInfo.GetLevelZeroSegmentIds())),
		)
		flushedIDs.Insert(channelInfo.GetFlushedSegmentIds()...)
	}

	segment2Binlogs := make(map[UniqueID][]*datapb.FieldBinlog)
	segment2StatsBinlogs := make(map[UniqueID][]*datapb.FieldBinlog)
	segment2DeltaBinlogs := make(map[UniqueID][]*datapb.FieldBinlog)
	segment2InsertChannel := make(map[UniqueID]string)
	segmentsNumOfRows := make(map[UniqueID]int64)
	segment2TextStatsLogs := make(map[UniqueID]map[UniqueID]*datapb.TextIndexStats)
	for id := range flushedIDs {
		segment := s.meta.GetSegment(ctx, id)
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

		segment2TextStatsLogs[id] = segment.GetTextStatsLogs()

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
			TextStatsLogs: segment2TextStatsLogs[segmentID],
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
	channels, err := s.getChannelsByCollectionID(ctx, collectionID)
	if err != nil {
		return &datapb.GetRecoveryInfoResponseV2{
			Status: merr.Status(err),
		}, nil
	}
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
			zap.Int("# of l0 segments", len(channelInfo.GetLevelZeroSegmentIds())),
			zap.Time("# of check point", tsoutil.PhysicalTime(channelInfo.GetSeekPosition().GetTimestamp())),
			zap.Time("# of delete check point", tsoutil.PhysicalTime(channelInfo.GetDeleteCheckpoint().GetTimestamp())),
		)
		flushedIDs.Insert(channelInfo.GetFlushedSegmentIds()...)
	}

	segmentInfos := make([]*datapb.SegmentInfo, 0)
	for id := range flushedIDs {
		segment := s.meta.GetSegment(ctx, id)
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
		if len(binlogs) == 0 && segment.GetLevel() != datapb.SegmentLevel_L0 {
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
			Level:         segment.GetLevel(),
			IsSorted:      segment.GetIsSorted(),
		})
	}

	resp.Channels = channelInfos
	resp.Segments = segmentInfos
	return resp, nil
}

// GetChannelRecoveryInfo get recovery channel info.
// Called by: StreamingNode.
func (s *Server) GetChannelRecoveryInfo(ctx context.Context, req *datapb.GetChannelRecoveryInfoRequest) (*datapb.GetChannelRecoveryInfoResponse, error) {
	log := log.Ctx(ctx).With(
		zap.String("vchannel", req.GetVchannel()),
	)
	log.Info("get channel recovery info request received")
	resp := &datapb.GetChannelRecoveryInfoResponse{
		Status: merr.Success(),
	}
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	collectionID := funcutil.GetCollectionIDFromVChannel(req.GetVchannel())

	channelInfo := s.handler.GetDataVChanPositions(&channelMeta{
		Name:         req.GetVchannel(),
		CollectionID: collectionID,
	}, allPartitionID)
	if channelInfo.SeekPosition == nil {
		log.Warn("channel recovery start position is not found, may collection is on creating")
		resp.Status = merr.Status(merr.WrapErrChannelNotAvailable(req.GetVchannel(), "start position is nil"))
		return resp, nil
	}
	segmentsNotCreatedByStreaming := make([]*datapb.SegmentNotCreatedByStreaming, 0)
	for _, segmentID := range channelInfo.GetUnflushedSegmentIds() {
		segment := s.meta.GetSegment(ctx, segmentID)
		if segment != nil && !segment.IsCreatedByStreaming {
			segmentsNotCreatedByStreaming = append(segmentsNotCreatedByStreaming, &datapb.SegmentNotCreatedByStreaming{
				CollectionId: segment.CollectionID,
				PartitionId:  segment.PartitionID,
				SegmentId:    segmentID,
			})
		}
	}

	log.Info("datacoord get channel recovery info",
		zap.String("channel", channelInfo.GetChannelName()),
		zap.Int("# of unflushed segments", len(channelInfo.GetUnflushedSegmentIds())),
		zap.Int("# of flushed segments", len(channelInfo.GetFlushedSegmentIds())),
		zap.Int("# of dropped segments", len(channelInfo.GetDroppedSegmentIds())),
		zap.Int("# of indexed segments", len(channelInfo.GetIndexedSegmentIds())),
		zap.Int("# of l0 segments", len(channelInfo.GetLevelZeroSegmentIds())),
		zap.Int("# of segments not created by streaming", len(segmentsNotCreatedByStreaming)),
	)

	resp.Info = channelInfo
	resp.Schema = nil // schema is managed by streaming node itself now.
	resp.SegmentsNotCreatedByStreaming = segmentsNotCreatedByStreaming
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
		segmentIDs = s.meta.GetSegmentsIDOfCollectionWithDropped(ctx, collectionID)
	} else {
		segmentIDs = s.meta.GetSegmentsIDOfPartitionWithDropped(ctx, collectionID, partitionID)
	}
	ret := make([]UniqueID, 0, len(segmentIDs))
	for _, id := range segmentIDs {
		segment := s.meta.GetSegment(ctx, id)
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
	channels, err := s.getChannelsByCollectionID(ctx, collectionID)
	if err != nil {
		return &datapb.GetSegmentsByStatesResponse{
			Status: merr.Status(err),
		}, nil
	}
	for _, channel := range channels {
		channelSegmentsView := s.handler.GetCurrentSegmentsView(ctx, channel, partitionID)
		if channelSegmentsView == nil {
			continue
		}
		segmentIDs = append(segmentIDs, channelSegmentsView.FlushedSegmentIDs...)
		segmentIDs = append(segmentIDs, channelSegmentsView.GrowingSegmentIDs...)
		segmentIDs = append(segmentIDs, channelSegmentsView.L0SegmentIDs...)
		segmentIDs = append(segmentIDs, channelSegmentsView.ImportingSegmentIDs...)
	}
	ret := make([]UniqueID, 0, len(segmentIDs))

	statesDict := make(map[commonpb.SegmentState]bool)
	for _, state := range states {
		statesDict[state] = true
	}
	for _, id := range segmentIDs {
		segment := s.meta.GetHealthySegment(ctx, id)
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
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		msg := "failed to get metrics"
		log.Warn(msg, zap.Error(err))
		return &milvuspb.GetMetricsResponse{
			Status: merr.Status(errors.Wrap(err, msg)),
		}, nil
	}

	resp := &milvuspb.GetMetricsResponse{
		Status: merr.Success(),
		ComponentName: metricsinfo.ConstructComponentName(typeutil.DataCoordRole,
			paramtable.GetNodeID()),
	}

	ret, err := s.metricsRequest.ExecuteMetricsRequest(ctx, req)
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp.Response = ret
	return resp, nil
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

	var id int64
	var err error
	if req.GetMajorCompaction() || req.GetL0Compaction() {
		id, err = s.compactionTriggerManager.ManualTrigger(ctx, req.CollectionID, req.GetMajorCompaction(), req.GetL0Compaction())
	} else {
		id, err = s.compactionTrigger.TriggerCompaction(ctx, NewCompactionSignal().
			WithIsForce(true).
			WithCollectionID(req.GetCollectionID()).
			WithPartitionID(req.GetPartitionId()).
			WithChannel(req.GetChannel()).
			WithSegmentIDs(req.GetSegmentIds()...),
		)
	}
	if err != nil {
		log.Error("failed to trigger manual compaction", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	taskCnt := s.compactionInspector.getCompactionTasksNumBySignalID(id)
	if taskCnt == 0 {
		resp.CompactionID = -1
		resp.CompactionPlanCount = 0
	} else {
		resp.CompactionID = id
		resp.CompactionPlanCount = int32(taskCnt)
	}

	log.Info("success to trigger manual compaction", zap.Bool("isL0Compaction", req.GetL0Compaction()),
		zap.Bool("isMajorCompaction", req.GetMajorCompaction()), zap.Int64("compactionID", id), zap.Int("taskNum", taskCnt))
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

	info := s.compactionInspector.getCompactionInfo(ctx, req.GetCompactionID())

	resp.State = info.state
	resp.ExecutingPlanNo = int64(info.executingCnt)
	resp.CompletedPlanNo = int64(info.completedCnt)
	resp.TimeoutPlanNo = int64(info.timeoutCnt)
	resp.FailedPlanNo = int64(info.failedCnt)
	log.Info("success to get compaction state", zap.Any("state", info.state), zap.Int("executing", info.executingCnt),
		zap.Int("completed", info.completedCnt), zap.Int("failed", info.failedCnt), zap.Int("timeout", info.timeoutCnt))

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

	info := s.compactionInspector.getCompactionInfo(ctx, req.GetCompactionID())
	resp.State = info.state
	resp.MergeInfos = lo.MapToSlice[int64, *milvuspb.CompactionMergeInfo](info.mergeInfos, func(_ int64, merge *milvuspb.CompactionMergeInfo) *milvuspb.CompactionMergeInfo {
		return merge
	})

	planIDs := lo.MapToSlice[int64, *milvuspb.CompactionMergeInfo](info.mergeInfos, func(planID int64, _ *milvuspb.CompactionMergeInfo) int64 { return planID })
	log.Info("success to get state with plans", zap.Any("state", info.state), zap.Any("merge infos", resp.MergeInfos),
		zap.Int64s("plans", planIDs))
	return resp, nil
}

// WatchChannels notifies DataCoord to watch vchannels of a collection.
// Deprecated: Redundant design by now, remove it in future.
func (s *Server) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Strings("channels", req.GetChannelNames()),
		zap.Any("dbProperties", req.GetDbProperties()),
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
		// TODO: redundant channel mark by now, remove it in future.
		if err := s.meta.catalog.MarkChannelAdded(ctx, channelName); err != nil {
			// TODO: add background task to periodically cleanup the orphaned channel add marks.
			log.Error("failed to mark channel added", zap.Error(err))
			resp.Status = merr.Status(err)
			return resp, nil
		}

		// try to init channel checkpoint, if failed, we will log it and continue
		startPos := toMsgPosition(channelName, req.GetStartPositions())
		if startPos != nil {
			startPos.Timestamp = req.GetCreateTimestamp()
			if err := s.meta.UpdateChannelCheckpoint(ctx, channelName, startPos); err != nil {
				log.Warn("failed to init channel checkpoint, meta update error", zap.String("channel", channelName), zap.Error(err))
			}
		} else {
			log.Info("skip to init channel checkpoint for nil startPosition", zap.String("channel", channelName))
		}
	}
	return resp, nil
}

// GetFlushState gets the flush state of the collection based on the provided flush ts and segment IDs.
func (s *Server) GetFlushState(ctx context.Context, req *datapb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	log := log.Ctx(ctx).With(zap.Int64("collection", req.GetCollectionID()),
		zap.Uint64("flushTs", req.GetFlushTs()),
		zap.Time("flushTs in time", tsoutil.PhysicalTime(req.GetFlushTs()))).
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
			segment := s.meta.GetHealthySegment(ctx, sid)
			// segment is nil if it was compacted, or it's an empty segment and is set to dropped
			// TODO: Here's a dirty implementation, because a growing segment may cannot be seen right away by mixcoord,
			// it can only be seen by streamingnode right away, so we need to check the flush state at streamingnode but not here.
			// use timetick for GetFlushState in-future but not segment list.
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

	channels, err := s.getChannelsByCollectionID(ctx, req.GetCollectionID())
	if err != nil {
		return &milvuspb.GetFlushStateResponse{
			Status: merr.Status(err),
		}, nil
	}
	if len(channels) == 0 { // For compatibility with old client
		resp.Flushed = true

		log.Info("GetFlushState all flushed without checking flush ts")
		return resp, nil
	}

	for _, channel := range channels {
		cp := s.meta.GetChannelCheckpoint(channel.GetName())
		if cp == nil || cp.GetTimestamp() < req.GetFlushTs() {
			resp.Flushed = false

			log.RatedInfo(10, "GetFlushState failed, channel unflushed", zap.String("channel", channel.GetName()),
				zap.Time("CP", tsoutil.PhysicalTime(cp.GetTimestamp())),
				zap.Duration("lag", tsoutil.PhysicalTime(req.GetFlushTs()).Sub(tsoutil.PhysicalTime(cp.GetTimestamp()))))
			return resp, nil
		}
	}

	resp.Flushed = true
	log.Info("GetFlushState all flushed")

	return resp, nil
}

// getChannelsByCollectionID gets the channels of the collection.
func (s *Server) getChannelsByCollectionID(ctx context.Context, collectionID int64) ([]RWChannel, error) {
	describeRsp, err := s.mixCoord.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_DescribeCollection,
		},
		CollectionID: collectionID,
	})
	if err != nil {
		return nil, err
	}
	channels := make([]RWChannel, 0, len(describeRsp.GetVirtualChannelNames()))
	for _, channel := range describeRsp.GetVirtualChannelNames() {
		startPos := toMsgPosition(channel, describeRsp.GetStartPositions())
		channels = append(channels, &channelMeta{
			Name:          channel,
			CollectionID:  collectionID,
			StartPosition: startPos,
		})
	}
	return channels, nil
}

// GetFlushAllState checks if all DML messages before `FlushAllTs` have been flushed.
func (s *Server) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error) {
	log := log.Ctx(ctx)
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &milvuspb.GetFlushAllStateResponse{
			Status: merr.Status(err),
		}, nil
	}

	resp := &milvuspb.GetFlushAllStateResponse{
		Status:      merr.Success(),
		FlushStates: make([]*milvuspb.FlushAllState, 0),
	}

	dbsRsp, err := s.broker.ListDatabases(ctx)
	if err != nil {
		log.Warn("failed to ListDatabases", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	// Determine which databases to check
	var targetDbs []string
	if len(req.GetFlushTargets()) > 0 {
		// Use flush_targets from request
		for _, target := range req.GetFlushTargets() {
			if target.GetDbName() != "" {
				if !lo.Contains(dbsRsp.DbNames, target.GetDbName()) {
					resp.Status = merr.Status(merr.WrapErrDatabaseNotFound(target.GetDbName()))
					return resp, nil
				}
				targetDbs = append(targetDbs, target.GetDbName())
			}
		}
	} else if req.GetDbName() != "" {
		if !lo.Contains(dbsRsp.DbNames, req.GetDbName()) {
			resp.Status = merr.Status(merr.WrapErrDatabaseNotFound(req.GetDbName()))
			return resp, nil
		}
		// Backward compatibility: use deprecated db_name field
		targetDbs = []string{req.GetDbName()}
	} else {
		// Check all databases
		targetDbs = dbsRsp.DbNames
	}

	// Remove duplicates
	targetDbs = lo.Uniq(targetDbs)
	allFlushed := true

	for _, dbName := range targetDbs {
		flushState := &milvuspb.FlushAllState{
			DbName:                dbName,
			CollectionFlushStates: make(map[string]bool),
		}

		// Get collections to check for this database
		var targetCollections []string
		if len(req.GetFlushTargets()) > 0 {
			// Check if specific collections are requested for this db
			for _, target := range req.GetFlushTargets() {
				if target.GetDbName() == dbName && len(target.GetCollectionNames()) > 0 {
					targetCollections = target.GetCollectionNames()
					break
				}
			}
		}

		showColRsp, err := s.broker.ShowCollections(ctx, dbName)
		if err != nil {
			log.Warn("failed to ShowCollections", zap.String("db", dbName), zap.Error(err))
			resp.Status = merr.Status(err)
			return resp, nil
		}

		for idx, collectionID := range showColRsp.GetCollectionIds() {
			collectionName := ""
			if idx < len(showColRsp.GetCollectionNames()) {
				collectionName = showColRsp.GetCollectionNames()[idx]
			}

			// If specific collections are requested, skip others
			if len(targetCollections) > 0 && !lo.Contains(targetCollections, collectionName) {
				continue
			}

			describeColRsp, err := s.broker.DescribeCollectionInternal(ctx, collectionID)
			if err != nil {
				log.Warn("failed to DescribeCollectionInternal",
					zap.Int64("collectionID", collectionID), zap.Error(err))
				resp.Status = merr.Status(err)
				return resp, nil
			}

			collectionFlushed := true
			for _, channel := range describeColRsp.GetVirtualChannelNames() {
				channelCP := s.meta.GetChannelCheckpoint(channel)
				if channelCP == nil || channelCP.GetTimestamp() < req.GetFlushAllTs() {
					collectionFlushed = false
					allFlushed = false
					break
				}
			}
			flushState.CollectionFlushStates[collectionName] = collectionFlushed
		}

		resp.FlushStates = append(resp.FlushStates, flushState)
	}

	resp.Flushed = allFlushed
	return resp, nil
}

// Deprecated
// UpdateSegmentStatistics updates a segment's stats.
func (s *Server) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	return merr.Success(), nil
}

// UpdateChannelCheckpoint updates channel checkpoint in dataCoord.
func (s *Server) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx)
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	nodeID := req.GetBase().GetSourceID()
	// For compatibility with old client
	if req.GetVChannel() != "" && req.GetPosition() != nil {
		channel := req.GetVChannel()
		targetID, err := snmanager.StaticStreamingNodeManager.GetLatestWALLocated(ctx, channel)
		if err != nil || targetID != nodeID {
			err := merr.WrapErrChannelNotFound(channel, fmt.Sprintf("for node %d", nodeID))
			log.Warn("failed to get latest wal allocated", zap.Error(err))
			return merr.Status(err), nil
		}
		if err := s.meta.UpdateChannelCheckpoint(ctx, req.GetVChannel(), req.GetPosition()); err != nil {
			log.Warn("failed to UpdateChannelCheckpoint", zap.String("vChannel", req.GetVChannel()), zap.Error(err))
			return merr.Status(err), nil
		}
		return merr.Success(), nil
	}

	checkpoints := lo.Filter(req.GetChannelCheckpoints(), func(cp *msgpb.MsgPosition, _ int) bool {
		channel := cp.GetChannelName()
		targetID, err := snmanager.StaticStreamingNodeManager.GetLatestWALLocated(ctx, channel)
		if err != nil || targetID != nodeID {
			err := merr.WrapErrChannelNotFound(channel, fmt.Sprintf("for node %d", nodeID))
			log.Warn("failed to get latest wal allocated", zap.Error(err))
			return false
		}
		return true
	})

	err := s.meta.UpdateChannelCheckpoints(ctx, checkpoints)
	if err != nil {
		log.Warn("failed to update channel checkpoint", zap.Error(err))
		return merr.Status(err), nil
	}

	for _, pos := range checkpoints {
		if pos == nil || pos.GetMsgID() == nil || pos.GetChannelName() == "" {
			continue
		}
		s.segmentManager.CleanZeroSealedSegmentsOfChannel(ctx, pos.GetChannelName(), pos.GetTimestamp())
	}

	return merr.Success(), nil
}

// ReportDataNodeTtMsgs gets timetick messages from datanode.
func (s *Server) ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest) (*commonpb.Status, error) {
	return merr.Success(), nil
}

// MarkSegmentsDropped marks the given segments as `Dropped`.
// An error status will be returned and error will be logged, if we failed to mark *all* segments.
// Deprecated, do not use it
func (s *Server) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest) (*commonpb.Status, error) {
	log.Ctx(ctx).Info("marking segments dropped", zap.Int64s("segments", req.GetSegmentIds()))
	var err error
	for _, segID := range req.GetSegmentIds() {
		if err = s.meta.SetState(ctx, segID, commonpb.SegmentState_Dropped); err != nil {
			// Fail-open.
			log.Ctx(ctx).Error("failed to set segment state as dropped", zap.Int64("segmentID", segID))
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
			DatabaseID:     req.GetDbID(),
			DatabaseName:   req.GetSchema().GetDbName(),
			VChannelNames:  req.GetVChannels(),
		}
		s.meta.AddCollection(collInfo)
		return merr.Success(), nil
	}

	clonedColl.Properties = properties
	// add field will change the schema
	clonedColl.Schema = req.GetSchema()
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

	if err := CheckCheckPointsHealth(s.meta); err != nil {
		return componentutil.CheckHealthRespWithErr(err), nil
	}

	return componentutil.CheckHealthRespWithErr(nil), nil
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

func (s *Server) GcControl(ctx context.Context, request *datapb.GcControlRequest) (*commonpb.Status, error) {
	status := &commonpb.Status{}
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	switch request.GetCommand() {
	case datapb.GcCommand_Pause:
		kv := lo.FindOrElse(request.GetParams(), nil, func(kv *commonpb.KeyValuePair) bool {
			return kv.GetKey() == "duration"
		})
		if kv == nil {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = "pause duration param not found"
			return status, nil
		}
		pauseSeconds, err := strconv.ParseInt(kv.GetValue(), 10, 64)
		if err != nil {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = fmt.Sprintf("pause duration not valid, %s", err.Error())
			return status, nil
		}
		if err := s.garbageCollector.Pause(ctx, time.Duration(pauseSeconds)*time.Second); err != nil {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = fmt.Sprintf("failed to pause gc, %s", err.Error())
			return status, nil
		}
	case datapb.GcCommand_Resume:
		if err := s.garbageCollector.Resume(ctx); err != nil {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = fmt.Sprintf("failed to pause gc, %s", err.Error())
			return status, nil
		}
	default:
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = fmt.Sprintf("unknown gc command: %d", request.GetCommand())
		return status, nil
	}

	return status, nil
}

func (s *Server) GetGcStatus(ctx context.Context) (*datapb.GetGcStatusResponse, error) {
	status := s.garbageCollector.GetStatus()
	var remainingSeconds int32
	if status.IsPaused {
		// Convert time.Duration to seconds, rounding to the nearest second.
		// Using Round() ensures accuracy when converting to an integer.
		remainingSeconds = int32(status.TimeRemaining.Round(time.Second).Seconds())
	}

	return &datapb.GetGcStatusResponse{
		IsPaused:             status.IsPaused,
		TimeRemainingSeconds: remainingSeconds,
	}, nil
}

func (s *Server) ImportV2(ctx context.Context, in *internalpb.ImportRequestInternal) (*internalpb.ImportResponse, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &internalpb.ImportResponse{
			Status: merr.Status(err),
		}, nil
	}

	resp := &internalpb.ImportResponse{
		Status: merr.Success(),
	}

	log := log.Ctx(ctx).With(zap.Int64("collection", in.GetCollectionID()),
		zap.Int64s("partitions", in.GetPartitionIDs()),
		zap.Strings("channels", in.GetChannelNames()))
	log.Info("receive import request", zap.Int("fileNum", len(in.GetFiles())),
		zap.Any("files", in.GetFiles()), zap.Any("options", in.GetOptions()))

	timeoutTs, err := importutilv2.GetTimeoutTs(in.GetOptions())
	if err != nil {
		resp.Status = merr.Status(merr.WrapErrImportFailed(err.Error()))
		return resp, nil
	}

	files := in.GetFiles()
	isBackup := importutilv2.IsBackup(in.GetOptions())
	if isBackup {
		files, err = ListBinlogImportRequestFiles(ctx, s.meta.chunkManager, files, in.GetOptions())
		if err != nil {
			resp.Status = merr.Status(err)
			return resp, nil
		}
	}

	// Allocate file ids.
	idStart, _, err := s.allocator.AllocN(int64(len(files)) + 1)
	if err != nil {
		resp.Status = merr.Status(merr.WrapErrImportFailed(fmt.Sprint("alloc id failed, err=%w", err)))
		return resp, nil
	}
	files = lo.Map(files, func(importFile *internalpb.ImportFile, i int) *internalpb.ImportFile {
		importFile.Id = idStart + int64(i) + 1
		return importFile
	})
	importCollectionInfo, err := s.handler.GetCollection(ctx, in.GetCollectionID())
	if errors.Is(err, merr.ErrCollectionNotFound) {
		resp.Status = merr.Status(merr.WrapErrCollectionNotFound(in.GetCollectionID()))
		return resp, nil
	}
	if err != nil {
		resp.Status = merr.Status(merr.WrapErrImportFailed(fmt.Sprint("get collection failed, err=%w", err)))
		return resp, nil
	}
	if importCollectionInfo == nil {
		resp.Status = merr.Status(merr.WrapErrCollectionNotFound(in.GetCollectionID()))
		return resp, nil
	}

	jobID := in.GetJobID()
	if jobID == 0 {
		jobID = idStart
	}
	createTime := time.Now()
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:          jobID,
			CollectionID:   in.GetCollectionID(),
			CollectionName: in.GetCollectionName(),
			PartitionIDs:   in.GetPartitionIDs(),
			Vchannels:      importCollectionInfo.VChannelNames,
			Schema:         in.GetSchema(),
			TimeoutTs:      timeoutTs,
			CleanupTs:      math.MaxUint64,
			State:          internalpb.ImportJobState_Pending,
			Files:          files,
			Options:        in.GetOptions(),
			CreateTime:     createTime.Format("2006-01-02T15:04:05Z07:00"),
			ReadyVchannels: in.GetChannelNames(),
			DataTs:         in.GetDataTimestamp(),
		},
		tr: timerecord.NewTimeRecorder("import job"),
	}
	err = s.importMeta.AddJob(ctx, job)
	if err != nil {
		resp.Status = merr.Status(merr.WrapErrImportFailed(fmt.Sprint("add import job failed, err=%w", err)))
		return resp, nil
	}

	resp.JobID = fmt.Sprint(job.GetJobID())
	log.Info("add import job done",
		zap.Int64("jobID", job.GetJobID()),
		zap.Int("fileNum", len(files)),
		zap.Any("files", files),
		zap.Strings("readyChannels", in.GetChannelNames()),
	)
	return resp, nil
}

func (s *Server) GetImportProgress(ctx context.Context, in *internalpb.GetImportProgressRequest) (*internalpb.GetImportProgressResponse, error) {
	log := log.Ctx(ctx).With(zap.String("jobID", in.GetJobID()))
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &internalpb.GetImportProgressResponse{
			Status: merr.Status(err),
		}, nil
	}

	resp := &internalpb.GetImportProgressResponse{
		Status: merr.Success(),
	}
	jobID, err := strconv.ParseInt(in.GetJobID(), 10, 64)
	if err != nil {
		resp.Status = merr.Status(merr.WrapErrImportFailed(fmt.Sprint("parse job id failed, err=%w", err)))
		return resp, nil
	}

	job := s.importMeta.GetJob(ctx, jobID)
	if job == nil {
		resp.Status = merr.Status(merr.WrapErrImportFailed(fmt.Sprintf("import job does not exist, jobID=%d", jobID)))
		return resp, nil
	}
	progress, state, importedRows, totalRows, reason := GetJobProgress(ctx, jobID, s.importMeta, s.meta)
	resp.State = state
	resp.Reason = reason
	resp.Progress = progress
	resp.CollectionName = job.GetCollectionName()
	resp.CreateTime = job.GetCreateTime()
	resp.CompleteTime = job.GetCompleteTime()
	resp.ImportedRows = importedRows
	resp.TotalRows = totalRows
	resp.TaskProgresses = GetTaskProgresses(ctx, jobID, s.importMeta, s.meta)
	log.Info("GetImportProgress done", zap.String("jobState", job.GetState().String()), zap.Any("resp", resp))
	return resp, nil
}

func (s *Server) ListImports(ctx context.Context, req *internalpb.ListImportsRequestInternal) (*internalpb.ListImportsResponse, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &internalpb.ListImportsResponse{
			Status: merr.Status(err),
		}, nil
	}

	resp := &internalpb.ListImportsResponse{
		Status:     merr.Success(),
		JobIDs:     make([]string, 0),
		States:     make([]internalpb.ImportJobState, 0),
		Reasons:    make([]string, 0),
		Progresses: make([]int64, 0),
	}

	var jobs []ImportJob
	if req.GetCollectionID() != 0 {
		jobs = s.importMeta.GetJobBy(ctx, WithCollectionID(req.GetCollectionID()))
	} else {
		jobs = s.importMeta.GetJobBy(ctx)
	}

	for _, job := range jobs {
		progress, state, _, _, reason := GetJobProgress(ctx, job.GetJobID(), s.importMeta, s.meta)
		resp.JobIDs = append(resp.JobIDs, fmt.Sprintf("%d", job.GetJobID()))
		resp.States = append(resp.States, state)
		resp.Reasons = append(resp.Reasons, reason)
		resp.Progresses = append(resp.Progresses, progress)
		resp.CollectionNames = append(resp.CollectionNames, job.GetCollectionName())
	}
	return resp, nil
}

// NotifyDropPartition notifies DataCoord to drop segments of specified partition
func (s *Server) NotifyDropPartition(ctx context.Context, channel string, partitionIDs []int64) error {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return err
	}
	log.Ctx(ctx).Info("receive NotifyDropPartition request",
		zap.String("channelname", channel),
		zap.Any("partitionID", partitionIDs))
	s.segmentManager.DropSegmentsOfPartition(ctx, channel, partitionIDs)
	// release all segments of the partition.
	return s.meta.DropSegmentsOfPartition(ctx, partitionIDs)
}

// AddFileResource add file resource to datacoord
func (s *Server) AddFileResource(ctx context.Context, req *milvuspb.AddFileResourceRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	log.Ctx(ctx).Info("receive AddFileResource request",
		zap.String("name", req.GetName()),
		zap.String("path", req.GetPath()))

	id, err := s.idAllocator.AllocOne()
	if err != nil {
		log.Ctx(ctx).Warn("AddFileResource alloc id failed", zap.Error(err))
		return merr.Status(err), nil
	}

	// Convert to internalpb.FileResourceInfo
	resource := &internalpb.FileResourceInfo{
		Id:   id,
		Name: req.GetName(),
		Path: req.GetPath(),
	}

	err = s.meta.AddFileResource(ctx, resource)
	if err != nil {
		log.Ctx(ctx).Warn("AddFileResource fail", zap.Error(err))
		return merr.Status(err), nil
	}
	s.fileManager.Notify()

	resources, version := s.meta.ListFileResource(ctx)
	s.mixCoord.SyncQcFileResource(ctx, resources, version)

	log.Ctx(ctx).Info("AddFileResource success")
	return merr.Success(), nil
}

// RemoveFileResource remove file resource from datacoord
func (s *Server) RemoveFileResource(ctx context.Context, req *milvuspb.RemoveFileResourceRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}

	log.Ctx(ctx).Info("receive RemoveFileResource request",
		zap.String("name", req.GetName()))

	err := s.meta.RemoveFileResource(ctx, req.GetName())
	if err != nil {
		log.Ctx(ctx).Warn("RemoveFileResource fail", zap.Error(err))
		return merr.Status(err), nil
	}
	s.fileManager.Notify()

	resources, version := s.meta.ListFileResource(ctx)
	s.mixCoord.SyncQcFileResource(ctx, resources, version)

	log.Ctx(ctx).Info("RemoveFileResource success")
	return merr.Success(), nil
}

// ListFileResources list file resources from datacoord
func (s *Server) ListFileResources(ctx context.Context, req *milvuspb.ListFileResourcesRequest) (*milvuspb.ListFileResourcesResponse, error) {
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &milvuspb.ListFileResourcesResponse{
			Status: merr.Status(err),
		}, nil
	}

	log.Ctx(ctx).Info("receive ListFileResources request")

	resources, _ := s.meta.ListFileResource(ctx)

	// Convert internal.FileResourceInfo to milvuspb.FileResourceInfo
	fileResources := make([]*milvuspb.FileResourceInfo, 0, len(resources))
	for _, resource := range resources {
		fileResources = append(fileResources, &milvuspb.FileResourceInfo{
			Name: resource.Name,
			Path: resource.Path,
		})
	}

	log.Ctx(ctx).Info("ListFileResources success", zap.Int("count", len(fileResources)))
	return &milvuspb.ListFileResourcesResponse{
		Status:    merr.Success(),
		Resources: fileResources,
	}, nil
}

// CreateExternalCollection creates an external collection in datacoord
// This is a skeleton implementation - details to be filled in later
func (s *Server) CreateExternalCollection(ctx context.Context, req *msgpb.CreateCollectionRequest) (*datapb.CreateExternalCollectionResponse, error) {
	log := log.Ctx(ctx).With(
		zap.String("dbName", req.GetDbName()),
		zap.String("collectionName", req.GetCollectionName()),
		zap.Int64("dbID", req.GetDbID()),
		zap.Int64("collectionID", req.GetCollectionID()))

	log.Info("receive CreateExternalCollection request")

	// Check if server is healthy
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		log.Warn("server is not healthy", zap.Error(err))
		return &datapb.CreateExternalCollectionResponse{
			Status: merr.Status(err),
		}, nil
	}

	// Create collection info and add to meta
	// This will make the collection visible to inspectors
	// The collection schema already contains external_source and external_spec fields
	collInfo := &collectionInfo{
		ID:            req.GetCollectionID(),
		Schema:        req.GetCollectionSchema(),
		Partitions:    req.GetPartitionIDs(),
		Properties:    make(map[string]string),
		DatabaseID:    req.GetDbID(),
		DatabaseName:  req.GetDbName(),
		VChannelNames: req.GetVirtualChannelNames(),
	}

	s.meta.AddCollection(collInfo)

	log.Info("CreateExternalCollection: collection added to meta",
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("collectionName", req.GetCollectionName()),
		zap.String("externalSource", req.GetCollectionSchema().GetExternalSource()),
		zap.String("externalSpec", req.GetCollectionSchema().GetExternalSpec()))

	return &datapb.CreateExternalCollectionResponse{
		Status: merr.Success(),
	}, nil
}

// first sync file resource data to qc when all coord init finished
func (s *Server) SyncFileResources(ctx context.Context) error {
	resources, version := s.meta.ListFileResource(ctx)
	return s.mixCoord.SyncQcFileResource(ctx, resources, version)
}
