package datacoord

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"go.uber.org/zap"
)

const serverNotServingErrMsg = "server is not serving"

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
		Value: Params.TimeTickChannelName,
	}, nil
}

// GetStatisticsChannel legacy API, returns statistics channel name
func (s *Server) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: Params.StatisticsChannelName,
	}, nil
}

// Flush notify segment to flush
// this api only guarantees all the segments requested is sealed
// these segments will be flushed only after the Flush policy is fulfilled
func (s *Server) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	log.Debug("receive flush request", zap.Int64("dbID", req.GetDbID()), zap.Int64("collectionID", req.GetCollectionID()))
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
	sealedSegments, err := s.segmentManager.SealAllSegments(ctx, req.CollectionID)
	if err != nil {
		resp.Status.Reason = fmt.Sprintf("failed to flush %d, %s", req.CollectionID, err)
		return resp, nil
	}
	log.Debug("flush response with segments", zap.Any("segments", sealedSegments))
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.DbID = req.GetDbID()
	resp.CollectionID = req.GetCollectionID()
	resp.SegmentIDs = sealedSegments
	return resp, nil
}

// AssignSegmentID applies for segment ids and make allocation for records
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
		log.Debug("handle assign segment request",
			zap.Int64("collectionID", r.GetCollectionID()),
			zap.Int64("partitionID", r.GetPartitionID()),
			zap.String("channelName", r.GetChannelName()),
			zap.Uint32("count", r.GetCount()))

		if coll := s.meta.GetCollection(r.CollectionID); coll == nil {
			if err := s.loadCollectionFromRootCoord(ctx, r.CollectionID); err != nil {
				log.Error("load collection from rootcoord error",
					zap.Int64("collectionID", r.CollectionID),
					zap.Error(err))
				continue
			}
		}

		s.cluster.Watch(r.ChannelName, r.CollectionID)

		allocations, err := s.segmentManager.AllocSegment(ctx,
			r.CollectionID, r.PartitionID, r.ChannelName, int64(r.Count))
		if err != nil {
			log.Warn("failed to alloc segment", zap.Any("request", r), zap.Error(err))
			continue
		}

		log.Debug("Assign segment success", zap.Any("assignments", allocations))

		for _, allocation := range allocations {
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
			Status:    &commonpb.Status{},
			SegmentID: segmentID,
		}
		segmentInfo := s.meta.GetSegment(segmentID)
		if segmentInfo == nil {
			state.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			state.Status.Reason = fmt.Sprintf("failed to get segment %d", segmentID)
		} else {
			state.Status.ErrorCode = commonpb.ErrorCode_Success
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
		paths = append(paths, &internalpb.StringList{Values: field.GetBinlogs()})
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.FieldIDs = fids
	resp.Paths = paths
	return resp, nil
}

// GetCollectionStatistics returns statistics for collection
// for now only row count is returned
func (s *Server) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
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
	return resp, nil
}

// GetPartitionStatistics return statistics for parition
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
		Value: Params.SegmentInfoChannelName,
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
		info := s.meta.GetSegment(id)
		if info == nil {
			resp.Status.Reason = fmt.Sprintf("failed to get segment %d", id)
			return resp, nil
		}
		infos = append(infos, info.SegmentInfo)
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.Infos = infos
	return resp, nil
}

// SaveBinlogPaths update segment related binlog path
// works for Checkpoints and Flush
func (s *Server) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	resp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}
	if s.isClosed() {
		resp.Reason = serverNotServingErrMsg
		return resp, nil
	}
	log.Debug("receive SaveBinlogPaths request",
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64("segmentID", req.GetSegmentID()),
		zap.Any("checkpoints", req.GetCheckPoints()))

	// set segment to SegmentState_Flushing and save binlogs and checkpoints
	err := s.meta.UpdateFlushSegmentsInfo(req.GetSegmentID(), req.GetFlushed(),
		req.GetField2BinlogPaths(), req.GetCheckPoints(), req.GetStartPositions())
	if err != nil {
		log.Error("save binlog and checkpoints failed",
			zap.Int64("segmentID", req.GetSegmentID()),
			zap.Error(err))
		resp.Reason = err.Error()
		return resp, nil
	}
	log.Debug("flush segment with meta", zap.Int64("id", req.SegmentID),
		zap.Any("meta", req.GetField2BinlogPaths()))

	if req.Flushed {
		s.segmentManager.DropSegment(ctx, req.SegmentID)
		s.flushCh <- req.SegmentID
	}
	resp.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// todo deprecated rpc
func (s *Server) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	resp := &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			NodeID:    Params.NodeID,
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
	segmentIDs := s.meta.GetSegmentsOfPartition(collectionID, partitionID)
	segment2Binlogs := make(map[UniqueID][]*datapb.FieldBinlog)
	segmentsNumOfRows := make(map[UniqueID]int64)
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

		binlogs := segment.GetBinlogs()
		field2Binlog := make(map[UniqueID][]string)
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
	}

	binlogs := make([]*datapb.SegmentBinlogs, 0, len(segment2Binlogs))
	for segmentID, fieldBinlogs := range segment2Binlogs {
		sbl := &datapb.SegmentBinlogs{
			SegmentID:    segmentID,
			NumOfRows:    segmentsNumOfRows[segmentID],
			FieldBinlogs: fieldBinlogs,
		}
		binlogs = append(binlogs, sbl)
	}

	dresp, err := s.rootCoordClient.DescribeCollection(s.ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_DescribeCollection,
			SourceID: Params.NodeID,
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
	vchans := make([]vchannel, 0, len(channels))
	for _, c := range channels {
		vchans = append(vchans, vchannel{
			CollectionID: collectionID,
			DmlChannel:   c,
		})
	}

	channelInfos, err := s.GetVChanPositions(vchans, false)
	if err != nil {
		log.Error("get channel positions failed",
			zap.Strings("channels", channels),
			zap.Error(err))
		resp.Status.Reason = err.Error()
		return resp, nil
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
	log.Debug("GetFlushedSegment",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID))
	if s.isClosed() {
		resp.Status.Reason = serverNotServingErrMsg
		return resp, nil
	}
	var segmentIDs []UniqueID
	if partitionID < 0 {
		segmentIDs = s.meta.GetSegmentsOfCollection(collectionID)
	} else {
		segmentIDs = s.meta.GetSegmentsOfPartition(collectionID, partitionID)
	}
	ret := make([]UniqueID, 0, len(segmentIDs))
	for _, id := range segmentIDs {
		s := s.meta.GetSegment(id)
		if s == nil || s.GetState() != commonpb.SegmentState_Flushed {
			continue
		}
		ret = append(ret, id)
	}
	resp.Segments = ret
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	return resp, nil
}

// GetMetrics returns DataCoord metrics info
// it may include SystemMetrics, Topology metrics, etc.
func (s *Server) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	log.Debug("DataCoord.GetMetrics",
		zap.Int64("node_id", Params.NodeID),
		zap.String("req", req.Request))

	if s.isClosed() {
		log.Warn("DataCoord.GetMetrics failed",
			zap.Int64("node_id", Params.NodeID),
			zap.String("req", req.Request),
			zap.Error(errDataCoordIsUnhealthy(Params.NodeID)))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    msgDataCoordIsUnhealthy(Params.NodeID),
			},
			Response: "",
		}, nil
	}

	metricType, err := metricsinfo.ParseMetricType(req.Request)
	if err != nil {
		log.Warn("DataCoord.GetMetrics failed to parse metric type",
			zap.Int64("node_id", Params.NodeID),
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
			zap.Int64("node_id", Params.NodeID),
			zap.String("req", req.Request),
			zap.String("metric_type", metricType),
			zap.Any("metrics", metrics), // TODO(dragondriver): necessary? may be very large
			zap.Error(err))

		s.metricsCacheManager.UpdateSystemInfoMetrics(metrics)

		return metrics, err
	}

	log.Debug("DataCoord.GetMetrics failed, request metric type is not implemented yet",
		zap.Int64("node_id", Params.NodeID),
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
