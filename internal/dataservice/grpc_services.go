package dataservice

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"go.uber.org/zap"
)

func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.isServing) != 2
}

func (s *Server) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: Params.TimeTickChannelName,
	}, nil
}

func (s *Server) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: Params.StatisticsChannelName,
	}, nil
}

func (s *Server) Flush(ctx context.Context, req *datapb.FlushRequest) (*commonpb.Status, error) {
	log.Debug("Receive flush request", zap.Int64("dbID", req.GetDbID()), zap.Int64("collectionID", req.GetCollectionID()))
	resp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}
	if s.isClosed() {
		resp.Reason = "server is closed"
		return resp, nil
	}
	if err := s.segmentManager.SealAllSegments(ctx, req.CollectionID); err != nil {
		resp.Reason = fmt.Sprintf("Seal all segments error %s", err)
		return resp, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (s *Server) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	if s.isClosed() {
		return &datapb.AssignSegmentIDResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, nil
	}

	assigns := make([]*datapb.SegmentIDAssignment, 0, len(req.SegmentIDRequests))

	var appendFailedAssignment = func(err string) {
		assigns = append(assigns, &datapb.SegmentIDAssignment{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err,
			},
		})
	}

	for _, r := range req.SegmentIDRequests {
		log.Debug("Handle assign segment request",
			zap.Int64("collectionID", r.GetCollectionID()),
			zap.Int64("partitionID", r.GetPartitionID()),
			zap.String("channelName", r.GetChannelName()),
			zap.Uint32("count", r.GetCount()))

		if !s.meta.HasCollection(r.CollectionID) {
			if err := s.loadCollectionFromMaster(ctx, r.CollectionID); err != nil {
				errMsg := fmt.Sprintf("can not load collection %d", r.CollectionID)
				appendFailedAssignment(errMsg)
				log.Error("load collection from master error",
					zap.Int64("collectionID", r.CollectionID),
					zap.Error(err))
				continue
			}
		}
		//if err := s.validateAllocRequest(r.CollectionID, r.PartitionID, r.ChannelName); err != nil {
		//result.Status.Reason = err.Error()
		//assigns = append(assigns, result)
		//continue
		//}

		s.cluster.watchIfNeeded(r.ChannelName, r.CollectionID)

		segmentID, retCount, expireTs, err := s.segmentManager.AllocSegment(ctx,
			r.CollectionID, r.PartitionID, r.ChannelName, int64(r.Count))
		if err != nil {
			errMsg := fmt.Sprintf("allocation of collection %d, partition %d, channel %s, count %d error:  %s",
				r.CollectionID, r.PartitionID, r.ChannelName, r.Count, err.Error())
			appendFailedAssignment(errMsg)
			continue
		}

		log.Debug("Assign segment success", zap.Int64("segmentID", segmentID),
			zap.Uint64("expireTs", expireTs))

		result := &datapb.SegmentIDAssignment{
			SegID:        segmentID,
			ChannelName:  r.ChannelName,
			Count:        uint32(retCount),
			CollectionID: r.CollectionID,
			PartitionID:  r.PartitionID,
			ExpireTime:   expireTs,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			},
		}
		assigns = append(assigns, result)
	}
	return &datapb.AssignSegmentIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		SegIDAssignments: assigns,
	}, nil
}

func (s *Server) ShowSegments(ctx context.Context, req *datapb.ShowSegmentsRequest) (*datapb.ShowSegmentsResponse, error) {
	resp := &datapb.ShowSegmentsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = "server is initializing"
		return resp, nil
	}
	ids := s.meta.GetSegmentsOfPartition(req.CollectionID, req.PartitionID)
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.SegmentIDs = ids
	return resp, nil
}

func (s *Server) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	resp := &datapb.GetSegmentStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = "server is initializing"
		return resp, nil
	}

	for _, segmentID := range req.SegmentIDs {
		state := &datapb.SegmentStateInfo{
			Status:    &commonpb.Status{},
			SegmentID: segmentID,
		}
		segmentInfo, err := s.meta.GetSegment(segmentID)
		if err != nil {
			state.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			state.Status.Reason = "get segment states error: " + err.Error()
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

func (s *Server) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	resp := &datapb.GetInsertBinlogPathsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = "server is initializing"
		return resp, nil
	}
	p := path.Join(Params.SegmentBinlogSubPath, strconv.FormatInt(req.SegmentID, 10)) + "/" // prefix/id/ instead of prefix/id
	_, values, err := s.kvClient.LoadWithPrefix(p)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	m := make(map[int64][]string)
	tMeta := &datapb.SegmentFieldBinlogMeta{}
	for _, v := range values {
		if err := proto.UnmarshalText(v, tMeta); err != nil {
			resp.Status.Reason = fmt.Errorf("DataService GetInsertBinlogPaths UnmarshalText datapb.SegmentFieldBinlogMeta err:%w", err).Error()
			return resp, nil
		}
		m[tMeta.FieldID] = append(m[tMeta.FieldID], tMeta.BinlogPath)
	}

	fids := make([]UniqueID, len(m))
	paths := make([]*internalpb.StringList, len(m))
	for k, v := range m {
		fids = append(fids, k)
		paths = append(paths, &internalpb.StringList{Values: v})
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.FieldIDs = fids
	resp.Paths = paths
	return resp, nil
}

func (s *Server) GetInsertChannels(ctx context.Context, req *datapb.GetInsertChannelsRequest) (*internalpb.StringList, error) {
	return &internalpb.StringList{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Values: []string{},
	}, nil
}

func (s *Server) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
	resp := &datapb.GetCollectionStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = "server is initializing"
		return resp, nil
	}
	nums, err := s.meta.GetNumRowsOfCollection(req.CollectionID)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.Stats = append(resp.Stats, &commonpb.KeyValuePair{Key: "row_count", Value: strconv.FormatInt(nums, 10)})
	return resp, nil
}

func (s *Server) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	resp := &datapb.GetPartitionStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = "server is initializing"
		return resp, nil
	}
	nums, err := s.meta.GetNumRowsOfPartition(req.CollectionID, req.PartitionID)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.Stats = append(resp.Stats, &commonpb.KeyValuePair{Key: "row_count", Value: strconv.FormatInt(nums, 10)})
	return resp, nil
}

func (s *Server) GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: Params.SegmentInfoChannelName,
	}, nil
}

func (s *Server) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	resp := &datapb.GetSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = "data service is not healthy"
		return resp, nil
	}
	infos := make([]*datapb.SegmentInfo, 0, len(req.SegmentIDs))
	for _, id := range req.SegmentIDs {
		info, err := s.meta.GetSegment(id)
		if err != nil {
			resp.Status.Reason = err.Error()
			return resp, nil
		}
		infos = append(infos, info)
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.Infos = infos
	return resp, nil
}

// SaveBinlogPaths implement DataServiceServer
func (s *Server) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	resp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}
	if s.isClosed() {
		resp.Reason = "server is closed"
		return resp, nil
	}
	log.Debug("Receive SaveBinlogPaths request",
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64("segmentID", req.GetSegmentID()))

	// check segment id & collection id matched
	_, err := s.meta.GetCollection(req.GetCollectionID())
	if err != nil {
		log.Error("Failed to get collection info", zap.Int64("collectionID", req.GetCollectionID()), zap.Error(err))
		resp.Reason = err.Error()
		return resp, nil
	}

	binlogs, err := s.prepareBinlog(req)
	if err != nil {
		log.Error("Prepare binlog meta failed", zap.Error(err))
		resp.Reason = err.Error()
		return resp, nil
	}

	// set segment to SegmentState_Flushing and save binlogs and checkpoints
	err = s.meta.SaveBinlogAndCheckPoints(req.GetSegmentID(), req.GetFlushed(),
		binlogs, req.GetCheckPoints(), req.GetStartPositions())
	if err != nil {
		log.Error("Save binlog and checkpoints failed",
			zap.Int64("segmentID", req.GetSegmentID()),
			zap.Error(err))
		resp.Reason = err.Error()
		return resp, nil
	}
	log.Debug("Flush segment with meta", zap.Int64("id", req.SegmentID),
		zap.Any("meta", binlogs))

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
			Role:      "dataservice",
			StateCode: 0,
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}
	state := atomic.LoadInt64(&s.isServing)
	switch state {
	case 1:
		resp.State.StateCode = internalpb.StateCode_Initializing
	case 2:
		resp.State.StateCode = internalpb.StateCode_Healthy
	default:
		resp.State.StateCode = internalpb.StateCode_Abnormal
	}
	return resp, nil
}

func (s *Server) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	collectionID := req.GetCollectionID()
	partitionID := req.GetPartitionID()
	log.Info("Receive get recovery info request",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID))
	resp := &datapb.GetRecoveryInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	if s.isClosed() {
		resp.Status.Reason = "server is initializing"
		return resp, nil
	}
	segmentIDs := s.meta.GetSegmentsOfPartition(collectionID, partitionID)
	segment2Binlogs := make(map[UniqueID][]*datapb.FieldBinlog)
	for _, id := range segmentIDs {
		segment, err := s.meta.GetSegment(id)
		if err != nil {
			log.Error("Get segment failed", zap.Int64("segmentID", id))
			resp.Status.Reason = err.Error()
			return resp, nil
		}
		if segment.State != commonpb.SegmentState_Flushed && segment.State != commonpb.SegmentState_Flushing {
			continue
		}

		meta, err := s.getSegmentBinlogMeta(id)
		if err != nil {
			log.Error("Get segment binlog meta failed", zap.Int64("segmentID", id))
			resp.Status.Reason = err.Error()
			return resp, nil
		}
		field2Binlog := make(map[UniqueID][]string)
		for _, m := range meta {
			field2Binlog[m.FieldID] = append(field2Binlog[m.FieldID], m.BinlogPath)
		}

		for f, paths := range field2Binlog {
			fieldBinlogs := &datapb.FieldBinlog{
				FieldID: f,
				Binlogs: paths,
			}
			segment2Binlogs[id] = append(segment2Binlogs[id], fieldBinlogs)
		}
	}

	binlogs := make([]*datapb.SegmentBinlogs, 0, len(segment2Binlogs))
	for segmentID, fieldBinlogs := range segment2Binlogs {
		sbl := &datapb.SegmentBinlogs{
			SegmentID:    segmentID,
			FieldBinlogs: fieldBinlogs,
		}
		binlogs = append(binlogs, sbl)
	}

	dresp, err := s.masterClient.DescribeCollection(s.ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_DescribeCollection,
			SourceID: Params.NodeID,
		},
		CollectionID: collectionID,
	})
	if err = VerifyResponse(dresp, err); err != nil {
		log.Error("Get collection info from master failed",
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
		log.Error("Get channel positions failed",
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

func (s *Server) RegisterNode(ctx context.Context, req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error) {
	return &datapb.RegisterNodeResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil

}
