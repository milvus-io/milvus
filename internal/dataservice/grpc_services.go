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
	resp := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	}
	if s.isClosed() {
		resp.Reason = "server is closed"
		return resp, nil
	}
	if err := s.segAllocator.SealAllSegments(ctx, req.CollectionID); err != nil {
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

		segmentID, retCount, expireTs, err := s.segAllocator.AllocSegment(ctx,
			r.CollectionID, r.PartitionID, r.ChannelName, int64(r.Count))
		if err != nil {
			errMsg := fmt.Sprintf("allocation of collection %d, partition %d, channel %s, count %d error:  %s",
				r.CollectionID, r.PartitionID, r.ChannelName, r.Count, err.Error())
			appendFailedAssignment(errMsg)
			continue
		}

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
			state.State = segmentInfo.State
			if segmentInfo.DmlPosition != nil {
				state.StartPosition = segmentInfo.DmlPosition.StartPosition
				state.EndPosition = segmentInfo.DmlPosition.EndPosition
			} else {

			}
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
	p := path.Join(Params.SegmentFlushMetaPath, strconv.FormatInt(req.SegmentID, 10))
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
	if s.flushMsgStream == nil {
		resp.Reason = "flush msg stream nil"
		return resp, nil
	}

	// check segment id & collection id matched
	_, err := s.meta.GetCollection(req.GetCollectionID())
	if err != nil {
		log.Error("Failed to get collection info", zap.Int64("collectionID", req.GetCollectionID()), zap.Error(err))
		resp.Reason = err.Error()
		return resp, nil
	}

	meta, err := s.prepareBinlog(req)
	if err != nil {
		log.Error("prepare binlog meta failed", zap.Error(err))
		resp.Reason = err.Error()
		return resp, nil
	}

	// set segment to SegmentState_Flushing
	err = s.meta.FlushSegmentWithBinlogAndPos(req.SegmentID, req.DmlPosition,
		req.DdlPosition, meta)
	if err != nil {
		resp.Reason = err.Error()
		return resp, nil
	}
	log.Debug("flush segment with meta", zap.Int64("id", req.SegmentID),
		zap.Any("meta", meta))

	s.segAllocator.DropSegment(ctx, req.SegmentID)

	s.flushCh <- req.SegmentID
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

func (s *Server) RegisterNode(ctx context.Context, req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error) {
	return &datapb.RegisterNodeResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil

}
