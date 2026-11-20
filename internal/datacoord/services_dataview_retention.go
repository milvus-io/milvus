package datacoord

import (
	"context"
	"math"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (s *Server) GetDataViewRetention(ctx context.Context, req *datapb.GetDataViewRetentionRequest) (*datapb.GetDataViewRetentionResponse, error) {
	resp := &datapb.GetDataViewRetentionResponse{Status: merr.Success()}
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	if s.dataViewManager == nil {
		resp.Status = merr.Status(merr.WrapErrServiceNotReadyMsg("DataView manager is not recovered"))
		return resp, nil
	}
	if req.GetPchannel() == "" {
		resp.Status = merr.Status(merr.WrapErrServiceInternalMsg("missing PChannel for DataView retention"))
		return resp, nil
	}
	frontiers := s.dataViewManager.DataViewRetention(req.GetPchannel())
	resp.CollectionIds = make([]int64, 0, len(frontiers))
	resp.VchannelIndices = make([]uint32, 0, len(frontiers))
	resp.TransformStartAfterTimeticks = make([]uint64, 0, len(frontiers))
	for channel, tick := range frontiers {
		_, collectionID, index, err := funcutil.ParseVChannel(channel)
		if err != nil {
			return &datapb.GetDataViewRetentionResponse{Status: merr.Status(err)}, nil
		}
		if index > math.MaxUint32 {
			return &datapb.GetDataViewRetentionResponse{Status: merr.Status(merr.WrapErrServiceInternalMsg("VChannel index exceeds uint32: %s", channel))}, nil
		}
		resp.CollectionIds = append(resp.CollectionIds, collectionID)
		resp.VchannelIndices = append(resp.VchannelIndices, uint32(index))
		resp.TransformStartAfterTimeticks = append(resp.TransformStartAfterTimeticks, tick)
	}
	return resp, nil
}
