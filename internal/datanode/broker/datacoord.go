package broker

import (
	"context"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type dataCoordBroker struct {
	client types.DataCoordClient
}

func (dc *dataCoordBroker) AssignSegmentID(ctx context.Context, reqs ...*datapb.SegmentIDRequest) ([]typeutil.UniqueID, error) {
	req := &datapb.AssignSegmentIDRequest{
		NodeID:            paramtable.GetNodeID(),
		PeerRole:          typeutil.ProxyRole,
		SegmentIDRequests: reqs,
	}

	resp, err := dc.client.AssignSegmentID(ctx, req)

	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to call datacoord AssignSegmentID", zap.Error(err))
		return nil, err
	}

	return lo.Map(resp.GetSegIDAssignments(), func(result *datapb.SegmentIDAssignment, _ int) typeutil.UniqueID {
		return result.GetSegID()
	}), nil
}

func (dc *dataCoordBroker) ReportTimeTick(ctx context.Context, msgs []*msgpb.DataNodeTtMsg) error {
	log := log.Ctx(ctx)

	req := &datapb.ReportDataNodeTtMsgsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DataNodeTt),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		Msgs: msgs,
	}

	resp, err := dc.client.ReportDataNodeTtMsgs(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to report datanodeTtMsgs", zap.Error(err))
		return err
	}
	return nil
}

func (dc *dataCoordBroker) GetSegmentInfo(ctx context.Context, segmentIDs []int64) ([]*datapb.SegmentInfo, error) {
	log := log.Ctx(ctx).With(
		zap.Int64s("segmentIDs", segmentIDs),
	)

	infoResp, err := dc.client.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_SegmentInfo),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		SegmentIDs:       segmentIDs,
		IncludeUnHealthy: true,
	})
	if err := merr.CheckRPCCall(infoResp, err); err != nil {
		log.Warn("Fail to get SegmentInfo by ids from datacoord", zap.Error(err))
		return nil, err
	}

	return infoResp.Infos, nil
}

func (dc *dataCoordBroker) UpdateChannelCheckpoint(ctx context.Context, channelName string, cp *msgpb.MsgPosition) error {
	channelCPTs, _ := tsoutil.ParseTS(cp.GetTimestamp())
	log := log.Ctx(ctx).With(
		zap.String("channelName", channelName),
		zap.Time("channelCheckpointTime", channelCPTs),
	)

	req := &datapb.UpdateChannelCheckpointRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		VChannel: channelName,
		Position: cp,
	}

	resp, err := dc.client.UpdateChannelCheckpoint(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to update channel checkpoint", zap.Error(err))
		return err
	}
	return nil
}

func (dc *dataCoordBroker) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) error {
	log := log.Ctx(ctx)

	resp, err := dc.client.SaveBinlogPaths(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to SaveBinlogPaths", zap.Error(err))
		return err
	}

	return nil
}

func (dc *dataCoordBroker) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	log := log.Ctx(ctx)

	resp, err := dc.client.DropVirtualChannel(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to SaveBinlogPaths", zap.Error(err))
		return resp, err
	}

	return resp, nil
}

func (dc *dataCoordBroker) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) error {
	log := log.Ctx(ctx)

	resp, err := dc.client.UpdateSegmentStatistics(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to UpdateSegmentStatistics", zap.Error(err))
		return err
	}

	return nil
}

func (dc *dataCoordBroker) SaveImportSegment(ctx context.Context, req *datapb.SaveImportSegmentRequest) error {
	log := log.Ctx(ctx)

	resp, err := dc.client.SaveImportSegment(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to UpdateSegmentStatistics", zap.Error(err))
		return err
	}

	return nil
}
