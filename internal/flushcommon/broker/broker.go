package broker

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Broker is the interface for datanode to interact with other components.
//
//go:generate mockery --name=Broker --structname=MockBroker --output=./  --filename=mock_broker.go --with-expecter --inpackage
type Broker interface {
	DataCoord
}

type coordBroker struct {
	*dataCoordBroker
}

func NewCoordBroker(dc types.DataCoordClient, serverID int64) Broker {
	return &coordBroker{
		dataCoordBroker: &dataCoordBroker{
			client:   dc,
			serverID: serverID,
		},
	}
}

// DataCoord is the interface wraps `DataCoord` grpc call
type DataCoord interface {
	AssignSegmentID(ctx context.Context, reqs ...*datapb.SegmentIDRequest) ([]typeutil.UniqueID, error)
	ReportTimeTick(ctx context.Context, msgs []*msgpb.DataNodeTtMsg) error
	GetSegmentInfo(ctx context.Context, segmentIDs []int64) ([]*datapb.SegmentInfo, error)
	UpdateChannelCheckpoint(ctx context.Context, channelCPs []*msgpb.MsgPosition) error
	SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) error
	DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error)
}
