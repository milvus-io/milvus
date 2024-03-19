package broker

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// Broker is the interface for datanode to interact with other components.
type Broker interface {
	RootCoord
	DataCoord
}

type coordBroker struct {
	*rootCoordBroker
	*dataCoordBroker
}

func NewCoordBroker(rc types.RootCoordClient, dc types.DataCoordClient, serverID int64) Broker {
	return &coordBroker{
		rootCoordBroker: &rootCoordBroker{
			client:   rc,
			serverID: serverID,
		},
		dataCoordBroker: &dataCoordBroker{
			client:   dc,
			serverID: serverID,
		},
	}
}

// RootCoord is the interface wraps `RootCoord` grpc call
type RootCoord interface {
	DescribeCollection(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*milvuspb.DescribeCollectionResponse, error)
	ShowPartitions(ctx context.Context, dbName, collectionName string) (map[string]int64, error)
	AllocTimestamp(ctx context.Context, num uint32) (ts uint64, count uint32, err error)
}

// DataCoord is the interface wraps `DataCoord` grpc call
type DataCoord interface {
	AssignSegmentID(ctx context.Context, reqs ...*datapb.SegmentIDRequest) ([]typeutil.UniqueID, error)
	ReportTimeTick(ctx context.Context, msgs []*msgpb.DataNodeTtMsg) error
	GetSegmentInfo(ctx context.Context, segmentIDs []int64) ([]*datapb.SegmentInfo, error)
	UpdateChannelCheckpoint(ctx context.Context, channelCPs []*msgpb.MsgPosition) error
	SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) error
	DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error)
	UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) error
}
