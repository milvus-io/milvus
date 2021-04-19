package grpcdataserviceclient

import (
	"context"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type Client struct {
	grpcClient datapb.DataServiceClient
	conn       *grpc.ClientConn
	ctx        context.Context
	addr       string
}

func NewClient(addr string) *Client {
	return &Client{
		addr: addr,
		ctx:  context.Background(),
	}
}

func (c *Client) Init() error {
	connectGrpcFunc := func() error {
		conn, err := grpc.DialContext(c.ctx, c.addr, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return err
		}
		c.conn = conn
		return nil
	}

	err := retry.Retry(100, time.Millisecond*200, connectGrpcFunc)
	if err != nil {
		return err
	}
	c.grpcClient = datapb.NewDataServiceClient(c.conn)

	return nil
}

func (c *Client) Start() error {
	return nil
}

func (c *Client) Stop() error {
	return c.conn.Close()
}

func (c *Client) GetComponentStates() (*internalpb2.ComponentStates, error) {
	return c.grpcClient.GetComponentStates(context.Background(), &commonpb.Empty{})
}

func (c *Client) GetTimeTickChannel() (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetTimeTickChannel(context.Background(), &commonpb.Empty{})
}

func (c *Client) GetStatisticsChannel() (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetStatisticsChannel(context.Background(), &commonpb.Empty{})
}

func (c *Client) RegisterNode(req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error) {
	return c.grpcClient.RegisterNode(context.Background(), req)
}

func (c *Client) Flush(req *datapb.FlushRequest) (*commonpb.Status, error) {
	return c.grpcClient.Flush(context.Background(), req)
}

func (c *Client) AssignSegmentID(req *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error) {
	return c.grpcClient.AssignSegmentID(context.Background(), req)
}

func (c *Client) ShowSegments(req *datapb.ShowSegmentRequest) (*datapb.ShowSegmentResponse, error) {
	return c.grpcClient.ShowSegments(context.Background(), req)
}

func (c *Client) GetSegmentStates(req *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error) {
	return c.grpcClient.GetSegmentStates(context.Background(), req)
}

func (c *Client) GetInsertBinlogPaths(req *datapb.InsertBinlogPathRequest) (*datapb.InsertBinlogPathsResponse, error) {
	return c.grpcClient.GetInsertBinlogPaths(context.Background(), req)
}

func (c *Client) GetInsertChannels(req *datapb.InsertChannelRequest) (*internalpb2.StringList, error) {
	return c.grpcClient.GetInsertChannels(context.Background(), req)
}

func (c *Client) GetCollectionStatistics(req *datapb.CollectionStatsRequest) (*datapb.CollectionStatsResponse, error) {
	return c.grpcClient.GetCollectionStatistics(context.Background(), req)
}

func (c *Client) GetPartitionStatistics(req *datapb.PartitionStatsRequest) (*datapb.PartitionStatsResponse, error) {
	return c.grpcClient.GetPartitionStatistics(context.Background(), req)
}

func (c *Client) GetSegmentInfoChannel() (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetSegmentInfoChannel(context.Background(), &commonpb.Empty{})
}

func (c *Client) GetCount(req *datapb.CollectionCountRequest) (*datapb.CollectionCountResponse, error) {
	return c.grpcClient.GetCount(context.Background(), req)
}

func (c *Client) GetSegmentInfo(req *datapb.SegmentInfoRequest) (*datapb.SegmentInfoResponse, error) {
	return c.grpcClient.GetSegmentInfo(context.Background(), req)
}
