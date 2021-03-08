package grpcdataserviceclient

import (
	"context"
	"time"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
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
	tracer := opentracing.GlobalTracer()
	connectGrpcFunc := func() error {
		conn, err := grpc.DialContext(c.ctx, c.addr, grpc.WithInsecure(), grpc.WithBlock(),
			grpc.WithUnaryInterceptor(
				otgrpc.OpenTracingClientInterceptor(tracer)),
			grpc.WithStreamInterceptor(
				otgrpc.OpenTracingStreamClientInterceptor(tracer)))
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

func (c *Client) GetComponentStates(ctx context.Context) (*internalpb2.ComponentStates, error) {
	return c.grpcClient.GetComponentStates(ctx, &commonpb.Empty{})
}

func (c *Client) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetTimeTickChannel(ctx, &commonpb.Empty{})
}

func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetStatisticsChannel(ctx, &commonpb.Empty{})
}

func (c *Client) RegisterNode(ctx context.Context, req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error) {
	return c.grpcClient.RegisterNode(ctx, req)
}

func (c *Client) Flush(ctx context.Context, req *datapb.FlushRequest) (*commonpb.Status, error) {
	return c.grpcClient.Flush(ctx, req)
}

func (c *Client) AssignSegmentID(ctx context.Context, req *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error) {
	return c.grpcClient.AssignSegmentID(ctx, req)
}

func (c *Client) ShowSegments(ctx context.Context, req *datapb.ShowSegmentRequest) (*datapb.ShowSegmentResponse, error) {
	return c.grpcClient.ShowSegments(ctx, req)
}

func (c *Client) GetSegmentStates(ctx context.Context, req *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error) {
	return c.grpcClient.GetSegmentStates(ctx, req)
}

func (c *Client) GetInsertBinlogPaths(ctx context.Context, req *datapb.InsertBinlogPathRequest) (*datapb.InsertBinlogPathsResponse, error) {
	return c.grpcClient.GetInsertBinlogPaths(ctx, req)
}

func (c *Client) GetInsertChannels(ctx context.Context, req *datapb.InsertChannelRequest) (*internalpb2.StringList, error) {
	return c.grpcClient.GetInsertChannels(ctx, req)
}

func (c *Client) GetCollectionStatistics(ctx context.Context, req *datapb.CollectionStatsRequest) (*datapb.CollectionStatsResponse, error) {
	return c.grpcClient.GetCollectionStatistics(ctx, req)
}

func (c *Client) GetPartitionStatistics(ctx context.Context, req *datapb.PartitionStatsRequest) (*datapb.PartitionStatsResponse, error) {
	return c.grpcClient.GetPartitionStatistics(ctx, req)
}

func (c *Client) GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetSegmentInfoChannel(ctx, &commonpb.Empty{})
}

func (c *Client) GetSegmentInfo(ctx context.Context, req *datapb.SegmentInfoRequest) (*datapb.SegmentInfoResponse, error) {
	return c.grpcClient.GetSegmentInfo(ctx, req)
}
