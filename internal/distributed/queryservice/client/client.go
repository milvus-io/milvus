package grpcqueryserviceclient

import (
	"context"
	"time"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type Client struct {
	grpcClient querypb.QueryServiceClient
	conn       *grpc.ClientConn

	addr    string
	timeout time.Duration
	retry   int
}

func NewClient(address string, timeout time.Duration) (*Client, error) {

	return &Client{
		grpcClient: nil,
		conn:       nil,
		addr:       address,
		timeout:    timeout,
		retry:      3,
	}, nil
}

func (c *Client) Init() error {
	tracer := opentracing.GlobalTracer()
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	var err error
	for i := 0; i < c.retry; i++ {
		if c.conn, err = grpc.DialContext(ctx, c.addr, grpc.WithInsecure(), grpc.WithBlock(),
			grpc.WithUnaryInterceptor(
				otgrpc.OpenTracingClientInterceptor(tracer)),
			grpc.WithStreamInterceptor(
				otgrpc.OpenTracingStreamClientInterceptor(tracer))); err == nil {
			break
		}
	}

	if err != nil {
		return err
	}

	c.grpcClient = querypb.NewQueryServiceClient(c.conn)
	log.Debug("connected to queryService", zap.String("queryService", c.addr))
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

func (c *Client) RegisterNode(ctx context.Context, req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error) {
	return c.grpcClient.RegisterNode(ctx, req)
}

func (c *Client) ShowCollections(ctx context.Context, req *querypb.ShowCollectionRequest) (*querypb.ShowCollectionResponse, error) {
	return c.grpcClient.ShowCollections(ctx, req)
}

func (c *Client) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	return c.grpcClient.LoadCollection(ctx, req)
}

func (c *Client) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return c.grpcClient.ReleaseCollection(ctx, req)
}

func (c *Client) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionRequest) (*querypb.ShowPartitionResponse, error) {
	return c.grpcClient.ShowPartitions(ctx, req)
}

func (c *Client) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionRequest) (*commonpb.Status, error) {
	return c.grpcClient.LoadPartitions(ctx, req)
}

func (c *Client) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionRequest) (*commonpb.Status, error) {
	return c.grpcClient.ReleasePartitions(ctx, req)
}

func (c *Client) CreateQueryChannel(ctx context.Context) (*querypb.CreateQueryChannelResponse, error) {
	return c.grpcClient.CreateQueryChannel(ctx, &commonpb.Empty{})
}

func (c *Client) GetPartitionStates(ctx context.Context, req *querypb.PartitionStatesRequest) (*querypb.PartitionStatesResponse, error) {
	return c.grpcClient.GetPartitionStates(ctx, req)
}

func (c *Client) GetSegmentInfo(ctx context.Context, req *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error) {
	return c.grpcClient.GetSegmentInfo(ctx, req)
}
