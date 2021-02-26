package grpcquerynodeclient

import (
	"context"
	"time"

	"google.golang.org/grpc"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

const (
	RPCConnectionTimeout = 30 * time.Second
	Retry                = 3
)

type Client struct {
	ctx        context.Context
	grpcClient querypb.QueryNodeClient
	conn       *grpc.ClientConn
	addr       string
}

func NewClient(address string) *Client {
	return &Client{
		addr: address,
	}
}

func (c *Client) Init() error {
	tracer := opentracing.GlobalTracer()
	ctx, cancel := context.WithTimeout(context.Background(), RPCConnectionTimeout)
	defer cancel()
	var err error
	for i := 0; i < Retry; i++ {
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
	c.grpcClient = querypb.NewQueryNodeClient(c.conn)
	return nil
}

func (c *Client) Start() error {
	return nil
}

func (c *Client) Stop() error {
	return c.conn.Close()
}

func (c *Client) GetComponentStates(ctx context.Context) (*internalpb2.ComponentStates, error) {
	return c.grpcClient.GetComponentStates(ctx, nil)
}

func (c *Client) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetTimeTickChannel(ctx, nil)
}

func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetStatsChannel(ctx, nil)
}

func (c *Client) AddQueryChannel(ctx context.Context, in *querypb.AddQueryChannelsRequest) (*commonpb.Status, error) {
	return c.grpcClient.AddQueryChannel(ctx, in)
}

func (c *Client) RemoveQueryChannel(ctx context.Context, in *querypb.RemoveQueryChannelsRequest) (*commonpb.Status, error) {
	return c.grpcClient.RemoveQueryChannel(ctx, in)
}

func (c *Client) WatchDmChannels(ctx context.Context, in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	return c.grpcClient.WatchDmChannels(ctx, in)
}

func (c *Client) LoadSegments(ctx context.Context, in *querypb.LoadSegmentRequest) (*commonpb.Status, error) {
	return c.grpcClient.LoadSegments(ctx, in)
}

func (c *Client) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return c.grpcClient.ReleaseCollection(ctx, in)
}

func (c *Client) ReleasePartitions(ctx context.Context, in *querypb.ReleasePartitionRequest) (*commonpb.Status, error) {
	return c.grpcClient.ReleasePartitions(ctx, in)
}

func (c *Client) ReleaseSegments(ctx context.Context, in *querypb.ReleaseSegmentRequest) (*commonpb.Status, error) {
	return c.grpcClient.ReleaseSegments(ctx, in)
}

func (c *Client) GetSegmentInfo(ctx context.Context, in *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error) {
	return c.grpcClient.GetSegmentInfo(ctx, in)
}
