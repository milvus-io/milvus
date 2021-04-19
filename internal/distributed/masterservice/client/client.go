package grpcmasterserviceclient

import (
	"context"
	"time"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"google.golang.org/grpc"
)

// grpc client
type GrpcClient struct {
	grpcClient masterpb.MasterServiceClient
	conn       *grpc.ClientConn

	//inner member
	addr        string
	timeout     time.Duration
	grpcTimeout time.Duration
	retry       int
}

func NewClient(addr string, timeout time.Duration) (*GrpcClient, error) {
	return &GrpcClient{
		grpcClient:  nil,
		conn:        nil,
		addr:        addr,
		timeout:     timeout,
		grpcTimeout: time.Second * 5,
		retry:       3,
	}, nil
}

func (c *GrpcClient) Init() error {
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
	c.grpcClient = masterpb.NewMasterServiceClient(c.conn)
	return nil
}

func (c *GrpcClient) Start() error {
	return nil
}
func (c *GrpcClient) Stop() error {
	return c.conn.Close()
}

// TODO: timeout need to be propagated through ctx
func (c *GrpcClient) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return c.grpcClient.GetComponentStates(ctx, &internalpb.GetComponentStatesRequest{})
}
func (c *GrpcClient) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetTimeTickChannel(ctx, &internalpb.GetTimeTickChannelRequest{})
}

//just define a channel, not used currently
func (c *GrpcClient) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
}

//receive ddl from rpc and time tick from proxy service, and put them into this channel
func (c *GrpcClient) GetDdChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetDdChannel(ctx, &internalpb.GetDdChannelRequest{})
}

//DDL request
func (c *GrpcClient) CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return c.grpcClient.CreateCollection(ctx, in)
}

func (c *GrpcClient) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return c.grpcClient.DropCollection(ctx, in)
}
func (c *GrpcClient) HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return c.grpcClient.HasCollection(ctx, in)
}
func (c *GrpcClient) DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return c.grpcClient.DescribeCollection(ctx, in)
}

func (c *GrpcClient) ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return c.grpcClient.ShowCollections(ctx, in)
}

func (c *GrpcClient) CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return c.grpcClient.CreatePartition(ctx, in)
}

func (c *GrpcClient) DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return c.grpcClient.DropPartition(ctx, in)
}

func (c *GrpcClient) HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return c.grpcClient.HasPartition(ctx, in)
}

func (c *GrpcClient) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return c.grpcClient.ShowPartitions(ctx, in)
}

//index builder service
func (c *GrpcClient) CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return c.grpcClient.CreateIndex(ctx, in)
}

func (c *GrpcClient) DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	return c.grpcClient.DropIndex(ctx, in)
}

func (c *GrpcClient) DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return c.grpcClient.DescribeIndex(ctx, in)
}

//global timestamp allocator
func (c *GrpcClient) AllocTimestamp(ctx context.Context, in *masterpb.AllocTimestampRequest) (*masterpb.AllocTimestampResponse, error) {
	return c.grpcClient.AllocTimestamp(ctx, in)
}
func (c *GrpcClient) AllocID(ctx context.Context, in *masterpb.AllocIDRequest) (*masterpb.AllocIDResponse, error) {
	return c.grpcClient.AllocID(ctx, in)
}

//receiver time tick from proxy service, and put it into this channel
func (c *GrpcClient) DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	return c.grpcClient.DescribeSegment(ctx, in)
}

func (c *GrpcClient) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	return c.grpcClient.ShowSegments(ctx, in)
}
