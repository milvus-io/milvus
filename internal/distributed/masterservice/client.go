package masterservice

import (
	"context"
	"time"

	cms "github.com/zilliztech/milvus-distributed/internal/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"google.golang.org/grpc"
)

// grpc client
type GrpcClient struct {
	grpcClient masterpb.MasterServiceClient
	conn       *grpc.ClientConn

	//inner member
	addr    string
	timeout time.Duration
	retry   int
}

func NewGrpcClient(addr string, timeout time.Duration) (*GrpcClient, error) {
	return &GrpcClient{
		grpcClient: nil,
		conn:       nil,
		addr:       addr,
		timeout:    timeout,
		retry:      3,
	}, nil
}

func (c *GrpcClient) Init(params *cms.InitParams) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	var err error
	for i := 0; i < c.retry; i++ {
		if c.conn, err = grpc.DialContext(ctx, c.addr, grpc.WithInsecure(), grpc.WithBlock()); err == nil {
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

//TODO, grpc, get service state from server
func (c *GrpcClient) GetServiceStates() (*internalpb2.ServiceStates, error) {
	return nil, nil
}

//DDL request
func (c *GrpcClient) CreateCollection(in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return c.grpcClient.CreateCollection(context.Background(), in)
}

func (c *GrpcClient) DropCollection(in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return c.grpcClient.DropCollection(context.Background(), in)
}
func (c *GrpcClient) HasCollection(in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return c.grpcClient.HasCollection(context.Background(), in)
}
func (c *GrpcClient) DescribeCollection(in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return c.grpcClient.DescribeCollection(context.Background(), in)
}
func (c *GrpcClient) GetCollectionStatistics(in *milvuspb.CollectionStatsRequest) (*milvuspb.CollectionStatsResponse, error) {
	return c.grpcClient.GetCollectionStatistics(context.Background(), in)
}
func (c *GrpcClient) ShowCollections(in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error) {
	return c.grpcClient.ShowCollections(context.Background(), in)
}
func (c *GrpcClient) CreatePartition(in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return c.grpcClient.CreatePartition(context.Background(), in)
}
func (c *GrpcClient) DropPartition(in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return c.grpcClient.DropPartition(context.Background(), in)
}
func (c *GrpcClient) HasPartition(in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return c.grpcClient.HasPartition(context.Background(), in)
}
func (c *GrpcClient) GetPartitionStatistics(in *milvuspb.PartitionStatsRequest) (*milvuspb.PartitionStatsResponse, error) {
	return c.grpcClient.GetPartitionStatistics(context.Background(), in)
}
func (c *GrpcClient) ShowPartitions(in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error) {
	return c.grpcClient.ShowPartitions(context.Background(), in)
}

//index builder service
func (c *GrpcClient) CreateIndex(in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	return c.grpcClient.CreateIndex(context.Background(), in)
}
func (c *GrpcClient) DescribeIndex(in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	return c.grpcClient.DescribeIndex(context.Background(), in)
}

//global timestamp allocator
func (c *GrpcClient) AllocTimestamp(in *masterpb.TsoRequest) (*masterpb.TsoResponse, error) {
	return c.grpcClient.AllocTimestamp(context.Background(), in)
}
func (c *GrpcClient) AllocID(in *masterpb.IDRequest) (*masterpb.IDResponse, error) {
	return c.grpcClient.AllocID(context.Background(), in)
}

//receiver time tick from proxy service, and put it into this channel
func (c *GrpcClient) GetTimeTickChannel(empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetTimeTickChannel(context.Background(), empty)
}

//receive ddl from rpc and time tick from proxy service, and put them into this channel
func (c *GrpcClient) GetDdChannel(in *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetDdChannel(context.Background(), in)
}

//just define a channel, not used currently
func (c *GrpcClient) GetStatisticsChannel(empty *commonpb.Empty) (*milvuspb.StringResponse, error) {
	return c.grpcClient.GetStatisticsChannel(context.Background(), empty)
}

func (c *GrpcClient) DescribeSegment(in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	return c.grpcClient.DescribeSegment(context.Background(), in)
}

func (c *GrpcClient) ShowSegments(in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error) {
	return c.grpcClient.ShowSegments(context.Background(), in)
}
