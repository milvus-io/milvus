package masterservice

import (
	"context"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/errors"
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

func (c *GrpcClient) Init() error {
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

func (c *GrpcClient) GetComponentStates() (*internalpb2.ComponentStates, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.GetComponentStatesRPC(ctx, &commonpb.Empty{})
}

//DDL request
func (c *GrpcClient) CreateCollection(in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.CreateCollection(ctx, in)
}

func (c *GrpcClient) DropCollection(in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.DropCollection(ctx, in)
}
func (c *GrpcClient) HasCollection(in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.HasCollection(ctx, in)
}
func (c *GrpcClient) DescribeCollection(in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.DescribeCollection(ctx, in)
}

func (c *GrpcClient) ShowCollections(in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.ShowCollections(ctx, in)
}

func (c *GrpcClient) CreatePartition(in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.CreatePartition(ctx, in)
}

func (c *GrpcClient) DropPartition(in *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.DropPartition(ctx, in)
}

func (c *GrpcClient) HasPartition(in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.HasPartition(ctx, in)
}

func (c *GrpcClient) ShowPartitions(in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.ShowPartitions(ctx, in)
}

//index builder service
func (c *GrpcClient) CreateIndex(in *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.CreateIndex(ctx, in)
}
func (c *GrpcClient) DescribeIndex(in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.DescribeIndex(ctx, in)
}

//global timestamp allocator
func (c *GrpcClient) AllocTimestamp(in *masterpb.TsoRequest) (*masterpb.TsoResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.AllocTimestamp(ctx, in)
}
func (c *GrpcClient) AllocID(in *masterpb.IDRequest) (*masterpb.IDResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.AllocID(ctx, in)
}

//receiver time tick from proxy service, and put it into this channel
func (c *GrpcClient) GetTimeTickChannel() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	rsp, err := c.grpcClient.GetTimeTickChannelRPC(ctx, &commonpb.Empty{})
	if err != nil {
		return "", err
	}
	if rsp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return "", errors.Errorf("%s", rsp.Status.Reason)
	}
	return rsp.Value, nil
}

//receive ddl from rpc and time tick from proxy service, and put them into this channel
func (c *GrpcClient) GetDdChannel() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	rsp, err := c.grpcClient.GetDdChannelRPC(ctx, &commonpb.Empty{})
	if err != nil {
		return "", err
	}
	if rsp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return "", errors.Errorf("%s", rsp.Status.Reason)
	}
	return rsp.Value, nil
}

//just define a channel, not used currently
func (c *GrpcClient) GetStatisticsChannel() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	rsp, err := c.grpcClient.GetStatisticsChannelRPC(ctx, &commonpb.Empty{})
	if err != nil {
		return "", err
	}
	if rsp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return "", errors.Errorf("%s", rsp.Status.Reason)
	}
	return rsp.Value, nil
}

func (c *GrpcClient) DescribeSegment(in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.DescribeSegment(ctx, in)
}

func (c *GrpcClient) ShowSegments(in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(cms.Params.Timeout))
	defer cancel()
	return c.grpcClient.ShowSegments(ctx, in)
}
