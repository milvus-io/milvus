package grpcindexserviceclient

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type Client struct {
	grpcClient indexpb.IndexServiceClient
	address    string
	ctx        context.Context
}

func (c *Client) Init() error {
	connectGrpcFunc := func() error {
		conn, err := grpc.DialContext(c.ctx, c.address, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return err
		}
		c.grpcClient = indexpb.NewIndexServiceClient(conn)
		return nil
	}
	err := retry.Retry(10, time.Millisecond*200, connectGrpcFunc)
	if err != nil {
		return err
	}
	return nil
}
func (c *Client) Start() error {
	return nil
}

func (c *Client) Stop() error {
	return nil
}

func (c *Client) GetComponentStates() (*internalpb2.ComponentStates, error) {
	ctx := context.TODO()
	return c.grpcClient.GetComponentStates(ctx, &commonpb.Empty{})
}

func (c *Client) GetTimeTickChannel() (string, error) {
	return "", nil
}

func (c *Client) GetStatisticsChannel() (string, error) {
	return "", nil
}

func (c *Client) RegisterNode(req *indexpb.RegisterNodeRequest) (*indexpb.RegisterNodeResponse, error) {
	ctx := context.TODO()
	return c.grpcClient.RegisterNode(ctx, req)
}

func (c *Client) BuildIndex(req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
	ctx := context.TODO()
	return c.grpcClient.BuildIndex(ctx, req)
}

func (c *Client) GetIndexStates(req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error) {
	ctx := context.TODO()
	return c.grpcClient.GetIndexStates(ctx, req)
}
func (c *Client) GetIndexFilePaths(req *indexpb.IndexFilePathsRequest) (*indexpb.IndexFilePathsResponse, error) {
	ctx := context.TODO()
	return c.grpcClient.GetIndexFilePaths(ctx, req)
}

func (c *Client) NotifyBuildIndex(nty *indexpb.BuildIndexNotification) (*commonpb.Status, error) {
	ctx := context.TODO()
	return c.grpcClient.NotifyBuildIndex(ctx, nty)
}

func NewClient(address string) *Client {

	log.Println("new index service, address = ", address)
	return &Client{
		address: address,
		ctx:     context.Background(),
	}
}
