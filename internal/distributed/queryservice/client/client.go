package grpcqueryserviceclient

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type Client struct {
	grpcClient querypb.QueryServiceClient
}

func (c *Client) Init() error {
	panic("implement me")
}

func (c *Client) Start() error {
	panic("implement me")
}

func (c *Client) Stop() error {
	panic("implement me")
}

func (c *Client) GetComponentStates() (*internalpb2.ComponentStates, error) {
	panic("implement me")
}

func (c *Client) GetTimeTickChannel() (string, error) {
	panic("implement me")
}

func (c *Client) GetStatisticsChannel() (string, error) {
	panic("implement me")
}

func (c *Client) RegisterNode(req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error) {
	return c.grpcClient.RegisterNode(context.TODO(), req)
}

func (c *Client) ShowCollections(req *querypb.ShowCollectionRequest) (*querypb.ShowCollectionResponse, error) {
	return c.grpcClient.ShowCollections(context.TODO(), req)
}

func (c *Client) LoadCollection(req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	return c.grpcClient.LoadCollection(context.TODO(), req)
}

func (c *Client) ReleaseCollection(req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return c.grpcClient.ReleaseCollection(context.TODO(), req)
}

func (c *Client) ShowPartitions(req *querypb.ShowPartitionRequest) (*querypb.ShowPartitionResponse, error) {
	return c.grpcClient.ShowPartitions(context.TODO(), req)
}

func (c *Client) LoadPartitions(req *querypb.LoadPartitionRequest) (*commonpb.Status, error) {
	return c.grpcClient.LoadPartitions(context.TODO(), req)
}

func (c *Client) ReleasePartitions(req *querypb.ReleasePartitionRequest) (*commonpb.Status, error) {
	return c.grpcClient.ReleasePartitions(context.TODO(), req)
}

func (c *Client) CreateQueryChannel() (*querypb.CreateQueryChannelResponse, error) {
	return c.grpcClient.CreateQueryChannel(context.TODO(), &commonpb.Empty{})
}

func (c *Client) GetPartitionStates(req *querypb.PartitionStatesRequest) (*querypb.PartitionStatesResponse, error) {
	return c.grpcClient.GetPartitionStates(context.TODO(), req)
}

func NewClient(address string) *Client {
	ctx1, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx1, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("connect to queryService failed, error= %v", err)
	}
	log.Printf("connected to queryService, queryService=%s", address)

	return &Client{
		grpcClient: querypb.NewQueryServiceClient(conn),
	}
}
