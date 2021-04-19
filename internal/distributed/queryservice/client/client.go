package grpcqueryserviceclient

import (
	"context"
	"errors"
	"log"
	"time"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
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
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	var err error
	for i := 0; i < c.retry; i++ {
		if c.conn, err = grpc.DialContext(ctx, c.addr, grpc.WithInsecure(), grpc.WithBlock()); err != nil {
			break
		}
	}

	if err != nil {
		return err
	}

	c.grpcClient = querypb.NewQueryServiceClient(c.conn)
	log.Printf("connected to queryService, queryService=%s", c.addr)
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

func (c *Client) GetTimeTickChannel() (string, error) {
	resp, err := c.grpcClient.GetTimeTickChannel(context.Background(), &commonpb.Empty{})
	if err != nil {
		return "", err
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return "", errors.New(resp.Status.Reason)
	}
	return resp.Value, nil
}

func (c *Client) GetStatisticsChannel() (string, error) {
	resp, err := c.grpcClient.GetStatisticsChannel(context.Background(), &commonpb.Empty{})
	if err != nil {
		return "", err
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return "", errors.New(resp.Status.Reason)
	}
	return resp.Value, nil
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
