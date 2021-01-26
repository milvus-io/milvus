package grpcindexnodeclient

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"google.golang.org/grpc"
)

type Client struct {
	grpcClient  indexpb.IndexNodeClient
	nodeAddress string
}

func (c Client) GetComponentStates() (*internalpb2.ComponentStates, error) {
	return c.grpcClient.GetComponentStates(context.Background(), &commonpb.Empty{})
}

func (c Client) GetTimeTickChannel() (string, error) {
	resp, err := c.grpcClient.GetTimeTickChannel(context.Background(), &commonpb.Empty{})

	if err != nil {
		return "", err
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return "", errors.New(resp.Status.Reason)
	}
	return resp.Value, nil
}

func (c Client) GetStatisticsChannel() (string, error) {
	resp, err := c.grpcClient.GetStatisticsChannel(context.Background(), &commonpb.Empty{})

	if err != nil {
		return "", err
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return "", errors.New(resp.Status.Reason)
	}
	return resp.Value, nil
}

func (c Client) Init() error {
	return nil
}

func (c Client) Start() error {
	return nil
}

func (c Client) Stop() error {
	return nil
}

func (c *Client) tryConnect() error {
	if c.grpcClient != nil {
		return nil
	}
	ctx1, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx1, c.nodeAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("Connect to IndexNode failed, error= %v", err)
		return err
	}
	c.grpcClient = indexpb.NewIndexNodeClient(conn)
	return nil
}

func (c *Client) BuildIndex(req *indexpb.BuildIndexCmd) (*commonpb.Status, error) {

	ctx := context.TODO()
	c.tryConnect()

	return c.grpcClient.BuildIndex(ctx, req)
}

func NewClient(nodeAddress string) (*Client, error) {

	return &Client{
		nodeAddress: nodeAddress,
	}, nil
}
