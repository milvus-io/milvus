package dataservice

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

const (
	timeout = 30 * time.Second
	retry   = 3
)

type Client struct {
	grpcClient datapb.DataServiceClient
	conn       *grpc.ClientConn
	addr       string
}

func NewClient(addr string) *Client {
	return &Client{
		addr: addr,
	}
}

func (c *Client) Init() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var err error
	for i := 0; i < retry; i++ {
		if c.conn, err = grpc.DialContext(ctx, c.addr, grpc.WithInsecure(), grpc.WithBlock()); err == nil {
			break
		}
	}
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

func (c *Client) RegisterNode(req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error) {
	return c.grpcClient.RegisterNode(context.Background(), req)
}

func (c *Client) Flush(req *datapb.FlushRequest) (*commonpb.Status, error) {
	return c.grpcClient.Flush(context.Background(), req)
}

func (c *Client) AssignSegmentID(req *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error) {
	return c.grpcClient.AssignSegmentID(context.Background(), req)
}

func (c *Client) ShowSegments(req *datapb.ShowSegmentRequest) (*datapb.ShowSegmentResponse, error) {
	return c.grpcClient.ShowSegments(context.Background(), req)
}

func (c *Client) GetSegmentStates(req *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error) {
	return c.grpcClient.GetSegmentStates(context.Background(), req)
}

func (c *Client) GetInsertBinlogPaths(req *datapb.InsertBinlogPathRequest) (*datapb.InsertBinlogPathsResponse, error) {
	return c.grpcClient.GetInsertBinlogPaths(context.Background(), req)
}

func (c *Client) GetInsertChannels(req *datapb.InsertChannelRequest) ([]string, error) {
	resp, err := c.grpcClient.GetInsertChannels(context.Background(), req)
	if err != nil {
		return nil, err
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return nil, errors.New(resp.Status.Reason)
	}
	return resp.Values, nil
}

func (c *Client) GetCollectionStatistics(req *datapb.CollectionStatsRequest) (*datapb.CollectionStatsResponse, error) {
	return c.grpcClient.GetCollectionStatistics(context.Background(), req)
}

func (c *Client) GetPartitionStatistics(req *datapb.PartitionStatsRequest) (*datapb.PartitionStatsResponse, error) {
	return c.grpcClient.GetPartitionStatistics(context.Background(), req)
}

func (c *Client) GetSegmentInfoChannel() (string, error) {
	resp, err := c.grpcClient.GetSegmentInfoChannel(context.Background(), &commonpb.Empty{})
	if err != nil {
		return "", err
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return "", errors.New(resp.Status.Reason)
	}
	return resp.Value, nil
}

func (c *Client) GetCount(req *datapb.CollectionCountRequest) (*datapb.CollectionCountResponse, error) {
	return c.grpcClient.GetCount(context.Background(), req)
}
