package datanode

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type Client struct {
	ctx context.Context

	// GOOSE TODO: add DataNodeClient
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

func (c *Client) GetComponentStates(empty *commonpb.Empty) (*internalpb2.ComponentStates, error) {
	panic("implement me")
}

func (c *Client) WatchDmChannels(in *datapb.WatchDmChannelRequest) error {
	panic("implement me")
}

func (c *Client) FlushSegments(in *datapb.FlushSegRequest) error {
	panic("implement me")
}
