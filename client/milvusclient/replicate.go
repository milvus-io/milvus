package milvusclient

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// UpdateReplicateConfiguration updates the replicate configuration to the milvus cluster.
func (c *Client) UpdateReplicateConfiguration(ctx context.Context, in *milvuspb.UpdateReplicateConfigurationRequest, opts ...grpc.CallOption) error {
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.UpdateReplicateConfiguration(ctx, in, opts...)
		return merr.CheckRPCCall(resp, err)
	})
	return err
}

func (c *Client) GetReplicateInfo(ctx context.Context, in *milvuspb.GetReplicateInfoRequest, opts ...grpc.CallOption) (*milvuspb.GetReplicateInfoResponse, error) {
	var resp *milvuspb.GetReplicateInfoResponse
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		var err error
		resp, err = milvusService.GetReplicateInfo(ctx, in, opts...)
		return merr.CheckRPCCall(resp, err)
	})
	return resp, err
}

func (c *Client) CreateReplicateStream(ctx context.Context, opts ...grpc.CallOption) (milvuspb.MilvusService_CreateReplicateStreamClient, error) {
	var resp milvuspb.MilvusService_CreateReplicateStreamClient
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		var err error
		resp, err = milvusService.CreateReplicateStream(ctx, opts...)
		return merr.CheckRPCCall(resp, err)
	})
	return resp, err
}
