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
