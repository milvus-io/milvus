package milvusclient

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// UpdateReplicateConfiguration updates the replicate configuration to the Milvus cluster.
// Use ReplicateConfigurationBuilder to build the configuration.
func (c *Client) UpdateReplicateConfiguration(ctx context.Context, config *commonpb.ReplicateConfiguration, opts ...grpc.CallOption) error {
	req := &milvuspb.UpdateReplicateConfigurationRequest{
		ReplicateConfiguration: config,
	}

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.UpdateReplicateConfiguration(ctx, req, opts...)
		return merr.CheckRPCCall(resp, err)
	})
	return err
}

// GetReplicateInfo gets replicate information from the Milvus cluster
func (c *Client) GetReplicateInfo(ctx context.Context, sourceClusterID string, opts ...grpc.CallOption) (*milvuspb.GetReplicateInfoResponse, error) {
	req := &milvuspb.GetReplicateInfoRequest{
		SourceClusterId: sourceClusterID,
	}
	var resp *milvuspb.GetReplicateInfoResponse
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		var err error
		resp, err = milvusService.GetReplicateInfo(ctx, req, opts...)
		return merr.CheckRPCCall(resp, err)
	})
	return resp, err
}

// CreateReplicateStream creates a replicate stream
func (c *Client) CreateReplicateStream(ctx context.Context, opts ...grpc.CallOption) (milvuspb.MilvusService_CreateReplicateStreamClient, error) {
	var streamClient milvuspb.MilvusService_CreateReplicateStreamClient
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		var err error
		streamClient, err = milvusService.CreateReplicateStream(ctx, opts...)
		if err != nil {
			return err
		}
		if streamClient == nil {
			return errors.New("stream client is nil")
		}
		return nil
	})
	return streamClient, err
}
