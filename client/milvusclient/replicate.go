package milvusclient

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// UpdateReplicateConfiguration updates the replicate configuration to the Milvus cluster.
// Use ReplicateConfigurationBuilder to build the configuration.
func (c *Client) UpdateReplicateConfiguration(ctx context.Context, req *milvuspb.UpdateReplicateConfigurationRequest, opts ...grpc.CallOption) error {
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.UpdateReplicateConfiguration(ctx, req, opts...)
		return merr.CheckRPCCall(resp, err)
	})
	return err
}

// GetReplicateConfiguration gets the current replicate configuration from the Milvus cluster.
func (c *Client) GetReplicateConfiguration(ctx context.Context, opts ...grpc.CallOption) (*commonpb.ReplicateConfiguration, error) {
	var config *commonpb.ReplicateConfiguration
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.GetReplicateConfiguration(ctx, &milvuspb.GetReplicateConfigurationRequest{}, opts...)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		config = resp.GetConfiguration()
		return nil
	})
	return config, err
}

// GetReplicateInfo gets replicate information from the Milvus cluster
func (c *Client) GetReplicateInfo(ctx context.Context, req *milvuspb.GetReplicateInfoRequest, opts ...grpc.CallOption) (*milvuspb.GetReplicateInfoResponse, error) {
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

// DumpMessages streams messages from a WAL range for data salvage.
// It is typically used after a force failover: callers obtain the salvage
// checkpoint from GetReplicateInfo and pass its message id as the request's
// StartMessageId to recover messages that were not yet synchronized.
//
// The returned stream yields DumpMessagesResponse frames; each frame carries
// either an error status or a non-system message. Iterate with stream.Recv()
// until io.EOF.
func (c *Client) DumpMessages(ctx context.Context, req *milvuspb.DumpMessagesRequest, opts ...grpc.CallOption) (milvuspb.MilvusService_DumpMessagesClient, error) {
	var streamClient milvuspb.MilvusService_DumpMessagesClient
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		var err error
		streamClient, err = milvusService.DumpMessages(ctx, req, opts...)
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
