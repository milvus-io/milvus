package client

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// syncGrpcClient is a wrapped sync stream rpc client.
type syncGrpcClient struct {
	viewpb.QueryViewSyncService_SyncClient
}

// SendView sends the view to server.
func (c *syncGrpcClient) SendViews(view *viewpb.SyncQueryViewsRequest) error {
	return c.Send(&viewpb.SyncRequest{
		Request: &viewpb.SyncRequest_Views{
			Views: view,
		},
	})
}

// SendClose sends the close request to server.
func (c *syncGrpcClient) SendClose() error {
	return c.Send(&viewpb.SyncRequest{
		Request: &viewpb.SyncRequest_Close{
			Close: &viewpb.SyncCloseRequest{},
		},
	})
}
