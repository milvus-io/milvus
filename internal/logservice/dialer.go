package logservice

import (
	"context"

	"github.com/milvus-io/milvus/internal/logcoord/client"
	"github.com/milvus-io/milvus/internal/lognode/client/handler"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// DialContext dials a log service client.
func DialContext(ctx context.Context, etcdCli *clientv3.Client) Client {
	logCoordClient := client.DialContext(ctx, etcdCli)
	handlerClient := handler.DialContext(ctx, logCoordClient.Assignment())
	return &clientImpl{
		logCoordClient: logCoordClient,
		handlerClient:  handlerClient,
	}
}
