package cluster

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

type ClusterClient interface {
	CreateMilvusClient(ctx context.Context, cluster *milvuspb.MilvusCluster) (*milvusclient.Client, error)
}

var _ ClusterClient = (*clusterClient)(nil)

type clusterClient struct{}

func NewClusterClient() ClusterClient {
	return &clusterClient{}
}

func (c *clusterClient) CreateMilvusClient(ctx context.Context, cluster *milvuspb.MilvusCluster) (*milvusclient.Client, error) {
	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: cluster.GetConnectionParam().GetUri(),
		APIKey:  cluster.GetConnectionParam().GetToken(),
	})
	if err != nil {
		log.Warn("failed to create milvus client", zap.Error(err))
		return nil, err
	}
	return cli, nil
}
