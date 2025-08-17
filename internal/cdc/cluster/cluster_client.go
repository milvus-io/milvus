package cluster

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

type ClusterClient interface {
	CreateMilvusClient(cluster *milvuspb.MilvusCluster) (milvusclient.Client, error)
}
