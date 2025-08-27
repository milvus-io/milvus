package replicateutil

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

type ConfigHelper interface {
	// Cluster related
	GetCluster(clusterID string) (*milvuspb.MilvusCluster, bool)
	GetSourceCluster() *milvuspb.MilvusCluster
	GetTargetClusters() []*milvuspb.MilvusCluster

	// PChannel related
	GetCurrentPChannels() []string
	GetClusterPChannels(clusterID string) ([]string, bool)
	GetPChannelToClusterMap() map[string]string
	GetPChannelIndex(pchannel string) (int, bool)
}
