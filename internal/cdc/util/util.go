package util

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func ValidateReplicateConfiguration(config *milvuspb.ReplicateConfiguration) error {
	// topo check
	return nil
}

func IsSourceCluster(cluster *milvuspb.MilvusCluster) bool {
	return cluster.GetClusterID() == paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
}

func GetMilvusCluster(clusterID string, config *milvuspb.ReplicateConfiguration) *milvuspb.MilvusCluster {
	for _, cluster := range config.GetClusters() {
		if cluster.GetClusterID() == clusterID {
			return cluster
		}
	}
	panic(fmt.Sprintf("cluster %s not found in replicate configuration", clusterID))
}
