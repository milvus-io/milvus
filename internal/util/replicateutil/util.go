package replicateutil

import (
	"fmt"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

func GetMilvusCluster(clusterID string, config *milvuspb.ReplicateConfiguration) (*milvuspb.MilvusCluster, error) {
	for _, cluster := range config.GetClusters() {
		if cluster.GetClusterId() == clusterID {
			return cluster, nil
		}
	}
	return nil, fmt.Errorf("cluster %s not found in replicate configuration", clusterID)
}

func MustGetMilvusCluster(clusterID string, config *milvuspb.ReplicateConfiguration) *milvuspb.MilvusCluster {
	for _, cluster := range config.GetClusters() {
		if cluster.GetClusterId() == clusterID {
			return cluster
		}
	}
	panic(fmt.Sprintf("cluster %s not found in replicate configuration", clusterID))
}

func mustGetClusterByPChannel(pchannel string, config *milvuspb.ReplicateConfiguration) *milvuspb.MilvusCluster {
	for _, cluster := range config.GetClusters() {
		for _, channel := range cluster.GetPchannels() {
			if channel == pchannel {
				return cluster
			}
		}
	}
	panic(fmt.Sprintf("pchannel %s not found in replicate configuration", pchannel))
}

func MustGetMappingChannel(targetClusterID string, sourcePChannel string, config *milvuspb.ReplicateConfiguration) string {
	sourceCluster := mustGetClusterByPChannel(sourcePChannel, config)
	targetCluster := MustGetMilvusCluster(targetClusterID, config)
	for i, pchannel := range sourceCluster.GetPchannels() {
		if pchannel == sourcePChannel {
			return targetCluster.GetPchannels()[i]
		}
	}
	panic(fmt.Sprintf("source pchannel %s not found, targetPchannels: %v", sourcePChannel, targetCluster.GetPchannels()))
}

func ConfigLogFields(config *milvuspb.ReplicateConfiguration) []zap.Field {
	fields := make([]zap.Field, 0)
	fields = append(fields, zap.Int("clusterCount", len(config.GetClusters())))
	fields = append(fields, zap.Strings("clusters", lo.Map(config.GetClusters(), func(cluster *milvuspb.MilvusCluster, _ int) string {
		return cluster.GetClusterId()
	})))
	fields = append(fields, zap.Int("topologyCount", len(config.GetCrossClusterTopology())))
	fields = append(fields, zap.Strings("topologies", lo.Map(config.GetCrossClusterTopology(), func(topology *milvuspb.CrossClusterTopology, _ int) string {
		return fmt.Sprintf("%s->%s", topology.GetSourceClusterId(), topology.GetTargetClusterId())
	})))
	for _, cluster := range config.GetClusters() {
		fields = append(fields, zap.String("clusterInfo", fmt.Sprintf("clusterID: %s, uri: %s, pchannels: %v",
			cluster.GetClusterId(), cluster.GetConnectionParam().GetUri(), cluster.GetPchannels())))
	}
	return fields
}
