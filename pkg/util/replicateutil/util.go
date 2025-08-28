package replicateutil

import (
	"fmt"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func ConfigLogFields(config *commonpb.ReplicateConfiguration) []zap.Field {
	fields := make([]zap.Field, 0)
	fields = append(fields, zap.Int("clusterCount", len(config.GetClusters())))
	fields = append(fields, zap.Strings("clusters", lo.Map(config.GetClusters(), func(cluster *commonpb.MilvusCluster, _ int) string {
		return cluster.GetClusterId()
	})))
	fields = append(fields, zap.Int("topologyCount", len(config.GetCrossClusterTopology())))
	fields = append(fields, zap.Strings("topologies", lo.Map(config.GetCrossClusterTopology(), func(topology *commonpb.CrossClusterTopology, _ int) string {
		return fmt.Sprintf("%s->%s", topology.GetSourceClusterId(), topology.GetTargetClusterId())
	})))
	for _, cluster := range config.GetClusters() {
		fields = append(fields, zap.String("clusterInfo", fmt.Sprintf("clusterID: %s, uri: %s, pchannels: %v",
			cluster.GetClusterId(), cluster.GetConnectionParam().GetUri(), cluster.GetPchannels())))
	}
	return fields
}
