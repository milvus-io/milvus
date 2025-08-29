// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
