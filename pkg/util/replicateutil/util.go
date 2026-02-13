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

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func ConfigLogField(config *commonpb.ReplicateConfiguration) zap.Field {
	return zap.Object("replicateConfiguration", zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
		enc.AddInt("clusterCount", len(config.GetClusters()))
		for i, cluster := range config.GetClusters() {
			enc.AddString(fmt.Sprintf("cluster-%d", i), cluster.GetClusterId())
		}
		enc.AddInt("topologyCount", len(config.GetCrossClusterTopology()))
		for i, topology := range config.GetCrossClusterTopology() {
			enc.AddString(fmt.Sprintf("topology-%d", i), fmt.Sprintf("%s->%s", topology.GetSourceClusterId(), topology.GetTargetClusterId()))
		}
		for i, cluster := range config.GetClusters() {
			enc.AddString(fmt.Sprintf("cluster-%d-info", i), fmt.Sprintf("clusterID: %s, uri: %s, pchannels: %v",
				cluster.GetClusterId(), cluster.GetConnectionParam().GetUri(), cluster.GetPchannels()))
		}
		return nil
	}))
}
