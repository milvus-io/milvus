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

package replicatemanager

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/cdc/cluster"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

func TestClusterReplicator_StartReplicateCluster(t *testing.T) {
	mockClusterClient := cluster.NewMockClusterClient(t)
	mockClusterClient.EXPECT().CreateMilvusClient(mock.Anything, mock.Anything).Return(nil, assert.AnError)
	resource.InitForTest(t,
		resource.OptClusterClient(mockClusterClient),
	)

	paramtable.Get().Save(paramtable.Get().CommonCfg.ClusterPrefix.Key, "source-cluster")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.ClusterPrefix.Key)

	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: "source-cluster",
				Pchannels: []string{"source-channel-1", "source-channel-2"},
			},
			{
				ClusterId: "target-cluster",
				Pchannels: []string{"target-channel-1", "target-channel-2"},
			},
		},
	}

	clusterReplicator := NewClusterReplicator(config.Clusters[1], config)
	clusterReplicator.StartReplicateCluster()
	clusterReplicator.StopReplicateCluster()
}
