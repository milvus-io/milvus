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

func TestReplicateManager_UpdateReplications(t *testing.T) {
	mockClusterClient := cluster.NewMockClusterClient(t)
	mockClusterClient.EXPECT().CreateMilvusClient(mock.Anything, mock.Anything).Return(nil, assert.AnError)
	resource.InitForTest(t,
		resource.OptClusterClient(mockClusterClient),
	)

	paramtable.Get().Save(paramtable.Get().CommonCfg.ClusterPrefix.Key, "source-cluster")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.ClusterPrefix.Key)

	replicateManager := NewReplicateManager()

	// S --> A
	// S --> B
	// S --> C
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: "source-cluster",
				Pchannels: []string{"source-channel-1", "source-channel-2"},
			},
			{
				ClusterId: "target-cluster-a",
				Pchannels: []string{"target-channel-a-1", "target-channel-a-2"},
			},
			{
				ClusterId: "target-cluster-b",
				Pchannels: []string{"target-channel-b-1", "target-channel-b-2"},
			},
			{
				ClusterId: "target-cluster-c",
				Pchannels: []string{"target-channel-c-1", "target-channel-c-2"},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: "source-cluster",
				TargetClusterId: "target-cluster-a",
			},
			{
				SourceClusterId: "source-cluster",
				TargetClusterId: "target-cluster-b",
			},
			{
				SourceClusterId: "source-cluster",
				TargetClusterId: "target-cluster-c",
			},
		},
	}

	replicateManager.UpdateReplications(config)
	assert.Equal(t, 3, len(replicateManager.clusterReplicators))

	// S --> A
	// S --> B
	config.CrossClusterTopology = []*commonpb.CrossClusterTopology{
		{
			SourceClusterId: "source-cluster",
			TargetClusterId: "target-cluster-a",
		},
		{
			SourceClusterId: "source-cluster",
			TargetClusterId: "target-cluster-b",
		},
	}
	replicateManager.UpdateReplications(config)
	assert.Equal(t, 2, len(replicateManager.clusterReplicators))

	// S --> A
	// S --> D
	config = &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: "source-cluster",
				Pchannels: []string{"source-channel-1", "source-channel-2"},
			},
			{
				ClusterId: "target-cluster-a",
				Pchannels: []string{"target-channel-a-1", "target-channel-a-2"},
			},
			{
				ClusterId: "target-cluster-d",
				Pchannels: []string{"target-channel-d-1", "target-channel-d-2"},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: "source-cluster",
				TargetClusterId: "target-cluster-a",
			},
			{
				SourceClusterId: "source-cluster",
				TargetClusterId: "target-cluster-d",
			},
		},
	}
	replicateManager.UpdateReplications(config)
	assert.Equal(t, 2, len(replicateManager.clusterReplicators))

	replicateManager.Close()
	assert.Equal(t, 0, len(replicateManager.clusterReplicators))
}
