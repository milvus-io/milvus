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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

// createValidValidatorConfig creates a valid ReplicateConfiguration for testing
func createValidValidatorConfig() *commonpb.ReplicateConfiguration {
	return &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
			{
				ClusterId: "cluster-2",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19531",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: "cluster-1",
				TargetClusterId: "cluster-2",
			},
		},
	}
}

// createStarTopologyConfig creates a valid star topology configuration
func createStarTopologyConfig() *commonpb.ReplicateConfiguration {
	return &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: "center-cluster",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
			{
				ClusterId: "leaf-cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19531",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
			{
				ClusterId: "leaf-cluster-2",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19532",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: "center-cluster",
				TargetClusterId: "leaf-cluster-1",
			},
			{
				SourceClusterId: "center-cluster",
				TargetClusterId: "leaf-cluster-2",
			},
		},
	}
}

func TestNewReplicateConfigValidator(t *testing.T) {
	config := createValidValidatorConfig()
	currentPChannels := []string{"channel-1", "channel-2"}

	t.Run("success - create validator without current config", func(t *testing.T) {
		validator := NewReplicateConfigValidator(config, nil, "cluster-1", currentPChannels)
		assert.NotNil(t, validator)
		assert.Equal(t, config, validator.incomingConfig)
		assert.Equal(t, currentPChannels, validator.currentPChannels)
		assert.NotNil(t, validator.clusterMap)
		assert.Equal(t, 0, len(validator.clusterMap)) // clusterMap is built during validation
		assert.Nil(t, validator.currentConfig)
	})

	t.Run("success - create validator with current config", func(t *testing.T) {
		currentConfig := createValidValidatorConfig()
		validator := NewReplicateConfigValidator(config, currentConfig, "cluster-1", currentPChannels)
		assert.NotNil(t, validator)
		assert.Equal(t, config, validator.incomingConfig)
		assert.Equal(t, currentConfig, validator.currentConfig)
		assert.Equal(t, currentPChannels, validator.currentPChannels)
		assert.NotNil(t, validator.clusterMap)
		assert.Equal(t, 0, len(validator.clusterMap)) // clusterMap is built during validation
	})
}

func TestReplicateConfigValidator_Validate(t *testing.T) {
	t.Run("success - valid configuration without current config", func(t *testing.T) {
		config := createValidValidatorConfig()
		currentPChannels := []string{"channel-1", "channel-2"}
		validator := NewReplicateConfigValidator(config, nil, "cluster-1", currentPChannels)

		err := validator.Validate()
		assert.NoError(t, err)
	})

	t.Run("success - valid configuration with current config", func(t *testing.T) {
		config := createValidValidatorConfig()
		currentConfig := createValidValidatorConfig()
		currentPChannels := []string{"channel-1", "channel-2"}
		validator := NewReplicateConfigValidator(config, currentConfig, "cluster-1", currentPChannels)

		err := validator.Validate()
		assert.NoError(t, err)
	})

	t.Run("error - nil incoming config", func(t *testing.T) {
		validator := NewReplicateConfigValidator(nil, nil, "cluster-1", []string{})
		err := validator.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "config cannot be nil")
	})

	t.Run("error - empty clusters", func(t *testing.T) {
		config := &commonpb.ReplicateConfiguration{
			Clusters:             []*commonpb.MilvusCluster{},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{},
		}
		validator := NewReplicateConfigValidator(config, nil, "cluster-1", []string{})
		err := validator.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "clusters list cannot be empty")
	})
}

func TestReplicateConfigValidator_validateClusterBasic(t *testing.T) {
	t.Run("success - valid clusters", func(t *testing.T) {
		clusters := []*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
			{
				ClusterId: "cluster-2",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19531",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
		}

		validator := &ReplicateConfigValidator{
			clusterMap: make(map[string]*commonpb.MilvusCluster),
		}

		err := validator.validateClusterBasic(clusters)
		assert.NoError(t, err)
		assert.Len(t, validator.clusterMap, 2)
		assert.NotNil(t, validator.clusterMap["cluster-1"])
		assert.NotNil(t, validator.clusterMap["cluster-2"])
	})

	t.Run("error - nil cluster", func(t *testing.T) {
		clusters := []*commonpb.MilvusCluster{
			nil,
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1"},
			},
		}

		validator := &ReplicateConfigValidator{
			clusterMap: make(map[string]*commonpb.MilvusCluster),
		}

		err := validator.validateClusterBasic(clusters)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cluster at index 0 is nil")
	})

	t.Run("error - empty cluster ID", func(t *testing.T) {
		clusters := []*commonpb.MilvusCluster{
			{
				ClusterId: "",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1"},
			},
		}

		validator := &ReplicateConfigValidator{
			clusterMap: make(map[string]*commonpb.MilvusCluster),
		}

		err := validator.validateClusterBasic(clusters)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "has empty clusterID")
	})

	t.Run("error - cluster ID with whitespace", func(t *testing.T) {
		clusters := []*commonpb.MilvusCluster{
			{
				ClusterId: "cluster 1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1"},
			},
		}

		validator := &ReplicateConfigValidator{
			clusterMap: make(map[string]*commonpb.MilvusCluster),
		}

		err := validator.validateClusterBasic(clusters)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "containing whitespace characters")
	})

	t.Run("error - nil connection param", func(t *testing.T) {
		clusters := []*commonpb.MilvusCluster{
			{
				ClusterId:       "cluster-1",
				ConnectionParam: nil,
				Pchannels:       []string{"channel-1"},
			},
		}

		validator := &ReplicateConfigValidator{
			clusterMap: make(map[string]*commonpb.MilvusCluster),
		}

		err := validator.validateClusterBasic(clusters)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "has nil connection_param")
	})

	t.Run("error - empty URI", func(t *testing.T) {
		clusters := []*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1"},
			},
		}

		validator := &ReplicateConfigValidator{
			clusterMap: make(map[string]*commonpb.MilvusCluster),
		}

		err := validator.validateClusterBasic(clusters)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "has empty URI")
	})

	t.Run("error - invalid URI format", func(t *testing.T) {
		clusters := []*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "invalid-uri-format",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1"},
			},
		}

		validator := &ReplicateConfigValidator{
			clusterMap: make(map[string]*commonpb.MilvusCluster),
		}

		err := validator.validateClusterBasic(clusters)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "has invalid URI format")
	})

	t.Run("error - empty pchannels", func(t *testing.T) {
		clusters := []*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{},
			},
		}

		validator := &ReplicateConfigValidator{
			clusterMap: make(map[string]*commonpb.MilvusCluster),
		}

		err := validator.validateClusterBasic(clusters)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "has empty pchannels")
	})

	t.Run("error - empty pchannel", func(t *testing.T) {
		clusters := []*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"", "channel-2"},
			},
		}

		validator := &ReplicateConfigValidator{
			clusterMap: make(map[string]*commonpb.MilvusCluster),
		}

		err := validator.validateClusterBasic(clusters)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "has empty pchannel at index 0")
	})

	t.Run("error - duplicate pchannel within cluster", func(t *testing.T) {
		clusters := []*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-1"},
			},
		}

		validator := &ReplicateConfigValidator{
			clusterMap: make(map[string]*commonpb.MilvusCluster),
		}

		err := validator.validateClusterBasic(clusters)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "has duplicate pchannel")
	})

	t.Run("error - inconsistent pchannel count", func(t *testing.T) {
		clusters := []*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
			{
				ClusterId: "cluster-2",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19531",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1"}, // Only 1 channel instead of 2
			},
		}

		validator := &ReplicateConfigValidator{
			clusterMap: make(map[string]*commonpb.MilvusCluster),
		}

		err := validator.validateClusterBasic(clusters)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "has 1 pchannels, but expected 2")
	})

	t.Run("error - duplicate cluster ID", func(t *testing.T) {
		clusters := []*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1"},
			},
			{
				ClusterId: "cluster-1", // Duplicate cluster ID
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19531",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1"},
			},
		}

		validator := &ReplicateConfigValidator{
			clusterMap: make(map[string]*commonpb.MilvusCluster),
		}

		err := validator.validateClusterBasic(clusters)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate clusterID found")
	})

	t.Run("error - duplicate URI across clusters", func(t *testing.T) {
		clusters := []*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1"},
			},
			{
				ClusterId: "cluster-2",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530", // Same URI as cluster-1
					Token: "test-token",
				},
				Pchannels: []string{"channel-1"},
			},
		}

		validator := &ReplicateConfigValidator{
			clusterMap: make(map[string]*commonpb.MilvusCluster),
		}

		err := validator.validateClusterBasic(clusters)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate URI found")
	})
}

func TestReplicateConfigValidator_validateRelevance(t *testing.T) {
	t.Run("success - current cluster included and pchannels match", func(t *testing.T) {
		validator := &ReplicateConfigValidator{
			currentClusterID: "cluster-1",
			currentPChannels: []string{"channel-1", "channel-2"},
			clusterMap: map[string]*commonpb.MilvusCluster{
				"cluster-1": {
					ClusterId: "cluster-1",
					Pchannels: []string{"channel-1", "channel-2"},
				},
			},
		}

		err := validator.validateRelevance()
		assert.NoError(t, err)
	})

	t.Run("error - current cluster not included", func(t *testing.T) {
		validator := &ReplicateConfigValidator{
			currentClusterID: "cluster-1",
			currentPChannels: []string{"channel-1"},
			clusterMap: map[string]*commonpb.MilvusCluster{
				"cluster-2": {
					ClusterId: "cluster-2",
					Pchannels: []string{"channel-1"},
				},
			},
		}

		err := validator.validateRelevance()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be included in the clusters list")
	})

	t.Run("error - pchannels don't match", func(t *testing.T) {
		validator := &ReplicateConfigValidator{
			currentClusterID: "cluster-1",
			currentPChannels: []string{"channel-1", "channel-2"},
			clusterMap: map[string]*commonpb.MilvusCluster{
				"cluster-1": {
					ClusterId: "cluster-1",
					Pchannels: []string{"channel-1", "channel-3"}, // Different channels
				},
			},
		}

		err := validator.validateRelevance()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "current pchannels do not match")
	})
}

func TestReplicateConfigValidator_validateTopologyEdgeUniqueness(t *testing.T) {
	validator := &ReplicateConfigValidator{
		clusterMap: map[string]*commonpb.MilvusCluster{
			"cluster-1": {ClusterId: "cluster-1"},
			"cluster-2": {ClusterId: "cluster-2"},
			"cluster-3": {ClusterId: "cluster-3"},
		},
	}

	t.Run("success - unique edges", func(t *testing.T) {
		topologies := []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: "cluster-1",
				TargetClusterId: "cluster-2",
			},
			{
				SourceClusterId: "cluster-1",
				TargetClusterId: "cluster-3",
			},
		}

		err := validator.validateTopologyEdgeUniqueness(topologies)
		assert.NoError(t, err)
	})

	t.Run("success - empty topologies", func(t *testing.T) {
		topologies := []*commonpb.CrossClusterTopology{}

		err := validator.validateTopologyEdgeUniqueness(topologies)
		assert.NoError(t, err)
	})

	t.Run("error - nil topology", func(t *testing.T) {
		topologies := []*commonpb.CrossClusterTopology{
			nil,
			{
				SourceClusterId: "cluster-1",
				TargetClusterId: "cluster-2",
			},
		}

		err := validator.validateTopologyEdgeUniqueness(topologies)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "topology at index 0 is nil")
	})

	t.Run("error - source cluster not exists", func(t *testing.T) {
		topologies := []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: "non-existent-cluster",
				TargetClusterId: "cluster-2",
			},
		}

		err := validator.validateTopologyEdgeUniqueness(topologies)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "references non-existent source cluster")
	})

	t.Run("error - target cluster not exists", func(t *testing.T) {
		topologies := []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: "cluster-1",
				TargetClusterId: "non-existent-cluster",
			},
		}

		err := validator.validateTopologyEdgeUniqueness(topologies)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "references non-existent target cluster")
	})

	t.Run("error - duplicate edge", func(t *testing.T) {
		topologies := []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: "cluster-1",
				TargetClusterId: "cluster-2",
			},
			{
				SourceClusterId: "cluster-1",
				TargetClusterId: "cluster-2", // Duplicate edge
			},
		}

		err := validator.validateTopologyEdgeUniqueness(topologies)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate topology relationship found")
	})
}

func TestReplicateConfigValidator_validateTopologyTypeConstraint(t *testing.T) {
	t.Run("success - valid star topology", func(t *testing.T) {
		validator := &ReplicateConfigValidator{
			clusterMap: map[string]*commonpb.MilvusCluster{
				"center-cluster": {ClusterId: "center-cluster"},
				"leaf-cluster-1": {ClusterId: "leaf-cluster-1"},
				"leaf-cluster-2": {ClusterId: "leaf-cluster-2"},
			},
		}

		topologies := []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: "center-cluster",
				TargetClusterId: "leaf-cluster-1",
			},
			{
				SourceClusterId: "center-cluster",
				TargetClusterId: "leaf-cluster-2",
			},
		}

		err := validator.validateTopologyTypeConstraint(topologies)
		assert.NoError(t, err)
	})

	t.Run("success - empty topologies", func(t *testing.T) {
		validator := &ReplicateConfigValidator{
			clusterMap: map[string]*commonpb.MilvusCluster{
				"cluster-1": {ClusterId: "cluster-1"},
			},
		}

		topologies := []*commonpb.CrossClusterTopology{}

		err := validator.validateTopologyTypeConstraint(topologies)
		assert.NoError(t, err)
	})

	t.Run("error - no center node", func(t *testing.T) {
		validator := &ReplicateConfigValidator{
			clusterMap: map[string]*commonpb.MilvusCluster{
				"cluster-1": {ClusterId: "cluster-1"},
				"cluster-2": {ClusterId: "cluster-2"},
			},
		}

		topologies := []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: "cluster-1",
				TargetClusterId: "cluster-2",
			},
			{
				SourceClusterId: "cluster-2",
				TargetClusterId: "cluster-1",
			},
		}

		err := validator.validateTopologyTypeConstraint(topologies)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no center node found")
	})

	t.Run("error - leaf node with wrong degrees", func(t *testing.T) {
		validator := &ReplicateConfigValidator{
			clusterMap: map[string]*commonpb.MilvusCluster{
				"center-cluster": {ClusterId: "center-cluster"},
				"leaf-cluster-1": {ClusterId: "leaf-cluster-1"},
				"leaf-cluster-2": {ClusterId: "leaf-cluster-2"},
			},
		}

		topologies := []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: "center-cluster",
				TargetClusterId: "leaf-cluster-1",
			},
			{
				SourceClusterId: "center-cluster",
				TargetClusterId: "leaf-cluster-2",
			},
			{
				SourceClusterId: "leaf-cluster-1",
				TargetClusterId: "leaf-cluster-2",
			},
		}

		err := validator.validateTopologyTypeConstraint(topologies)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not follow star topology pattern")
	})
}

func TestEqualIgnoreOrder(t *testing.T) {
	t.Run("success - same slices in different order", func(t *testing.T) {
		a := []string{"a", "b", "c"}
		b := []string{"c", "a", "b"}
		result := equalIgnoreOrder(a, b)
		assert.True(t, result)
	})

	t.Run("success - same slices in same order", func(t *testing.T) {
		a := []string{"a", "b", "c"}
		b := []string{"a", "b", "c"}
		result := equalIgnoreOrder(a, b)
		assert.True(t, result)
	})

	t.Run("success - empty slices", func(t *testing.T) {
		a := []string{}
		b := []string{}
		result := equalIgnoreOrder(a, b)
		assert.True(t, result)
	})

	t.Run("failure - different lengths", func(t *testing.T) {
		a := []string{"a", "b"}
		b := []string{"a", "b", "c"}
		result := equalIgnoreOrder(a, b)
		assert.False(t, result)
	})

	t.Run("failure - different content", func(t *testing.T) {
		a := []string{"a", "b", "c"}
		b := []string{"a", "b", "d"}
		result := equalIgnoreOrder(a, b)
		assert.False(t, result)
	})

	t.Run("failure - different counts of same elements", func(t *testing.T) {
		a := []string{"a", "a", "b"}
		b := []string{"a", "b", "b"}
		result := equalIgnoreOrder(a, b)
		assert.False(t, result)
	})
}

func TestReplicateConfigValidator_validateConfigComparison(t *testing.T) {
	// Helper function to create a config with specific clusters
	createConfigWithClusters := func(clusters []*commonpb.MilvusCluster) *commonpb.ReplicateConfiguration {
		return &commonpb.ReplicateConfiguration{
			Clusters:             clusters,
			CrossClusterTopology: []*commonpb.CrossClusterTopology{},
		}
	}

	t.Run("success - no current config", func(t *testing.T) {
		config := createValidValidatorConfig()
		currentPChannels := []string{"channel-1", "channel-2"}
		validator := NewReplicateConfigValidator(config, nil, "cluster-1", currentPChannels)

		err := validator.Validate()
		assert.NoError(t, err)
	})

	t.Run("success - new cluster added", func(t *testing.T) {
		currentConfig := createConfigWithClusters([]*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
		})

		incomingConfig := createConfigWithClusters([]*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
			{
				ClusterId: "cluster-2", // New cluster
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19531",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
		})

		validator := NewReplicateConfigValidator(incomingConfig, currentConfig, "cluster-1", []string{"channel-1", "channel-2"})
		err := validator.Validate()
		assert.NoError(t, err)
	})

	t.Run("error - ConnectionParam changed", func(t *testing.T) {
		currentConfig := createConfigWithClusters([]*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "old-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
		})

		incomingConfig := createConfigWithClusters([]*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "new-token", // Token changed - should fail
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
		})

		// Test the config comparison validation directly
		validator := &ReplicateConfigValidator{
			incomingConfig: incomingConfig,
			currentConfig:  currentConfig,
		}
		err := validator.validateConfigComparison()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection_param.token cannot be changed")
	})

	t.Run("error - pchannels changed", func(t *testing.T) {
		currentConfig := createConfigWithClusters([]*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
		})

		incomingConfig := createConfigWithClusters([]*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-3"}, // Different pchannels
			},
		})

		// Test the config comparison validation directly
		validator := &ReplicateConfigValidator{
			incomingConfig: incomingConfig,
			currentConfig:  currentConfig,
		}
		err := validator.validateConfigComparison()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pchannels cannot be changed")
	})

	t.Run("error - ConnectionParam URI changed", func(t *testing.T) {
		currentConfig := createConfigWithClusters([]*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
		})

		incomingConfig := createConfigWithClusters([]*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19531", // URI changed - should fail
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
		})

		// Test the config comparison validation directly
		validator := &ReplicateConfigValidator{
			incomingConfig: incomingConfig,
			currentConfig:  currentConfig,
		}
		err := validator.validateConfigComparison()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection_param.uri cannot be changed")
	})

	t.Run("success - same cluster with no changes", func(t *testing.T) {
		currentConfig := createConfigWithClusters([]*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
		})

		incomingConfig := createConfigWithClusters([]*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-1", // Same cluster ID
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"channel-1", "channel-2"},
			},
		})

		// Test the config comparison validation directly
		validator := &ReplicateConfigValidator{
			incomingConfig: incomingConfig,
			currentConfig:  currentConfig,
		}
		err := validator.validateConfigComparison()
		assert.NoError(t, err) // This should pass since it's the same cluster
	})
}
