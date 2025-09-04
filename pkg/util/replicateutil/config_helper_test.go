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

// createValidConfig creates a valid ReplicateConfiguration for testing
func createValidConfig() *commonpb.ReplicateConfiguration {
	return &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: "source-cluster",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"source-cluster-channel-1", "source-cluster-channel-2"},
			},
			{
				ClusterId: "target-cluster-a",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19531",
					Token: "test-token",
				},
				Pchannels: []string{"target-cluster-a-channel-1", "target-cluster-a-channel-2"},
			},
			{
				ClusterId: "target-cluster-b",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19532",
					Token: "test-token",
				},
				Pchannels: []string{"target-cluster-b-channel-1", "target-cluster-b-channel-2"},
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
		},
	}
}

// createConfigWithDifferentChannelCounts creates a config with different channel counts for edge case testing
func createConfigWithDifferentChannelCounts() *commonpb.ReplicateConfiguration {
	return &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: "source-cluster",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"source-channel-1", "source-channel-2", "source-channel-3"},
			},
			{
				ClusterId: "target-cluster-a",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "localhost:19531",
					Token: "test-token",
				},
				Pchannels: []string{"target-channel-1", "target-channel-2"},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{
				SourceClusterId: "source-cluster",
				TargetClusterId: "target-cluster-a",
			},
		},
	}
}

func TestNewConfigHelper(t *testing.T) {
	tests := []struct {
		name   string
		config *commonpb.ReplicateConfiguration
	}{
		{
			name:   "success - valid config",
			config: createValidConfig(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			helper := MustNewConfigHelper("source-cluster", tt.config)
			assert.NotNil(t, helper)
		})
	}
}

func TestConfigHelper_GetCluster(t *testing.T) {
	config := createValidConfig()
	helper := MustNewConfigHelper("source-cluster", config)

	tests := []struct {
		name      string
		clusterID string
		wantErr   bool
		expected  *commonpb.MilvusCluster
	}{
		{
			name:      "success - find source cluster",
			clusterID: "source-cluster",
			wantErr:   false,
			expected:  config.GetClusters()[0],
		},
		{
			name:      "success - find target cluster a",
			clusterID: "target-cluster-a",
			wantErr:   false,
			expected:  config.GetClusters()[1],
		},
		{
			name:      "success - find target cluster b",
			clusterID: "target-cluster-b",
			wantErr:   false,
			expected:  config.GetClusters()[2],
		},
		{
			name:      "panic - cluster not found",
			clusterID: "non-existent-cluster",
			wantErr:   true,
			expected:  nil,
		},
		{
			name:      "panic - empty cluster ID",
			clusterID: "",
			wantErr:   true,
			expected:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr {
				result := helper.GetCluster(tt.clusterID)
				assert.Nil(t, result)
			} else {
				result := helper.GetCluster(tt.clusterID)
				assert.Equal(t, tt.expected, result.MilvusCluster)
			}
		})
	}
}

func TestConfigHelper_GetSourceCluster(t *testing.T) {
	config := createValidConfig()
	helper := MustNewConfigHelper("target-cluster-a", config)

	t.Run("success - get source cluster", func(t *testing.T) {
		result := helper.GetCurrentCluster().SourceCluster()
		assert.NotNil(t, result)
		assert.Equal(t, "source-cluster", result.GetClusterId())
		assert.Equal(t, config.GetClusters()[0], result.MilvusCluster)
	})
}

func TestConfigHelper_GetTargetClusters(t *testing.T) {
	config := createValidConfig()
	helper := MustNewConfigHelper("source-cluster", config)

	t.Run("success - get target clusters", func(t *testing.T) {
		result := helper.GetCurrentCluster().TargetClusters()
		assert.Len(t, result, 2)

		// Check that we have both target clusters
		clusterIDs := make(map[string]bool)
		for _, cluster := range result {
			clusterIDs[cluster.GetClusterId()] = true
		}
		assert.True(t, clusterIDs["target-cluster-a"])
		assert.True(t, clusterIDs["target-cluster-b"])
	})
}

func TestConfigHelper_GetSourceChannel(t *testing.T) {
	config := createValidConfig()
	helper := MustNewConfigHelper("source-cluster", config)

	tests := []struct {
		name              string
		clusterID         string
		targetChannelName string
		wantErr           bool
		expected          string
	}{
		{
			name:              "success - get source channel for target cluster a channel 1",
			clusterID:         "target-cluster-a",
			targetChannelName: "target-cluster-a-channel-1",
			wantErr:           false,
			expected:          "source-cluster-channel-1",
		},
		{
			name:              "success - get source channel for target cluster a channel 2",
			clusterID:         "target-cluster-a",
			targetChannelName: "target-cluster-a-channel-2",
			wantErr:           false,
			expected:          "source-cluster-channel-2",
		},
		{
			name:              "success - get source channel for target cluster b channel 1",
			clusterID:         "target-cluster-b",
			targetChannelName: "target-cluster-b-channel-1",
			wantErr:           false,
			expected:          "source-cluster-channel-1",
		},
		{
			name:              "success - get source channel for target cluster b channel 2",
			clusterID:         "target-cluster-b",
			targetChannelName: "target-cluster-b-channel-2",
			wantErr:           false,
			expected:          "source-cluster-channel-2",
		},
		{
			name:              "panic - target channel not found",
			clusterID:         "target-cluster-a",
			targetChannelName: "non-existent-channel",
			wantErr:           true,
			expected:          "",
		},
		{
			name:              "panic - empty target channel name",
			clusterID:         "target-cluster-a",
			targetChannelName: "",
			wantErr:           true,
			expected:          "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr {
				assert.Panics(t, func() {
					helper.GetCluster(tt.clusterID).MustGetSourceChannel(tt.targetChannelName)
				})
			} else {
				result := helper.GetCluster(tt.clusterID).MustGetSourceChannel(tt.targetChannelName)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestConfigHelper_GetTargetChannel(t *testing.T) {
	config := createValidConfig()
	helper := MustNewConfigHelper("source-cluster", config)

	tests := []struct {
		name              string
		sourceChannelName string
		targetClusterID   string
		wantErr           bool
		expected          string
	}{
		{
			name:              "success - get target channel for source channel 1 in cluster a",
			sourceChannelName: "source-cluster-channel-1",
			targetClusterID:   "target-cluster-a",
			wantErr:           false,
			expected:          "target-cluster-a-channel-1",
		},
		{
			name:              "success - get target channel for source channel 2 in cluster a",
			sourceChannelName: "source-cluster-channel-2",
			targetClusterID:   "target-cluster-a",
			wantErr:           false,
			expected:          "target-cluster-a-channel-2",
		},
		{
			name:              "success - get target channel for source channel 1 in cluster b",
			sourceChannelName: "source-cluster-channel-1",
			targetClusterID:   "target-cluster-b",
			wantErr:           false,
			expected:          "target-cluster-b-channel-1",
		},
		{
			name:              "success - get target channel for source channel 2 in cluster b",
			sourceChannelName: "source-cluster-channel-2",
			targetClusterID:   "target-cluster-b",
			wantErr:           false,
			expected:          "target-cluster-b-channel-2",
		},
		{
			name:              "panic - target cluster not found",
			sourceChannelName: "source-cluster-channel-1",
			targetClusterID:   "non-existent-cluster",
			wantErr:           true,
			expected:          "",
		},
		{
			name:              "panic - source channel not found in target cluster",
			sourceChannelName: "non-existent-channel",
			targetClusterID:   "target-cluster-a",
			wantErr:           true,
			expected:          "",
		},
		{
			name:              "panic - empty source channel name",
			sourceChannelName: "",
			targetClusterID:   "target-cluster-a",
			wantErr:           true,
			expected:          "",
		},
		{
			name:              "panic - empty target cluster ID",
			sourceChannelName: "source-cluster-channel-1",
			targetClusterID:   "",
			wantErr:           true,
			expected:          "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr {
				_, err := helper.GetCurrentCluster().GetTargetChannel(tt.sourceChannelName, tt.targetClusterID)
				assert.Error(t, err)
			} else {
				result, err := helper.GetCurrentCluster().GetTargetChannel(tt.sourceChannelName, tt.targetClusterID)
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestConfigHelper_EdgeCases(t *testing.T) {
	t.Run("config with different channel counts", func(t *testing.T) {
		config := createConfigWithDifferentChannelCounts()
		// helper can not be created
		assert.Panics(t, func() {
			MustNewConfigHelper("source-cluster", config)
		})
	})

	t.Run("config with single cluster", func(t *testing.T) {
		config := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{
					ClusterId: "single-cluster",
					ConnectionParam: &commonpb.ConnectionParam{
						Uri:   "localhost:19530",
						Token: "test-token",
					},
					Pchannels: []string{"channel-1", "channel-2"},
				},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{},
		}

		helper := MustNewConfigHelper("single-cluster", config)

		// Source cluster should be nil when no topology is defined
		assert.Nil(t, helper.GetCurrentCluster().SourceCluster())

		// Target clusters should be empty
		assert.Len(t, helper.GetCurrentCluster().TargetClusters(), 0)
	})

	t.Run("config with empty clusters", func(t *testing.T) {
		config := &commonpb.ReplicateConfiguration{
			Clusters:             []*commonpb.MilvusCluster{},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{},
		}

		assert.Panics(t, func() {
			MustNewConfigHelper("source-cluster", config)
		})
	})
}

func TestConfigHelper_ChannelMappingConsistency(t *testing.T) {
	config := createValidConfig()
	helper := MustNewConfigHelper("source-cluster", config)

	t.Run("bidirectional channel mapping consistency", func(t *testing.T) {
		// Test that source -> target -> source mapping is consistent
		sourceChannel := "source-cluster-channel-1"
		targetClusterID := "target-cluster-a"

		targetChannel, err := helper.GetCurrentCluster().GetTargetChannel(sourceChannel, targetClusterID)
		assert.Equal(t, "target-cluster-a-channel-1", targetChannel)
		assert.NoError(t, err)

		// Reverse mapping should give us back the original source channel
		reverseSourceChannel := helper.GetCluster(targetClusterID).MustGetSourceChannel(targetChannel)
		assert.Equal(t, sourceChannel, reverseSourceChannel)
	})

	t.Run("all channel mappings are consistent", func(t *testing.T) {
		sourceCluster := helper.GetCurrentCluster()
		targetClusters := helper.GetCurrentCluster().TargetClusters()

		for _, targetCluster := range targetClusters {
			targetClusterID := targetCluster.GetClusterId()

			for i, sourceChannel := range sourceCluster.GetPchannels() {
				targetChannel, err := helper.GetCurrentCluster().GetTargetChannel(sourceChannel, targetClusterID)
				assert.NoError(t, err)

				// Verify the reverse mapping
				reverseSourceChannel := helper.GetCluster(targetClusterID).MustGetSourceChannel(targetChannel)
				assert.Equal(t, sourceChannel, reverseSourceChannel,
					"Channel mapping inconsistency for cluster %s, channel index %d", targetClusterID, i)
			}
		}
	})
}
