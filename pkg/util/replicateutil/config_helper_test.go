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
		{
			name:   "success - config with different channel counts",
			config: createConfigWithDifferentChannelCounts(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			helper := NewConfigHelper(tt.config)
			assert.NotNil(t, helper)
			assert.Implements(t, (*ConfigHelper)(nil), helper)
		})
	}
}

func TestConfigHelper_GetCluster(t *testing.T) {
	config := createValidConfig()
	helper := NewConfigHelper(config)

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
				assert.Panics(t, func() {
					helper.GetCluster(tt.clusterID)
				})
			} else {
				result := helper.GetCluster(tt.clusterID)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestConfigHelper_GetSourceCluster(t *testing.T) {
	config := createValidConfig()
	helper := NewConfigHelper(config)

	t.Run("success - get source cluster", func(t *testing.T) {
		result := helper.GetSourceCluster()
		assert.NotNil(t, result)
		assert.Equal(t, "source-cluster", result.GetClusterId())
		assert.Equal(t, config.GetClusters()[0], result)
	})
}

func TestConfigHelper_GetTargetClusters(t *testing.T) {
	config := createValidConfig()
	helper := NewConfigHelper(config)

	t.Run("success - get target clusters", func(t *testing.T) {
		result := helper.GetTargetClusters()
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
	helper := NewConfigHelper(config)

	tests := []struct {
		name              string
		targetChannelName string
		wantErr           bool
		expected          string
	}{
		{
			name:              "success - get source channel for target cluster a channel 1",
			targetChannelName: "target-cluster-a-channel-1",
			wantErr:           false,
			expected:          "source-cluster-channel-1",
		},
		{
			name:              "success - get source channel for target cluster a channel 2",
			targetChannelName: "target-cluster-a-channel-2",
			wantErr:           false,
			expected:          "source-cluster-channel-2",
		},
		{
			name:              "success - get source channel for target cluster b channel 1",
			targetChannelName: "target-cluster-b-channel-1",
			wantErr:           false,
			expected:          "source-cluster-channel-1",
		},
		{
			name:              "success - get source channel for target cluster b channel 2",
			targetChannelName: "target-cluster-b-channel-2",
			wantErr:           false,
			expected:          "source-cluster-channel-2",
		},
		{
			name:              "panic - target channel not found",
			targetChannelName: "non-existent-channel",
			wantErr:           true,
			expected:          "",
		},
		{
			name:              "panic - empty target channel name",
			targetChannelName: "",
			wantErr:           true,
			expected:          "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr {
				assert.Panics(t, func() {
					helper.GetSourceChannel(tt.targetChannelName)
				})
			} else {
				result := helper.GetSourceChannel(tt.targetChannelName)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestConfigHelper_GetTargetChannel(t *testing.T) {
	config := createValidConfig()
	helper := NewConfigHelper(config)

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
				assert.Panics(t, func() {
					helper.GetTargetChannel(tt.sourceChannelName, tt.targetClusterID)
				})
			} else {
				result := helper.GetTargetChannel(tt.sourceChannelName, tt.targetClusterID)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestConfigHelper_EdgeCases(t *testing.T) {
	t.Run("config with different channel counts", func(t *testing.T) {
		config := createConfigWithDifferentChannelCounts()
		helper := NewConfigHelper(config)

		// Test that the helper handles mismatched channel counts gracefully
		// The current implementation will panic if channel counts don't match
		// This test documents the current behavior
		assert.Panics(t, func() {
			helper.GetTargetChannel("source-channel-3", "target-cluster-a")
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

		helper := NewConfigHelper(config)

		// Source cluster should be nil when no topology is defined
		assert.Nil(t, helper.GetSourceCluster())

		// Target clusters should be empty
		assert.Len(t, helper.GetTargetClusters(), 0)
	})

	t.Run("config with empty clusters", func(t *testing.T) {
		config := &commonpb.ReplicateConfiguration{
			Clusters:             []*commonpb.MilvusCluster{},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{},
		}

		helper := NewConfigHelper(config)

		// Source cluster should be nil
		assert.Nil(t, helper.GetSourceCluster())

		// Target clusters should be empty
		assert.Len(t, helper.GetTargetClusters(), 0)
	})
}

func TestConfigHelper_ChannelMappingConsistency(t *testing.T) {
	config := createValidConfig()
	helper := NewConfigHelper(config)

	t.Run("bidirectional channel mapping consistency", func(t *testing.T) {
		// Test that source -> target -> source mapping is consistent
		sourceChannel := "source-cluster-channel-1"
		targetClusterID := "target-cluster-a"

		targetChannel := helper.GetTargetChannel(sourceChannel, targetClusterID)
		assert.Equal(t, "target-cluster-a-channel-1", targetChannel)

		// Reverse mapping should give us back the original source channel
		reverseSourceChannel := helper.GetSourceChannel(targetChannel)
		assert.Equal(t, sourceChannel, reverseSourceChannel)
	})

	t.Run("all channel mappings are consistent", func(t *testing.T) {
		sourceCluster := helper.GetSourceCluster()
		targetClusters := helper.GetTargetClusters()

		for _, targetCluster := range targetClusters {
			targetClusterID := targetCluster.GetClusterId()

			for i, sourceChannel := range sourceCluster.GetPchannels() {
				targetChannel := helper.GetTargetChannel(sourceChannel, targetClusterID)

				// Verify the reverse mapping
				reverseSourceChannel := helper.GetSourceChannel(targetChannel)
				assert.Equal(t, sourceChannel, reverseSourceChannel,
					"Channel mapping inconsistency for cluster %s, channel index %d", targetClusterID, i)
			}
		}
	})
}

func TestConfigHelper_PanicMessages(t *testing.T) {
	config := createValidConfig()
	helper := NewConfigHelper(config)

	t.Run("panic message for cluster not found", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				panicMsg := r.(string)
				assert.Contains(t, panicMsg, "cluster non-existent-cluster not found")
			}
		}()
		helper.GetCluster("non-existent-cluster")
	})

	t.Run("panic message for source channel not found", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				panicMsg := r.(string)
				assert.Contains(t, panicMsg, "source channel not found for target channel non-existent-channel")
			}
		}()
		helper.GetSourceChannel("non-existent-channel")
	})

	t.Run("panic message for target cluster not found", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				panicMsg := r.(string)
				assert.Contains(t, panicMsg, "target cluster non-existent-cluster not found")
			}
		}()
		helper.GetTargetChannel("source-cluster-channel-1", "non-existent-cluster")
	})

	t.Run("panic message for target channel not found", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				panicMsg := r.(string)
				assert.Contains(t, panicMsg, "target channel not found for source channel non-existent-channel, target cluster target-cluster-a")
			}
		}()
		helper.GetTargetChannel("non-existent-channel", "target-cluster-a")
	})
}
