package replicateutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

// createValidConfig creates a valid ReplicateConfiguration for testing
func createValidConfig() *milvuspb.ReplicateConfiguration {
	return &milvuspb.ReplicateConfiguration{
		Clusters: []*milvuspb.MilvusCluster{
			{
				ClusterId: "source-cluster",
				ConnectionParam: &milvuspb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"source-cluster-channel-1", "source-cluster-channel-2"},
			},
			{
				ClusterId: "target-cluster-a",
				ConnectionParam: &milvuspb.ConnectionParam{
					Uri:   "localhost:19531",
					Token: "test-token",
				},
				Pchannels: []string{"target-cluster-a-channel-1", "target-cluster-a-channel-2"},
			},
			{
				ClusterId: "target-cluster-b",
				ConnectionParam: &milvuspb.ConnectionParam{
					Uri:   "localhost:19532",
					Token: "test-token",
				},
				Pchannels: []string{"target-cluster-b-channel-1", "target-cluster-b-channel-2"},
			},
		},
		CrossClusterTopology: []*milvuspb.CrossClusterTopology{
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

func TestGetMilvusCluster(t *testing.T) {
	config := createValidConfig()

	tests := []struct {
		name      string
		clusterID string
		config    *milvuspb.ReplicateConfiguration
		wantErr   bool
		expected  *milvuspb.MilvusCluster
	}{
		{
			name:      "success - find source cluster",
			clusterID: "source-cluster",
			config:    config,
			wantErr:   false,
			expected:  config.GetClusters()[0],
		},
		{
			name:      "success - find target cluster a",
			clusterID: "target-cluster-a",
			config:    config,
			wantErr:   false,
			expected:  config.GetClusters()[1],
		},
		{
			name:      "error - cluster not found",
			clusterID: "non-existent-cluster",
			config:    config,
			wantErr:   true,
			expected:  nil,
		},
		{
			name:      "error - empty cluster ID",
			clusterID: "",
			config:    config,
			wantErr:   true,
			expected:  nil,
		},
		{
			name:      "error - nil config",
			clusterID: "source-cluster",
			config:    nil,
			wantErr:   true,
			expected:  nil,
		},
		{
			name:      "error - empty clusters",
			clusterID: "source-cluster",
			config:    &milvuspb.ReplicateConfiguration{},
			wantErr:   true,
			expected:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetMilvusCluster(tt.clusterID, tt.config)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, result)
				if tt.config != nil {
					assert.Contains(t, err.Error(), tt.clusterID)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestMustGetMilvusCluster(t *testing.T) {
	config := createValidConfig()

	tests := []struct {
		name        string
		clusterID   string
		config      *milvuspb.ReplicateConfiguration
		shouldPanic bool
		expected    *milvuspb.MilvusCluster
	}{
		{
			name:        "success - find source cluster",
			clusterID:   "source-cluster",
			config:      config,
			shouldPanic: false,
			expected:    config.GetClusters()[0],
		},
		{
			name:        "success - find target cluster b",
			clusterID:   "target-cluster-b",
			config:      config,
			shouldPanic: false,
			expected:    config.GetClusters()[2],
		},
		{
			name:        "panic - cluster not found",
			clusterID:   "non-existent-cluster",
			config:      config,
			shouldPanic: true,
			expected:    nil,
		},
		{
			name:        "panic - empty cluster ID",
			clusterID:   "",
			config:      config,
			shouldPanic: true,
			expected:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.shouldPanic {
				assert.Panics(t, func() {
					MustGetMilvusCluster(tt.clusterID, tt.config)
				})
			} else {
				result := MustGetMilvusCluster(tt.clusterID, tt.config)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestMustGetTargetChannelName(t *testing.T) {
	config := createValidConfig()

	tests := []struct {
		name              string
		targetClusterID   string
		sourceChannelName string
		config            *milvuspb.ReplicateConfiguration
		shouldPanic       bool
		expectedChannel   string
	}{
		{
			name:              "success - get target channel for source channel 1",
			targetClusterID:   "target-cluster-a",
			sourceChannelName: "source-cluster-channel-1",
			config:            config,
			shouldPanic:       false,
			expectedChannel:   "target-cluster-a-channel-1",
		},
		{
			name:              "success - get target channel for source channel 2",
			targetClusterID:   "target-cluster-b",
			sourceChannelName: "source-cluster-channel-2",
			config:            config,
			shouldPanic:       false,
			expectedChannel:   "target-cluster-b-channel-2",
		},
		{
			name:              "panic - source channel not found",
			targetClusterID:   "target-cluster-a",
			sourceChannelName: "non-existent-channel",
			config:            config,
			shouldPanic:       true,
			expectedChannel:   "",
		},
		{
			name:              "panic - target cluster not found",
			targetClusterID:   "non-existent-target",
			sourceChannelName: "source-cluster-channel-1",
			config:            config,
			shouldPanic:       true,
			expectedChannel:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.shouldPanic {
				assert.Panics(t, func() {
					MustGetTargetChannelName(tt.targetClusterID, tt.sourceChannelName, tt.config)
				})
			} else {
				result := MustGetTargetChannelName(tt.targetClusterID, tt.sourceChannelName, tt.config)
				assert.Equal(t, tt.expectedChannel, result)
			}
		})
	}
}

func TestMustGetSourceChannelName(t *testing.T) {
	config := createValidConfig()

	tests := []struct {
		name              string
		sourceClusterID   string
		targetChannelName string
		config            *milvuspb.ReplicateConfiguration
		shouldPanic       bool
		expectedChannel   string
	}{
		{
			name:              "success - get source channel for target channel 1",
			sourceClusterID:   "source-cluster",
			targetChannelName: "target-cluster-a-channel-1",
			config:            config,
			shouldPanic:       false,
			expectedChannel:   "source-cluster-channel-1",
		},
		{
			name:              "success - get source channel for target channel 2",
			sourceClusterID:   "source-cluster",
			targetChannelName: "target-cluster-b-channel-2",
			config:            config,
			shouldPanic:       false,
			expectedChannel:   "source-cluster-channel-2",
		},
		{
			name:              "panic - target channel not found",
			sourceClusterID:   "source-cluster",
			targetChannelName: "non-existent-channel",
			config:            config,
			shouldPanic:       true,
			expectedChannel:   "",
		},
		{
			name:              "panic - source cluster not found",
			sourceClusterID:   "non-existent-source",
			targetChannelName: "target-cluster-a-channel-1",
			config:            config,
			shouldPanic:       true,
			expectedChannel:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.shouldPanic {
				assert.Panics(t, func() {
					MustGetSourceChannelName(tt.sourceClusterID, tt.targetChannelName, tt.config)
				})
			} else {
				result := MustGetSourceChannelName(tt.sourceClusterID, tt.targetChannelName, tt.config)
				assert.Equal(t, tt.expectedChannel, result)
			}
		})
	}
}

// Test edge cases and boundary conditions
func TestEdgeCases(t *testing.T) {
	t.Run("empty cluster ID", func(t *testing.T) {
		config := createValidConfig()
		_, err := GetMilvusCluster("", config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cluster  not found")
	})

	t.Run("whitespace cluster ID", func(t *testing.T) {
		config := createValidConfig()
		_, err := GetMilvusCluster("   ", config)
		assert.Error(t, err)
		// Just check that the error contains the cluster ID and the basic message
		assert.Contains(t, err.Error(), "cluster")
		assert.Contains(t, err.Error(), "not found in replicate configuration")
	})

	t.Run("very long cluster ID", func(t *testing.T) {
		config := createValidConfig()
		longID := string(make([]byte, 1000))
		for i := range longID {
			longID = longID[:i] + "a" + longID[i+1:]
		}
		_, err := GetMilvusCluster(longID, config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), longID)
	})
}

// Test panic functions with various invalid inputs
func TestPanicFunctions(t *testing.T) {
	t.Run("MustGetMilvusCluster with nil config", func(t *testing.T) {
		assert.Panics(t, func() {
			MustGetMilvusCluster("test", nil)
		})
	})

	t.Run("MustGetMilvusCluster with empty clusters", func(t *testing.T) {
		emptyConfig := &milvuspb.ReplicateConfiguration{}
		assert.Panics(t, func() {
			MustGetMilvusCluster("test", emptyConfig)
		})
	})

	t.Run("MustGetTargetChannelName with invalid source channel", func(t *testing.T) {
		config := createValidConfig()
		assert.Panics(t, func() {
			MustGetTargetChannelName("target-cluster-a", "invalid-channel", config)
		})
	})

	t.Run("MustGetSourceChannelName with invalid target channel", func(t *testing.T) {
		config := createValidConfig()
		assert.Panics(t, func() {
			MustGetSourceChannelName("source-cluster", "invalid-channel", config)
		})
	})
}

// Test with single cluster configuration
func TestSingleClusterConfig(t *testing.T) {
	singleClusterConfig := &milvuspb.ReplicateConfiguration{
		Clusters: []*milvuspb.MilvusCluster{
			{
				ClusterId: "single-cluster",
				ConnectionParam: &milvuspb.ConnectionParam{
					Uri:   "localhost:19530",
					Token: "test-token",
				},
				Pchannels: []string{"single-cluster-channel-1", "single-cluster-channel-2"},
			},
		},
		CrossClusterTopology: []*milvuspb.CrossClusterTopology{},
	}

	// Test GetMilvusCluster
	cluster, err := GetMilvusCluster("single-cluster", singleClusterConfig)
	assert.NoError(t, err)
	assert.Equal(t, "single-cluster", cluster.GetClusterId())

	// Test MustGetMilvusCluster
	cluster = MustGetMilvusCluster("single-cluster", singleClusterConfig)
	assert.Equal(t, "single-cluster", cluster.GetClusterId())
}
