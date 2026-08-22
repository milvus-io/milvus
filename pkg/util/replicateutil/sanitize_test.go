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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

func TestSanitizeReplicateConfiguration(t *testing.T) {
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: "cluster1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "http://cluster1:19530",
					Token: "secret-token-1",
				},
				Pchannels: []string{"channel1"},
			},
			{
				ClusterId: "cluster2",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "http://cluster2:19530",
					Token: "secret-token-2",
				},
				Pchannels: []string{"channel2"},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "cluster1", TargetClusterId: "cluster2"},
		},
	}

	sanitized := SanitizeReplicateConfiguration(config)

	// Tokens should be empty
	assert.Empty(t, sanitized.Clusters[0].ConnectionParam.Token)
	assert.Empty(t, sanitized.Clusters[1].ConnectionParam.Token)

	// URIs should be preserved
	assert.Equal(t, "http://cluster1:19530", sanitized.Clusters[0].ConnectionParam.Uri)
	assert.Equal(t, "http://cluster2:19530", sanitized.Clusters[1].ConnectionParam.Uri)

	// Topology should be preserved
	assert.Len(t, sanitized.CrossClusterTopology, 1)
	assert.Equal(t, "cluster1", sanitized.CrossClusterTopology[0].SourceClusterId)

	// Original should be unchanged
	assert.Equal(t, "secret-token-1", config.Clusters[0].ConnectionParam.Token)
}

func TestSanitizeReplicateConfiguration_NilInput(t *testing.T) {
	sanitized := SanitizeReplicateConfiguration(nil)
	assert.Nil(t, sanitized)
}

func TestSanitizeReplicateConfiguration_NilConnectionParam(t *testing.T) {
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId:       "cluster1",
				ConnectionParam: nil,
				Pchannels:       []string{"channel1"},
			},
		},
	}

	sanitized := SanitizeReplicateConfiguration(config)
	assert.Nil(t, sanitized.Clusters[0].ConnectionParam)
}

func TestSanitizeReplicateConfiguration_EmptyClusters(t *testing.T) {
	config := &commonpb.ReplicateConfiguration{
		Clusters:             []*commonpb.MilvusCluster{},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{},
	}

	sanitized := SanitizeReplicateConfiguration(config)
	assert.NotNil(t, sanitized)
	assert.Empty(t, sanitized.Clusters)
	assert.Empty(t, sanitized.CrossClusterTopology)
}

func TestSanitizeReplicateConfiguration_EmptyToken(t *testing.T) {
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: "cluster1",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "http://cluster1:19530",
					Token: "",
				},
				Pchannels: []string{"channel1"},
			},
		},
	}

	sanitized := SanitizeReplicateConfiguration(config)
	assert.Empty(t, sanitized.Clusters[0].ConnectionParam.Token)
	assert.Equal(t, "http://cluster1:19530", sanitized.Clusters[0].ConnectionParam.Uri)
}

func TestSanitizeReplicateConfiguration_PreservesClusterIDs(t *testing.T) {
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId: "cluster-a",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "http://a:19530",
					Token: "token-a",
				},
				Pchannels: []string{"a-ch1", "a-ch2"},
			},
			{
				ClusterId: "cluster-b",
				ConnectionParam: &commonpb.ConnectionParam{
					Uri:   "http://b:19530",
					Token: "token-b",
				},
				Pchannels: []string{"b-ch1"},
			},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "cluster-a", TargetClusterId: "cluster-b"},
		},
	}

	sanitized := SanitizeReplicateConfiguration(config)
	assert.Len(t, sanitized.Clusters, 2)
	assert.Equal(t, "cluster-a", sanitized.Clusters[0].ClusterId)
	assert.Equal(t, "cluster-b", sanitized.Clusters[1].ClusterId)
	assert.Equal(t, []string{"a-ch1", "a-ch2"}, sanitized.Clusters[0].Pchannels)
	assert.Equal(t, []string{"b-ch1"}, sanitized.Clusters[1].Pchannels)

	// Tokens are cleared
	assert.Empty(t, sanitized.Clusters[0].ConnectionParam.Token)
	assert.Empty(t, sanitized.Clusters[1].ConnectionParam.Token)

	// Original tokens unchanged
	assert.Equal(t, "token-a", config.Clusters[0].ConnectionParam.Token)
	assert.Equal(t, "token-b", config.Clusters[1].ConnectionParam.Token)
}

func TestFillRedactedConnectionTokens(t *testing.T) {
	current := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId:       "cluster-1",
				ConnectionParam: &commonpb.ConnectionParam{Uri: "localhost:19530", Token: "secret-token-1"},
				Pchannels:       []string{"channel-1"},
			},
			{
				ClusterId:       "cluster-2",
				ConnectionParam: &commonpb.ConnectionParam{Uri: "localhost:19531", Token: "secret-token-2"},
				Pchannels:       []string{"channel-1"},
			},
		},
	}

	t.Run("redacted tokens are taken from the stored configuration", func(t *testing.T) {
		incoming := SanitizeReplicateConfiguration(current)
		filled := FillRedactedConnectionTokens(incoming, current)
		assert.Equal(t, "secret-token-1", filled.Clusters[0].ConnectionParam.Token)
		assert.Equal(t, "secret-token-2", filled.Clusters[1].ConnectionParam.Token)
		// The caller's configuration is not modified.
		assert.Empty(t, incoming.Clusters[0].ConnectionParam.Token)
	})

	t.Run("a token the caller supplied is left alone", func(t *testing.T) {
		incoming := SanitizeReplicateConfiguration(current)
		incoming.Clusters[0].ConnectionParam.Token = "a-different-token"
		filled := FillRedactedConnectionTokens(incoming, current)
		assert.Equal(t, "a-different-token", filled.Clusters[0].ConnectionParam.Token)
		assert.Equal(t, "secret-token-2", filled.Clusters[1].ConnectionParam.Token)
	})

	t.Run("a cluster that is not in the stored configuration is left alone", func(t *testing.T) {
		incoming := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{
					ClusterId:       "cluster-3",
					ConnectionParam: &commonpb.ConnectionParam{Uri: "localhost:19532"},
				},
			},
		}
		filled := FillRedactedConnectionTokens(incoming, current)
		assert.Empty(t, filled.Clusters[0].ConnectionParam.Token)
	})

	t.Run("nil arguments and empty stored tokens are passed through", func(t *testing.T) {
		assert.Nil(t, FillRedactedConnectionTokens(nil, current))
		incoming := SanitizeReplicateConfiguration(current)
		assert.Same(t, incoming, FillRedactedConnectionTokens(incoming, nil))
		assert.Same(t, incoming, FillRedactedConnectionTokens(incoming, SanitizeReplicateConfiguration(current)))
	})

	t.Run("a nil connection param is left alone", func(t *testing.T) {
		incoming := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{{ClusterId: "cluster-1"}},
		}
		filled := FillRedactedConnectionTokens(incoming, current)
		assert.Nil(t, filled.Clusters[0].ConnectionParam)
	})
}

// A configuration that was read back from the cluster must be writable again.
// The read redacts the connection tokens, and the validator requires connection
// parameters to be unchanged, so the two together used to make the
// read-modify-write round trip impossible.
func TestValidateConfigurationReadBackFromTheCluster(t *testing.T) {
	current := createValidValidatorConfig()
	pchannels := []string{"channel-1", "channel-2"}

	t.Run("rejected without the tokens being restored", func(t *testing.T) {
		incoming := SanitizeReplicateConfiguration(current)
		err := NewReplicateConfigValidator(incoming, current, "cluster-1", pchannels).Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection_param.token cannot be changed")
	})

	t.Run("accepted once the tokens are restored", func(t *testing.T) {
		incoming := FillRedactedConnectionTokens(SanitizeReplicateConfiguration(current), current)
		assert.NoError(t, NewReplicateConfigValidator(incoming, current, "cluster-1", pchannels).Validate())
	})

	t.Run("the topology can be edited on the round trip", func(t *testing.T) {
		incoming := FillRedactedConnectionTokens(SanitizeReplicateConfiguration(current), current)
		incoming.CrossClusterTopology = nil
		assert.NoError(t, NewReplicateConfigValidator(incoming, current, "cluster-1", pchannels).Validate())
	})

	t.Run("changing a token is still rejected", func(t *testing.T) {
		incoming := FillRedactedConnectionTokens(SanitizeReplicateConfiguration(current), current)
		incoming.Clusters[0].ConnectionParam.Token = "a-new-token"
		err := NewReplicateConfigValidator(incoming, current, "cluster-1", pchannels).Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection_param.token cannot be changed")
	})
}
