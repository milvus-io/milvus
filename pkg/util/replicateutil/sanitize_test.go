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
