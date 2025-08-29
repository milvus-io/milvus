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

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

func TestReplicateManager_CreateReplicator(t *testing.T) {
	manager := NewReplicateManager()

	// Test creating first replicator
	replicateInfo := &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "test-source-channel-1",
		TargetChannelName: "test-target-channel-1",
		TargetCluster: &commonpb.MilvusCluster{
			ClusterId: "test-cluster-1",
		},
	}

	manager.CreateReplicator(replicateInfo)

	// Verify replicator was created
	assert.Equal(t, 1, len(manager.replicators))
	replicator, exists := manager.replicators["test-source-channel-1"]
	assert.True(t, exists)
	assert.NotNil(t, replicator)

	// Test creating second replicator
	replicateInfo2 := &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "test-source-channel-2",
		TargetChannelName: "test-target-channel-2",
		TargetCluster: &commonpb.MilvusCluster{
			ClusterId: "test-cluster-2",
		},
	}

	manager.CreateReplicator(replicateInfo2)

	// Verify second replicator was created
	assert.Equal(t, 2, len(manager.replicators))
	replicator2, exists := manager.replicators["test-source-channel-2"]
	assert.True(t, exists)
	assert.NotNil(t, replicator2)

	// Verify first replicator still exists
	replicator1, exists := manager.replicators["test-source-channel-1"]
	assert.True(t, exists)
	assert.NotNil(t, replicator1)
}
