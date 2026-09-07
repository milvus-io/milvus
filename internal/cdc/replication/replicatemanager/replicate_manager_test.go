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
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/cdc/cluster"
	"github.com/milvus-io/milvus/internal/cdc/meta"
	"github.com/milvus-io/milvus/internal/cdc/replication/replicatestream"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type testReplicator struct {
	stopped bool
}

func (*testReplicator) StartReplication() {}

func (r *testReplicator) StopReplication() {
	r.stopped = true
}

func newTestReplicateChannel(key string, revision int64, source, target string) *meta.ReplicateChannel {
	return &meta.ReplicateChannel{
		Key:         key,
		ModRevision: revision,
		Value: &streamingpb.ReplicatePChannelMeta{
			SourceChannelName: source,
			TargetChannelName: target,
			TargetCluster: &commonpb.MilvusCluster{
				ClusterId: "test-target-cluster",
			},
		},
	}
}

func TestReplicateManager_CreateReplicator(t *testing.T) {
	paramtable.Get().Save(paramtable.Get().CommonCfg.ClusterPrefix.Key, "test-source")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.ClusterPrefix.Key)

	mockMilvusClient := cluster.NewMockMilvusClient(t)
	mockMilvusClient.EXPECT().GetReplicateInfo(mock.Anything, mock.Anything).
		Return(nil, assert.AnError).Maybe()
	mockMilvusClient.EXPECT().Close(mock.Anything).Return(nil).Maybe()

	manager := NewReplicateManager()

	// Test creating first replicator
	replicateInfo := &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "test-source-channel-1",
		TargetChannelName: "test-target-channel-1",
		TargetCluster: &commonpb.MilvusCluster{
			ClusterId: "test-cluster-1",
		},
	}
	key := "test-replicate-key-1"
	replicateMeta := &meta.ReplicateChannel{
		Key:         key,
		Value:       replicateInfo,
		ModRevision: 0,
	}

	manager.CreateReplicator(replicateMeta)

	// Verify replicator was created
	assert.Equal(t, 1, len(manager.replicators))
	replicator, exists := manager.replicators[buildReplicatorKey(key, 0)]
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
	key2 := "test-replicate-key-2"
	replicateMeta2 := &meta.ReplicateChannel{
		Key:         key2,
		Value:       replicateInfo2,
		ModRevision: 0,
	}

	manager.CreateReplicator(replicateMeta2)

	// Verify second replicator was created
	assert.Equal(t, 2, len(manager.replicators))
	replicator2, exists := manager.replicators[buildReplicatorKey(key2, 0)]
	assert.True(t, exists)
	assert.NotNil(t, replicator2)

	// Verify first replicator still exists
	replicator1, exists := manager.replicators[buildReplicatorKey(key, 0)]
	assert.True(t, exists)
	assert.NotNil(t, replicator1)
}

func TestReplicateManagerRemoveReplicatorDeletesLagSeries(t *testing.T) {
	source, target := "remove-source", "remove-target"
	channel := newTestReplicateChannel("remove-key", 10, source, target)
	manager := NewReplicateManager()
	repKey := buildReplicatorKey(channel.Key, channel.ModRevision)
	replicator := &testReplicator{}
	manager.replicators[repKey] = replicator
	manager.replicatorChannels[repKey] = channel
	replicatestream.InitLastReplicatedTimeTick(
		channel.Value,
		tsoutil.ComposeTSByTime(time.Now()),
	)

	manager.RemoveReplicator(channel.Key, channel.ModRevision)

	assert.True(t, replicator.stopped)
	assert.False(t, metrics.CDCLastReplicatedTimeTick.DeleteLabelValues(source, target))
}

func TestRemoveOutdatedReplicatorsKeepsLiveLagSeries(t *testing.T) {
	source, target := "replace-source", "replace-target"
	oldChannel := newTestReplicateChannel("replace-key", 10, source, target)
	newChannel := newTestReplicateChannel("replace-key", 11, source, target)
	manager := NewReplicateManager()
	oldKey := buildReplicatorKey(oldChannel.Key, oldChannel.ModRevision)
	oldReplicator := &testReplicator{}
	manager.replicators[oldKey] = oldReplicator
	manager.replicatorChannels[oldKey] = oldChannel

	newValue := tsoutil.ComposeTSByTime(time.Now())
	replicatestream.InitLastReplicatedTimeTick(newChannel.Value, newValue)
	defer metrics.CDCLastReplicatedTimeTick.DeleteLabelValues(source, target)

	manager.RemoveOutdatedReplicators([]*meta.ReplicateChannel{newChannel})

	assert.True(t, oldReplicator.stopped)
	got := testutil.ToFloat64(metrics.CDCLastReplicatedTimeTick.WithLabelValues(source, target))
	assert.InDelta(t, tsoutil.PhysicalTimeSeconds(newValue), got, 1)
}
