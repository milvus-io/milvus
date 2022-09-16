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

package querynode

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/api/schemapb"
)

func genSimpleQueryShard(ctx context.Context) (*queryShard, error) {
	tSafe := newTSafeReplica()
	replica, err := genSimpleReplica()
	if err != nil {
		return nil, err
	}
	tSafe.addTSafe(defaultDMLChannel)
	tSafe.addTSafe(defaultDeltaChannel)
	localCM, err := genLocalChunkManager()
	if err != nil {
		return nil, err
	}

	remoteCM, err := genRemoteChunkManager(ctx)
	if err != nil {
		return nil, err
	}

	shardCluster := NewShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel,
		&mockNodeDetector{}, &mockSegmentDetector{}, buildMockQueryNode)
	shardClusterService := &ShardClusterService{
		clusters: sync.Map{},
	}
	shardClusterService.clusters.Store(defaultDMLChannel, shardCluster)

	qs, err := newQueryShard(ctx, defaultCollectionID, defaultDMLChannel, defaultReplicaID, shardClusterService,
		replica, tSafe, localCM, remoteCM, false)
	if err != nil {
		return nil, err
	}
	qs.deltaChannel = defaultDeltaChannel
	return qs, nil
}

func updateQueryShardTSafe(qs *queryShard, timestamp Timestamp) error {
	err := qs.tSafeReplica.setTSafe(defaultDMLChannel, timestamp)
	if err != nil {
		return err
	}
	return qs.tSafeReplica.setTSafe(defaultDeltaChannel, timestamp)
}

func TestNewQueryShard_IllegalCases(t *testing.T) {
	ctx := context.Background()
	tSafe := newTSafeReplica()
	replica, err := genSimpleReplica()
	require.NoError(t, err)

	localCM, err := genLocalChunkManager()
	require.NoError(t, err)

	remoteCM, err := genRemoteChunkManager(ctx)
	require.NoError(t, err)

	shardCluster := NewShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel,
		&mockNodeDetector{}, &mockSegmentDetector{}, buildMockQueryNode)
	shardClusterService := &ShardClusterService{
		clusters: sync.Map{},
	}
	shardClusterService.clusters.Store(defaultDMLChannel, shardCluster)

	_, err = newQueryShard(ctx, defaultCollectionID-1, defaultDMLChannel, defaultReplicaID, shardClusterService,
		replica, tSafe, localCM, remoteCM, false)
	assert.Error(t, err)

	_, err = newQueryShard(ctx, defaultCollectionID, defaultDMLChannel, defaultReplicaID, shardClusterService,
		replica, tSafe, nil, remoteCM, false)
	assert.Error(t, err)

	_, err = newQueryShard(ctx, defaultCollectionID, defaultDMLChannel, defaultReplicaID, shardClusterService,
		replica, tSafe, localCM, nil, false)
	assert.Error(t, err)
}

func TestQueryShard_Close(t *testing.T) {
	qs, err := genSimpleQueryShard(context.Background())
	assert.NoError(t, err)

	qs.Close()
}

func TestQueryShard_getServiceableTime(t *testing.T) {
	qs, err := genSimpleQueryShard(context.Background())
	assert.NoError(t, err)

	timestamp := Timestamp(1000)
	err = updateQueryShardTSafe(qs, timestamp)
	assert.NoError(t, err)

	dmlTimestamp, err := qs.getServiceableTime(qs.channel)
	assert.NoError(t, err)
	assert.Equal(t, timestamp, dmlTimestamp)

	deltaTimestamp, err := qs.getServiceableTime(qs.deltaChannel)
	assert.NoError(t, err)
	assert.Equal(t, timestamp, deltaTimestamp)
}

func genSearchResultData(nq int64, topk int64, ids []int64, scores []float32, topks []int64) *schemapb.SearchResultData {
	return &schemapb.SearchResultData{
		NumQueries: 1,
		TopK:       topk,
		FieldsData: nil,
		Scores:     scores,
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: ids,
				},
			},
		},
		Topks: topks,
	}
}
