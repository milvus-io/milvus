package querynode

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

func TestShardClusterService(t *testing.T) {
	client := v3client.New(embedetcdServer.Server)
	defer client.Close()
	session := sessionutil.NewSession(context.Background(), "/by-dev/sessions/unittest/querynode/", client)
	clusterService := newShardClusterService(client, session, nil)

	assert.NotPanics(t, func() {
		clusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel, defaultVersion)
	})

	shardCluster, ok := clusterService.getShardCluster(defaultDMLChannel)
	assert.True(t, ok)
	assert.NotNil(t, shardCluster)

	_, ok = clusterService.getShardCluster("non-exist-channel")
	assert.False(t, ok)

	clusterService.releaseShardCluster(defaultDMLChannel)
	shardCluster, ok = clusterService.getShardCluster(defaultDMLChannel)
	assert.False(t, ok)
	assert.Nil(t, shardCluster)
}

func TestShardClusterService_SyncReplicaSegments(t *testing.T) {
	qn, err := genSimpleQueryNode(context.Background())
	require.NoError(t, err)
	defer qn.Stop()

	client := v3client.New(embedetcdServer.Server)
	defer client.Close()
	session := sessionutil.NewSession(context.Background(), "/by-dev/sessions/unittest/querynode/", client)
	clusterService := newShardClusterService(client, session, qn)

	t.Run("sync non-exist shard cluster", func(t *testing.T) {
		err := clusterService.SyncReplicaSegments(defaultDMLChannel, nil)
		assert.Error(t, err)
	})

	t.Run("sync initailizing shard cluster", func(t *testing.T) {
		clusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel, defaultVersion)

		sc, ok := clusterService.getShardCluster(defaultDMLChannel)
		require.True(t, ok)
		assert.NotPanics(t, func() {
			err := clusterService.SyncReplicaSegments(defaultDMLChannel, []*querypb.ReplicaSegmentsInfo{
				{
					NodeId:      1,
					PartitionId: defaultPartitionID,

					SegmentIds: []int64{1},
					Versions:   []int64{1},
				},
			})

			assert.NoError(t, err)
			assert.Nil(t, sc.currentVersion)
		})
	})

	t.Run("sync shard cluster", func(t *testing.T) {
		clusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel, defaultVersion)

		sc, ok := clusterService.getShardCluster(defaultDMLChannel)
		require.True(t, ok)
		sc.SetupFirstVersion()

		err := clusterService.SyncReplicaSegments(defaultDMLChannel, []*querypb.ReplicaSegmentsInfo{
			{
				NodeId:      1,
				PartitionId: defaultPartitionID,

				SegmentIds: []int64{1},
				Versions:   []int64{1},
			},
		})

		assert.NoError(t, err)

		cs, ok := clusterService.getShardCluster(defaultDMLChannel)
		require.True(t, ok)
		segment, ok := cs.getSegment(1)
		assert.True(t, ok)
		assert.Equal(t, common.InvalidNodeID, segment.nodeID)
		assert.Equal(t, defaultPartitionID, segment.partitionID)
		assert.Equal(t, segmentStateLoaded, segment.state)
	})
}

func TestShardClusterService_close(t *testing.T) {
	client := v3client.New(embedetcdServer.Server)
	defer client.Close()
	session := sessionutil.NewSession(context.Background(), "/by-dev/sessions/unittest/querynode/", client)
	clusterService := newShardClusterService(client, session, nil)

	t.Run("close ok", func(t *testing.T) {
		clusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel, defaultVersion)

		cnt := 0
		clusterService.clusters.Range(func(key, value any) bool {
			cnt++
			return true
		})
		assert.Equal(t, 1, cnt)

		err := clusterService.close()
		assert.NoError(t, err)
	})

	t.Run("close fail", func(t *testing.T) {
		clusterService.clusters.Store("key", "error")
		err := clusterService.close()
		assert.Error(t, err)
	})
}
