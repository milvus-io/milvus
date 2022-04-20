package querynode

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

func TestShardClusterService(t *testing.T) {
	client := v3client.New(embedetcdServer.Server)
	defer client.Close()
	session := sessionutil.NewSession(context.Background(), "/by-dev/sessions/unittest/querynode/", client)
	clusterService := newShardClusterService(client, session, nil)

	assert.NotPanics(t, func() {
		clusterService.addShardCluster(defaultCollectionID, defaultReplicaID, defaultDMLChannel)
	})

	shardCluster, ok := clusterService.getShardCluster(defaultDMLChannel)
	assert.True(t, ok)
	assert.NotNil(t, shardCluster)

	_, ok = clusterService.getShardCluster("non-exist-channel")
	assert.False(t, ok)

	err := clusterService.releaseShardCluster(defaultDMLChannel)
	assert.NoError(t, err)

	err = clusterService.releaseShardCluster("non-exist-channel")
	assert.Error(t, err)
}
