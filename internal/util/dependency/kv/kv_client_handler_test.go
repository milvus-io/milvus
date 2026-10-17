package kvfactory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestCloseEtcdClient_ContextCanceled(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:1"}})
	require.NoError(t, err)
	// the client is already closed by the time shutdown closes it again,
	// which makes Close return context.Canceled
	_ = cli.Close()
	require.ErrorIs(t, cli.Close(), context.Canceled)

	clientCreator.mu.Lock()
	clientCreator.client = cli
	path := "root"
	clientCreator.rootpath = &path
	clientCreator.mu.Unlock()

	assert.NotPanics(t, CloseEtcdClient)
	assert.Nil(t, clientCreator.client)
	assert.Nil(t, clientCreator.rootpath)
}
