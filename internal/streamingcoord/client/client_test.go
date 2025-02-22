package client

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestDial(t *testing.T) {
	paramtable.Init()

	err := etcd.InitEtcdServer(true, "", t.TempDir(), "stdout", "info")
	assert.NoError(t, err)
	defer etcd.StopEtcdServer()
	c, err := etcd.GetEmbedEtcdClient()
	assert.NoError(t, err)
	assert.NotNil(t, c)

	client := NewClient(c)
	assert.NotNil(t, client)
	client.Close()
}
