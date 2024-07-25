package server

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestServer(t *testing.T) {
	paramtable.Init()

	params := paramtable.Get()

	endpoints := params.EtcdCfg.Endpoints.GetValue()
	etcdEndpoints := strings.Split(endpoints, ",")
	c, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
	assert.NoError(t, err)
	assert.NotNil(t, c)

	b := NewServerBuilder()
	metaKV := etcdkv.NewEtcdKV(c, "test")
	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetServerID().Return(1)

	newServer := b.WithETCD(c).
		WithMetaKV(metaKV).
		WithSession(s).
		Build()

	ctx := context.Background()
	err = newServer.Init(ctx)
	assert.NoError(t, err)
	newServer.Start()
	newServer.Stop()
}
