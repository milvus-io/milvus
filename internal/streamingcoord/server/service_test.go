package server

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
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
	f := syncutil.NewFuture[types.RootCoordClient]()
	newServer := b.WithETCD(c).
		WithMetaKV(metaKV).
		WithSession(s).
		WithRootCoordClient(f).
		Build()

	ctx := context.Background()
	err = newServer.Start(ctx)
	assert.NoError(t, err)
	newServer.Stop()
}
