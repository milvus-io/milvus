package balancer_test

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Existing balancer tests exercise UpdateBalancePolicy; it now requires a
// durable config source instead of a process-local ParamItem override.
func setupBalancerConfigSource(t *testing.T) {
	t.Helper()
	paramtable.Init()
	client, err := clientv3.New(clientv3.Config{Endpoints: paramtable.Get().EtcdCfg.Endpoints.GetAsStrings(), DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	prefix := "wal-balance-config-" + funcutil.GenRandomStr()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := client.Delete(ctx, prefix+"/", clientv3.WithPrefix())
		require.NoError(t, err)
	})
	base := paramtable.NewBaseTable(paramtable.SkipRemote(true))
	t.Cleanup(base.Manager().Close)
	params := &paramtable.ComponentParam{}
	params.Init(base)
	get := mockey.Mock(paramtable.Get).Return(params).Build()
	t.Cleanup(func() { get.UnPatch() })
	getBase := mockey.Mock(paramtable.GetBaseTable).Return(base).Build()
	t.Cleanup(func() { getBase.UnPatch() })
	source, err := config.NewEtcdSource(client, &config.EtcdInfo{KeyPrefix: prefix, RefreshInterval: time.Hour})
	require.NoError(t, err)
	require.NoError(t, base.Manager().AddSource(source))
}
