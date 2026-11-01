package querycoordv2

import (
	"path"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestBalanceConfigPersistence(t *testing.T) {
	paramtable.Init()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: paramtable.Get().EtcdCfg.Endpoints.GetAsStrings(), DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()
	prefix := "balance-config-" + funcutil.GenRandomStr()
	defer func() {
		_, err := client.Delete(t.Context(), prefix+"/", clientv3.WithPrefix())
		require.NoError(t, err)
	}()
	base := paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files([]string{}))
	defer base.Manager().Close()
	p := &paramtable.ComponentParam{}
	p.Init(base)
	getParams := mockey.Mock(paramtable.Get).Return(p).Build()
	defer getParams.UnPatch()
	getBase := mockey.Mock(paramtable.GetBaseTable).Return(base).Build()
	defer getBase.UnPatch()
	s := &Server{}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	key := p.QueryViewCfg.BalancerOptimizationEnabled.Key
	check := func(want bool) {
		t.Helper()
		resp, err := s.CheckBalanceStatus(t.Context(), &querypb.CheckBalanceStatusRequest{})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		require.Equal(t, want, resp.GetIsActive())
	}
	// No local-only fallback when the persistent configuration source is absent.
	status, err := s.SuspendBalance(t.Context(), &querypb.SuspendBalanceRequest{})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrServiceUnavailable)
	check(true)
	status, err = s.ResumeBalance(t.Context(), &querypb.ResumeBalanceRequest{})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), merr.ErrServiceUnavailable)

	info := &config.EtcdInfo{KeyPrefix: prefix, RefreshInterval: time.Hour}
	source, err := config.NewEtcdSource(client, info)
	require.NoError(t, err)
	require.NoError(t, base.Manager().AddSource(source))
	s.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err = s.SuspendBalance(t.Context(), &querypb.SuspendBalanceRequest{})
	require.NoError(t, err)
	require.False(t, merr.Ok(status))
	status, err = s.ResumeBalance(t.Context(), &querypb.ResumeBalanceRequest{})
	require.NoError(t, err)
	require.False(t, merr.Ok(status))
	s.UpdateStateCode(commonpb.StateCode_Healthy)

	assertPersisted := func(want string) {
		t.Helper()
		stored, err := client.Get(t.Context(), path.Join(prefix, "config", config.EtcdConfigKey(key)))
		require.NoError(t, err)
		require.Len(t, stored.Kvs, 1)
		require.Equal(t, want, string(stored.Kvs[0].Value))
		owner, value, err := base.Manager().GetConfig(key)
		require.NoError(t, err)
		require.Equal(t, source.GetSourceName(), owner, "no runtime overlay may shadow AlterConfig")
		require.Equal(t, want, value)
	}
	status, err = s.SuspendBalance(t.Context(), &querypb.SuspendBalanceRequest{})
	require.NoError(t, merr.CheckRPCCall(status, err))
	check(false) // The write must refresh locally without waiting for the hourly poll.
	assertPersisted("false")

	// A fresh configuration reader recovers the setting without the original
	// process's ParamTable, overlay or cached value.
	reloaded := paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files([]string{}))
	defer reloaded.Manager().Close()
	reloadedSource, err := config.NewEtcdSource(client, info)
	require.NoError(t, err)
	require.NoError(t, reloaded.Manager().AddSource(reloadedSource))
	reloadedParams := &paramtable.ComponentParam{}
	reloadedParams.Init(reloaded)
	require.False(t, reloadedParams.QueryViewCfg.BalancerOptimizationEnabled.GetAsBool())

	// Alternate with AlterConfig's exact storage entry point, including reset.
	require.NoError(t, base.Manager().AlterConfigsInEtcd(source, map[string]string{key: "true"}, nil))
	check(true)
	assertPersisted("true")
	status, err = s.SuspendBalance(t.Context(), &querypb.SuspendBalanceRequest{})
	require.NoError(t, merr.CheckRPCCall(status, err))
	check(false)
	require.NoError(t, base.Manager().AlterConfigsInEtcd(source, nil, []string{key}))
	check(true)
	status, err = s.ResumeBalance(t.Context(), &querypb.ResumeBalanceRequest{})
	require.NoError(t, merr.CheckRPCCall(status, err))
	assertPersisted("true")
	require.NoError(t, base.Manager().AlterConfigsInEtcd(source, map[string]string{key: "false"}, nil))
	check(false)

	// A failed write must not report success or change the in-process setting.
	failure := merr.WrapErrServiceUnavailableMsg("test etcd write failure")
	patch := mockey.Mock((*config.Manager).UpdateConfigInEtcd).Return(failure).Build()
	defer patch.UnPatch()
	status, err = s.ResumeBalance(t.Context(), &querypb.ResumeBalanceRequest{})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(status), failure)
	check(false)
	assertPersisted("false")
}
