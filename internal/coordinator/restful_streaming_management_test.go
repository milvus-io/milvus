package coordinator

import (
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querycoordv2"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestStreamingBalanceConfigPersistence(t *testing.T) {
	paramtable.Init()
	client, err := clientv3.New(clientv3.Config{Endpoints: paramtable.Get().EtcdCfg.Endpoints.GetAsStrings(), DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer client.Close()
	prefix := "rest-wal-config-" + funcutil.GenRandomStr()
	defer func() {
		_, err := client.Delete(t.Context(), prefix+"/", clientv3.WithPrefix())
		require.NoError(t, err)
	}()
	base := paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files([]string{}))
	defer base.Manager().Close()
	params := &paramtable.ComponentParam{}
	params.Init(base)
	get := mockey.Mock(paramtable.Get).Return(params).Build()
	defer get.UnPatch()
	getBase := mockey.Mock(paramtable.GetBaseTable).Return(base).Build()
	defer getBase.UnPatch()
	coord := &mixCoordImpl{} // No QueryCoord or WAL client: config is authoritative.
	put := func(state string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		coord.HandleStreamingBalanceStatus(w, httptest.NewRequest(http.MethodPut, "/", strings.NewReader(`{"status":"`+state+`"}`)))
		return w
	}
	check := func(state string) {
		w := httptest.NewRecorder()
		coord.HandleStreamingBalanceStatus(w, httptest.NewRequest(http.MethodGet, "/", nil))
		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, w.Body.String(), `"status":"`+state+`"`)
	}
	check("active")
	require.Equal(t, http.StatusInternalServerError, put("suspended").Code)
	check("active")
	info := &config.EtcdInfo{KeyPrefix: prefix, RefreshInterval: time.Hour}
	source, err := config.NewEtcdSource(client, info)
	require.NoError(t, err)
	require.NoError(t, base.Manager().AddSource(source))
	require.Equal(t, http.StatusOK, put("suspended").Code)
	check("suspended")
	key := params.StreamingCfg.WALBalancerPolicyAllowRebalance.Key
	stored, err := client.Get(t.Context(), path.Join(prefix, "config", config.EtcdConfigKey(key)))
	require.NoError(t, err)
	require.Len(t, stored.Kvs, 1)
	require.Equal(t, "false", string(stored.Kvs[0].Value))
	owner, value, err := base.Manager().GetConfig(key)
	require.NoError(t, err)
	require.Equal(t, source.GetSourceName(), owner)
	require.Equal(t, "false", value)

	reloaded := paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files([]string{}))
	defer reloaded.Manager().Close()
	reloadedSource, err := config.NewEtcdSource(client, info)
	require.NoError(t, err)
	require.NoError(t, reloaded.Manager().AddSource(reloadedSource))
	_, value, err = reloaded.Manager().GetConfig(key)
	require.NoError(t, err)
	require.Equal(t, "false", value)

	// A reset through the actual AlterConfig endpoint must remove the override.
	w := httptest.NewRecorder()
	coord.HandleAlterConfig(w, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"key":"`+key+`"}`)))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	check("active")
	require.Equal(t, http.StatusOK, put("suspended").Code)
	require.Equal(t, http.StatusOK, put("resumed").Code)
	check("active")
	require.Equal(t, http.StatusBadRequest, put("invalid").Code)
	patch := mockey.Mock((*config.Manager).UpdateConfigInEtcd).Return(merr.ErrServiceUnavailable).Build()
	defer patch.UnPatch()
	require.Equal(t, http.StatusInternalServerError, put("suspended").Code)
	check("active")
}

func TestStreamingNodeManagement(t *testing.T) {
	previous := streaming.WAL()
	streaming.SetupNoopWALForTest()
	defer streaming.SetWALForTest(previous)
	b := streaming.WAL().Balancer()
	list := mockey.Mock(mockey.GetMethod(b, "ListStreamingNode")).Return([]types.StreamingNodeInfo{{ServerID: 1, Address: "sn1"}, {ServerID: 2, Address: "sn2"}}, nil).Build()
	defer list.UnPatch()
	frozen := mockey.Mock(mockey.GetMethod(b, "GetFrozenNodeIDs")).Return([]int64{2}, nil).Build()
	defer frozen.UnPatch()
	coord := &mixCoordImpl{} // Any legacy QueryCoord fallback would panic.
	invoke := func(handler http.HandlerFunc, method, url, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		handler(w, httptest.NewRequest(method, url, strings.NewReader(body)))
		return w
	}
	w := invoke(coord.HandleStreamingNodes, http.MethodGet, "/", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"msg":"OK","nodeInfos":[{"ID":1,"address":"sn1","state":"active"},{"ID":2,"address":"sn2","state":"suspended"}]}`, w.Body.String())
	for _, tc := range []struct{ id, state string }{{"1", "active"}, {"2", "suspended"}} {
		w = invoke(coord.HandleStreamingNodeStatus, http.MethodGet, "/?node_id="+tc.id, "")
		require.Equal(t, http.StatusOK, w.Code)
		require.Contains(t, w.Body.String(), `"status":"`+tc.state+`"`)
	}
	w = invoke(coord.HandleStreamingNodeStatus, http.MethodGet, "/?node_id=99", "")
	require.Equal(t, http.StatusNotFound, w.Code)
	freeze := mockey.Mock(mockey.GetMethod(b, "FreezeNodeIDs")).Return(nil).Build()
	defer freeze.UnPatch()
	defreeze := mockey.Mock(mockey.GetMethod(b, "DefreezeNodeIDs")).Return(nil).Build()
	defer defreeze.UnPatch()
	w = invoke(coord.HandleStreamingNodeStatus, http.MethodPut, "/", `{"node_id":1,"status":"suspended"}`)
	require.Equal(t, http.StatusOK, w.Code)
	w = invoke(coord.HandleStreamingNodeStatus, http.MethodPut, "/", `{"node_id":1,"status":"active"}`)
	require.Equal(t, http.StatusOK, w.Code)
	freeze.Return(merr.ErrNodeNotFound)
	w = invoke(coord.HandleStreamingNodeStatus, http.MethodPut, "/", `{"node_id":99,"status":"suspended"}`)
	require.Equal(t, http.StatusNotFound, w.Code)

	dist := mockey.Mock(mockey.GetMethod(b, "GetWALDistribution")).Return(&types.StreamingNodeAssignment{Channels: map[string]types.PChannelInfo{}}, nil).Build()
	defer dist.UnPatch()
	w = invoke(coord.GetStreamingNodeDistribution, http.MethodGet, "/?node_id=1", "")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"channel_names":[],"sealed_segmentIDs":[]}`, w.Body.String())
	dist.Return(nil, merr.ErrNodeNotFound)
	w = invoke(coord.GetStreamingNodeDistribution, http.MethodGet, "/?node_id=99", "")
	require.Equal(t, http.StatusNotFound, w.Code)
	list.Return(nil, merr.ErrServiceUnavailable)
	w = invoke(coord.HandleStreamingNodes, http.MethodGet, "/", "")
	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestClearReadTaskQueueDeprecated(t *testing.T) {
	coord := &mixCoordImpl{}
	resp, err := coord.ClearReadTaskQueue(t.Context(), &internalpb.ClearReadTaskQueueRequest{})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrServiceUnimplemented)
	require.Empty(t, resp.Results)
	qc := &querycoordv2.Server{}
	resp, err = qc.ClearReadTaskQueue(t.Context(), &internalpb.ClearReadTaskQueueRequest{})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrServiceUnimplemented)
	require.Empty(t, resp.Results)
}
