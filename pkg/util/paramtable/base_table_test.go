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

package paramtable

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/config"
	etcdkv "github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var baseParams = NewBaseTable(SkipRemote(true))

func TestMain(m *testing.M) {
	baseParams.init()
	code := m.Run()
	// Stop the shared embedded etcd server (started lazily by the version-gate
	// tests) after all tests.
	if etcdkv.HasServer() {
		etcdkv.StopEtcdServer()
	}
	os.Exit(code)
}

func TestBaseTable_DuplicateValues(t *testing.T) {
	baseParams.Save("rootcoord.dmlchannelnum", "10")
	baseParams.Save("rootcoorddmlchannelnum", "11")

	prefix := "rootcoord."
	configs := baseParams.mgr.GetConfigs()

	configsWithPrefix := make(map[string]string)
	for k, v := range configs {
		if strings.HasPrefix(k, prefix) {
			configsWithPrefix[k] = v
		}
	}

	rootconfigs := baseParams.mgr.GetBy(config.WithPrefix(prefix))

	assert.Equal(t, len(rootconfigs), len(configsWithPrefix))
	assert.Equal(t, "11", rootconfigs["rootcoord.dmlchannelnum"])
}

func TestBaseTable_RemoveThenSaveGroupMember(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MILVUSCONF", dir)
	require.NoError(t, os.WriteFile(dir+"/milvus.yaml", []byte("public:\n  group:\n    member: source-value\n"), 0o600))
	base := NewBaseTable(Files([]string{"milvus.yaml"}), SkipRemote(true), SkipEnv(true), Interval(0))
	t.Cleanup(base.mgr.Close)
	group := ParamGroup{KeyPrefix: "public.group."}
	group.Init(base.mgr)

	const key = "public.group.member"
	require.NoError(t, base.Remove(key))
	assert.Empty(t, group.GetValue())
	require.NoError(t, base.Save(key, "restored-value"))
	source, value, err := base.mgr.GetRegisteredConfig(key)
	require.NoError(t, err)
	assert.Equal(t, config.RuntimeSource, source)
	assert.Equal(t, "restored-value", value)
	assert.Equal(t, value, base.Get(key))
	assert.Equal(t, value, group.GetValue()["member"])
}

func TestBaseTable_SaveAndLoad(t *testing.T) {
	err1 := baseParams.Save("int", "10")
	assert.Nil(t, err1)

	err2 := baseParams.Save("string", "testSaveAndLoad")
	assert.Nil(t, err2)

	err3 := baseParams.Save("float", "1.234")
	assert.Nil(t, err3)

	r1, _ := baseParams.Load("int")
	assert.Equal(t, "10", r1)

	r2, _ := baseParams.Load("string")
	assert.Equal(t, "testSaveAndLoad", r2)

	r3, _ := baseParams.Load("float")
	assert.Equal(t, "1.234", r3)

	err4 := baseParams.Remove("int")
	assert.Nil(t, err4)

	err5 := baseParams.Remove("string")
	assert.Nil(t, err5)

	err6 := baseParams.Remove("float")
	assert.Nil(t, err6)
}

func TestBaseTable_Remove(t *testing.T) {
	err1 := baseParams.Save("RemoveInt", "10")
	assert.Nil(t, err1)

	err2 := baseParams.Save("RemoveString", "testRemove")
	assert.Nil(t, err2)

	err3 := baseParams.Save("RemoveFloat", "1.234")
	assert.Nil(t, err3)

	err4 := baseParams.Remove("RemoveInt")
	assert.Nil(t, err4)

	err5 := baseParams.Remove("RemoveString")
	assert.Nil(t, err5)

	err6 := baseParams.Remove("RemoveFloat")
	assert.Nil(t, err6)
}

func TestBaseTable_Get(t *testing.T) {
	err := baseParams.Save("key", "10")
	assert.NoError(t, err)

	v := baseParams.Get("key")
	assert.Equal(t, "10", v)

	v2 := baseParams.Get("none")
	assert.Equal(t, "", v2)
}

func TestBaseTable_Pulsar(t *testing.T) {
	// test PULSAR ADDRESS
	t.Setenv("PULSAR_ADDRESS", "pulsar://localhost:6650")
	baseParams.init()

	address := baseParams.Get("pulsar.address")
	assert.Equal(t, "pulsar://localhost:6650", address)

	port := baseParams.Get("pulsar.port")
	assert.NotEqual(t, "", port)
}

func TestBaseTable_Env(t *testing.T) {
	t.Setenv("milvus.test", "test")
	t.Setenv("milvus.test.test2", "test2")

	baseParams.init()
	result, _ := baseParams.Load("test")
	assert.Equal(t, result, "test")

	result, _ = baseParams.Load("test.test2")
	assert.Equal(t, result, "test2")

	t.Setenv("milvus.invalid", "xxx=test")

	baseParams.init()
	result, _ = baseParams.Load("invalid")
	assert.Equal(t, result, "xxx=test")
}

func TestNewBaseTableFromYamlOnly(t *testing.T) {
	var yaml string
	var gp *BaseTable
	yaml = "not_exist.yaml"
	gp = NewBaseTableFromYamlOnly(yaml)
	assert.Empty(t, gp.Get("key"))
}

// A lazy unauthenticated client and an authenticating client must both wait
// for etcd and interrupt the in-flight request on shutdown.
func TestWaitForRemoteConfigDuringPartition(t *testing.T) {
	for _, auth := range []string{"false", "true"} {
		t.Run("auth="+auth, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer listener.Close()
			bt := NewBaseTable(SkipRemote(true), SkipEnv(true), Interval(0))
			bt.config.skipRemote = false
			defer bt.mgr.Close()
			bt.Save("etcd.endpoints", listener.Addr().String())
			bt.Save("etcd.auth.enabled", auth)
			bt.Save("etcd.auth.userName", "test")
			bt.Save("etcd.auth.password", "test")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- bt.WaitForRemoteConfig(ctx) }()
			require.NoError(t, listener.(*net.TCPListener).SetDeadline(time.Now().Add(5*time.Second)))
			conn, err := listener.Accept()
			require.NoError(t, err)
			defer conn.Close()
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("etcd startup continued waiting after shutdown")
			}
			if bt.etcdClient != nil {
				bt.etcdClient.Close()
			}
		})
	}
}

// Exercise the process singleton in a child so that early package initialization,
// frozen ParamItems and the globals used by RPC and storage follow the real path.
func TestStartupSnapshotReachesComponents(t *testing.T) {
	const child = "MILVUS_TEST_STARTUP_SNAPSHOT_CHILD"
	if os.Getenv(child) == "1" {
		p := Get()
		bt := GetBaseTable()
		require.Equal(t, "local-prefix", p.CommonCfg.ClusterPrefix.GetValue())
		require.True(t, FastPBEnabled())
		require.Equal(t, 32, typeutil.GetVarCharEstimateLength())
		handlers := len(bt.mgr.Dispatcher.Get(p.EtcdCfg.Endpoints.Key))
		for i := 0; i < 3; i++ {
			require.Error(t, bt.initConfigsFromRemote(t.Context()))
			require.Len(t, bt.mgr.Dispatcher.Get(p.EtcdCfg.Endpoints.Key), handlers)
		}
		// Make the remote source usable after the singleton has frozen its local values.
		require.NoError(t, bt.Save("etcd.ssl.enabled", "false"))
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		require.NoError(t, bt.WaitForRemoteConfig(ctx))
		defer bt.etcdClient.Close()
		defer bt.mgr.Close()
		require.Equal(t, "remote-prefix", p.CommonCfg.ClusterPrefix.GetValue())
		require.Contains(t, p.CommonCfg.RootCoordDml.GetValue(), "remote-prefix")
		root := filepath.Join(os.Getenv("MILVUSCONF"), "remote-storage")
		require.Equal(t, root, p.LocalStorageCfg.Path.GetValue())
		require.Equal(t, filepath.Join(root, "pprof"), p.ProfileCfg.PprofPath.GetValue())
		require.Equal(t, filepath.Join(root, "mmap"), p.QueryNodeCfg.MmapDirPath.GetValue())
		require.True(t, funcutil.PreferIPv6LocalIP.Load())
		require.Equal(t, "127.0.0.2", p.QueryNodeGrpcServerCfg.IP)
		require.False(t, p.CommonCfg.EnableFastPB.GetAsBool())
		require.False(t, FastPBEnabled())
		require.Equal(t, 77, p.CommonCfg.VarCharLengthAvg.GetAsInt())
		require.Equal(t, 77, typeutil.GetVarCharEstimateLength())
		require.Equal(t, 88, typeutil.GetDynamicFieldEstimateLength())
		require.Equal(t, 99, typeutil.GetSparseFloatVectorEstimateLength())
		require.Len(t, bt.mgr.Dispatcher.Get(p.EtcdCfg.Endpoints.Key), handlers)
		require.True(t, p.DataCoordCfg.SegmentIndexSummariesEnabled.GetAsBool())

		// A subsequent poll can change live settings but must keep the startup prefix.
		prefix := p.EtcdCfg.RootPath.GetValue() + "/config/"
		_, err := bt.etcdClient.Put(ctx, prefix+p.CommonCfg.ClusterPrefix.Key, "later-prefix")
		require.NoError(t, err)
		_, err = bt.etcdClient.Put(ctx, prefix+p.CommonCfg.VarCharLengthAvg.Key, "32")
		require.NoError(t, err)
		_, err = bt.etcdClient.Put(ctx, prefix+p.CommonCfg.UseLoonFFI.Key, "false")
		require.NoError(t, err)
		_, err = bt.RefreshRemoteConfigsLinearizable()
		require.NoError(t, err)
		require.Equal(t, "remote-prefix", p.CommonCfg.ClusterPrefix.GetValue())
		require.Equal(t, 32, typeutil.GetVarCharEstimateLength())
		require.True(t, p.CommonCfg.UseLoonFFI.GetAsBool())
		return
	}

	cli, port := setupEmbedEtcd(t)
	defer cli.Close()
	rootPath := fmt.Sprintf("startup-snapshot-%d", time.Now().UnixNano())
	dir := t.TempDir()
	for key, value := range map[string]string{
		"localStorage.path":                            filepath.Join(dir, "remote-storage"),
		"queryNode.ip":                                 "127.0.0.2",
		"common.preferIPv6":                            "true",
		"msgChannel.chanNamePrefix.cluster":            "remote-prefix",
		"common.enableFastPB":                          "false",
		"dataCoord.index.segmentIndexSummariesEnabled": "true",
		"common.estimate.varCharLengthAvg":             "77",
		"common.estimate.dynamicFieldLengthAvg":        "88",
		"common.estimate.sparseFloatVectorSize":        "99",
	} {
		_, err := cli.Put(t.Context(), rootPath+"/config/"+key, value)
		require.NoError(t, err)
	}
	// Missing TLS files cause the initial client construction to fail before a
	// source or its handlers can be attached, even on a fast machine.
	yaml := fmt.Sprintf(`etcd:
  endpoints: 127.0.0.1:%d
  rootPath: %s
  ssl:
    enabled: true
    tlsCert: %s
msgChannel:
  chanNamePrefix:
    cluster: local-prefix
common:
  enableFastPB: true
  estimate:
    varCharLengthAvg: 32
`, port, rootPath, filepath.Join(dir, "missing.pem"))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "milvus.yaml"), []byte(yaml), 0o600))
	executable, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestStartupSnapshotReachesComponents$") // #nosec G204 -- Re-exec this test binary with a fixed filter.
	cmd.Env = append(os.Environ(), child+"=1", "MILVUSCONF="+dir, fmt.Sprintf("ETCD_ENDPOINTS=127.0.0.1:%d", port))
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
}
