// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v3/config"
	etcdkv "github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	testConfigKey   = "function.testGate"
	testConfigValue = "true"
)

// testRootSeq makes each testRoots() call unique, so repeated runs (-count=N)
// against the shared embedded etcd server never see leftovers of a previous
// iteration (e.g. a config key flipped by the same test name).
var testRootSeq int64

// testRoots returns an etcd key root unique to the test, so tests running
// against the shared embedded etcd server never see each other's sessions or
// flipped config values.
func testRoots(t *testing.T) (metaRoot, configRoot string) {
	t.Helper()
	root := fmt.Sprintf("test-root-%s-%d", strings.ReplaceAll(t.Name(), "/", "-"), atomic.AddInt64(&testRootSeq, 1))
	return path.Join(root, "meta"), root
}

// newTestConfirmator builds a confirmator against the embedded etcd server
// through the same paramtable etcd-config path used in production (the
// confirmator creates its own client from the etcd config).
func newTestConfirmator(t *testing.T, clientPort int, metaRoot, configRoot string) *Confirmator {
	t.Helper()
	etcdCfg := &EtcdConfig{}
	etcdCfg.Init(NewBaseTable(SkipRemote(true)))
	etcdCfg.Endpoints.SwapTempValue(fmt.Sprintf("127.0.0.1:%d", clientPort))
	c, err := NewConfirmator(etcdCfg, metaRoot, configRoot)
	require.NoError(t, err)
	return c
}

func TestConfirmator_RegisterGate(t *testing.T) {
	_, clientPort := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	c := newTestConfirmator(t, clientPort, metaRoot, configRoot)

	// nil switcher is rejected.
	assert.Error(t, c.RegisterGate(testConfigKey, nil))
	// unparseable gate version is rejected.
	assert.Error(t, c.RegisterGate(testConfigKey, &VersionGateSwitcher{GateVersion: "not-a-version"}))

	require.NoError(t, c.RegisterGate(testConfigKey, gateSwitcher("2.6.23", 10*time.Millisecond)))
	require.NoError(t, c.Start(context.Background()))
	defer c.Stop()

	// one-shot: no gate can be registered after Start.
	assert.Error(t, c.RegisterGate(testConfigKey, gateSwitcher("2.6.23", 10*time.Millisecond)))
}

func TestConfirmator_FlipsAfterAllUpAndDelay(t *testing.T) {
	cli, clientPort := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putAllUpSessions(t, cli, metaRoot)

	c := newTestConfirmator(t, clientPort, metaRoot, configRoot)
	require.NoError(t, c.RegisterGate(testConfigKey, gateSwitcher("2.6.23", 50*time.Millisecond)))
	require.NoError(t, c.Start(context.Background()))
	defer c.Stop()

	waitConfigValue(t, cli, configRoot, testConfigKey, testConfigValue)
}

func TestConfirmator_NoSessionsNoFlip(t *testing.T) {
	// No session at all: the minimum online version is unknown (zero), so no
	// gate can ever be above its GateVersion and nothing is flipped.
	cli, clientPort := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)

	c := newTestConfirmator(t, clientPort, metaRoot, configRoot)
	require.NoError(t, c.RegisterGate(testConfigKey, gateSwitcher("2.6.23", 50*time.Millisecond)))
	require.NoError(t, c.Start(context.Background()))
	defer c.Stop()

	assertNoFlip(t, cli, configRoot, testConfigKey)
}

func TestConfirmator_MixedVersionsNoFlip(t *testing.T) {
	cli, clientPort := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putSession(t, cli, metaRoot, typeutil.DataNodeRole, "node-1", "2.6.23")
	putSession(t, cli, metaRoot, typeutil.ProxyRole, "node-1", "2.6.22")

	c := newTestConfirmator(t, clientPort, metaRoot, configRoot)
	require.NoError(t, c.RegisterGate(testConfigKey, gateSwitcher("2.6.23", 30*time.Millisecond)))
	require.NoError(t, c.Start(context.Background()))
	defer c.Stop()

	assertNoFlip(t, cli, configRoot, testConfigKey)
}

func TestConfirmator_SessionDipResetsStabilityWindow(t *testing.T) {
	cli, clientPort := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putSession(t, cli, metaRoot, typeutil.ProxyRole, "node-1", "2.6.23")
	putSession(t, cli, metaRoot, typeutil.QueryNodeRole, "node-1", "2.6.23")

	c := newTestConfirmator(t, clientPort, metaRoot, configRoot)
	require.NoError(t, c.RegisterGate(testConfigKey, gateSwitcher("2.6.23", 200*time.Millisecond)))
	require.NoError(t, c.Start(context.Background()))
	defer c.Stop()

	// A session dips below the gate version during the stability window:
	// the window must reset and the flip must not happen.
	time.Sleep(50 * time.Millisecond)
	putSession(t, cli, metaRoot, typeutil.ProxyRole, "node-1", "2.6.22")
	assertNoFlip(t, cli, configRoot, testConfigKey)

	// The session comes back above the gate version: the window restarts and
	// the gate flips.
	putSession(t, cli, metaRoot, typeutil.ProxyRole, "node-1", "2.6.23")
	waitConfigValue(t, cli, configRoot, testConfigKey, testConfigValue)
}

func TestConfirmator_ExplicitEtcdValueWins(t *testing.T) {
	cli, clientPort := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putAllUpSessions(t, cli, metaRoot)
	putConfig(t, cli, configRoot, testConfigKey, "false")

	c := newTestConfirmator(t, clientPort, metaRoot, configRoot)
	require.NoError(t, c.RegisterGate(testConfigKey, gateSwitcher("2.6.23", 30*time.Millisecond)))
	require.NoError(t, c.Start(context.Background()))
	defer c.Stop()

	// The explicit value must not be overwritten by the flip.
	waitConfigValue(t, cli, configRoot, testConfigKey, "false")
}

func TestConfirmator_AlreadyFlippedAtStart(t *testing.T) {
	cli, clientPort := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putConfig(t, cli, configRoot, testConfigKey, testConfigValue)

	c := newTestConfirmator(t, clientPort, metaRoot, configRoot)
	require.NoError(t, c.RegisterGate(testConfigKey, gateSwitcher("2.6.23", 10*time.Millisecond)))
	require.NoError(t, c.Start(context.Background()))
	defer c.Stop()

	// The gate is resolved immediately: no session watch is running.
	c.mu.Lock()
	resolved := c.gates[0].resolved
	c.mu.Unlock()
	assert.True(t, resolved)
}

func TestConfirmator_MultipleGatesIndependent(t *testing.T) {
	cli, clientPort := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putAllUpSessions(t, cli, metaRoot)

	c := newTestConfirmator(t, clientPort, metaRoot, configRoot)
	require.NoError(t, c.RegisterGate(testConfigKey, gateSwitcher("2.6.23", 50*time.Millisecond)))
	require.NoError(t, c.RegisterGate("function.testGate2", gateSwitcher("3.0.0", 50*time.Millisecond)))
	require.NoError(t, c.Start(context.Background()))
	defer c.Stop()

	// The 2.6.23 gate flips; the 3.0.0 gate stays pending.
	waitConfigValue(t, cli, configRoot, testConfigKey, testConfigValue)
	time.Sleep(150 * time.Millisecond)
	assertConfigAbsent(t, cli, configRoot, "function.testGate2")
}

func TestInitVersionGatesSkipRemote(t *testing.T) {
	// initVersionGates is a no-op for a skip-remote param table (the common
	// test setup): no confirmator is created and no goroutine leaks.
	p := &ComponentParam{}
	p.Init(NewBaseTable(SkipRemote(true)))
	assert.Nil(t, p.versionGates)
}

func gateSwitcher(gateVersion string, delay time.Duration) *VersionGateSwitcher {
	return &VersionGateSwitcher{
		EnableAutoSwitchValue: "auto",
		PreSwitchValue:        "false",
		GateVersion:           gateVersion,
		TargetValue:           testConfigValue,
		SwitchDelay:           delay,
	}
}

// putAllUpSessions registers one session above the gate version for each of
// the common roles, so a watch over the whole session prefix sees them all.
func putAllUpSessions(t *testing.T, cli *clientv3.Client, metaRoot string) {
	t.Helper()
	for _, role := range []string{
		typeutil.ProxyRole,
		typeutil.DataNodeRole,
		typeutil.QueryNodeRole,
		typeutil.StreamingNodeRole,
	} {
		putSession(t, cli, metaRoot, role, "node-1", "2.6.23")
	}
}

func putConfig(t *testing.T, cli *clientv3.Client, configRoot, key, value string) {
	t.Helper()
	_, err := cli.Put(context.Background(), path.Join(configRoot, "config", config.FormatKey(key)), value)
	require.NoError(t, err)
}

func getConfigValue(t *testing.T, cli *clientv3.Client, configRoot, key string) (string, bool) {
	t.Helper()
	resp, err := cli.Get(context.Background(), path.Join(configRoot, "config", config.FormatKey(key)))
	require.NoError(t, err)
	if len(resp.Kvs) == 0 {
		return "", false
	}
	return string(resp.Kvs[0].Value), true
}

// waitConfigValue polls until the config-center key holds the expected value.
func waitConfigValue(t *testing.T, cli *clientv3.Client, configRoot, key, expected string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := getConfigValue(t, cli, configRoot, key); ok && v == expected {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("config %s did not reach value %q", key, expected)
}

// assertNoFlip asserts that the config-center key stays absent/unchanged for a
// while, i.e. the gate did not flip.
func assertNoFlip(t *testing.T, cli *clientv3.Client, configRoot, key string) {
	t.Helper()
	time.Sleep(300 * time.Millisecond)
	assertConfigAbsent(t, cli, configRoot, key)
}

func assertConfigAbsent(t *testing.T, cli *clientv3.Client, configRoot, key string) {
	t.Helper()
	_, ok := getConfigValue(t, cli, configRoot, key)
	assert.False(t, ok, "config %s should not be flipped", key)
}

// embedEtcdClientPort remembers the client port of the singleton embedded etcd
// server, so later tests can point their own confirmator client at it.
var embedEtcdClientPort int

// setupEmbedEtcd starts the embedded etcd server (singleton) and returns a
// client plus the client port. The server is stopped by TestMain after all
// tests.
func setupEmbedEtcd(t *testing.T) (*clientv3.Client, int) {
	t.Helper()
	if etcdkv.HasServer() {
		cli, err := etcdkv.GetEmbedEtcdClient()
		require.NoError(t, err)
		return cli, embedEtcdClientPort
	}
	clientPort, peerPort := freePort(t), freePort(t)
	dataDir, err := os.MkdirTemp("", "test-versiongate-etcd-*")
	require.NoError(t, err)
	cfgFile, err := os.CreateTemp("", "test-versiongate-etcd-*.yaml")
	require.NoError(t, err)
	_, err = fmt.Fprintf(cfgFile, `name: default
data-dir: %s
listen-client-urls: http://127.0.0.1:%d
advertise-client-urls: http://127.0.0.1:%d
listen-peer-urls: http://127.0.0.1:%d
initial-advertise-peer-urls: http://127.0.0.1:%d
initial-cluster: default=http://127.0.0.1:%d
initial-cluster-state: new
`, dataDir, clientPort, clientPort, peerPort, peerPort, peerPort)
	require.NoError(t, err)
	require.NoError(t, cfgFile.Close())
	t.Cleanup(func() {
		os.RemoveAll(dataDir)
		os.Remove(cfgFile.Name())
	})

	require.NoError(t, etcdkv.InitEtcdServer(true, cfgFile.Name(), dataDir, "stdout", "error"))
	cli, err := etcdkv.GetEmbedEtcdClient()
	require.NoError(t, err)
	embedEtcdClientPort = clientPort
	// The embedded etcd server starts asynchronously; wait until it is ready.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := cli.Get(context.Background(), "health")
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			require.NoError(t, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cli, clientPort
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return port
}

// putSession registers a fake session with the given version into etcd.
func putSession(t *testing.T, cli *clientv3.Client, metaRoot, role, nodeID, version string) {
	t.Helper()
	key := path.Join(metaRoot, "session", role, nodeID)
	_, err := cli.Put(context.Background(), key, fmt.Sprintf(`{"Version":%q}`, version))
	require.NoError(t, err)
}
