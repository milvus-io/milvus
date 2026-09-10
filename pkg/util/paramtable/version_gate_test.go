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
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
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
// using the same shared etcd client that production injects (the confirmator
// never opens its own connection).
func newTestConfirmator(t *testing.T, etcdCli *clientv3.Client, metaRoot, configRoot string) *confirmator {
	t.Helper()
	return newConfirmator(etcdCli, metaRoot, configRoot)
}

func TestConfirmator_RegisterGate(t *testing.T) {
	cli, _ := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	c := newTestConfirmator(t, cli, metaRoot, configRoot)

	// nil switcher is rejected.
	assert.Error(t, c.registerGate(testConfigKey, nil))
	// unparseable gate version is rejected.
	assert.Error(t, c.registerGate(testConfigKey, &VersionGateSwitcher{GateVersion: "not-a-version"}))

	require.NoError(t, c.registerGate(testConfigKey, gateSwitcher("2.6.23", 10*time.Millisecond)))
	require.NoError(t, c.start(context.Background()))
	defer c.close()

	// one-shot: no gate can be registered after Start.
	assert.Error(t, c.registerGate(testConfigKey, gateSwitcher("2.6.23", 10*time.Millisecond)))
}

func TestConfirmator_FlipsAfterAllUpAndDelay(t *testing.T) {
	cli, _ := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putAllUpSessions(t, cli, metaRoot)

	c := newTestConfirmator(t, cli, metaRoot, configRoot)
	require.NoError(t, c.registerGate(testConfigKey, gateSwitcher("2.6.23", 50*time.Millisecond)))
	require.NoError(t, c.start(context.Background()))
	defer c.close()

	waitConfigValue(t, cli, configRoot, testConfigKey, testConfigValue)
}

func TestConfirmator_NoSessionsNoFlip(t *testing.T) {
	// No session at all: the minimum online version is unknown (zero), so no
	// gate can ever be above its GateVersion and nothing is flipped.
	cli, _ := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)

	c := newTestConfirmator(t, cli, metaRoot, configRoot)
	require.NoError(t, c.registerGate(testConfigKey, gateSwitcher("2.6.23", 50*time.Millisecond)))
	require.NoError(t, c.start(context.Background()))
	defer c.close()

	assertNoFlip(t, cli, configRoot, testConfigKey)
}

func TestConfirmator_MixedVersionsNoFlip(t *testing.T) {
	cli, _ := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putSession(t, cli, metaRoot, typeutil.DataNodeRole, "node-1", "2.6.23")
	putSession(t, cli, metaRoot, typeutil.ProxyRole, "node-1", "2.6.22")

	c := newTestConfirmator(t, cli, metaRoot, configRoot)
	require.NoError(t, c.registerGate(testConfigKey, gateSwitcher("2.6.23", 30*time.Millisecond)))
	require.NoError(t, c.start(context.Background()))
	defer c.close()

	assertNoFlip(t, cli, configRoot, testConfigKey)
}

func TestConfirmator_SessionDipResetsStabilityWindow(t *testing.T) {
	cli, _ := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putSession(t, cli, metaRoot, typeutil.ProxyRole, "node-1", "2.6.23")
	putSession(t, cli, metaRoot, typeutil.QueryNodeRole, "node-1", "2.6.23")

	c := newTestConfirmator(t, cli, metaRoot, configRoot)
	require.NoError(t, c.registerGate(testConfigKey, gateSwitcher("2.6.23", 200*time.Millisecond)))
	require.NoError(t, c.start(context.Background()))
	defer c.close()

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
	cli, _ := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putAllUpSessions(t, cli, metaRoot)
	putConfig(t, cli, configRoot, testConfigKey, "false")

	c := newTestConfirmator(t, cli, metaRoot, configRoot)
	require.NoError(t, c.registerGate(testConfigKey, gateSwitcher("2.6.23", 30*time.Millisecond)))
	require.NoError(t, c.start(context.Background()))
	defer c.close()

	// The explicit value must not be overwritten by the flip.
	waitConfigValue(t, cli, configRoot, testConfigKey, "false")
}

func TestConfirmator_LocalValueChangeBeforeFlipWins(t *testing.T) {
	// An explicit value set from a non-etcd source (file/env) while the
	// confirmator is running — after start but before the stability window
	// expires — must win over the flip: no etcd write occurs and the gate
	// resolves as-is (adversarial review finding on flip).
	cli, _ := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putAllUpSessions(t, cli, metaRoot)

	// Point the process-local config at a test base table so flip's
	// currentConfigValue re-read can observe the runtime value change.
	oldBaseTable := params.baseTable
	bt := NewBaseTable(SkipRemote(true))
	params.baseTable = bt
	defer func() { params.baseTable = oldBaseTable }()

	c := newTestConfirmator(t, cli, metaRoot, configRoot)
	require.NoError(t, c.registerGate(testConfigKey, gateSwitcher("2.6.23", 300*time.Millisecond)))
	require.NoError(t, c.start(context.Background()))
	defer c.close()

	// Operator sets the "false" escape hatch in a non-etcd source while the
	// confirmator is running, before the stability window elapses.
	bt.Manager().SetConfig(testConfigKey, "false")

	// The gate resolves without writing the config-center key.
	waitGateResolved(t, c)
	assertConfigAbsent(t, cli, configRoot, testConfigKey)
}

func TestConfirmator_AlreadyFlippedAtStart(t *testing.T) {
	cli, _ := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putConfig(t, cli, configRoot, testConfigKey, testConfigValue)

	c := newTestConfirmator(t, cli, metaRoot, configRoot)
	require.NoError(t, c.registerGate(testConfigKey, gateSwitcher("2.6.23", 10*time.Millisecond)))
	require.NoError(t, c.start(context.Background()))
	defer c.close()

	// The gate is resolved immediately: no session watch is running.
	c.mu.Lock()
	resolved := c.gates[0].resolved
	c.mu.Unlock()
	assert.True(t, resolved)
}

func TestConfirmator_MultipleGatesIndependent(t *testing.T) {
	cli, _ := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putAllUpSessions(t, cli, metaRoot)

	c := newTestConfirmator(t, cli, metaRoot, configRoot)
	require.NoError(t, c.registerGate(testConfigKey, gateSwitcher("2.6.23", 50*time.Millisecond)))
	require.NoError(t, c.registerGate("function.testGate2", gateSwitcher("3.0.0", 50*time.Millisecond)))
	require.NoError(t, c.start(context.Background()))
	defer c.close()

	// The 2.6.23 gate flips; the 3.0.0 gate stays pending.
	waitConfigValue(t, cli, configRoot, testConfigKey, testConfigValue)
	time.Sleep(150 * time.Millisecond)
	assertConfigAbsent(t, cli, configRoot, "function.testGate2")
}

func TestConfirmator_DependencyOrdersFlip(t *testing.T) {
	// The WAL payload chunking pair: streaming.splitChunkSN flips to "true"
	// first; proxy.splitChunk (which declares DependsOn on it) flips to
	// "false" only after the dependency holds in the config center.
	cli, _ := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putAllUpSessionsAt(t, cli, metaRoot, "3.1.0")

	c := newTestConfirmator(t, cli, metaRoot, configRoot)
	require.NoError(t, c.registerGate("streaming.splitChunkSN", &VersionGateSwitcher{
		EnableAutoSwitchValue: "auto",
		PreSwitchValue:        "false",
		GateVersion:           "3.1.0",
		TargetValue:           "true",
		SwitchDelay:           50 * time.Millisecond,
	}))
	require.NoError(t, c.registerGate("proxy.splitChunk", &VersionGateSwitcher{
		EnableAutoSwitchValue: "auto",
		PreSwitchValue:        "true",
		GateVersion:           "3.1.0",
		TargetValue:           "false",
		SwitchDelay:           80 * time.Millisecond,
		DependsOn:             "streaming.splitChunkSN",
		DependsOnValue:        "true",
	}))
	require.NoError(t, c.start(context.Background()))
	defer c.close()

	waitConfigValue(t, cli, configRoot, "streaming.splitChunkSN", "true")
	waitConfigValue(t, cli, configRoot, "proxy.splitChunk", "false")
}

func TestConfirmator_DependencyBlocksFlipUntilSatisfied(t *testing.T) {
	// Without the dependency (streaming.splitChunkSN=true) the proxy gate stays
	// pending no matter how long its own stability window has elapsed — e.g. an
	// operator's explicit splitChunkSN=false escape hatch blocks the dependent
	// flip forever. Once the dependency holds, the flip happens.
	cli, _ := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putAllUpSessionsAt(t, cli, metaRoot, "3.1.0")

	c := newTestConfirmator(t, cli, metaRoot, configRoot)
	require.NoError(t, c.registerGate("proxy.splitChunk", &VersionGateSwitcher{
		EnableAutoSwitchValue: "auto",
		PreSwitchValue:        "true",
		GateVersion:           "3.1.0",
		TargetValue:           "false",
		SwitchDelay:           50 * time.Millisecond,
		DependsOn:             "streaming.splitChunkSN",
		DependsOnValue:        "true",
	}))
	require.NoError(t, c.start(context.Background()))
	defer c.close()

	assertNoFlip(t, cli, configRoot, "proxy.splitChunk")

	putConfig(t, cli, configRoot, "streaming.splitChunkSN", "true")
	waitConfigValue(t, cli, configRoot, "proxy.splitChunk", "false")
}

func TestStartVersionGatesSkipRemote(t *testing.T) {
	// startVersionGates is a no-op for a skip-remote param table (the common
	// test setup): no confirmator is created and no goroutine leaks.
	p := &ComponentParam{}
	p.Init(NewBaseTable(SkipRemote(true)))
	p.startVersionGates()
	assert.Nil(t, p.versionGates)
}

func TestVersionGateItems_SplitChunkWiring(t *testing.T) {
	// The WAL payload chunking capability is wired into the confirmator as an
	// ordered pair: streaming.splitChunkSN auto-flips to "true" at 3.1, and
	// proxy.splitChunk (registered AFTER it) auto-flips to "false" only once
	// the SN gate's "true" is confirmed in the config center (DependsOn check),
	// enforcing the design doc §7 etcd write ordering as a state check.
	// Registration order is the flip order; per-node observation is deliberately
	// outside the scope of the gate.
	p := &ComponentParam{}
	p.Init(NewBaseTable(SkipRemote(true)))

	items := p.versionGateItems()
	var sn, proxy *ParamItem
	snIdx, proxyIdx := -1, -1
	for i, item := range items {
		switch item.Key {
		case "streaming.splitChunkSN":
			sn, snIdx = item, i
		case "proxy.splitChunk":
			proxy, proxyIdx = item, i
		}
	}
	require.NotNil(t, sn)
	require.NotNil(t, proxy, "proxy.splitChunk must be registered for the dependency-checked flip")
	assert.Less(t, snIdx, proxyIdx, "proxy.splitChunk must flip after streaming.splitChunkSN")
	require.NotNil(t, sn.VersionGateSwitcher)
	assert.Equal(t, "auto", sn.VersionGateSwitcher.EnableAutoSwitchValue)
	assert.Equal(t, "false", sn.VersionGateSwitcher.PreSwitchValue)
	assert.Equal(t, "3.1.0", sn.VersionGateSwitcher.GateVersion)
	assert.Equal(t, "true", sn.VersionGateSwitcher.TargetValue)
	require.NotNil(t, proxy.VersionGateSwitcher)
	assert.Equal(t, "true", proxy.VersionGateSwitcher.PreSwitchValue)
	assert.Equal(t, "3.1.0", proxy.VersionGateSwitcher.GateVersion)
	assert.Equal(t, "false", proxy.VersionGateSwitcher.TargetValue)
	assert.Equal(t, "streaming.splitChunkSN", proxy.VersionGateSwitcher.DependsOn)
	assert.Equal(t, "true", proxy.VersionGateSwitcher.DependsOnValue)
}

func TestStartVersionGatesEmbeddedEtcd(t *testing.T) {
	// Embedded-etcd deployments are single-process: when the local version
	// already satisfies the gate, startVersionGates resolves the item directly
	// (localSatisfied -> TargetValue) instead of creating a confirmator.
	t.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
	p := &ComponentParam{}
	bt := NewBaseTable(SkipRemote(true))
	p.Init(bt)
	item := &p.FunctionCfg.EnableWriteBeforeMaterialization
	require.NotNil(t, item.VersionGateSwitcher)
	assert.False(t, item.VersionGateSwitcher.localSatisfied)

	// Same package: flip skipRemote off so startVersionGates actually runs, and
	// enable embedded etcd. FunctionCfg is already initialized by Init.
	bt.config.skipRemote = false
	p.EtcdCfg.UseEmbedEtcd.SwapTempValue("true")
	defer func() {
		p.EtcdCfg.UseEmbedEtcd.SwapTempValue("")
		bt.config.skipRemote = true
	}()

	p.startVersionGates()
	// Local version (common.Version, 3.0.0-beta on master) >= 2.6.23: the gate
	// is locally satisfied and no confirmator is created. startVersionGates
	// itself evicts the value cache, so the resolution is observable directly.
	assert.True(t, item.VersionGateSwitcher.localSatisfied)
	assert.Nil(t, p.versionGates)
	assert.Equal(t, "true", item.GetValue())
	assert.True(t, item.GetAsBool())

	// The split-chunk capability gate (3.1.0) is NOT satisfied by the local
	// build version (3.0.0-beta on master): it stays closed and reads the
	// pre-switch "false", so a standalone build below 3.1 keeps the legacy
	// single-record path. proxy.splitChunk (registered, but its 3.1.0 gate is
	// likewise below the local version) keeps the pre-switch "true".
	sn := &p.StreamingCfg.SplitChunkSN
	assert.False(t, sn.VersionGateSwitcher.localSatisfied)
	sn.manager.EvictCachedValue(sn.Key)
	assert.Equal(t, "false", sn.GetValue())
	assert.False(t, sn.GetAsBool())
	proxy := &p.ProxyCfg.SplitChunkProxy
	assert.False(t, proxy.VersionGateSwitcher.localSatisfied)
	proxy.manager.EvictCachedValue(proxy.Key)
	assert.Equal(t, "true", proxy.GetValue())
	assert.True(t, proxy.GetAsBool())

	// Negative branch: a gate whose version is above the local version stays
	// closed — localSatisfied remains false and the item reads PreSwitchValue.
	// (Reset the hint set by the positive branch, then re-run.)
	item.VersionGateSwitcher.localSatisfied = false
	oldGateVersion := item.VersionGateSwitcher.GateVersion
	item.VersionGateSwitcher.GateVersion = "99.0.0"
	defer func() { item.VersionGateSwitcher.GateVersion = oldGateVersion }()

	p.startVersionGates()
	assert.False(t, item.VersionGateSwitcher.localSatisfied)
	item.manager.EvictCachedValue(item.Key)
	assert.Equal(t, "false", item.GetValue())
	assert.False(t, item.GetAsBool())
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
	putAllUpSessionsAt(t, cli, metaRoot, "2.6.23")
}

// putAllUpSessionsAt is putAllUpSessions with an explicit session version.
func putAllUpSessionsAt(t *testing.T, cli *clientv3.Client, metaRoot, version string) {
	t.Helper()
	for _, role := range []string{
		typeutil.ProxyRole,
		typeutil.DataNodeRole,
		typeutil.QueryNodeRole,
		typeutil.StreamingNodeRole,
	} {
		putSession(t, cli, metaRoot, role, "node-1", version)
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

// waitGateResolved polls until the confirmator has marked the (single) gate as
// resolved, e.g. superseded by an explicit value without an etcd write.
func waitGateResolved(t *testing.T, c *confirmator) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		resolved := len(c.gates) == 1 && c.gates[0].resolved
		c.mu.Unlock()
		if resolved {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("gate did not resolve within timeout")
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
