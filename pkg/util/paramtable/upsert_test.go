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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestUpsertMessageVersionGate(t *testing.T) {
	p := &ComponentParam{}
	p.Init(NewBaseTable(SkipRemote(true)))
	item := &p.StreamingCfg.EnableUpsertMessage
	require.Equal(t, "streaming.wal.enableUpsertMessage", item.Key)
	require.Equal(t, "auto", item.DefaultValue)
	require.Nil(t, item.Formatter)
	require.Contains(t, p.versionGateItems(), item)
	require.NotNil(t, item.VersionGateSwitcher)
	require.Equal(t, "auto", item.VersionGateSwitcher.EnableAutoSwitchValue)
	require.Equal(t, "false", item.VersionGateSwitcher.PreSwitchValue)
	require.Equal(t, "3.1.0", item.VersionGateSwitcher.GateVersion)
	require.Equal(t, "true", item.VersionGateSwitcher.TargetValue)
	require.Equal(t, time.Minute, item.VersionGateSwitcher.SwitchDelay)
	require.False(t, item.GetAsBool())

	for _, value := range []string{"true", "false", "auto"} {
		require.NoError(t, p.Save(item.Key, value))
		require.Equal(t, value == "true", item.GetAsBool())
	}
	// Explicit configuration also wins after the local version condition is met.
	item.VersionGateSwitcher.localSatisfied = true
	item.manager.EvictCachedValue(item.Key)
	require.True(t, item.GetAsBool())
	require.NoError(t, p.Save(item.Key, "false"))
	require.False(t, item.GetAsBool())
	require.NoError(t, p.Save(item.Key, "true"))
	require.True(t, item.GetAsBool())
}

func TestUpsertMessageIgnoresDraftKeys(t *testing.T) {
	p := &ComponentParam{}
	p.Init(NewBaseTable(SkipRemote(true)))
	item := &p.StreamingCfg.EnableUpsertMessage
	for _, key := range []string{"streaming.wal.upsert.unifiedEnabled", "streaming.wal.upsert.enabled"} {
		require.NoError(t, p.Save(key, "true"))
		require.False(t, item.GetAsBool(), "draft key %s must not enable the new format", key)
	}
}

func TestUpsertMessageVersionGateWaitsForCluster(t *testing.T) {
	p := &ComponentParam{}
	p.Init(NewBaseTable(SkipRemote(true)))
	item := &p.StreamingCfg.EnableUpsertMessage
	require.NotNil(t, item.VersionGateSwitcher)
	require.Equal(t, time.Minute, item.VersionGateSwitcher.SwitchDelay)
	// Use the production descriptor with a shorter window for this mechanism test.
	switcher := *item.VersionGateSwitcher
	switcher.SwitchDelay = 200 * time.Millisecond
	oldBaseTable := params.baseTable
	params.baseTable = p.baseTable
	defer func() { params.baseTable = oldBaseTable }()
	cli, _ := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	for _, role := range []string{typeutil.ProxyRole, typeutil.DataNodeRole, typeutil.QueryNodeRole, typeutil.StreamingNodeRole} {
		putSession(t, cli, metaRoot, role, "node-1", "3.1.0")
	}
	putSession(t, cli, metaRoot, typeutil.QueryNodeRole, "node-1", "3.0.99")
	c := newTestConfirmator(t, cli, metaRoot, configRoot)
	require.NoError(t, c.registerGate(item.Key, &switcher))
	require.NoError(t, c.start(context.Background()))
	defer c.close()
	assertNoFlip(t, cli, configRoot, item.Key)
	c.mu.Lock()
	armedBeforeUpgrade := !c.gates[0].armedAt.IsZero()
	c.mu.Unlock()
	require.False(t, armedBeforeUpgrade)
	upgradedAt := time.Now()
	putSession(t, cli, metaRoot, typeutil.QueryNodeRole, "node-1", "3.1.0")
	waitConfigValue(t, cli, configRoot, item.Key, "true")
	require.GreaterOrEqual(t, time.Since(upgradedAt), switcher.SwitchDelay)
}

func TestEnableUpsertMessageFromEnvironment(t *testing.T) {
	for _, value := range []string{"true", "false"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("STREAMING_WAL_ENABLEUPSERTMESSAGE", value)
			p := &ComponentParam{}
			p.Init(NewBaseTable(SkipRemote(true), Files(nil)))
			require.Equal(t, value == "true", p.StreamingCfg.EnableUpsertMessage.GetAsBool())
		})
	}
}
