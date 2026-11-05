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

func TestUpsertWALGenerationDefaultsOffAndRefreshes(t *testing.T) {
	p := &ComponentParam{}
	p.Init(NewBaseTable(SkipRemote(true)))
	item := &p.StreamingCfg.UpsertWALWriteEnabled
	require.Equal(t, "streaming.wal.upsert.enabled", item.Key)
	require.False(t, item.GetAsBool())
	require.NoError(t, p.Save(item.Key, "true"))
	require.True(t, item.GetAsBool())
	require.NoError(t, p.Save(item.Key, "false"))
	require.False(t, item.GetAsBool())
}

func TestUpsertVersionGateRegistrationAndEffectiveValues(t *testing.T) {
	p := &ComponentParam{}
	p.Init(NewBaseTable(SkipRemote(true)))
	item := &p.StreamingCfg.UpsertWALWriteEnabled
	require.Contains(t, p.versionGateItems(), item)
	require.Equal(t, "false", item.DefaultValue)
	require.NotNil(t, item.VersionGateSwitcher)
	require.Equal(t, "3.0.7", item.VersionGateSwitcher.GateVersion)
	require.Equal(t, time.Minute, item.VersionGateSwitcher.SwitchDelay)
	require.NoError(t, p.Save(item.Key, "auto"))
	require.False(t, item.GetAsBool())
	item.VersionGateSwitcher.localSatisfied = true
	item.manager.EvictCachedValue(item.Key)
	require.True(t, item.GetAsBool())
	require.NoError(t, p.Save(item.Key, "false"))
	require.False(t, item.GetAsBool())
	item.VersionGateSwitcher.localSatisfied = false
	item.manager.EvictCachedValue(item.Key)
	require.NoError(t, p.Save(item.Key, "true"))
	require.True(t, item.GetAsBool(), "explicit true remains the operator override")
}

func TestUpsertVersionGateWaitsForAllOnlineRoles(t *testing.T) {
	Init()
	params := Get()
	item := &params.StreamingCfg.UpsertWALWriteEnabled
	require.NotNil(t, item.VersionGateSwitcher)
	previous, present := currentConfigValue(item.Key)
	require.NoError(t, params.Save(item.Key, "auto"))
	defer func() {
		if present {
			require.NoError(t, params.Save(item.Key, previous))
		} else {
			require.NoError(t, params.Reset(item.Key))
		}
	}()
	cli, _ := setupEmbedEtcd(t)
	metaRoot, configRoot := testRoots(t)
	putSession(t, cli, metaRoot, typeutil.ProxyRole, "proxy", "3.0.7")
	putSession(t, cli, metaRoot, typeutil.StreamingNodeRole, "streaming", "3.0.6")
	putSession(t, cli, metaRoot, typeutil.QueryNodeRole, "query", "3.0.7")
	c := newTestConfirmator(t, cli, metaRoot, configRoot)
	switcher := *item.VersionGateSwitcher
	switcher.SwitchDelay = 20 * time.Millisecond // Exercise the real switcher without a one-minute test sleep.
	require.NoError(t, c.registerGate(item.Key, &switcher))
	require.NoError(t, c.start(context.Background()))
	defer c.close()
	assertNoFlip(t, cli, configRoot, item.Key)
	putSession(t, cli, metaRoot, typeutil.StreamingNodeRole, "streaming", "3.0.7")
	waitConfigValue(t, cli, configRoot, item.Key, "true")
}
