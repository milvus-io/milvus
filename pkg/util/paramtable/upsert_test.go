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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpsertWALGenerationDefaultsOffAndRefreshes(t *testing.T) {
	p := &ComponentParam{}
	p.Init(NewBaseTable(SkipRemote(true)))
	item := &p.StreamingCfg.UpsertWALWriteEnabled
	require.Equal(t, "streaming.wal.upsert.unifiedEnabled", item.Key)
	require.False(t, item.GetAsBool())
	require.NoError(t, p.Save(item.Key, "true"))
	require.True(t, item.GetAsBool())
	require.NoError(t, p.Save(item.Key, "false"))
	require.False(t, item.GetAsBool())
}

func TestUnifiedUpsertDoesNotUseDraftVersionGate(t *testing.T) {
	p := &ComponentParam{}
	p.Init(NewBaseTable(SkipRemote(true)))
	item := &p.StreamingCfg.UpsertWALWriteEnabled
	require.NotContains(t, p.versionGateItems(), item)
	require.Nil(t, item.VersionGateSwitcher, "the new capability has no approved release threshold")
	require.NoError(t, p.Save("streaming.wal.upsert.enabled", "true"))
	require.False(t, item.GetAsBool(), "the draft switch cannot activate the new format")
	require.NoError(t, p.Save(item.Key, "auto"))
	require.False(t, item.GetAsBool())
	require.NoError(t, p.Save(item.Key, "true"))
	require.True(t, item.GetAsBool(), "explicit enablement requires reader-first deployment")
}
