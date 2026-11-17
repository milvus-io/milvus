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

package initcore

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestExprCacheFlightConfigRefresh(t *testing.T) {
	pt := &paramtable.ComponentParam{}
	pt.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files(nil)))
	var applied exprCacheFlightConfig
	var mu sync.Mutex
	apply := func(cfg exprCacheFlightConfig) error {
		mu.Lock()
		defer mu.Unlock()
		applied = cfg
		return nil
	}
	current := func() exprCacheFlightConfig {
		mu.Lock()
		defer mu.Unlock()
		return applied
	}
	enabled := pt.QueryNodeCfg.ExprResCacheSingleFlightEnabled.Key
	wait := pt.QueryNodeCfg.ExprResCacheSingleFlightMaxWaitMs.Key
	require.NoError(t, pt.Save(enabled, "true"))
	require.NoError(t, registerExprCacheFlightConfig(t.Context(), pt, apply))
	assert.True(t, current().enabled)
	assert.EqualValues(t, 1000, current().maxWaitMs)
	require.NoError(t, pt.Save(wait, "75"))
	assert.EqualValues(t, 75, current().maxWaitMs)
	// Normalized management spelling must reach the runtime watcher too.
	require.NoError(t, pt.Save(config.EtcdConfigKey(enabled), "false"))
	assert.False(t, current().enabled)
	require.NoError(t, pt.Save(enabled, "true"))
	assert.True(t, current().enabled)
	require.NoError(t, pt.Save(wait, "0"))
	assert.False(t, current().enabled)
	require.NoError(t, pt.Remove(wait))
	assert.True(t, current().enabled)
	assert.EqualValues(t, 1000, current().maxWaitMs)
	require.NoError(t, pt.Remove(enabled))
	assert.False(t, current().enabled)
	// No callback changes the ordinary cache enable flag.
	assert.False(t, pt.QueryNodeCfg.ExprResCacheEnabled.GetAsBool())
}

func TestExprCacheFlightConfigValidation(t *testing.T) {
	for _, value := range []string{"0", "-1", "bad", "60001"} {
		t.Run(value, func(t *testing.T) {
			pt := &paramtable.ComponentParam{}
			pt.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files(nil)))
			require.NoError(t, pt.Save(pt.QueryNodeCfg.ExprResCacheSingleFlightMaxWaitMs.Key, value))
			_, err := readExprCacheFlightConfig(pt)
			require.Error(t, err)
		})
	}
}

// Reject invalid limits on refresh and recover on removal; otherwise malformed
// management updates could leave the old coordination policy active indefinitely.
func TestExprCacheFlightLimitRefreshRecovery(t *testing.T) {
	for _, name := range []string{"maxWaitMs", "maxFlights", "maxWaiters", "maxWaitersPerKey"} {
		t.Run(name, func(t *testing.T) {
			pt := &paramtable.ComponentParam{}
			pt.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files(nil)))
			require.NoError(t, pt.Save(pt.QueryNodeCfg.ExprResCacheSingleFlightEnabled.Key, "true"))
			var applied exprCacheFlightConfig
			require.NoError(t, registerExprCacheFlightConfig(t.Context(), pt, func(cfg exprCacheFlightConfig) error { applied = cfg; return nil }))
			expected := applied
			key := "queryNode.exprCache.singleFlight." + name
			for _, invalid := range []string{"0", "-1", "bad", "9223372036854775808"} {
				require.NoError(t, pt.Save(key, invalid))
				assert.False(t, applied.enabled, invalid)
				require.NoError(t, pt.Remove(key))
				assert.Equal(t, expected, applied, "removing invalid setting must restore defaults")
			}
			require.NoError(t, pt.Save(key, "1"))
			assert.True(t, applied.enabled, "minimum positive limit must be accepted")
		})
	}
}
