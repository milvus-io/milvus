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
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

func TestExprCacheFlightInvalidStartupConfig(t *testing.T) {
	for _, cacheEnabled := range []string{"false", "true"} {
		for _, flightEnabled := range []string{"false", "true"} {
			for _, invalid := range []struct {
				name, value string
			}{
				{"enabled", "bad"},
				{"maxWaitMs", "0"},
				{"maxWaitMs", "60001"},
				{"maxFlights", "-1"},
				{"maxWaiters", "bad"},
				{"maxWaitersPerKey", "9223372036854775808"},
			} {
				t.Run(cacheEnabled+"/"+flightEnabled+"/"+invalid.name+"/"+invalid.value, func(t *testing.T) {
					pt := &paramtable.ComponentParam{}
					pt.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files(nil)))
					require.NoError(t, pt.Save(pt.QueryNodeCfg.ExprResCacheEnabled.Key, cacheEnabled))
					enabledKey := pt.QueryNodeCfg.ExprResCacheSingleFlightEnabled.Key
					require.NoError(t, pt.Save(enabledKey, flightEnabled))
					key := "queryNode.exprCache.singleFlight." + invalid.name
					require.NoError(t, pt.Save(key, invalid.value))

					var applied []exprCacheFlightConfig
					require.NoError(t, registerExprCacheFlightConfig(t.Context(), pt, func(cfg exprCacheFlightConfig) error {
						applied = append(applied, cfg)
						return nil
					}))
					require.Len(t, applied, 1)
					assert.False(t, applied[0].enabled)
					assert.Equal(t, cacheEnabled, pt.QueryNodeCfg.ExprResCacheEnabled.GetValue())

					// Watchers remain registered after an invalid startup configuration.
					require.NoError(t, pt.Remove(key))
					require.Len(t, applied, 2)
					assert.Equal(t, pt.QueryNodeCfg.ExprResCacheSingleFlightEnabled.GetAsBool(), applied[1].enabled)
					assert.Equal(t, cacheEnabled, pt.QueryNodeCfg.ExprResCacheEnabled.GetValue())
				})
			}
		}
	}
}

func TestExprCacheFlightStartupApplyFailure(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(strconv.FormatBool(invalid), func(t *testing.T) {
			pt := &paramtable.ComponentParam{}
			pt.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Files(nil)))
			if invalid {
				require.NoError(t, pt.Save(pt.QueryNodeCfg.ExprResCacheSingleFlightMaxWaitMs.Key, "0"))
			}
			applyErr := merr.WrapErrServiceInternalMsg("configuration apply failed")
			err := registerExprCacheFlightConfig(t.Context(), pt, func(cfg exprCacheFlightConfig) error {
				return applyErr
			})
			require.ErrorIs(t, err, applyErr)
			assert.Equal(t, merr.Code(applyErr), merr.Code(err))
			if invalid {
				assert.ErrorContains(t, err, "disable expression single-flight after invalid configuration")
			}
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
