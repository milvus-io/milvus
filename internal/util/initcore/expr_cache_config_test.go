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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestExprCacheEnableCallbackAppliesConfigBeforeEnabling(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	SetupCoreConfigChangelCallback()
	key := pt.QueryNodeCfg.ExprResCacheEnabled.Key
	previous := pt.QueryNodeCfg.ExprResCacheEnabled.GetValue()

	var backendEnabled bool
	var enableCalls []bool
	var configCalls int
	var configAccepted bool
	enablePatch := mockey.Mock(UpdateExprResCacheEnable).To(func(enabled bool) {
		enableCalls = append(enableCalls, enabled)
		backendEnabled = enabled
	}).Build()
	t.Cleanup(func() { enablePatch.UnPatch() })
	configPatch := mockey.Mock(UpdateExprResCacheConfig).To(func() {
		configCalls++
		// Observe the backend while configuration is being applied, rather
		// than only checking its final state after an invalid config disables it.
		assert.False(t, backendEnabled, "cache was enabled before configuration completed")
		// Model the C API's success/failure contract: only successful setup
		// enables the cache. The callback must not override a rejected setup.
		backendEnabled = configAccepted
	}).Build()
	t.Cleanup(func() { configPatch.UnPatch() })
	t.Cleanup(func() {
		assert.NoError(t, pt.Save(key, previous))
	})

	for _, tc := range []struct {
		name     string
		accepted bool
	}{
		{"successful setup", true},
		{"rejected setup", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, pt.Save(key, "false"))
			backendEnabled = false
			enableCalls = nil
			configCalls = 0
			configAccepted = tc.accepted

			require.NoError(t, pt.Save(key, "true"))
			assert.Equal(t, 1, configCalls)
			assert.Empty(t, enableCalls, "enabling must be owned by configuration setup")
			assert.Equal(t, tc.accepted, backendEnabled)

			require.NoError(t, pt.Save(key, "false"))
			assert.False(t, backendEnabled)
			assert.Equal(t, []bool{false}, enableCalls)
			assert.Equal(t, 1, configCalls, "disabling must not rebuild the backend")
		})
	}
}
