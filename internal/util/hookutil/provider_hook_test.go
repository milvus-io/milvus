/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hookutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	ext "github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// hookProvider is a form that supplies nothing but a hook, which is the
// smallest thing a distribution can do to take over the request path.
type hookProvider struct {
	hook hook.Hook
}

func (hookProvider) Name() string                 { return "test" }
func (hookProvider) Requires() []ext.CapabilityID { return nil }
func (p hookProvider) Capabilities() ext.Capabilities {
	return ext.Capabilities{Hook: p.hook}
}

func installHookProvider(t *testing.T, h hook.Hook) {
	t.Helper()
	ext.ResetForTest()
	t.Cleanup(ext.ResetForTest)
	require.NoError(t, ext.SetProvider(hookProvider{hook: h}))
}

// A form's hook becomes the hook the proxy's interceptor consults, without a
// plug-in file existing anywhere.
func TestInitHookUsesTheProvidersHook(t *testing.T) {
	paramtable.Init()
	installHookProvider(t, MockAPIHook{User: "root"})

	require.NoError(t, initHook())

	got, err := GetHook().VerifyAPIKey("whatever")
	assert.NoError(t, err)
	assert.Equal(t, "root", got)
}

// With no provider - a stock binary - nothing changes: the default hook, and
// then whatever proxy.soPath says.
func TestInitHookWithoutAProviderIsUnchanged(t *testing.T) {
	paramtable.Init()
	ext.ResetForTest()
	t.Cleanup(ext.ResetForTest)

	require.NoError(t, initHook())
	_, ok := GetHook().(DefaultHook)
	assert.True(t, ok, "a stock binary keeps the default hook")
}

// A provider that fills in no hook is a form that does not want the request
// path, and it must not displace a plug-in or the default.
func TestInitHookIgnoresAProviderWithoutAHook(t *testing.T) {
	paramtable.Init()
	installHookProvider(t, nil)

	require.NoError(t, initHook())
	_, ok := GetHook().(DefaultHook)
	assert.True(t, ok)
}

// Two authorities for the same question is a deployment mistake, and it is
// reported rather than silently resolved by start-up order.
func TestInitHookRefusesAProviderHookBesideAPlugin(t *testing.T) {
	paramtable.Init()
	installHookProvider(t, MockAPIHook{User: "root"})
	p := paramtable.Get()
	require.NoError(t, p.Save(p.ProxyCfg.SoPath.Key, "/tmp/some-hook.so"))
	t.Cleanup(func() { p.Reset(p.ProxyCfg.SoPath.Key) })

	err := initHook()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only one can")
}
