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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	ext "github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// hookProvider is a form that supplies nothing but a hook, which is the
// smallest thing a distribution can do to take over the request path.
func installHook(t *testing.T, h hook.Hook) {
	t.Helper()
	ext.ResetForTest()
	t.Cleanup(ext.ResetForTest)
	ext.SetHook(h)
}

func TestInitHookUsesTheCompiledInHook(t *testing.T) {
	paramtable.Init()
	installHook(t, MockAPIHook{User: "root"})

	require.NoError(t, initHook())

	got, err := GetHook().VerifyAPIKey("whatever")
	assert.NoError(t, err)
	assert.Equal(t, "root", got)
}

// With no provider - a stock binary - nothing changes: the default hook, and
// then whatever proxy.soPath says.
func TestInitHookWithoutACompiledInHookIsUnchanged(t *testing.T) {
	paramtable.Init()
	ext.ResetForTest()
	t.Cleanup(ext.ResetForTest)

	require.NoError(t, initHook())
	_, ok := GetHook().(DefaultHook)
	assert.True(t, ok, "a stock binary keeps the default hook")
}

// A provider that fills in no hook is a form that does not want the request
// path, and it must not displace a plug-in or the default.
func TestInitHookIgnoresANilHook(t *testing.T) {
	paramtable.Init()
	installHook(t, nil)

	require.NoError(t, initHook())
	_, ok := GetHook().(DefaultHook)
	assert.True(t, ok)
}

// Two authorities for the same question is a deployment mistake, and it is
// reported rather than silently resolved by start-up order.
func TestInitHookRefusesACompiledInHookBesideAPlugin(t *testing.T) {
	paramtable.Init()
	installHook(t, MockAPIHook{User: "root"})
	p := paramtable.Get()
	require.NoError(t, p.Save(p.ProxyCfg.SoPath.Key, "/tmp/some-hook.so"))
	t.Cleanup(func() { p.Reset(p.ProxyCfg.SoPath.Key) })

	err := initHook()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only one can")
}

// initRecordingHook is a compiled-in hook that remembers how it was
// initialised, and can refuse.
type initRecordingHook struct {
	MockAPIHook
	params  map[string]string
	initErr error
	inits   int
}

func (h *initRecordingHook) Init(params map[string]string) error {
	h.inits++
	h.params = params
	return h.initErr
}

// A compiled-in hook is initialised the way a plug-in is: once, with the hook
// configuration, before it is stored. It is the one call that tells the hook
// it runs in the proxy process.
func TestInitHookInitialisesTheCompiledInHookWithTheHookConfig(t *testing.T) {
	paramtable.Init()
	hp := paramtable.GetHookParams()
	require.NoError(t, hp.Save("somekey", "someValue"))
	t.Cleanup(func() { _ = hp.Save("somekey", "") })
	h := &initRecordingHook{MockAPIHook: MockAPIHook{User: "root"}}
	installHook(t, h)

	require.NoError(t, initHook())

	assert.Equal(t, 1, h.inits, "initialised exactly once")
	assert.Equal(t, "someValue", h.params["somekey"], "the hook sees the hook.* configuration, as a plug-in does")
	assert.Same(t, h, GetHook(), "the initialised hook is the one stored")
}

// A hook that cannot initialise is a proxy that does not start, exactly as
// for a plug-in.
func TestInitHookFailsWhenTheCompiledInHookCannotInitialise(t *testing.T) {
	paramtable.Init()
	h := &initRecordingHook{initErr: errors.New("the internal port is taken")}
	installHook(t, h)

	err := initHook()
	require.Error(t, err)
	assert.ErrorContains(t, err, "the internal port is taken")
	_, isDefault := GetHook().(DefaultHook)
	assert.True(t, isDefault, "a hook that failed to initialise is not stored")
}
