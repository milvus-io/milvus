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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	ext "github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// saveHookKey writes one hook.* configuration key and clears it again when the
// test ends, so that no test inherits the configuration another one left
// behind and the order they run in does not matter.
func saveHookKey(t *testing.T, key, value string) {
	t.Helper()
	hp := paramtable.GetHookParams()
	require.NoError(t, hp.Save(key, value))
	t.Cleanup(func() { _ = hp.Save(key, "") })
}

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
// initialized, and can refuse. The config-reload watcher re-initializes the
// hook from its own goroutine, so the record is taken under a lock.
type initRecordingHook struct {
	MockAPIHook
	initErr error

	mu     sync.Mutex
	params map[string]string
	inits  int
}

func (h *initRecordingHook) Init(params map[string]string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.inits++
	h.params = params
	return h.initErr
}

// failInitsWith makes every later Init refuse, which is what an operator
// editing a hook.* key to a value the hook cannot accept looks like from here.
func (h *initRecordingHook) failInitsWith(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.initErr = err
}

// initCount and initParams read what Init recorded.
func (h *initRecordingHook) initCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.inits
}

func (h *initRecordingHook) initParams() map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.params
}

// A compiled-in hook is initialized the way a plug-in is: once, with the hook
// configuration, before it is stored. It is the one call that tells the hook
// it runs in the proxy process.
func TestInitHookInitialisesTheCompiledInHookWithTheHookConfig(t *testing.T) {
	paramtable.Init()
	saveHookKey(t, "somekey", "someValue")
	h := &initRecordingHook{MockAPIHook: MockAPIHook{User: "root"}}
	installHook(t, h)

	require.NoError(t, initHook())

	assert.Equal(t, 1, h.initCount(), "initialized exactly once")
	assert.Equal(t, "someValue", h.initParams()["somekey"], "the hook sees the hook.* configuration, as a plug-in does")
	assert.Same(t, h, GetHook(), "the initialized hook is the one stored")
}

// A hook that cannot initialize is a proxy that does not start, exactly as
// for a plug-in.
func TestInitHookFailsWhenTheCompiledInHookCannotInitialise(t *testing.T) {
	paramtable.Init()
	h := &initRecordingHook{initErr: errors.New("the internal port is taken")}
	installHook(t, h)

	err := initHook()
	require.Error(t, err)
	assert.ErrorContains(t, err, "the internal port is taken")
	_, isDefault := GetHook().(DefaultHook)
	assert.True(t, isDefault, "a hook that failed to initialize is not stored")
}

// A compiled-in hook is reconfigured the way a plug-in is too: editing a
// hook.* key re-initializes the hook that is installed, whichever way it got
// there. Without the watcher, a form compiled into the binary would keep the
// configuration it was started with forever while a plug-in picked the change
// up, and the two would answer the same config edit differently.
func TestConfigChangeReinitializesTheCompiledInHook(t *testing.T) {
	paramtable.Init()
	h := &initRecordingHook{MockAPIHook: MockAPIHook{User: "root"}}
	installHook(t, h)

	require.NoError(t, initHook())
	require.Equal(t, 1, h.initCount())

	saveHookKey(t, "reloadedkey", "reloadedValue")

	assert.Eventually(t, func() bool {
		return h.initCount() > 1 && h.initParams()["reloadedkey"] == "reloadedValue"
	}, 10*time.Second, 10*time.Millisecond,
		"a hook.* config edit must re-initialize the compiled-in hook with the new configuration")
	assert.Same(t, h, GetHook(), "the re-initialized hook is the one that stays installed")
}

// A hook.* edit the compiled-in hook refuses must not take the proxy down: it
// is already serving, and the edit can be made at any moment. The refusal is
// reported and the configuration that was working stays in place. (Start-up is
// the other way round, and is covered above: a hook that cannot initialize
// there is a proxy that does not start.)
func TestAConfigChangeRefusedByTheCompiledInHookKeepsTheProxyUp(t *testing.T) {
	paramtable.Init()
	h := &initRecordingHook{MockAPIHook: MockAPIHook{User: "root"}}
	installHook(t, h)

	require.NoError(t, initHook())
	// Start-up is over before any edit arrives, as it is in a running proxy:
	// the refusal below has to be judged on the refresh path alone.
	require.Same(t, h, GetHook())
	initsBeforeTheEdit := h.initCount()

	h.failInitsWith(errors.New("hook.someKey is not a duration"))
	saveHookKey(t, "refusedkey", "nonsense")

	assert.Eventually(t, func() bool { return h.initCount() > initsBeforeTheEdit },
		10*time.Second, 10*time.Millisecond,
		"the refused edit must still have been offered to the hook")
	assert.Same(t, h, GetHook(), "the hook that refused the new configuration stays installed")
	h.failInitsWith(nil)
}
