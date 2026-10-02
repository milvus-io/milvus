// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hookutil

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type configErrorCipher struct {
	testCipher
	init func(map[string]string) error
}

func (c configErrorCipher) Init(params map[string]string) error { return c.init(params) }

type configErrorHook struct {
	DefaultHook
	init func(map[string]string) error
}

func (h configErrorHook) Init(params map[string]string) error { return h.init(params) }

func TestCipherReloadFailureLogsProtectConfig(t *testing.T) {
	paramtable.Init()
	InitOnceCipher()
	params := paramtable.GetCipherParams()
	for _, item := range []*paramtable.ParamItem{&params.DefaultRootKey, &params.RotationPeriodInHours} {
		t.Run(item.Key, func(t *testing.T) {
			const canary = "cipher-reload-secret-canary"
			oldSecret, oldValue := params.DefaultRootKey.GetValue(), item.GetValue()
			require.NoError(t, params.Save(params.DefaultRootKey.Key, canary))
			expectedSecret := canary
			var pluginErr error
			storeCipher(configErrorCipher{init: func(values map[string]string) error {
				require.Equal(t, expectedSecret, values[config.EtcdConfigKey(params.DefaultRootKey.Key)])
				pluginErr = merr.WrapErrServiceUnavailableMsg("cannot load %s", values[config.EtcdConfigKey(params.DefaultRootKey.Key)])
				return pluginErr
			}})
			item.RegisterCallback(reloadCipherConfig)
			t.Cleanup(func() {
				item.UnregisterCallback()
				params.Save(item.Key, oldValue)
				params.Save(params.DefaultRootKey.Key, oldSecret)
				storeCipher(nil)
			})
			sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
			// The direct consumer must still receive the original code and cause.
			err := reloadCipherConfig(context.Background(), item.Key, "", "")
			require.Same(t, pluginErr, err)
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			// Save dispatches the real ParamItem callback and its failure log.
			newValue := oldValue + "1"
			if item == &params.DefaultRootKey {
				expectedSecret = canary + "-updated"
				newValue = expectedSecret
			}
			require.NoError(t, params.Save(item.Key, newValue))
			assert.Contains(t, sink.String(), "param change callback failed")
			assert.NotContains(t, sink.String(), canary)
		})
	}
}

func TestCipherStartupFailureLogsProtectConfig(t *testing.T) {
	paramtable.Init()
	params := paramtable.GetCipherParams()
	const canary = "cipher-startup-secret-canary"
	for item, value := range map[*paramtable.ParamItem]string{
		&params.SoPathGo: "cipher-path-canary.so", &params.SoPathCpp: "cipher-path-canary-cpp.so", &params.DefaultRootKey: canary,
	} {
		old := item.GetValue()
		require.NoError(t, params.Save(item.Key, value))
		t.Cleanup(func() { params.Save(item.Key, old) })
	}
	var cause error
	cipher := configErrorCipher{init: func(values map[string]string) error {
		require.Equal(t, canary, values[config.EtcdConfigKey(params.DefaultRootKey.Key)])
		cause = merr.WrapErrServiceUnavailableMsg("cannot load %s", values[config.EtcdConfigKey(params.DefaultRootKey.Key)])
		return cause
	}}
	patch := mockey.MockGeneric(LoadPlugin[hook.Cipher]).Return(cipher, nil).Build()
	defer patch.UnPatch()
	err := initCipher()
	require.ErrorIs(t, err, cause)
	require.Equal(t, merr.Code(cause), merr.Code(err))
	sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
	initCipherOnce = sync.Once{}
	t.Cleanup(func() { initCipherOnce = sync.Once{}; storeCipher(nil) })
	assert.Panics(t, InitOnceCipher)
	assert.Contains(t, sink.String(), "fail to init cipher plugin")
	assert.NotContains(t, sink.String(), canary)
	assert.NotContains(t, sink.String(), "cipher-path-canary")
}

func TestHookStartupFailureLogsProtectConfig(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	const key, canary = "opaqueSetting", "hook-startup-secret-canary"
	require.NoError(t, paramtable.GetHookParams().Save(key, canary))
	t.Cleanup(func() { paramtable.GetHookParams().Save(key, "") })
	require.NoError(t, params.Save(params.ProxyCfg.SoPath.Key, "hook.so"))
	t.Cleanup(func() { params.Reset(params.ProxyCfg.SoPath.Key) })
	h := configErrorHook{init: func(values map[string]string) error {
		require.Equal(t, canary, values["opaquesetting"])
		return merr.WrapErrServiceUnavailableMsg("cannot load %s", values["opaquesetting"])
	}}
	patch := mockey.MockGeneric(LoadPlugin[hook.Hook]).Return(h, nil).Build()
	defer patch.UnPatch()
	for _, panicOnError := range []string{"true", "false"} {
		t.Run(panicOnError, func(t *testing.T) {
			require.NoError(t, params.Save(params.CommonCfg.PanicWhenPluginFail.Key, panicOnError))
			t.Cleanup(func() { params.Reset(params.CommonCfg.PanicWhenPluginFail.Key) })
			sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
			initOnce = sync.Once{}
			if panicOnError == "true" {
				assert.Panics(t, InitOnceHook)
			} else {
				assert.NotPanics(t, InitOnceHook)
				assert.IsType(t, DefaultHook{}, GetHook(), "disabled failure panic retains the default hook")
			}
			assert.Contains(t, sink.String(), "fail to init hook")
			assert.NotContains(t, sink.String(), canary)
		})
	}
}

func TestHookRefreshFailureLogsProtectConfig(t *testing.T) {
	const childEnv = "MILVUS_TEST_HOOK_REFRESH_FAILURE"
	if os.Getenv(childEnv) != "1" {
		executable, err := os.Executable()
		require.NoError(t, err)
		cmd := exec.Command(executable, "-test.run=^TestHookRefreshFailureLogsProtectConfig$") // #nosec G204 -- Re-exec the current test binary with a fixed test filter.
		cmd.Env = append(os.Environ(), childEnv+"=1")
		output, err := cmd.CombinedOutput()
		require.Error(t, err, "the refresh failure must still panic")
		assert.Contains(t, string(output), "fail to init configs for the hook when refreshing")
		assert.NotContains(t, string(output), "hook-refresh-secret-canary")
		assert.NotContains(t, string(output), "hook-refresh-key-canary")
		return
	}
	paramtable.Init()
	params := paramtable.Get()
	const key, canary = "hook-refresh-key-canary", "hook-refresh-secret-canary"
	require.NoError(t, params.Save(params.ProxyCfg.SoPath.Key, "hook.so"))
	t.Cleanup(func() { params.Reset(params.ProxyCfg.SoPath.Key) })
	require.NoError(t, paramtable.GetHookParams().Save(key, ""))
	h := configErrorHook{init: func(values map[string]string) error {
		if value := values[key]; value != "" {
			return merr.WrapErrServiceUnavailableMsg("cannot load %s", value)
		}
		return nil
	}}
	hookPatch := mockey.MockGeneric(LoadPlugin[hook.Hook]).Return(h, nil).Build()
	defer hookPatch.UnPatch()
	extPatch := mockey.MockGeneric(LoadPlugin[hook.Extension]).Return(DefaultExtension{}, nil).Build()
	defer extPatch.UnPatch()
	initOnce = sync.Once{}
	InitOnceHook()
	// Run the real async watcher through its terminal Panic log in a subprocess.
	require.NoError(t, paramtable.GetHookParams().Save(key, canary))
	time.Sleep(time.Second)
	t.Fatal("hook refresh did not panic")
}
