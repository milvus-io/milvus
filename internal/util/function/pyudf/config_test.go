// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pyudf

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func swapParam(t *testing.T, item *paramtable.ParamItem, value string) {
	t.Helper()
	old := item.SwapTempValue(value)
	t.Cleanup(func() { item.SwapTempValue(old) })
}

func defaultTestConfig() Config {
	return Config{
		Address: "127.0.0.1:19090", RPCTimeout: 30 * time.Second,
		ConnectionPoolSize: 10, MaxMessageBytes: 64 << 20,
		Server: ServerConfig{WorkerCount: 1, GRPCConcurrency: 10, MaxConcurrentRPCs: 100, ShutdownTimeout: 30 * time.Second},
	}
}

func TestNewConfig(t *testing.T) {
	f := &paramtable.Get().FunctionCfg
	swapParam(t, &f.PyUDFEnabled, "false")
	config, err := NewConfig(context.Background())
	require.NoError(t, err)
	assert.Equal(t, defaultTestConfig(), config)
	swapParam(t, &f.PyUDFEnabled, "true")
	swapParam(t, &f.PyUDFAddress, "127.0.0.2:19091")
	swapParam(t, &f.PyUDFConnectionPoolSize, "3")
	swapParam(t, &f.PyUDFWorkerCount, "2")
	swapParam(t, &f.PyUDFGRPCConcurrency, "4")
	swapParam(t, &f.PyUDFMaxConcurrentRPCs, "40")
	swapParam(t, &f.PyUDFRPCTimeout, "5500ms")
	config, err = NewConfig(context.Background())
	require.NoError(t, err)
	assert.True(t, config.Enabled)
	assert.Equal(t, "127.0.0.2:19091", config.Address)
	assert.Equal(t, 3, config.ConnectionPoolSize)
	assert.Equal(t, 2, config.Server.WorkerCount)
	assert.Equal(t, 4, config.Server.GRPCConcurrency)
	assert.Equal(t, 40, config.Server.MaxConcurrentRPCs)
	assert.Equal(t, 5500*time.Millisecond, config.RPCTimeout)

	swapParam(t, &f.PyUDFEnabled, "enabled")
	config, err = NewConfig(context.Background())
	require.NoError(t, err)
	assert.False(t, config.Enabled)
}

func TestNewConfigInvalidValuesUseDefaults(t *testing.T) {
	f := &paramtable.Get().FunctionCfg
	swapParam(t, &f.PyUDFEnabled, "true")
	tests := []struct {
		item   *paramtable.ParamItem
		values []string
	}{
		{&f.PyUDFEnabled, []string{"", "enabled"}},
		{&f.PyUDFAddress, []string{"", "invalid", "localhost:19090", "127.0.0.1:65536", "0.0.0.0:19090", "127.0.0.1:0", "[::1]:19090", "224.0.0.1:19090", "255.255.255.255:19090", "127.0.0.1:019090"}},
		{&f.PyUDFConnectionPoolSize, []string{"", "invalid", "1.5", "0", "-1", "1025", "999999999999999999999"}},
		{&f.PyUDFWorkerCount, []string{"invalid", "0", "257"}},
		{&f.PyUDFGRPCConcurrency, []string{"invalid", "0", "1025"}},
		{&f.PyUDFMaxConcurrentRPCs, []string{"invalid", "-1", "65537"}},
		{&f.PyUDFMaxMessageBytes, []string{"invalid", "64MiB", "1048575", "1073741825"}},
		{&f.PyUDFRPCTimeout, []string{"", "30", "forever", "999999999999999999999h", "0", "-1s", "500us", "1.5ms", "4999ms", "5000.5ms", "300001ms", "6m", "24h", "25h"}},
		{&f.PyUDFShutdownTimeout, []string{"30", "forever", "0", "-1s", "999ms", "1000.5ms", "4999ms", "5000.5ms", "60001ms", "11m"}},
	}
	for _, test := range tests {
		for _, value := range test.values {
			t.Run(test.item.Key+"/"+value, func(t *testing.T) {
				swapParam(t, test.item, value)
				got, err := NewConfig(context.Background())
				require.NoError(t, err)
				want := defaultTestConfig()
				want.Enabled = test.item != &f.PyUDFEnabled
				require.Equal(t, want, got)
				_, err = got.SupervisorArgs()
				require.NoError(t, err)
				isolateRuntimeConfig(t)
				require.NoError(t, processConfig.initialize(context.Background()))
				startup, err := RuntimeConfig()
				require.NoError(t, err)
				if want.Enabled {
					require.Equal(t, want, startup)
				} else {
					require.False(t, startup.Enabled)
				}
			})
		}
	}
}

func TestNewConfigIntegerBoundaries(t *testing.T) {
	f := &paramtable.Get().FunctionCfg
	for _, upper := range []bool{false, true} {
		t.Run(strconv.FormatBool(upper), func(t *testing.T) {
			counts := []int{1, 1, 1, 1, 1 << 20}
			if upper {
				counts = []int{1024, 256, 1024, 65536, 1 << 30}
			}
			for i, item := range []*paramtable.ParamItem{
				&f.PyUDFConnectionPoolSize, &f.PyUDFWorkerCount, &f.PyUDFGRPCConcurrency,
				&f.PyUDFMaxConcurrentRPCs, &f.PyUDFMaxMessageBytes,
			} {
				swapParam(t, item, strconv.Itoa(counts[i]))
			}
			got, err := NewConfig(context.Background())
			require.NoError(t, err)
			require.Equal(t, counts, []int{
				got.ConnectionPoolSize, got.Server.WorkerCount,
				got.Server.GRPCConcurrency, got.Server.MaxConcurrentRPCs, got.MaxMessageBytes,
			})
		})
	}
}

func TestWorkerConfigAddressAndLimits(t *testing.T) {
	for _, address := range []string{"127.0.0.1:19090", "10.0.0.1:19090", "169.254.1.1:19090"} {
		c := defaultTestConfig()
		c.Address = address
		require.NoError(t, c.Validate(), "binding verifies host ownership later")
	}
	for _, address := range []string{"localhost:19090", "0.0.0.0:19090", "[::1]:19090", "127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:019090", "224.0.0.1:19090", "http://127.0.0.1:19090"} {
		t.Run(address, func(t *testing.T) { c := defaultTestConfig(); c.Address = address; require.Error(t, c.Validate()) })
	}
	c := defaultTestConfig()
	c.Server.WorkerCount = 100
	c.Server.GRPCConcurrency = 100
	require.NoError(t, c.Validate(), "no derived client concurrency cap")
	c.MaxMessageBytes = 1 << 30
	c.RPCTimeout = 5 * time.Minute
	require.NoError(t, c.Validate())
}

func TestSupervisorArgs(t *testing.T) {
	c := defaultTestConfig()
	args, err := c.SupervisorArgs()
	require.NoError(t, err)
	assert.Equal(t, []string{
		"--address", "127.0.0.1:19090", "--worker-count", "1", "--grpc-concurrency", "10",
		"--max-concurrent-rpcs", "100",
		"--max-message-bytes", "67108864",
		"--shutdown-timeout-ms", "30000",
	}, args)
	c.ConnectionPoolSize = 20
	c.RPCTimeout = 5 * time.Second
	same, err := c.SupervisorArgs()
	require.NoError(t, err)
	assert.Equal(t, args, same, "client-only settings never reach Python")
}

func TestCheckEnabled(t *testing.T) {
	isolateRuntimeConfig(t)
	item := &paramtable.Get().FunctionCfg.PyUDFEnabled
	swapParam(t, item, "false")
	require.NoError(t, processConfig.initialize(context.Background()))
	err := CheckEnabled()
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.ErrorContains(t, err, disabledReason)
	status := merr.Status(err)
	assert.EqualValues(t, 1100, status.Code)
	assert.Equal(t, "true", status.ExtraInfo[merr.InputErrorFlagKey])
	assert.False(t, status.Retriable)
	swapParam(t, item, "true")
	require.ErrorIs(t, CheckEnabled(), merr.ErrParameterInvalid, "enabling requires restart")
	processConfig = &runtimeConfiguration{}
	require.NoError(t, processConfig.initialize(context.Background()))
	assert.NoError(t, CheckEnabled())
}

func isolateRuntimeConfig(t *testing.T) {
	t.Helper()
	previous := processConfig
	processConfig = &runtimeConfiguration{}
	t.Cleanup(func() { processConfig = previous })
}

func TestRuntimeConfigRequiresInitialization(t *testing.T) {
	isolateRuntimeConfig(t)
	_, err := RuntimeConfig()
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.ErrorIs(t, CheckEnabled(), merr.ErrServiceUnavailable)
}

func TestRuntimeConfigFrozenAfterStartup(t *testing.T) {
	isolateRuntimeConfig(t)
	f := &paramtable.Get().FunctionCfg
	swapParam(t, &f.PyUDFEnabled, "true")
	require.NoError(t, processConfig.initialize(context.Background()))
	want, err := RuntimeConfig()
	require.NoError(t, err)
	client, err := NewClient(want)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, CloseClients()) })
	for _, item := range []*paramtable.ParamItem{
		&f.PyUDFEnabled, &f.PyUDFAddress, &f.PyUDFRPCTimeout,
		&f.PyUDFConnectionPoolSize, &f.PyUDFMaxMessageBytes, &f.PyUDFWorkerCount,
		&f.PyUDFGRPCConcurrency, &f.PyUDFMaxConcurrentRPCs, &f.PyUDFShutdownTimeout,
	} {
		// Mutate the backing manager, as a configuration refresh does, rather
		// than mocking the reader that the regression is intended to exercise.
		old := paramtable.Get().GetWithDefault(item.Key, item.DefaultValue)
		require.NoError(t, paramtable.Get().Save(item.Key, "invalid"))
		t.Cleanup(func() { require.NoError(t, paramtable.Get().Save(item.Key, old)) })
	}
	require.NoError(t, processConfig.initialize(context.Background()))
	got, err := RuntimeConfig()
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.NoError(t, CheckEnabled())
	same, err := NewClient(got)
	require.NoError(t, err)
	require.Same(t, client, same)
	args, err := got.SupervisorArgs()
	require.NoError(t, err)
	wantArgs, err := want.SupervisorArgs()
	require.NoError(t, err)
	require.Equal(t, wantArgs, args)
	got.Address = "127.0.0.2:19091"
	unchanged, err := RuntimeConfig()
	require.NoError(t, err)
	require.Equal(t, want, unchanged, "callers receive a copy")
}

func TestRuntimeConfigFallbackIsFrozen(t *testing.T) {
	isolateRuntimeConfig(t)
	f := &paramtable.Get().FunctionCfg
	swapParam(t, &f.PyUDFEnabled, "true")
	swapParam(t, &f.PyUDFAddress, "0.0.0.0:19090")
	swapParam(t, &f.PyUDFRPCTimeout, "25h")
	swapParam(t, &f.PyUDFWorkerCount, "invalid")
	swapParam(t, &f.PyUDFConnectionPoolSize, "3")
	require.NoError(t, processConfig.initialize(context.Background()))
	got, err := RuntimeConfig()
	require.NoError(t, err)
	want := defaultTestConfig()
	want.Enabled = true
	want.ConnectionPoolSize = 3
	require.Equal(t, want, got, "only invalid settings fall back")
	swapParam(t, &f.PyUDFAddress, "127.0.0.2:19091")
	swapParam(t, &f.PyUDFRPCTimeout, "2s")
	require.NoError(t, processConfig.initialize(context.Background()))
	got, err = RuntimeConfig()
	require.NoError(t, err)
	require.Equal(t, want, got, "fallback values remain frozen until restart")
}

func TestRuntimeConfigInvalidEnabledDefaultsToDisabled(t *testing.T) {
	isolateRuntimeConfig(t)
	swapParam(t, &paramtable.Get().FunctionCfg.PyUDFEnabled, "enabled")
	require.NoError(t, processConfig.initialize(context.Background()))
	got, err := RuntimeConfig()
	require.NoError(t, err)
	require.False(t, got.Enabled)
	require.ErrorIs(t, CheckEnabled(), merr.ErrParameterInvalid)
}

func TestRuntimeConfigLogsFallbackOnce(t *testing.T) {
	isolateRuntimeConfig(t)
	f := &paramtable.Get().FunctionCfg
	swapParam(t, &f.PyUDFEnabled, "true")
	// Use the backing configuration manager to exercise the real read path.
	old := paramtable.Get().GetWithDefault(f.PyUDFRPCTimeout.Key, f.PyUDFRPCTimeout.DefaultValue)
	require.NoError(t, paramtable.Get().Save(f.PyUDFRPCTimeout.Key, "25h"))
	t.Cleanup(func() { require.NoError(t, paramtable.Get().Save(f.PyUDFRPCTimeout.Key, old)) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	defer mockey.Mock(mlog.Error).To(func(gotCtx context.Context, message string, fields ...mlog.Field) {
		calls++
		assert.Same(t, ctx, gotCtx)
		assert.Equal(t, "invalid PyUDF configuration, using default", message)
		strings := make(map[string]string)
		for _, field := range fields {
			strings[field.Key] = field.String
			if field.Key == "error" {
				assert.NotNil(t, field.Interface)
			}
		}
		assert.Equal(t, f.PyUDFRPCTimeout.Key, strings["key"])
		assert.Equal(t, "25h", strings["value"])
		assert.Equal(t, "30s", strings["default"])
		assert.Contains(t, strings, "error")
	}).Build().UnPatch()
	require.NoError(t, processConfig.initialize(ctx))
	require.NoError(t, processConfig.initialize(ctx))
	got, err := RuntimeConfig()
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, got.RPCTimeout)
	assert.Equal(t, 1, calls)
}

func TestNewConfigDurationBoundaries(t *testing.T) {
	f := &paramtable.Get().FunctionCfg
	for _, test := range []struct {
		rpc, shutdown string
	}{
		{"5s", "5s"},
		{"300s", "60s"},
	} {
		t.Run(test.rpc, func(t *testing.T) {
			swapParam(t, &f.PyUDFRPCTimeout, test.rpc)
			swapParam(t, &f.PyUDFShutdownTimeout, test.shutdown)
			got, err := NewConfig(context.Background())
			require.NoError(t, err)
			rpc, err := time.ParseDuration(test.rpc)
			require.NoError(t, err)
			shutdown, err := time.ParseDuration(test.shutdown)
			require.NoError(t, err)
			assert.Equal(t, rpc, got.RPCTimeout)
			assert.Equal(t, shutdown, got.Server.ShutdownTimeout)
		})
	}
}
