//go:build linux || darwin

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
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Re-exec the Go test binary to exercise process ownership without Python.
func TestSupervisorProcessHelper(t *testing.T) {
	args := os.Args
	if len(args) < 4 || args[len(args)-3] != "pyudf-go-helper" {
		return
	}
	mode, directory := args[len(args)-2], args[len(args)-1]
	if mode == "exit" {
		os.Exit(7)
	}
	if mode == "exit-success" {
		os.Exit(0)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	if err := os.WriteFile(filepath.Join(directory, "ready"), nil, 0o600); err != nil { //nolint:gosec // G703: directory is the parent test's t.TempDir passed to this helper.
		os.Exit(8)
	}
	<-signals
	if mode == "delayed-stop" {
		for {
			if _, err := os.Stat(filepath.Join(directory, "release")); err == nil { //nolint:gosec // G703: directory is the parent test's t.TempDir passed to this helper.
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	os.Exit(0)
}

func supervisorTestCommand(t *testing.T, state *supervisor, mode string) (*exec.Cmd, string) {
	t.Helper()
	binary, err := os.Executable()
	require.NoError(t, err)
	directory := t.TempDir()
	cmd := exec.Command(binary, "-test.run=^TestSupervisorProcessHelper$", "--", "pyudf-go-helper", mode, directory) //nolint:gosec // G204: re-executes this test binary with test-controlled arguments.
	t.Cleanup(func() {
		_ = os.WriteFile(filepath.Join(directory, "release"), nil, 0o600)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := state.stop(ctx); err != nil && cmd.Process != nil {
			_ = cmd.Process.Kill()
			if state.done != nil {
				<-state.done
			}
		}
	})
	return cmd, directory
}

func requireHelperReady(t *testing.T, directory string) {
	t.Helper()
	require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(directory, "ready")); return err == nil }, 5*time.Second, 10*time.Millisecond)
}

func requireProcessGone(t *testing.T, pid int) {
	t.Helper()
	require.Eventually(t, func() bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) }, 3*time.Second, 20*time.Millisecond)
}

func TestSupervisorConcurrentStartStop(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	state := &supervisor{}
	cmd, directory := supervisorTestCommand(t, state, "normal")
	var calls atomic.Int32
	prepare := func() (*exec.Cmd, error) { calls.Add(1); return cmd, nil }
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() { results <- state.start(context.Background(), prepare) }()
	}
	for i := 0; i < 8; i++ {
		require.NoError(t, <-results)
	}
	require.EqualValues(t, 1, calls.Load())
	requireHelperReady(t, directory)
	for i := 0; i < 8; i++ {
		go func() { results <- state.stop(context.Background()) }()
	}
	for i := 0; i < 8; i++ {
		require.NoError(t, <-results)
	}
	requireProcessGone(t, cmd.Process.Pid)
	require.NoError(t, state.start(context.Background(), prepare))
	require.EqualValues(t, 1, calls.Load())
}

func TestSupervisorStartDoesNotWaitForHealth(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	state := &supervisor{}
	cmd, directory := supervisorTestCommand(t, state, "normal")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, state.start(ctx, func() (*exec.Cmd, error) { return cmd, nil }))
	requireHelperReady(t, directory)
	cancel()
	select {
	case <-state.done:
		t.Fatal("startup context cancellation stopped the process")
	default:
	}
	require.NoError(t, state.stop(context.Background()))
	requireProcessGone(t, cmd.Process.Pid)
}

func TestSupervisorLaterExitDoesNotChangeLaunchResult(t *testing.T) {
	state := &supervisor{}
	cmd, _ := supervisorTestCommand(t, state, "exit")
	calls := 0
	prepare := func() (*exec.Cmd, error) { calls++; return cmd, nil }
	require.NoError(t, state.start(context.Background(), prepare))
	select {
	case <-state.done:
	case <-time.After(5 * time.Second):
		t.Fatal("child did not exit")
	}
	require.NoError(t, state.start(context.Background(), prepare))
	require.Equal(t, 1, calls)
	require.ErrorIs(t, state.stop(context.Background()), merr.ErrServiceUnavailable)
}

func TestSupervisorUnexpectedExitLog(t *testing.T) {
	t.Setenv("PATH", t.TempDir()) // Helpers re-exec Go; Python is not required.
	for _, tc := range []struct {
		name, mode string
		kill, stop bool
		exitCode   int
		errorText  string
	}{
		{name: "nonzero_exit", mode: "exit", exitCode: 7, errorText: "exit status 7"},
		{name: "unexpected_zero_exit", mode: "exit-success", exitCode: 0},
		{name: "signal_exit", mode: "normal", kill: true, exitCode: -1, errorText: "signal: killed"},
		{name: "requested_shutdown", mode: "normal", stop: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug", DisableTimestamp: true})
			ctx := mlog.WithFields(context.Background(), mlog.String("testCase", tc.name))
			state := &supervisor{}
			cmd, directory := supervisorTestCommand(t, state, tc.mode)
			require.NoError(t, state.start(ctx, func() (*exec.Cmd, error) { return cmd, nil }))
			if tc.kill || tc.stop {
				requireHelperReady(t, directory)
			}
			if tc.kill {
				require.NoError(t, cmd.Process.Kill())
			}
			if tc.stop {
				require.NoError(t, state.stop(ctx))
			}
			select {
			case <-state.done:
			case <-time.After(5 * time.Second):
				t.Fatal("child did not exit")
			}
			logs := sink.String()
			if tc.stop {
				require.NotContains(t, logs, "PyUDF supervisor exited unexpectedly")
				return
			}
			require.Equal(t, 1, strings.Count(logs, "PyUDF supervisor exited unexpectedly"))
			require.Contains(t, logs, "[ERROR]")
			require.Contains(t, logs, "restart Milvus to recover")
			require.Contains(t, logs, "pid="+strconv.Itoa(cmd.Process.Pid))
			require.Contains(t, logs, "exitCode="+strconv.Itoa(tc.exitCode))
			require.Contains(t, logs, "testCase="+tc.name)
			if tc.errorText != "" {
				require.Contains(t, logs, tc.errorText)
			}
		})
	}
}

func TestSupervisorStopDuringPreparationPreventsLaunch(t *testing.T) {
	state := &supervisor{}
	entered, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- state.start(context.Background(), func() (*exec.Cmd, error) { close(entered); <-release; return exec.Command("must-not-run"), nil })
	}()
	<-entered
	require.NoError(t, state.stop(context.Background()))
	close(release)
	require.ErrorIs(t, <-result, merr.ErrServiceUnavailable)
	require.Nil(t, state.cmd)
}

func TestSupervisorCancelledContextPreventsLaunch(t *testing.T) {
	state := &supervisor{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := state.start(ctx, func() (*exec.Cmd, error) { return exec.Command("must-not-run"), nil })
	require.ErrorIs(t, err, context.Canceled)
	require.EqualValues(t, 10000, merr.Code(err))
	require.Nil(t, state.cmd)
}

func TestSupervisorStateGuards(t *testing.T) {
	state := &supervisor{}
	require.NoError(t, state.stop(context.Background()))
	require.ErrorIs(t, state.start(context.Background(), nil), merr.ErrServiceUnavailable)
	state = &supervisor{}
	require.ErrorIs(t, state.start(nil, nil), merr.ErrServiceInternal) //nolint:staticcheck // SA1012: exercises the nil-context guard.
	require.ErrorIs(t, state.stop(nil), merr.ErrServiceInternal)       //nolint:staticcheck // SA1012: exercises the nil-context guard.
}

func TestSupervisorDisabledPublicEntry(t *testing.T) {
	isolateRuntimeConfig(t)
	f := &paramtable.Get().FunctionCfg
	swapParam(t, &f.PyUDFEnabled, "false")
	swapParam(t, &f.PyUDFAddress, "invalid")
	require.NoError(t, StartSupervisor(nil)) //nolint:staticcheck // SA1012: disabled supervisor must return before inspecting the context.
	swapParam(t, &f.PyUDFEnabled, "true")
	require.NoError(t, StartSupervisor(nil)) //nolint:staticcheck // SA1012: runtime remains disabled until restart.
	require.ErrorIs(t, CheckEnabled(), merr.ErrParameterInvalid)
}

func TestStopSupervisorBoundsBackgroundWait(t *testing.T) {
	isolateRuntimeConfig(t)
	config := defaultTestConfig()
	config.Enabled = true
	config.Server.ShutdownTimeout = time.Second
	processConfig.value.Store(&config)
	previous := processSupervisor
	state := &supervisor{}
	processSupervisor = state
	t.Cleanup(func() { processSupervisor = previous })
	cmd, directory := supervisorTestCommand(t, state, "delayed-stop")
	require.NoError(t, state.start(context.Background(), func() (*exec.Cmd, error) { return cmd, nil }))
	requireHelperReady(t, directory)
	// A live configuration change must not extend the startup wait budget.
	swapParam(t, &paramtable.Get().FunctionCfg.PyUDFShutdownTimeout, "10m")
	result := make(chan error, 1)
	started := time.Now()
	go func() { result <- StopSupervisor(context.Background()) }()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.GreaterOrEqual(t, time.Since(started), 6*time.Second)
	case <-time.After(10 * time.Second):
		t.Fatal("StopSupervisor did not bound a background-context wait")
	}
	select {
	case <-state.done:
		t.Fatal("timeout must not kill the supervisor or discard its waiter")
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, StopSupervisor(ctx), context.DeadlineExceeded)
	require.NoError(t, os.WriteFile(filepath.Join(directory, "release"), nil, 0o600))
	select {
	case <-state.done:
		require.NoError(t, state.waitErr)
	case <-time.After(5 * time.Second):
		t.Fatal("original waiter did not reap the released supervisor")
	}
	requireProcessGone(t, cmd.Process.Pid)
}

func TestSupervisorAndClientShareStartupConfig(t *testing.T) {
	isolateRuntimeConfig(t)
	previous := processSupervisor
	processSupervisor = &supervisor{}
	t.Cleanup(func() { processSupervisor = previous })
	cmd, directory := supervisorTestCommand(t, processSupervisor, "wait")
	f := &paramtable.Get().FunctionCfg
	swapParam(t, &f.PyUDFEnabled, "true")
	// Malformed startup settings must reach process creation with defaults.
	swapParam(t, &f.PyUDFRPCTimeout, "30")
	swapParam(t, &f.PyUDFShutdownTimeout, "invalid")
	swapParam(t, &f.PyUDFWorkerCount, "invalid")

	var workerConfig Config
	defer mockey.Mock(supervisorCommand).To(func(config Config) (*exec.Cmd, error) {
		workerConfig = config
		return cmd, nil
	}).Build().UnPatch()
	require.NoError(t, StartSupervisor(context.Background()))
	want := defaultTestConfig()
	want.Enabled = true
	require.Equal(t, want, workerConfig)
	_, err := workerConfig.SupervisorArgs()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(directory, "ready"))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	swapParam(t, &f.PyUDFEnabled, "false")
	swapParam(t, &f.PyUDFAddress, "127.0.0.2:19091")
	swapParam(t, &f.PyUDFRPCTimeout, "1s")
	swapParam(t, &f.PyUDFConnectionPoolSize, "2")
	swapParam(t, &f.PyUDFMaxMessageBytes, "1048576")
	require.NoError(t, StartSupervisor(context.Background()))
	require.NoError(t, CheckEnabled())
	config, err := RuntimeConfig()
	require.NoError(t, err)
	require.Equal(t, workerConfig, config)
	// The first request may arrive only after the backing configuration changed.
	client, err := NewClient(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, CloseClients()) })
	require.Equal(t, workerConfig.Address, client.address)
	require.Equal(t, workerConfig.RPCTimeout, client.rpcTimeout)
	require.Len(t, client.conns, workerConfig.ConnectionPoolSize)
	require.Equal(t, workerConfig.MaxMessageBytes, client.maxMessageBytes)
}

func TestSupervisorExecFailure(t *testing.T) {
	cmd := exec.Command(filepath.Join(t.TempDir(), "missing-executable")) //nolint:gosec // G204: intentionally missing executable in a fresh test directory.
	state := &supervisor{}
	calls := 0
	prepare := func() (*exec.Cmd, error) { calls++; return cmd, nil }
	err := state.start(context.Background(), prepare)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Equal(t, err, state.start(context.Background(), prepare))
	require.Equal(t, 1, calls)
	require.NoError(t, state.stop(context.Background()))
}

func TestSupervisorConcurrentStopBeforeStart(t *testing.T) {
	state := &supervisor{}
	var wait sync.WaitGroup
	for i := 0; i < 8; i++ {
		wait.Add(1)
		go func() { defer wait.Done(); assert.NoError(t, state.stop(context.Background())) }()
	}
	wait.Wait()
	require.ErrorIs(t, state.start(context.Background(), nil), merr.ErrServiceUnavailable)
}

func TestSupervisorWaitContinuesAfterCallerCancellation(t *testing.T) {
	state := &supervisor{}
	cmd, directory := supervisorTestCommand(t, state, "delayed-stop")
	require.NoError(t, state.start(context.Background(), func() (*exec.Cmd, error) { return cmd, nil }))
	requireHelperReady(t, directory)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, state.stop(ctx), context.Canceled)
	select {
	case <-state.done:
		t.Fatal("child should still be waiting for release")
	default:
	}
	require.NoError(t, os.WriteFile(filepath.Join(directory, "release"), nil, 0o600))
	require.NoError(t, state.stop(context.Background()))
	requireProcessGone(t, cmd.Process.Pid)
}

func TestSupervisorCommand(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	config := defaultTestConfig()
	cmd, err := supervisorCommand(config)
	require.NoError(t, err)
	args, err := config.SupervisorArgs()
	require.NoError(t, err)
	require.Equal(t, append([]string{"python3", "-I", "-m", "milvus_pyudf_runtime.supervisor"}, args...), cmd.Args)
	require.Empty(t, cmd.ExtraFiles)
	require.Nil(t, cmd.SysProcAttr)
	// Only command construction is tested; the interpreter is never executed.
}
