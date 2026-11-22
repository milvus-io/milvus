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

package milvus

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/cmd/components"
	grpcproxy "github.com/milvus-io/milvus/internal/distributed/proxy"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	supervisorHelperRoleEnv   = "MILVUS_TEST_SUPERVISOR_ROLE"
	supervisorHelperActionEnv = "MILVUS_TEST_SUPERVISOR_ACTION"
	supervisorHelperCodeEnv   = "MILVUS_TEST_SUPERVISOR_CODE"
	supervisorHelperSignalEnv = "MILVUS_TEST_SUPERVISOR_SIGNAL"
	supervisorHelperDirEnv    = "MILVUS_TEST_SUPERVISOR_DIR"
	supervisorHelperTargetEnv = "MILVUS_TEST_SUPERVISOR_TARGET_BIN"
	supervisorHelperServerEnv = "MILVUS_TEST_SUPERVISOR_SERVER_ID"
)

// Main must return normally on too few arguments, taking the usage branch
// rather than panicking or exiting. That is the contract a distribution's own
// main relies on.
func TestMainWithoutSubcommandReturns(t *testing.T) {
	Main([]string{"milvus"})
}

// Main must leave the caller's argument vector as it found it: a distribution
// may keep using its own os.Args after the call.
func TestMainDoesNotModifyCallerArgs(t *testing.T) {
	args := []string{"milvus", "unknown-command"}
	want := append([]string(nil), args...)
	Main(args)
	assert.Equal(t, want, args)
}

// The one in-place change Main used to make - deleting the subprocess marker -
// now lives in a pure helper, so it is tested here without exec'ing a
// subprocess: the marker is stripped from a copy, and the caller's vector is
// returned untouched.
func TestSubprocessArgsStripsTheMarker(t *testing.T) {
	args := []string{"milvus", "--run-with-subprocess", "run", "--config", "x"}
	want := append([]string(nil), args...)

	subArgs, ok := subprocessArgs(args)
	assert.True(t, ok)
	assert.Equal(t, []string{"milvus", "run", "--config", "x"}, subArgs)
	assert.Equal(t, want, args, "the caller's vector must be untouched")
}

func TestSubprocessArgsWithoutTheMarker(t *testing.T) {
	args := []string{"milvus", "run"}
	subArgs, ok := subprocessArgs(args)
	assert.False(t, ok)
	assert.Equal(t, args, subArgs, "no marker means no strip")
}

func TestRunMainInvalidNativeProfileReturnsNonZero(t *testing.T) {
	code := runMain([]string{"milvus", "run", "standalone", "--native-profile=not-a-bool"})
	assert.Equal(t, 1, code)
}

func TestSubprocessExitCodeFallbackOnNilState(t *testing.T) {
	assert.Equal(t, 0, subprocessExitCode(nil, nil))
	assert.Equal(t, 1, subprocessExitCode(errors.New("wait failed"), nil))
}

func TestSuperviseSubprocessWithStubbedCleanup(t *testing.T) {
	t.Run("start failure returns 1 without calling session cleanup", func(t *testing.T) {
		cleanupCalled := false
		missingBin := filepath.Join(t.TempDir(), "does-not-exist")
		code := superviseSubprocess([]string{missingBin, "run", "standalone"}, os.Environ(), func(pid int) error {
			cleanupCalled = true
			return nil
		})
		assert.Equal(t, 1, code)
		assert.False(t, cleanupCalled)
	})

	t.Run("cleanup failure preserves child nonzero exit status", func(t *testing.T) {
		var cleanedPid int
		env := append(os.Environ(),
			supervisorHelperRoleEnv+"=child",
			supervisorHelperActionEnv+"=exit",
			supervisorHelperCodeEnv+"=7",
			supervisorHelperDirEnv+"="+t.TempDir(),
		)
		code := superviseSubprocess(
			[]string{os.Args[0], "-test.run=^TestSupervisorHelperProcess$", "standalone"},
			env,
			func(pid int) error {
				cleanedPid = pid
				return errors.New("simulated etcd failure")
			},
		)
		assert.Equal(t, 7, code)
		assert.Greater(t, cleanedPid, 0)
	})

	t.Run("cleanup failure keeps successful child exit zero", func(t *testing.T) {
		var cleanedPid int
		env := append(os.Environ(),
			supervisorHelperRoleEnv+"=child",
			supervisorHelperActionEnv+"=exit",
			supervisorHelperCodeEnv+"=0",
			supervisorHelperDirEnv+"="+t.TempDir(),
		)
		code := superviseSubprocess(
			[]string{os.Args[0], "-test.run=^TestSupervisorHelperProcess$", "standalone"},
			env,
			func(pid int) error {
				cleanedPid = pid
				return errors.New("simulated etcd failure")
			},
		)
		assert.Equal(t, 0, code)
		assert.Greater(t, cleanedPid, 0)
	})
}

func writeSupervisorHelperMarker(dir, name, content string) {
	if dir == "" {
		return
	}
	_ = os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600) //nolint:gosec // test-only marker under t.TempDir()
}

// TestSupervisorHelperProcess serves as both the outer supervisor helper and
// the inner child process when invoked by TestMainSubprocessExitPropagation.
func TestSupervisorHelperProcess(t *testing.T) {
	role := os.Getenv(supervisorHelperRoleEnv)
	if role == "" {
		return
	}

	dir := os.Getenv(supervisorHelperDirEnv)
	switch role {
	case "supervisor":
		require.NoError(t, os.Setenv(supervisorHelperRoleEnv, "child"))
		bin := os.Args[0]
		if targetBin := os.Getenv(supervisorHelperTargetEnv); targetBin != "" {
			bin = targetBin
		}
		Main([]string{bin, "-test.run=^TestSupervisorHelperProcess$", "standalone", "--run-with-subprocess"})
		writeSupervisorHelperMarker(dir, "main_returned", "ok")
		os.Exit(0)

	case "child":
		writeSupervisorHelperMarker(dir, "child_pid", strconv.Itoa(os.Getpid()))
		if rawID := os.Getenv(supervisorHelperServerEnv); rawID != "" {
			serverID, err := strconv.ParseInt(rawID, 10, 64)
			if err == nil {
				sessionutil.SaveServerInfo(typeutil.MixCoordRole, serverID)
			}
		}

		switch os.Getenv(supervisorHelperActionEnv) {
		case "exit":
			code, _ := strconv.Atoi(os.Getenv(supervisorHelperCodeEnv))
			os.Exit(code)
		case "signal":
			sigNum, _ := strconv.Atoi(os.Getenv(supervisorHelperSignalEnv))
			sig := syscall.Signal(sigNum)
			signal.Reset(sig)
			_ = syscall.Kill(os.Getpid(), sig)
			time.Sleep(5 * time.Second)
			os.Exit(0)
		case "wait-signal":
			sigNum, _ := strconv.Atoi(os.Getenv(supervisorHelperSignalEnv))
			sig := syscall.Signal(sigNum)
			ch := make(chan os.Signal, 1)
			signal.Notify(ch, sig)
			writeSupervisorHelperMarker(dir, "child_ready", "ready")
			<-ch
			writeSupervisorHelperMarker(dir, "child_signaled", "received")
			signal.Stop(ch)
			signal.Reset(sig)
			_ = syscall.Kill(os.Getpid(), sig)
			time.Sleep(5 * time.Second)
			os.Exit(0)
		case "stop-timeout":
			patch := mockey.Mock((*grpcproxy.Server).Stop).To(func(_ *grpcproxy.Server) error {
				select {}
			}).Build()
			defer patch.UnPatch()
			_ = (&components.Proxy{}).Stop()
			os.Exit(0)
		}
	}
}

type outerSupervisorRun struct {
	action        string
	exitCode      int
	signal        syscall.Signal
	targetBin     string
	serverID      int64
	etcdEndpoints string
	etcdRootPath  string
	extraEnv      []string
	onChildReady  func(supervisorProc *os.Process)
}

type outerSupervisorResult struct {
	outerExitCode int
	output        string
	childPid      int
	mainReturned  bool
	childSignaled bool
}

func runOuterSupervisorHelper(t *testing.T, cfg outerSupervisorRun) outerSupervisorResult {
	t.Helper()
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSupervisorHelperProcess$") //nolint:gosec // re-execs the current test binary
	env := append(os.Environ(),
		supervisorHelperRoleEnv+"=supervisor",
		supervisorHelperActionEnv+"="+cfg.action,
		supervisorHelperCodeEnv+"="+strconv.Itoa(cfg.exitCode),
		supervisorHelperSignalEnv+"="+strconv.Itoa(int(cfg.signal)),
		supervisorHelperDirEnv+"="+dir,
	)
	if cfg.targetBin != "" {
		env = append(env, supervisorHelperTargetEnv+"="+cfg.targetBin)
	}
	if cfg.serverID != 0 {
		env = append(env, supervisorHelperServerEnv+"="+strconv.FormatInt(cfg.serverID, 10))
	}
	if cfg.etcdEndpoints != "" {
		env = append(env, "ETCD_ENDPOINTS="+cfg.etcdEndpoints)
	}
	if cfg.etcdRootPath != "" {
		env = append(env, "ETCD_ROOTPATH="+cfg.etcdRootPath)
	}
	env = append(env, cfg.extraEnv...)
	cmd.Env = env

	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf

	require.NoError(t, cmd.Start())

	if cfg.onChildReady != nil {
		readyPath := filepath.Join(dir, "child_ready")
		require.Eventually(t, func() bool {
			_, err := os.Stat(readyPath)
			return err == nil
		}, 10*time.Second, 20*time.Millisecond, "child did not become ready")
		cfg.onChildReady(cmd.Process)
	}

	waitErr := cmd.Wait()
	outerCode := 0
	if waitErr != nil {
		var exitErr *exec.ExitError
		require.ErrorAs(t, waitErr, &exitErr, "unexpected wait error: %v\noutput:\n%s", waitErr, buf.String())
		outerCode = exitErr.ExitCode()
	}

	var childPid int
	if rawPid, err := os.ReadFile(filepath.Join(dir, "child_pid")); err == nil {
		childPid, _ = strconv.Atoi(strings.TrimSpace(string(rawPid)))
	}
	_, mainReturnedErr := os.Stat(filepath.Join(dir, "main_returned"))
	_, childSignaledErr := os.Stat(filepath.Join(dir, "child_signaled"))

	return outerSupervisorResult{
		outerExitCode: outerCode,
		output:        buf.String(),
		childPid:      childPid,
		mainReturned:  mainReturnedErr == nil,
		childSignaled: childSignaledErr == nil,
	}
}

func TestMainSubprocessExitPropagation(t *testing.T) {
	etcdClient, etcdEndpoint := newSessionCleanupEtcdWithEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seedChildSession := func(t *testing.T, rootPath string, serverID int64) string {
		t.Helper()
		sessionKey := path.Join(rootPath, "meta", sessionutil.DefaultServiceRoot, fmt.Sprintf("%s-%d", typeutil.MixCoordRole, serverID))
		_, err := etcdClient.Put(ctx, sessionKey, sessionCleanupJSON(serverID))
		require.NoError(t, err)
		return sessionKey
	}

	assertCleanedUp := func(t *testing.T, sessionKey string, childPid int) {
		t.Helper()
		require.Greater(t, childPid, 0)
		resp, err := etcdClient.Get(ctx, sessionKey)
		require.NoError(t, err)
		assert.Empty(t, resp.Kvs, "supervisor must delete the child session key before exiting")
		_, statErr := os.Stat(sessionutil.GetServerInfoFilePath(childPid))
		assert.ErrorIs(t, statErr, os.ErrNotExist, "deferred server-info file removal must run before os.Exit")
	}

	t.Run("child exit 0 returns normally after cleanup", func(t *testing.T) {
		rootPath := "sup-exit-0"
		key := seedChildSession(t, rootPath, 101)

		res := runOuterSupervisorHelper(t, outerSupervisorRun{
			action:        "exit",
			exitCode:      0,
			serverID:      101,
			etcdEndpoints: etcdEndpoint,
			etcdRootPath:  rootPath,
		})
		assert.Equal(t, 0, res.outerExitCode, res.output)
		assert.True(t, res.mainReturned, "Main must return normally on exit status 0")
		assertCleanedUp(t, key, res.childPid)
	})

	t.Run("child exit 1 propagates after cleanup", func(t *testing.T) {
		rootPath := "sup-exit-1"
		key := seedChildSession(t, rootPath, 102)

		res := runOuterSupervisorHelper(t, outerSupervisorRun{
			action:        "exit",
			exitCode:      1,
			serverID:      102,
			etcdEndpoints: etcdEndpoint,
			etcdRootPath:  rootPath,
		})
		assert.Equal(t, 1, res.outerExitCode, res.output)
		assert.False(t, res.mainReturned)
		assert.Contains(t, res.output, "subprocess exit,  exit status 1")
		assertCleanedUp(t, key, res.childPid)
	})

	t.Run("child exit 7 propagates after cleanup", func(t *testing.T) {
		rootPath := "sup-exit-7"
		key := seedChildSession(t, rootPath, 107)

		res := runOuterSupervisorHelper(t, outerSupervisorRun{
			action:        "exit",
			exitCode:      7,
			serverID:      107,
			etcdEndpoints: etcdEndpoint,
			etcdRootPath:  rootPath,
		})
		assert.Equal(t, 7, res.outerExitCode, res.output)
		assert.False(t, res.mainReturned)
		assert.Contains(t, res.output, "subprocess exit,  exit status 7")
		assertCleanedUp(t, key, res.childPid)
	})

	t.Run("child killed by SIGKILL maps to 128+SIGKILL after cleanup", func(t *testing.T) {
		rootPath := "sup-sigkill"
		key := seedChildSession(t, rootPath, 109)

		res := runOuterSupervisorHelper(t, outerSupervisorRun{
			action:        "signal",
			signal:        syscall.SIGKILL,
			serverID:      109,
			etcdEndpoints: etcdEndpoint,
			etcdRootPath:  rootPath,
		})
		assert.Equal(t, 128+int(syscall.SIGKILL), res.outerExitCode, res.output)
		assert.False(t, res.mainReturned)
		assertCleanedUp(t, key, res.childPid)
	})

	t.Run("signal sent to supervisor is forwarded to live child and mapped after cleanup", func(t *testing.T) {
		rootPath := "sup-forward-sigterm"
		key := seedChildSession(t, rootPath, 115)

		res := runOuterSupervisorHelper(t, outerSupervisorRun{
			action:        "wait-signal",
			signal:        syscall.SIGTERM,
			serverID:      115,
			etcdEndpoints: etcdEndpoint,
			etcdRootPath:  rootPath,
			onChildReady: func(supervisorProc *os.Process) {
				require.NoError(t, supervisorProc.Signal(syscall.SIGTERM))
			},
		})
		assert.True(t, res.childSignaled, "child must receive forwarded SIGTERM; output:\n%s", res.output)
		assert.Equal(t, 128+int(syscall.SIGTERM), res.outerExitCode, res.output)
		assert.False(t, res.mainReturned)
		assertCleanedUp(t, key, res.childPid)
	})

	t.Run("child start failure exits outer supervisor with 1", func(t *testing.T) {
		res := runOuterSupervisorHelper(t, outerSupervisorRun{
			targetBin: filepath.Join(t.TempDir(), "missing-milvus-binary"),
		})
		assert.Equal(t, 1, res.outerExitCode, res.output)
		assert.False(t, res.mainReturned)
	})

	t.Run("cleanup failure in outer Main preserves child failure status and removes server info file", func(t *testing.T) {
		res := runOuterSupervisorHelper(t, outerSupervisorRun{
			action:        "exit",
			exitCode:      7,
			serverID:      120,
			etcdEndpoints: "127.0.0.1:1",
			etcdRootPath:  "sup-cleanup-fail",
			extraEnv:      []string{"ETCD_DIALTIMEOUT=100"},
		})
		assert.Equal(t, 7, res.outerExitCode, res.output)
		assert.Contains(t, res.output, "clean session failed")
		assert.Contains(t, res.output, "subprocess exit,  exit status 7")
		require.Greater(t, res.childPid, 0)
		_, statErr := os.Stat(sessionutil.GetServerInfoFilePath(res.childPid))
		assert.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("component stop timeout os.Exit(1) propagates as outer exit code 1", func(t *testing.T) {
		rootPath := "sup-stop-timeout"
		key := seedChildSession(t, rootPath, 130)

		res := runOuterSupervisorHelper(t, outerSupervisorRun{
			action:        "stop-timeout",
			serverID:      130,
			etcdEndpoints: etcdEndpoint,
			etcdRootPath:  rootPath,
			extraEnv: []string{
				"PROXY_GRACEFULSTOPTIMEOUT=0.05",
				"COMMON_PPROFPATH=" + filepath.Join(t.TempDir(), "pprof"),
			},
		})
		assert.Equal(t, 1, res.outerExitCode, res.output)
		assert.Contains(t, res.output, "stop progress timeout, force exit")
		assert.Contains(t, res.output, "subprocess exit,  exit status 1")
		assertCleanedUp(t, key, res.childPid)
	})
}
