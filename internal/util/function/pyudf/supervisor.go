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
	"sync"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Go owns one Python command. Python owns workers and their termination.
type supervisor struct {
	once     sync.Once
	mu       sync.Mutex
	stopped  bool
	cmd      *exec.Cmd
	done     chan struct{}
	startErr error
	waitErr  error // published by closing done; Wait has exactly one caller
}

var processSupervisor = &supervisor{}

// StartSupervisor starts supervisor.py once and returns after cmd.Start.
// Success means the process was created, not that workers can accept RPCs.
// The context does not control the process lifetime after success.
func StartSupervisor(ctx context.Context) error {
	if err := processConfig.initialize(ctx); err != nil {
		return err
	}
	config, err := RuntimeConfig()
	if err != nil {
		return err
	}
	if !config.Enabled {
		return nil
	}
	return processSupervisor.start(ctx, func() (*exec.Cmd, error) {
		return supervisorCommand(config)
	})
}

// StopSupervisor notifies Python and waits for it. Python terminates/reaps its
// workers; canceling this wait does not cancel that cleanup or the sole Wait.
// Allow five seconds beyond Python's TERM-to-KILL deadline for process reaping.
// A timeout bounds only this caller's wait; it does not prove descendants exited.
func StopSupervisor(ctx context.Context) error {
	if ctx == nil {
		return processSupervisor.stop(ctx)
	}
	timeout := 5 * time.Second
	if config := processConfig.value.Load(); config != nil {
		timeout += config.Server.ShutdownTimeout
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return processSupervisor.stop(waitCtx)
}

func supervisorCommand(config Config) (*exec.Cmd, error) {
	args, err := config.SupervisorArgs()
	if err != nil {
		return nil, err
	}
	return exec.Command("python3", append([]string{"-I", "-m", "milvus_pyudf_runtime.supervisor"}, args...)...), nil //nolint:gosec // G204: fixed interpreter/module; SupervisorArgs validates address and numeric config, with no shell.
}

func (s *supervisor) start(ctx context.Context, prepare func() (*exec.Cmd, error)) error {
	s.once.Do(func() { s.startErr = s.startOnce(ctx, prepare) })
	return s.startErr
}

func (s *supervisor) startOnce(ctx context.Context, prepare func() (*exec.Cmd, error)) error {
	if ctx == nil {
		return merr.WrapErrServiceInternalMsg("py_udf: startup context is nil")
	}
	s.mu.Lock()
	stopped := s.stopped
	s.mu.Unlock()
	if stopped {
		return merr.WrapErrServiceUnavailableMsg("py_udf: supervisor is stopped")
	}
	cmd, err := prepare()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return merr.WrapErrServiceUnavailableMsg("py_udf: supervisor is stopped")
	}
	if err := ctx.Err(); err != nil {
		return merr.Wrap(err, "py_udf: supervisor startup")
	}
	if cmd.Stdout == nil {
		cmd.Stdout = os.Stdout
	}
	if cmd.Stderr == nil {
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		return merr.WrapErrServiceUnavailableErr(err, "py_udf: start supervisor")
	}
	s.cmd, s.done = cmd, make(chan struct{})
	go func() {
		s.waitErr = cmd.Wait()
		s.mu.Lock()
		stopped := s.stopped
		s.mu.Unlock()
		if !stopped {
			exitCode := -1
			if cmd.ProcessState != nil {
				exitCode = cmd.ProcessState.ExitCode()
			}
			mlog.Error(ctx, "PyUDF supervisor exited unexpectedly; restart Milvus to recover",
				mlog.Int("pid", cmd.Process.Pid), mlog.Int("exitCode", exitCode), mlog.Err(s.waitErr))
		}
		close(s.done)
	}()
	return nil
}

func (s *supervisor) stop(ctx context.Context) error {
	if ctx == nil {
		return merr.WrapErrServiceInternalMsg("py_udf: shutdown context is nil")
	}
	s.mu.Lock()
	alreadyStopped := s.stopped
	s.stopped = true
	cmd, done := s.cmd, s.done
	s.mu.Unlock()
	if cmd == nil {
		return nil
	}
	if !alreadyStopped {
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return merr.WrapErrServiceUnavailableErr(err, "py_udf: signal supervisor")
		}
	}
	select {
	case <-done:
		if s.waitErr != nil {
			return merr.WrapErrServiceUnavailableErr(s.waitErr, "py_udf: supervisor exited")
		}
		return nil
	case <-ctx.Done():
		return merr.Wrap(ctx.Err(), "py_udf: wait for supervisor shutdown")
	}
}
