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

package process

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestInitCmdMQType(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{"default", map[string]string{}, "pulsar"},
		{"override", map[string]string{"MQ_TYPE": "woodpecker"}, "woodpecker"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(root, "bin"), 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(root, "bin", "milvus"), nil, 0o600))
			t.Setenv("MQ_TYPE", "inherited-value")
			mp := &MilvusProcess{workDir: root, env: tc.env, role: "querynode", nodeID: 42}
			mp.initCmd()
			require.Contains(t, mp.cmd.Environ(), "MQ_TYPE="+tc.want)
			require.NotContains(t, mp.cmd.Environ(), "MQ_TYPE=inherited-value")
		})
	}
}

func TestProcessPIDAndKill(t *testing.T) {
	cmd := exec.CommandContext(t.Context(), "sleep", "60")
	require.NoError(t, cmd.Start())
	notifier := syncutil.NewAsyncTaskNotifier[error]()
	t.Cleanup(notifier.Cancel)
	go func() { notifier.Finish(cmd.Wait()) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		notifier.BlockUntilFinish()
	})
	mp := &MilvusProcess{cmd: cmd, notifier: notifier, graceful: lifetime.NewSafeChan()}
	require.Equal(t, cmd.Process.Pid, mp.PID())

	err := mp.Kill()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.False(t, cmd.ProcessState.Success())
	select {
	case <-mp.graceful.CloseCh():
	default:
		t.Fatal("Kill must close the graceful shutdown channel after killing the process")
	}
	require.ErrorIs(t, mp.Kill(), os.ErrProcessDone)
}

func TestGetAddressReturnsBeforeProcessJoin(t *testing.T) {
	notifier := syncutil.NewAsyncTaskNotifier[error]()
	mp := &MilvusProcess{notifier: notifier}
	// Process exit cancels the workers before background can join them and
	// finish the notifier. Address discovery must return without that result.
	notifier.Cancel()
	t.Cleanup(func() { notifier.Finish(nil) })
	done := make(chan error, 1)
	go func() {
		_, err := mp.GetAddress(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("address discovery is waiting for the worker join that depends on it")
	}
}
