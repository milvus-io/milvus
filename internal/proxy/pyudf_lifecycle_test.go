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

package proxy

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	internalhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/function/pyudf"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestProxyStopPyUDF(t *testing.T) {
	item := &paramtable.Get().FunctionCfg.PyUDFEnabled
	old := item.SwapTempValue("false")
	t.Cleanup(func() { item.SwapTempValue(old) })
	for _, tc := range []struct {
		name, enabled     string
		stopErr           error
		closeErr          error
		disableDuringStop bool
	}{
		{name: "disabled", enabled: "false"},
		{name: "enabled", enabled: "true"},
		{name: "disabled_during_stop", enabled: "true", disableDuringStop: true},
		{name: "disabled_with_errors", enabled: "false", stopErr: merr.ErrServiceUnavailable, closeErr: merr.ErrServiceInternal},
		{name: "stop_error", enabled: "true", stopErr: merr.ErrServiceUnavailable},
		{name: "stop_timeout", enabled: "true", stopErr: merr.Wrap(context.DeadlineExceeded, "py_udf: wait for supervisor shutdown")},
		{name: "close_error", enabled: "true", closeErr: merr.ErrServiceInternal},
		{name: "both_errors", enabled: "true", stopErr: merr.ErrServiceUnavailable, closeErr: merr.ErrServiceInternal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item.SwapTempValue(tc.enabled)
			sink := mlog.CaptureGlobalLogs(t, &mlog.Config{
				Level: "warn", Format: "json", DisableCaller: true,
				DisableTimestamp: true, DisableStacktrace: true,
			})
			var events []string
			nodeCtx, cancelNodeCtx := context.WithCancel(mlog.WithFields(context.Background(),
				mlog.String("pyudf_cleanup_case", t.Name())))
			cancelNodeCtx() // Cleanup must still notify Python after context cancellation.
			node := &Proxy{ctx: nodeCtx, cancel: func() { events = append(events, "cancel") }}
			node.AddCloseCallback(func() { events = append(events, "close_callback") })
			mock := mockey.Mock(pyudf.StopSupervisor).To(func(ctx context.Context) error {
				require.NoError(t, ctx.Err())
				events = append(events, "stop_python")
				if tc.disableDuringStop {
					item.SwapTempValue("false")
				}
				return tc.stopErr
			}).Build()
			defer mock.UnPatch()
			closeMock := mockey.Mock(pyudf.CloseClients).To(func() error { events = append(events, "close_client"); return tc.closeErr }).Build()
			defer closeMock.UnPatch()
			// Unrelated goroutines may log even the same message with another
			// context. Capture safely and filter instead of patching mlog.Warn.
			backgroundDone := make(chan struct{})
			go func() {
				defer close(backgroundDone)
				for range 50 {
					mlog.Warn(context.TODO(), "PyUDF supervisor cleanup failed; continuing Proxy shutdown",
						mlog.Err(merr.ErrServiceUnavailable))
				}
			}()
			err := node.Stop()
			<-backgroundDone
			require.NoError(t, err)
			require.Equal(t, []string{"stop_python", "close_client", "close_callback", "cancel"}, events)
			warnings := make(map[string]string)
			for _, line := range strings.Split(strings.TrimSpace(sink.String()), "\n") {
				var entry struct {
					Message string `json:"message"`
					Level   string `json:"level"`
					Error   string `json:"error"`
					Test    string `json:"pyudf_cleanup_case"`
				}
				require.NoError(t, json.Unmarshal([]byte(line), &entry))
				if entry.Test != t.Name() || !strings.HasPrefix(entry.Message, "PyUDF ") {
					continue
				}
				require.Equal(t, "WARN", entry.Level)
				require.NotContains(t, warnings, entry.Message, "cleanup error must be logged once")
				warnings[entry.Message] = entry.Error
			}
			wantWarnings := make(map[string]string)
			if tc.stopErr != nil {
				wantWarnings["PyUDF supervisor cleanup failed; continuing Proxy shutdown"] = tc.stopErr.Error()
			}
			if tc.closeErr != nil {
				wantWarnings["PyUDF client cleanup failed; continuing Proxy shutdown"] = tc.closeErr.Error()
			}
			require.Equal(t, wantWarnings, warnings)
		})
	}
}

func TestProxyInitPyUDF(t *testing.T) {
	item := &paramtable.Get().FunctionCfg.PyUDFEnabled
	old := item.SwapTempValue("false")
	mode := fileresource.GetLocalMode()
	t.Cleanup(func() { item.SwapTempValue(old); fileresource.SetLocalMode(mode) })
	for _, tc := range []struct {
		enabled  string
		mode     fileresource.Mode
		startErr error
	}{
		{"true", fileresource.CloseMode, nil},
		{"true", fileresource.RefMode, nil},
		{"true", fileresource.SyncMode, nil},
		{"false", fileresource.CloseMode, nil},
		{"false", fileresource.RefMode, nil},
		{"false", fileresource.SyncMode, nil},
		{"true", fileresource.SyncMode, merr.ErrServiceUnavailable},
	} {
		name := tc.enabled + "_" + tc.mode.String()
		if tc.startErr != nil {
			name += "_start_error"
		}
		t.Run(name, func(t *testing.T) {
			item.SwapTempValue(tc.enabled)
			fileresource.SetLocalMode(tc.mode)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			factory := dependency.NewMockFactory(t)
			chunkManager := mocks.NewChunkManager(t)
			if tc.mode == fileresource.SyncMode {
				factory.EXPECT().Init(paramtable.Get()).Once()
				factory.EXPECT().NewPersistentStorageChunkManager(ctx).Return(chunkManager, nil).Once()
			}

			// Let Init reach its PyUDF hook without etcd, storage I/O, Python,
			// or changes to process-wide file managers and management verifiers.
			defer mockey.Mock((*Proxy).initSession).Return(nil).Build().UnPatch()
			defer mockey.Mock((*Proxy).initRateCollector).Return(nil).Build().UnPatch()
			defer mockey.Mock(initMetaCache).Return(NewMockCache(t), nil).Build().UnPatch()
			defer mockey.Mock(internalhttp.RegisterManagementVerifier).Return().Build().UnPatch()
			var events []string
			defer mockey.Mock(fileresource.InitManager).To(func(manager storage.ChunkManager, mode fileresource.Mode) {
				require.Equal(t, tc.mode, mode)
				if mode == fileresource.SyncMode {
					require.Same(t, chunkManager, manager)
				} else {
					require.Nil(t, manager)
				}
				events = append(events, "file_resources")
			}).Build().UnPatch()
			defer mockey.Mock(pyudf.StartSupervisor).To(func(startCtx context.Context) error {
				require.Same(t, ctx, startCtx)
				require.NoError(t, startCtx.Err())
				events = append(events, "start_supervisor")
				return tc.startErr
			}).Build().UnPatch()

			node := &Proxy{ctx: ctx, cancel: cancel, factory: factory}
			t.Cleanup(func() {
				if node.rowIDAllocator != nil {
					node.rowIDAllocator.Close()
				}
				if node.sched != nil {
					node.sched.Close()
				}
				if node.shardMgr != nil {
					node.shardMgr.Close()
				}
				if node.lbPolicy != nil {
					node.lbPolicy.Close()
				}
			})
			err := node.Init()
			if tc.startErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.startErr)
				require.Contains(t, err.Error(), "initialize Proxy PyUDF process")
			}
			// Proxy always calls the hook; enabled gating belongs to pyudf.
			require.Equal(t, []string{"file_resources", "start_supervisor"}, events)
		})
	}
}
