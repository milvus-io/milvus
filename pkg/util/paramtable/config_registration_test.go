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

package paramtable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func TestConfigDeclarationPublishesSensitivityBeforeVisibility(t *testing.T) {
	for _, test := range []struct {
		name    string
		key     string
		pauseAt string
		prefix  bool
		init    func(*config.Manager)
	}{
		{
			name: "scalar", key: "minio.port", pauseAt: "minio.port",
			init: func(mgr *config.Manager) {
				item := ParamItem{Key: "minio.port", Sensitivity: Sensitive}
				item.Init(mgr)
			},
		},
		{
			name: "fallback", key: "legacy.port", pauseAt: "legacy.port",
			init: func(mgr *config.Manager) {
				item := ParamItem{Key: "minio.port", FallbackKeys: []string{"legacy.port"}, Sensitivity: Sensitive}
				item.Init(mgr)
			},
		},
		{
			name: "group", key: "provider.route", pauseAt: "provider.", prefix: true,
			init: func(mgr *config.Manager) {
				group := ParamGroup{KeyPrefix: "provider.", Sensitive: true, NonSensitiveSuffixes: []string{"enable"}}
				group.Init(mgr)
			},
		},
		{
			name: "empty_group", key: "opaque", prefix: true,
			init: func(mgr *config.Manager) {
				group := ParamGroup{Sensitive: true}
				group.Init(mgr)
			},
		},
		{
			name: "hook_scalar_inherits_group_policy", key: "soPath", prefix: true,
			init: func(mgr *config.Manager) {
				params := hookConfig{}
				params.init(&BaseTable{mgr: mgr})
			},
		},
		{
			name: "tls_cluster", key: "tls.clusters.prod.caPemPath", pauseAt: "tls.clusters.", prefix: true,
			init: func(mgr *config.Manager) {
				params := grpcConfig{}
				params.init("queryNode", &BaseTable{mgr: mgr})
			},
		},
		{
			name: "grpc_cluster", key: "grpc.clusters.prod.authority", pauseAt: "grpc.clusters.", prefix: true,
			init: func(mgr *config.Manager) {
				params := grpcConfig{}
				params.init("queryNode", &BaseTable{mgr: mgr})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			const canary = "registration-window-secret-canary"
			filename := filepath.Join(t.TempDir(), "milvus.yaml")
			writeValue := func(value string) {
				require.NoError(t, os.WriteFile(filename, []byte(fmt.Sprintf("%s: %q\n", test.key, value)), 0o600))
			}
			writeValue("old")
			mgr := config.NewManager()
			t.Cleanup(mgr.Close)
			source := config.NewFileSource(&config.FileInfo{Files: []string{filename}})
			require.NoError(t, mgr.AddSource(source))
			sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})

			// Pause the actual declaration at its sensitivity registration call.
			// A real file refresh can run here because BaseTable starts sources
			// before initializing its ParamItems and ParamGroups.
			var original func(*config.Manager, string)
			called := false
			register := (*config.Manager).RegisterSensitiveKey
			if test.prefix {
				register = (*config.Manager).RegisterSensitivePrefix
			}
			patch := mockey.Mock(register).To(func(manager *config.Manager, key string) {
				if manager == mgr && key == test.pauseAt {
					called = true
					writeValue(canary)
					_, err := source.GetConfigurations()
					require.NoError(t, err)
					for key, value := range mgr.ProjectConfigs() {
						assert.NotContains(t, value, canary, "projection during registration: %s", key)
					}
					assert.NotContains(t, sink.String(), canary)
				}
				original(manager, key)
			}).Origin(&original).Build()
			defer patch.UnPatch()
			test.init(mgr)
			require.True(t, called, "the real initialization must reach the paused registration")
			assert.Contains(t, sink.String(), "receive update event")
			assert.NotContains(t, sink.String(), canary)
			assert.Equal(t, config.RedactedValue, mgr.RedactValue(test.key, canary))
			_, value, err := mgr.GetConfig(test.key)
			require.NoError(t, err)
			assert.Equal(t, canary, value, "runtime reads remain raw")
		})
	}
}
