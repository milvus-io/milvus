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
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestBaseTableRemoteFailureLogsProtectConfig(t *testing.T) {
	for _, key := range []string{"tlsCert", "tlsKey", "tlsCACert", "tlsMinVersion"} {
		for _, enableAuth := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/auth=%t", key, enableAuth), func(t *testing.T) {
				dir := t.TempDir()
				t.Setenv("MILVUSCONF", dir)
				canary := "etcd-" + key + "-secret-canary"
				value := filepath.Join(dir, canary)
				info := &config.EtcdInfo{
					EnableAuth: enableAuth,
					UserName:   "etcd-user-secret-canary",
					PassWord:   "etcd-password-secret-canary",
					UseSSL:     true,
					Endpoints:  []string{"https://etcd-endpoint-secret-canary.invalid:2379"},
					CertFile:   "../../../configs/cert/client.pem",
					KeyFile:    "../../../configs/cert/client.key",
					CaCertFile: "../../../configs/cert/ca.pem",
					MinVersion: "1.3",
				}
				switch key {
				case "tlsCert":
					info.CertFile = value
				case "tlsKey":
					info.KeyFile = value
				case "tlsCACert":
					info.CaCertFile = value
				case "tlsMinVersion":
					value = canary
					info.MinVersion = value
				}
				content := fmt.Sprintf(`etcd.endpoints: %q
etcd.auth.enabled: %t
etcd.auth.userName: %q
etcd.auth.password: %q
etcd.ssl.enabled: true
etcd.ssl.tlsCert: %q
etcd.ssl.tlsKey: %q
etcd.ssl.tlsCACert: %q
etcd.ssl.tlsMinVersion: %q
`, info.Endpoints[0], enableAuth, info.UserName, info.PassWord,
					info.CertFile, info.KeyFile, info.CaCertFile, info.MinVersion)
				require.NoError(t, os.WriteFile(filepath.Join(dir, "milvus.yaml"), []byte(content), 0o600))
				sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})

				// Both configuration entrypoints use the real TLS loader. Each
				// failure occurs before dialing, even for the invalid TLS version.
				mgr, err := config.Init(config.WithEtcdSource(info))
				require.Nil(t, mgr)
				require.ErrorContains(t, err, canary, "in-process errors retain their original details")
				if key == "tlsMinVersion" {
					require.ErrorIs(t, err, merr.ErrParameterInvalid)
					assert.Equal(t, merr.Code(merr.ErrParameterInvalid), merr.Code(err))
				} else {
					var pathErr *os.PathError
					require.ErrorAs(t, err, &pathErr)
					assert.Equal(t, value, pathErr.Path)
					assert.ErrorIs(t, err, os.ErrNotExist)
				}

				base := NewBaseTable(Files([]string{"milvus.yaml"}), SkipRemote(true), SkipEnv(true), Interval(0))
				t.Cleanup(base.Manager().Close)
				require.PanicsWithValue(t, "init with etcd client failed", base.initConfigsFromRemote)
				assert.Equal(t, value, base.Get("etcd.ssl."+key), "runtime configuration stays raw")
				assert.Equal(t, config.RedactedValue, base.Manager().ProjectConfigs()[strings.ToLower("etcd.ssl."+key)])
				output := sink.String()
				assert.Contains(t, output, "init with etcd client failed")
				assert.Contains(t, output, "endpointCount")
				for _, secret := range []string{canary, info.Endpoints[0], info.UserName, info.PassWord} {
					assert.NotContains(t, output, secret)
				}
				for _, field := range []string{"useSSL", "minVersion", "enable auth"} {
					assert.NotContains(t, output, field, "constructor logs must omit protected transport and auth settings")
				}
			})
		}
	}
}

func TestBaseTableRemoteUnavailablePanics(t *testing.T) {
	base := NewBaseTable(SkipRemote(true), SkipEnv(true), Files(nil), Interval(0))
	t.Cleanup(base.Manager().Close)
	require.NoError(t, base.Save("etcd.endpoints", fmt.Sprintf("127.0.0.1:%d", freePort(t))))
	require.NoError(t, base.Save("etcd.dialTimeout", "20"))

	sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
	require.PanicsWithValue(t, "init with etcd client failed", base.initConfigsFromRemote)
	assert.Nil(t, base.etcdClient)
	assert.Contains(t, sink.String(), config.RedactedValue)
}

func TestBaseTableRemoteSourceFailuresPanic(t *testing.T) {
	cli, port := setupEmbedEtcd(t)
	t.Cleanup(func() { _ = cli.Close() })
	for _, sourceCreationFails := range []bool{true, false} {
		t.Run(fmt.Sprintf("sourceCreationFails=%t", sourceCreationFails), func(t *testing.T) {
			base := NewBaseTable(SkipRemote(true), SkipEnv(true), Files(nil), Interval(0))
			t.Cleanup(func() {
				base.Manager().Close()
				if base.etcdClient != nil {
					_ = base.etcdClient.Close()
				}
			})
			require.NoError(t, base.Save("etcd.endpoints", fmt.Sprintf("127.0.0.1:%d", port)))
			const canary = "etcd-config-read-secret-canary"
			wantPanic := "load initial etcd configuration failed"
			if sourceCreationFails {
				patch := mockey.Mock(config.NewEtcdSource).Return(nil, errors.New(canary)).Build()
				t.Cleanup(func() { patch.UnPatch() })
				wantPanic = "init with etcd source failed"
			} else {
				patch := mockey.Mock((*config.EtcdSource).GetConfigurations).Return(nil, errors.New(canary)).Build()
				t.Cleanup(func() { patch.UnPatch() })
			}
			sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
			require.PanicsWithValue(t, wantPanic, base.initConfigsFromRemote)
			assert.Contains(t, sink.String(), config.RedactedValue)
			assert.NotContains(t, sink.String(), canary)
		})
	}
}
