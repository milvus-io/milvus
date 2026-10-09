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

package coordinator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"

	pkgconfig "github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Use an isolated manager and real etcd transactions without changing the
// process-global security configuration or relying on an external service.
func configWriteTestBase(t *testing.T) *paramtable.BaseTable {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{{Scheme: "http", Host: "127.0.0.1:0"}}
	cfg.ListenPeerUrls = []url.URL{{Scheme: "http", Host: "127.0.0.1:0"}}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	server, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(server.Close)
	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.Fatal("embedded etcd did not become ready")
	}
	client := v3client.New(server.Server)
	t.Cleanup(func() { client.Close() })
	base := paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true), paramtable.Interval(0))
	t.Cleanup(base.Manager().Close)
	require.NoError(t, base.Save("localStorage.path", t.TempDir()))
	params := &paramtable.ComponentParam{}
	params.Init(base)
	source, err := pkgconfig.NewEtcdSource(client, &pkgconfig.EtcdInfo{KeyPrefix: "write-compatibility"})
	require.NoError(t, err)
	require.NoError(t, base.Manager().AddSource(source))
	patch := mockey.Mock(paramtable.GetBaseTable).Return(base).Build()
	t.Cleanup(func() { patch.UnPatch() })
	paramsPatch := mockey.Mock(paramtable.Get).Return(params).Build()
	t.Cleanup(func() { paramsPatch.UnPatch() })
	return base
}

func postConfigWrite(t *testing.T, body interface{}) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, "/management/config/alter", bytes.NewReader(payload))
	response := httptest.NewRecorder()
	(&mixCoordImpl{}).HandleAlterConfig(response, request)
	return response
}

func TestHandleAlterConfigWriteCompatibility(t *testing.T) {
	base := configWriteTestBase(t)
	manager := base.Manager()
	for _, test := range []struct {
		key      string
		value    string
		readMode string
	}{
		{"minio.secretAccessKey", "write-value-canary", "masked"},
		{"minio.address", "write-target-canary.invalid", "masked"},
		{"pulsar.tenant", "write-pulsar-resource-canary", "masked"},
		{"pulsar/tenant", "write-pulsar-resource-canary", "masked"},
		{"PULSAR_TENANT", "write-pulsar-resource-canary", "masked"},
		{"pulsartenant", "write-pulsar-resource-canary", "masked"},
		{"pulsar.namespace", "write-pulsar-resource-canary", "masked"},
		{"pulsar/namespace", "write-pulsar-resource-canary", "masked"},
		{"PULSAR_NAMESPACE", "write-pulsar-resource-canary", "masked"},
		{"pulsarnamespace", "write-pulsar-resource-canary", "masked"},
		{"common.security.defaultRootPassword", "write-password-canary", "masked"},
		{"common.security.authorizationEnabled", "true", "public"},
		{"common/security/superUsers", "write-user-canary", "public"},
		{"COMMON_SECURITY_EXPR_ENABLED", "true", "hidden"},
		{"proxy.enablePublicPrivilege", "false", "hidden"},
		{"proxy.soPath", "/write-plugin-canary.so", "public"},
		{"common.panicWhenPluginFail", "false", "public"},
		{"builtinRoles.enable", "true", "public"},
		{"builtinRoles.roles", `{"public":{"privileges":[]}}`, "public"},
		{"kafka.consumer.sasl.password", "write-group-canary", "masked"},
		{"credential.compat.secret_access_key", "write-credential-canary", "masked"},
		{"test.alter.write-name-canary", "write-unknown-canary", "hidden"},
		{"test.alter.emptyvalue", "", "hidden"},
		// The knowhere prefix preserves case and separators in storage. The
		// handler must not resolve these inputs into a different stored key.
		{"knowhere.DISKANN/build/search_list", "101", "storage-only"},
		{"KNOWHERE.DISKANN.build.search_list", "102", "storage-only"},
	} {
		t.Run(test.key, func(t *testing.T) {
			_, original, originalErr := manager.GetConfig(test.key)
			sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
			for _, legacy := range []bool{true, false} {
				entry := map[string]interface{}{"key": test.key, "value": test.value}
				var body interface{} = entry
				if !legacy {
					body = map[string]interface{}{"configs": []map[string]interface{}{entry}}
				}
				response := postConfigWrite(t, body)
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				source, value, err := manager.GetConfig(test.key)
				require.NoError(t, err)
				require.Equal(t, "EtcdSource", source)
				require.Equal(t, test.value, value, "a successful write must preserve the raw value")

				request := httptest.NewRequest(http.MethodGet, "/management/config/get?keys="+url.QueryEscape(test.key), nil)
				read := httptest.NewRecorder()
				(&mixCoordImpl{}).HandleGetConfig(read, request)
				require.Equal(t, http.StatusOK, read.Code)
				var result struct {
					Configs []struct {
						Value string `json:"value"`
						Error string `json:"error"`
					} `json:"configs"`
				}
				require.NoError(t, json.Unmarshal(read.Body.Bytes(), &result))
				require.Len(t, result.Configs, 1)
				switch test.readMode {
				case "hidden":
					require.Contains(t, result.Configs[0].Error, "unregistered")
					require.Empty(t, result.Configs[0].Value)
				case "masked":
					require.Empty(t, result.Configs[0].Error)
					require.Equal(t, pkgconfig.RedactedValue, result.Configs[0].Value)
				case "public":
					require.Empty(t, result.Configs[0].Error)
					require.Equal(t, test.value, result.Configs[0].Value)
				}

				delete(entry, "value")
				if !legacy {
					entry["value"] = nil // Both omitted and explicit null reset.
				}
				response = postConfigWrite(t, body)
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				_, value, err = manager.GetConfig(test.key)
				if originalErr == nil {
					require.NoError(t, err)
					require.Equal(t, original, value, "deleting the etcd override must restore the previous source")
				} else {
					require.ErrorIs(t, err, pkgconfig.ErrKeyNotFound)
				}
			}
			require.Contains(t, sink.String(), "HandleAlterConfig success")
			require.Contains(t, sink.String(), "configs atomically altered in etcd")
			require.NotContains(t, sink.String(), "canary")
		})
	}
}

func TestHandleAlterConfigLegacyValidation(t *testing.T) {
	base := configWriteTestBase(t)
	manager := base.Manager()
	const key = "test.alter.batch"
	manager.ImmutableUpdate("test.alter.immutable")
	for _, blocked := range []struct {
		key     string
		message string
	}{
		{"mq.type", "mqtype configuration cannot be modified"},
		{"MQ/TYPE", "mqtype configuration cannot be modified"},
		{"custom.mqtype.option", "mqtype configuration cannot be modified"},
		{"MQ_TYPE", "mqtype configuration cannot be modified"},
		{"common.security.adminAuthEnabled", "cannot be modified through this endpoint; set it in the configuration file"},
		{"common_security_adminAuthEnabled", "cannot be modified through this endpoint; set it in the configuration file"},
		{"common/security/adminAuthEnabled", "cannot be modified through this endpoint; set it in the configuration file"},
		{"COMMON.SECURITY.ADMINAUTHENABLED", "cannot be modified through this endpoint; set it in the configuration file"},
		{"commonsecurityadminauthenabled", "cannot be modified through this endpoint; set it in the configuration file"},
		{"kafka.producer.message.max.bytes", "immutable configuration cannot be modified"},
		{"test.alter.immutable", "immutable configuration cannot be modified"},
		{"TEST_ALTER_IMMUTABLE", "immutable configuration cannot be modified"},
	} {
		for _, operation := range []string{"set", "delete"} {
			t.Run(blocked.key+"/"+operation, func(t *testing.T) {
				sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
				entry := map[string]interface{}{"key": blocked.key}
				if operation == "set" {
					entry["value"] = "write-rejected-canary"
				}
				response := postConfigWrite(t, map[string]interface{}{"configs": []map[string]interface{}{
					{"key": key, "value": "must-not-commit"}, entry,
				}})
				require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
				require.Contains(t, response.Body.String(), blocked.message)
				_, _, err := manager.GetConfig(key)
				require.ErrorIs(t, err, pkgconfig.ErrKeyNotFound, "validation must precede the entire transaction")
				require.NotContains(t, sink.String(), blocked.key)
				require.NotContains(t, sink.String(), "canary")
			})
		}
	}

	t.Run("literal duplicates are rejected before storage", func(t *testing.T) {
		response := postConfigWrite(t, map[string]interface{}{"configs": []map[string]interface{}{
			{"key": key, "value": "first"}, {"key": key, "value": "second"},
		}})
		require.Equal(t, http.StatusBadRequest, response.Code)
		require.Contains(t, response.Body.String(), "duplicate key found")
	})

	t.Run("alias collisions retain the storage error and atomicity", func(t *testing.T) {
		sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})
		response := postConfigWrite(t, map[string]interface{}{"configs": []map[string]interface{}{
			{"key": "test.alter.write-error-canary", "value": "must-not-commit-canary"},
			{"key": key, "value": "first-canary"},
			{"key": strings.ToUpper(strings.ReplaceAll(key, ".", "_")), "value": "second-canary"},
		}})
		require.Equal(t, http.StatusInternalServerError, response.Code)
		require.Contains(t, response.Body.String(), "failed to atomically alter configurations in etcd")
		for _, check := range []string{key, "test.alter.write-error-canary"} {
			_, _, err := manager.GetConfig(check)
			require.ErrorIs(t, err, pkgconfig.ErrKeyNotFound)
		}
		require.Contains(t, sink.String(), "HandleAlterConfig failed to atomically alter configs in etcd")
		require.NotContains(t, sink.String(), "canary")
	})
}
