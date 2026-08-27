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

package coordinator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgconfig "github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestHandleAlterConfig(t *testing.T) {
	paramtable.Init()
	mgr := paramtable.GetBaseTable().Manager()

	// Verify etcd source is available (requires external etcd running)
	_, hasEtcd := mgr.GetEtcdSource()
	require.True(t, hasEtcd, "etcd source is required for this test, ensure etcd is running")

	// Mark some keys as immutable for testing
	mgr.ImmutableUpdate("test.immutable.key1")
	for _, key := range []string{
		"test.alter.config.key1",
		"test.alter.config.legacy",
		"test.alter.config.empty",
		"test.alter.config.reset_me",
		"test.alter.mixed.keep",
		"test.alter.mixed.remove",
		"test.alter.config.key2",
		"test.alter.config.key3",
		"test.immutable.key1",
	} {
		mgr.RegisterConfigKey(key)
	}

	coord := &mixCoordImpl{}

	t.Run("single config update", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"configs": []map[string]interface{}{
				{"key": "test.alter.config.key1", "value": "value1"},
			},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()

		coord.HandleAlterConfig(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var resp map[string]string
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Equal(t, "OK", resp["msg"])

		assert.Eventually(t, func() bool {
			_, value, err := mgr.GetConfig("test.alter.config.key1")
			return err == nil && value == "value1"
		}, time.Second*10, 100*time.Millisecond)
	})

	t.Run("legacy single-key format", func(t *testing.T) {
		reqBody := map[string]string{
			"key":   "test.alter.config.legacy",
			"value": "legacy_value",
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()

		coord.HandleAlterConfig(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		assert.Eventually(t, func() bool {
			_, value, err := mgr.GetConfig("test.alter.config.legacy")
			return err == nil && value == "legacy_value"
		}, time.Second*10, 100*time.Millisecond)
	})

	t.Run("set config with empty value", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"configs": []map[string]interface{}{
				{"key": "test.alter.config.empty", "value": ""},
			},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()
		coord.HandleAlterConfig(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		assert.Eventually(t, func() bool {
			_, value, err := mgr.GetConfig("test.alter.config.empty")
			return err == nil && value == ""
		}, time.Second*10, 100*time.Millisecond)
	})

	t.Run("reset config by omitting value", func(t *testing.T) {
		// First set a value
		reqBody := map[string]interface{}{
			"configs": []map[string]interface{}{
				{"key": "test.alter.config.reset_me", "value": "to_be_reset"},
			},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()
		coord.HandleAlterConfig(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		assert.Eventually(t, func() bool {
			_, value, err := mgr.GetConfig("test.alter.config.reset_me")
			return err == nil && value == "to_be_reset"
		}, time.Second*10, 100*time.Millisecond)

		// Reset by omitting value (value is null/absent → delete from etcd)
		reqBody = map[string]interface{}{
			"configs": []map[string]interface{}{
				{"key": "test.alter.config.reset_me"},
			},
		}
		body, _ = json.Marshal(reqBody)
		req = httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w = httptest.NewRecorder()
		coord.HandleAlterConfig(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("mixed update and reset in one request", func(t *testing.T) {
		// Setup: write two configs
		reqBody := map[string]interface{}{
			"configs": []map[string]interface{}{
				{"key": "test.alter.mixed.keep", "value": "old_value"},
				{"key": "test.alter.mixed.remove", "value": "to_remove"},
			},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()
		coord.HandleAlterConfig(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		assert.Eventually(t, func() bool {
			_, v1, err1 := mgr.GetConfig("test.alter.mixed.keep")
			_, v2, err2 := mgr.GetConfig("test.alter.mixed.remove")
			return err1 == nil && v1 == "old_value" && err2 == nil && v2 == "to_remove"
		}, time.Second*10, 100*time.Millisecond)

		// Atomically: update one, reset (delete) the other
		reqBody = map[string]interface{}{
			"configs": []map[string]interface{}{
				{"key": "test.alter.mixed.keep", "value": "new_value"},
				{"key": "test.alter.mixed.remove"}, // no value → reset
			},
		}
		body, _ = json.Marshal(reqBody)
		req = httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w = httptest.NewRecorder()
		coord.HandleAlterConfig(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		assert.Eventually(t, func() bool {
			_, v, err := mgr.GetConfig("test.alter.mixed.keep")
			return err == nil && v == "new_value"
		}, time.Second*10, 100*time.Millisecond)
	})

	t.Run("multiple configs atomic update", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"configs": []map[string]interface{}{
				{"key": "test.alter.config.key1", "value": "atomic_value1"},
				{"key": "test.alter.config.key2", "value": "atomic_value2"},
				{"key": "test.alter.config.key3", "value": "atomic_value3"},
			},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()

		coord.HandleAlterConfig(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		assert.Eventually(t, func() bool {
			_, v1, err1 := mgr.GetConfig("test.alter.config.key1")
			_, v2, err2 := mgr.GetConfig("test.alter.config.key2")
			_, v3, err3 := mgr.GetConfig("test.alter.config.key3")
			return err1 == nil && v1 == "atomic_value1" &&
				err2 == nil && v2 == "atomic_value2" &&
				err3 == nil && v3 == "atomic_value3"
		}, time.Second*10, 100*time.Millisecond)
	})

	t.Run("empty configs array should fail", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"configs": []map[string]interface{}{},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()

		coord.HandleAlterConfig(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "configs array is required")
	})

	t.Run("missing key should fail", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"configs": []map[string]interface{}{
				{"value": "value_without_key"},
			},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()

		coord.HandleAlterConfig(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "non-empty key")
	})

	t.Run("duplicate keys should fail", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"configs": []map[string]interface{}{
				{"key": "test.alter.config.key1", "value": "value1"},
				{"key": "test.alter.config.key1", "value": "value2"},
			},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()

		coord.HandleAlterConfig(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "duplicate key")
	})

	t.Run("mqtype config should fail", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"configs": []map[string]interface{}{
				{"key": "mq.type", "value": "pulsar"},
			},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()

		coord.HandleAlterConfig(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "mqtype configuration cannot be modified")
		assert.Contains(t, w.Body.String(), "alterWAL endpoint")
	})

	t.Run("immutable config should fail", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"configs": []map[string]interface{}{
				{"key": "test.immutable.key1", "value": "value"},
			},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()

		coord.HandleAlterConfig(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "immutable configuration cannot be modified")
	})

	t.Run("kafka producer message max bytes should be immutable", func(t *testing.T) {
		key := paramtable.Get().KafkaCfg.ProducerMessageMaxBytes.Key
		require.True(t, mgr.IsImmutable(key))

		reqBody := map[string]interface{}{
			"configs": []map[string]interface{}{
				{"key": key, "value": "20971520"},
			},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()

		coord.HandleAlterConfig(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "immutable configuration cannot be modified")
	})

	t.Run("wrong HTTP method should fail", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/config/alter", nil)
		w := httptest.NewRecorder()

		coord.HandleAlterConfig(w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
		assert.Contains(t, w.Body.String(), "Method not allowed")
	})

	t.Run("invalid JSON should fail", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader([]byte("invalid json")))
		w := httptest.NewRecorder()

		coord.HandleAlterConfig(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "Invalid request body")
	})
}

// These reject before any etcd access, so they must not sit behind the live-etcd
// requirement of TestHandleAlterConfig — they are the checks that keep a
// credential or an undeclared key out of etcd in the first place.
func TestHandleAlterConfigValidation(t *testing.T) {
	paramtable.Init()
	mgr := paramtable.GetBaseTable().Manager()
	coord := &mixCoordImpl{}

	t.Run("security-governing config cannot be altered at all", func(t *testing.T) {
		params := paramtable.Get()
		for _, key := range []string{
			params.CommonCfg.AuthorizationEnabled.Key,
			params.CommonCfg.SuperUsers.Key,
			params.CommonCfg.DefaultRootPassword.Key,
			// The privilege tables and the /expr switches are the ones the
			// original two-name fence let through.
			params.RbacConfig.ClusterAdminPrivileges.Key,
			params.CommonCfg.EnablePublicPrivilege.Key,
			params.CommonCfg.ExprEnabled.Key,
			params.CommonCfg.ExprAuthMode.Key,
			// Declared outside common.security. but decides RBAC alias handling.
			params.ProxyCfg.ResolveAliasForPrivilege.Key,
			// Undeclared legacy alias read by EnablePublicPrivilege's Formatter:
			// nothing normalises its spelling, so the fence has to.
			"proxy.enablePublicPrivilege",
			"proxy_enablePublicPrivilege",
			"PROXY_ENABLEPUBLICPRIVILEGE",
			"proxyenablepublicprivilege",
			// Re-spellings of a declared fenced key.
			"COMMON_SECURITY_SUPERUSERS",
			"common/security/superUsers",
		} {
			for _, cfg := range []map[string]interface{}{{"key": key, "value": "false"}, {"key": key}} {
				reqBody := map[string]interface{}{"configs": []map[string]interface{}{cfg}}
				body, _ := json.Marshal(reqBody)
				req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
				w := httptest.NewRecorder()
				coord.HandleAlterConfig(w, req)
				assert.Equal(t, http.StatusBadRequest, w.Code, key)
				assert.Contains(t, w.Body.String(), "security-governing configuration", key)
			}
		}
	})

	t.Run("sensitive config should fail before etcd access", func(t *testing.T) {
		const key = "test.alter.sensitive"
		mgr.RegisterConfigKey(key)
		mgr.RegisterSensitiveKey(key)
		reqBody := map[string]interface{}{"configs": []map[string]interface{}{{"key": key, "value": "secret"}}}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()
		coord.HandleAlterConfig(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "sensitive configuration")
	})

	t.Run("sensitive config cannot be deleted either", func(t *testing.T) {
		const key = "test.alter.sensitive.delete"
		mgr.RegisterConfigKey(key)
		mgr.RegisterSensitiveKey(key)
		reqBody := map[string]interface{}{"configs": []map[string]interface{}{{"key": key}}}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()
		coord.HandleAlterConfig(w, req)
		// A delete is not a removal, it is a reversion: whatever the yaml or the
		// compiled default says comes back, and for minio.secretAccessKey that
		// is "minioadmin". An endpoint with no authentication in front of it
		// does not get to do that, for the same reason it does not get to write
		// the key.
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "sensitive configuration")
	})

	t.Run("undeclared config may still be deleted, secret-named or not", func(t *testing.T) {
		// Where the reasoning above stops. An operator holding a key an older
		// build wrote needs some way to remove it, and a secret-named one is
		// precisely the case that argument is about — so the name-pattern guess
		// must not fence it. Only a key Milvus itself declares to be a
		// sensitive configuration is undeletable.
		for _, legacy := range []string{
			"test.alter.undeclared.password",
			"test.alter.undeclared.apiKey",
			"test.alter.undeclared.private_key",
		} {
			reqBody := map[string]interface{}{"configs": []map[string]interface{}{{"key": legacy}}}
			body, _ := json.Marshal(reqBody)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
			w := httptest.NewRecorder()
			coord.HandleAlterConfig(w, req)
			assert.NotEqual(t, http.StatusBadRequest, w.Code, legacy)
			assert.NotContains(t, w.Body.String(), "sensitive configuration", legacy)
		}
	})

	t.Run("undeclared config may still be deleted", func(t *testing.T) {
		reqBody := map[string]interface{}{"configs": []map[string]interface{}{{"key": "test.alter.undeclared.legacy"}}}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()
		coord.HandleAlterConfig(w, req)
		// Validation is what this asserts, so assert only that. What happens
		// after it depends on whether this package has an etcd to talk to, and
		// TestHandleAlterConfig above requires that it does.
		assert.NotEqual(t, http.StatusBadRequest, w.Code)
		assert.NotContains(t, w.Body.String(), "sensitive configuration")
	})

	t.Run("sensitive ParamGroup members remain deletable", func(t *testing.T) {
		// A group member has no declared default to restore. Keeping deletion as a
		// cleanup escape hatch avoids stranding entries written by an older build;
		// only setting a sensitive member is refused below.
		for _, key := range []string{
			"credential.aksk1.secret_access_key",
			"kafka.consumer.sasl.password",
			"function.textEmbedding.providers.openai.credential",
			"function.textEmbedding.providers.openai.url",
			"function.analyzer.lindera.download_urls.ipadic",
		} {
			reqBody := map[string]interface{}{"configs": []map[string]interface{}{{"key": key}}}
			body, _ := json.Marshal(reqBody)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
			w := httptest.NewRecorder()
			coord.HandleAlterConfig(w, req)
			assert.NotEqual(t, http.StatusBadRequest, w.Code, key)
			assert.NotContains(t, w.Body.String(), "sensitive configuration", key)
		}
	})

	t.Run("sensitive ParamGroup members cannot be set", func(t *testing.T) {
		// A member of a declared sensitive ParamGroup needs no prior registration
		// to resolve. Credential values and topology targets are both refused.
		for _, key := range []string{
			"credential.aksk1.secret_access_key",
			"function.textEmbedding.providers.openai.url",
			"function.models.zilliz.endpoint",
			"function.analyzer.lindera.download_urls.ipadic",
		} {
			reqBody := map[string]interface{}{"configs": []map[string]interface{}{{"key": key, "value": "sensitive-value"}}}
			body, _ := json.Marshal(reqBody)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
			w := httptest.NewRecorder()
			coord.HandleAlterConfig(w, req)
			assert.Equal(t, http.StatusBadRequest, w.Code, key)
			assert.Contains(t, w.Body.String(), "sensitive configuration", key)
		}
	})

	t.Run("unregistered config may still be deleted", func(t *testing.T) {
		// A key an older build wrote must remain removable even though nothing
		// declares it any more; only setting one is refused.
		const key = "test.alter.no.longer.declared"
		reqBody := map[string]interface{}{"configs": []map[string]interface{}{{"key": key}}}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()
		coord.HandleAlterConfig(w, req)
		assert.NotEqual(t, http.StatusBadRequest, w.Code)
		assert.NotContains(t, w.Body.String(), "unregistered configuration")
	})

	t.Run("unregistered config should fail before etcd access", func(t *testing.T) {
		const key = "test.alter.unregistered"
		reqBody := map[string]interface{}{"configs": []map[string]interface{}{{"key": key, "value": "value"}}}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()
		coord.HandleAlterConfig(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "unregistered configuration")
	})
}

func TestHandleGetConfig(t *testing.T) {
	paramtable.Init()
	mgr := paramtable.GetBaseTable().Manager()

	coord := &mixCoordImpl{}

	// Seed configs directly via Manager (no etcd needed). Scalars go through
	// SetConfig; ParamGroup members go through SetMapConfig, which keeps the
	// dotted identity a file or etcd source would have given them.
	scalars := map[string]string{
		"test.getconfig.key1":   "val1",
		"test.getconfig.key2":   "val2",
		"test.getconfig.key3":   "val3",
		"test.getconfig.opaque": "opaque-secret",
		"pulsar.authParams":     "token:broker-secret",
		"AWS_SECRET_ACCESS_KEY": "environment-secret",
	}
	groupMembers := map[string]string{
		"credential.aksk1.secret_access_key":             "param-group-secret",
		"kafka.consumer.ssl.key.pem":                     "inline-private-key",
		"function.analyzer.lindera.download_urls.ipadic": "https://example.invalid/dict",
	}
	for key, value := range scalars {
		mgr.SetConfig(key, value)
	}
	for key, value := range groupMembers {
		mgr.SetMapConfig(key, value)
	}
	t.Cleanup(func() {
		// This manager is the process-global paramtable; leaving these behind
		// would leak into every other test in the package.
		for key := range scalars {
			mgr.ResetConfig(key)
		}
		for key := range groupMembers {
			mgr.ResetConfig(key)
		}
	})
	for _, key := range []string{"test.getconfig.key1", "test.getconfig.key2", "test.getconfig.key3", "test.getconfig.opaque"} {
		mgr.RegisterConfigKey(key)
	}
	mgr.RegisterSensitiveKey("test.getconfig.opaque")

	type configResult struct {
		Key    string `json:"key"`
		Value  string `json:"value,omitempty"`
		Source string `json:"source,omitempty"`
		Error  string `json:"error,omitempty"`
	}

	parseResponse := func(t *testing.T, w *httptest.ResponseRecorder) []configResult {
		var resp struct {
			Configs []configResult `json:"configs"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		return resp.Configs
	}

	t.Run("get single key", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/management/config/get?keys=test.getconfig.key1", nil)
		w := httptest.NewRecorder()
		coord.HandleGetConfig(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		configs := parseResponse(t, w)
		require.Len(t, configs, 1)
		assert.Equal(t, "test.getconfig.key1", configs[0].Key)
		assert.Equal(t, "val1", configs[0].Value)
		assert.NotEmpty(t, configs[0].Source)
		assert.Empty(t, configs[0].Error)
	})

	t.Run("get multiple keys preserves order", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/management/config/get?keys=test.getconfig.key3,test.getconfig.key1,test.getconfig.key2", nil)
		w := httptest.NewRecorder()
		coord.HandleGetConfig(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		configs := parseResponse(t, w)
		require.Len(t, configs, 3)
		assert.Equal(t, "test.getconfig.key3", configs[0].Key)
		assert.Equal(t, "val3", configs[0].Value)
		assert.Equal(t, "test.getconfig.key1", configs[1].Key)
		assert.Equal(t, "val1", configs[1].Value)
		assert.Equal(t, "test.getconfig.key2", configs[2].Key)
		assert.Equal(t, "val2", configs[2].Value)
	})

	t.Run("nonexistent key returns error field", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/management/config/get?keys=test.getconfig.nonexistent", nil)
		w := httptest.NewRecorder()
		coord.HandleGetConfig(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		configs := parseResponse(t, w)
		require.Len(t, configs, 1)
		assert.Equal(t, "test.getconfig.nonexistent", configs[0].Key)
		assert.NotEmpty(t, configs[0].Error)
		assert.Empty(t, configs[0].Value)
		assert.Empty(t, configs[0].Source)
	})

	t.Run("mix of existing and nonexistent keys", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/management/config/get?keys=test.getconfig.key1,test.getconfig.missing,test.getconfig.key2", nil)
		w := httptest.NewRecorder()
		coord.HandleGetConfig(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		configs := parseResponse(t, w)
		require.Len(t, configs, 3)
		assert.Equal(t, "test.getconfig.key1", configs[0].Key)
		assert.Equal(t, "val1", configs[0].Value)
		assert.Empty(t, configs[0].Error)
		assert.Equal(t, "test.getconfig.missing", configs[1].Key)
		assert.NotEmpty(t, configs[1].Error)
		assert.Equal(t, "test.getconfig.key2", configs[2].Key)
		assert.Equal(t, "val2", configs[2].Value)
		assert.Empty(t, configs[2].Error)
	})

	t.Run("empty keys with spaces and commas are skipped", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/management/config/get?keys=test.getconfig.key1,,+,test.getconfig.key2", nil)
		w := httptest.NewRecorder()
		coord.HandleGetConfig(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		configs := parseResponse(t, w)
		require.Len(t, configs, 2)
		assert.Equal(t, "test.getconfig.key1", configs[0].Key)
		assert.Equal(t, "test.getconfig.key2", configs[1].Key)
	})

	t.Run("missing keys parameter should fail", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/management/config/get", nil)
		w := httptest.NewRecorder()
		coord.HandleGetConfig(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "keys")
	})

	t.Run("sensitive keys are redacted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/management/config/get?keys=minio.secretAccessKey,test.getconfig.key1,etcd.auth.password,test.getconfig.opaque", nil)
		w := httptest.NewRecorder()
		coord.HandleGetConfig(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		configs := parseResponse(t, w)
		require.Len(t, configs, 4)
		assert.Equal(t, pkgconfig.RedactedValue, configs[0].Value)
		assert.Empty(t, configs[0].Error)
		assert.Equal(t, "val1", configs[1].Value)
		assert.Equal(t, pkgconfig.RedactedValue, configs[2].Value)
		assert.Empty(t, configs[2].Error)
		assert.Equal(t, pkgconfig.RedactedValue, configs[3].Value)
		assert.Empty(t, configs[3].Error)
		assert.NotContains(t, w.Body.String(), "opaque-secret")
	})

	t.Run("sensitive ParamGroup members are redacted and undeclared keys are denied", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/management/config/get?keys=credential.aksk1.secret_access_key,kafka.consumer.ssl.key.pem,pulsar.authParams,AWS_SECRET_ACCESS_KEY,function.analyzer.lindera.download_urls.ipadic,kafkaconsumersslkeypem", nil)
		w := httptest.NewRecorder()
		coord.HandleGetConfig(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		configs := parseResponse(t, w)
		require.Len(t, configs, 6)
		for _, index := range []int{0, 1, 2, 4, 5} {
			assert.Equal(t, pkgconfig.RedactedValue, configs[index].Value)
			assert.Empty(t, configs[index].Error)
		}
		// A process environment variable is not declared configuration even when
		// its name looks sensitive, so its key remains unregistered.
		assert.Contains(t, configs[3].Error, "unregistered")
		// The collapsed ParamGroup spelling at index 5 and the topology value at
		// index 4 are both covered by the redaction assertions above.
		assert.NotContains(t, w.Body.String(), "param-group-secret")
		assert.NotContains(t, w.Body.String(), "inline-private-key")
		assert.NotContains(t, w.Body.String(), "broker-secret")
		assert.NotContains(t, w.Body.String(), "environment-secret")
	})

	t.Run("unregistered keys are denied", func(t *testing.T) {
		mgr.SetConfig("test.getconfig.unknown", "unknown-secret")
		req := httptest.NewRequest(http.MethodGet, "/management/config/get?keys=test.getconfig.unknown", nil)
		w := httptest.NewRecorder()
		coord.HandleGetConfig(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		configs := parseResponse(t, w)
		require.Len(t, configs, 1)
		assert.Contains(t, configs[0].Error, "unregistered")
		assert.NotContains(t, w.Body.String(), "unknown-secret")
	})

	t.Run("all empty keys should fail", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/management/config/get?keys=,,+,", nil)
		w := httptest.NewRecorder()
		coord.HandleGetConfig(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "no valid keys")
	})

	t.Run("wrong HTTP method should fail", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/management/config/get?keys=test.getconfig.key1", nil)
		w := httptest.NewRecorder()
		coord.HandleGetConfig(w, req)

		assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
		assert.Contains(t, w.Body.String(), "Method not allowed")
	})
}
