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

func TestHandleGetConfig(t *testing.T) {
	paramtable.Init()
	mgr := paramtable.GetBaseTable().Manager()

	coord := &mixCoordImpl{}

	// Seed configs directly via Manager.SetConfig (no etcd needed).
	mgr.SetConfig("test.getconfig.key1", "val1")
	mgr.SetConfig("test.getconfig.key2", "val2")
	mgr.SetConfig("test.getconfig.key3", "val3")

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
		req := httptest.NewRequest(http.MethodGet, "/management/config/get?keys=minio.secretAccessKey,test.getconfig.key1,etcd.auth.password", nil)
		w := httptest.NewRecorder()
		coord.HandleGetConfig(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		configs := parseResponse(t, w)
		require.Len(t, configs, 3)
		assert.Contains(t, configs[0].Error, "sensitive")
		assert.Equal(t, "val1", configs[1].Value)
		assert.Contains(t, configs[2].Error, "sensitive")
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
