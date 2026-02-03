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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestHandleAlterConfig(t *testing.T) {
	// Setup embedded etcd
	cfg, err := embed.ConfigFromFile("../../configs/advanced/etcd.yaml")
	require.NoError(t, err)
	cfg.Dir = fmt.Sprintf("/tmp/milvus/test_handle_alter_config_%d", time.Now().UnixNano())
	etcdServer, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	defer etcdServer.Close()
	defer os.RemoveAll(cfg.Dir)

	// Get etcd endpoint
	etcdEndpoint := cfg.AdvertiseClientUrls[0].String()

	// Setup manager
	mgr, err := config.Init(
		config.WithEtcdSource(&config.EtcdInfo{
			Endpoints:       []string{etcdEndpoint},
			KeyPrefix:       "test_config",
			RefreshInterval: 10 * time.Millisecond,
		}),
	)
	require.NoError(t, err)
	defer mgr.Close()

	// Mark some keys as immutable for testing
	mgr.ImmutableUpdate("test.immutable.key1")
	mgr.ImmutableUpdate("test.immutable.key2")
	mgr.ImmutableUpdate("test.immutable.key3")

	// Mock paramtable.GetBaseTable to return a BaseTable with our test manager
	mockGetBaseTable := func() *paramtable.BaseTable {
		bt := &paramtable.BaseTable{}
		// Use reflection or mock to inject the manager
		return bt
	}

	// Use mockey to mock GetBaseTable().Manager() to return our test manager
	mocker := mockey.Mock((*paramtable.BaseTable).Manager).To(func(*paramtable.BaseTable) *config.Manager {
		return mgr
	}).Build()
	defer mocker.UnPatch()

	mocker2 := mockey.Mock(paramtable.GetBaseTable).To(mockGetBaseTable).Build()
	defer mocker2.UnPatch()

	// Create a mock mixCoordImpl
	coord := &mixCoordImpl{}

	t.Run("single config update", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"configs": []map[string]string{
				{"key": "test.immutable.key1", "value": "value1"},
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

		// Verify the config was written
		assert.Eventually(t, func() bool {
			_, value, err := mgr.GetConfig("test.immutable.key1")
			return err == nil && value == "value1"
		}, time.Second*5, 100*time.Millisecond)
	})

	t.Run("multiple configs atomic update", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"configs": []map[string]string{
				{"key": "test.immutable.key1", "value": "atomic_value1"},
				{"key": "test.immutable.key2", "value": "atomic_value2"},
				{"key": "test.immutable.key3", "value": "atomic_value3"},
			},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()

		coord.HandleAlterConfig(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		// Verify all configs were written atomically
		assert.Eventually(t, func() bool {
			_, v1, err1 := mgr.GetConfig("test.immutable.key1")
			_, v2, err2 := mgr.GetConfig("test.immutable.key2")
			_, v3, err3 := mgr.GetConfig("test.immutable.key3")
			return err1 == nil && v1 == "atomic_value1" &&
				err2 == nil && v2 == "atomic_value2" &&
				err3 == nil && v3 == "atomic_value3"
		}, time.Second*5, 100*time.Millisecond)
	})

	t.Run("empty configs array should fail", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"configs": []map[string]string{},
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
			"configs": []map[string]string{
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

	t.Run("missing value should fail", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"configs": []map[string]string{
				{"key": "test.immutable.key1"},
			},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()

		coord.HandleAlterConfig(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "empty value")
	})

	t.Run("duplicate keys should fail", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"configs": []map[string]string{
				{"key": "test.immutable.key1", "value": "value1"},
				{"key": "test.immutable.key1", "value": "value2"},
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
		mgr.ImmutableUpdate("mq.type")
		reqBody := map[string]interface{}{
			"configs": []map[string]string{
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

	t.Run("non-immutable config should fail", func(t *testing.T) {
		reqBody := map[string]interface{}{
			"configs": []map[string]string{
				{"key": "test.non.immutable.key", "value": "value"},
			},
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/config/alter", bytes.NewReader(body))
		w := httptest.NewRecorder()

		coord.HandleAlterConfig(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "only immutable configurations can be modified")
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
