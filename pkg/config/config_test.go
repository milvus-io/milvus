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

package config

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

func TestConfigFromEnv(t *testing.T) {
	mgr, _ := Init()
	_, _, err := mgr.GetConfig("test.env")
	assert.ErrorIs(t, err, ErrKeyNotFound)

	t.Setenv("TEST_ENV", "value")
	mgr, _ = Init(WithEnvSource(formatKey))

	_, v, err := mgr.GetConfig("test.env")
	assert.NoError(t, err)
	assert.Equal(t, "value", v)

	_, v, err = mgr.GetConfig("TEST_ENV")
	assert.NoError(t, err)
	assert.Equal(t, "value", v)
}

func TestComplexArrayAndStruct(t *testing.T) {
	// Create a temporary YAML file with complex array structures
	yamlContent := `
test:
  simpleArray:
    - item1
    - item2
    - item3
  complexArray:
    - name: region1
      seeds:
        - n1
        - n2
        - n3
    - name: region2
      seeds:
        - n4
        - n5
        - n6
  nestedConfig:
    placement:
      - name: replica-1
        region: default-region-pool
        az: az-1
        resourceGroup: rg.*
      - name: replica-2
        region: default-region-pool
        az: az-2
        resourceGroup: rg.*
  scalarValue: 10000
`
	// Create temporary file
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test_complex_array_and_struct_config.yaml")
	err := os.WriteFile(tmpFile, []byte(yamlContent), 0o600)
	assert.NoError(t, err)

	// Initialize manager with temporary file
	mgr, err := Init(WithFilesSource(&FileInfo{[]string{tmpFile}, -1}))
	assert.NoError(t, err)

	t.Run("test complex array serialization", func(t *testing.T) {
		// Test complexArray with structs containing nested arrays
		_, v, err := mgr.GetConfig("test.complexArray")
		assert.NoError(t, err)
		// Should be a JSON string
		expectedComplexArray := `[{"name":"region1","seeds":["n1","n2","n3"]},{"name":"region2","seeds":["n4","n5","n6"]}]`
		assert.JSONEq(t, expectedComplexArray, v)

		// Test placement array with structs
		_, v, err = mgr.GetConfig("test.nestedConfig.placement")
		assert.NoError(t, err)
		// Should be a JSON string
		expectedPlacementArray := `[{"az":"az-1","name":"replica-1","region":"default-region-pool","resourceGroup":"rg.*"},{"az":"az-2","name":"replica-2","region":"default-region-pool","resourceGroup":"rg.*"}]`
		assert.JSONEq(t, expectedPlacementArray, v)
	})

	t.Run("test simple array serialization", func(t *testing.T) {
		// Test that simple arrays still work with comma-separated values
		_, v, err := mgr.GetConfig("test.simpleArray")
		assert.NoError(t, err)
		assert.Equal(t, "item1,item2,item3", v)

		// Test scalar values still work
		_, v, err = mgr.GetConfig("test.scalarValue")
		assert.NoError(t, err)
		assert.Equal(t, "10000", v)
	})
}

func TestComplexArrayAndMixStruct(t *testing.T) {
	testCases := []struct {
		name           string
		yamlContent    string
		configKey      string
		expectedJSON   string // Expected JSON string if correctly detected as complex array
		expectedSimple string // Expected comma-separated string if incorrectly treated as simple array
		shouldBeJSON   bool   // Whether it should be serialized as JSON (complex) or comma-separated (simple)
	}{
		{
			name: "mixed array with simple first element",
			yamlContent: `
test:
  mixedArray1:
    - 1
    - key: value
      name: test
`,
			configKey:      "test.mixedArray1",
			expectedJSON:   `[1,{"key":"value","name":"test"}]`,
			expectedSimple: "1", // Bug: only first element would be serialized
			shouldBeJSON:   true,
		},
		{
			name: "mixed array with nil first element",
			yamlContent: `
test:
  mixedArray2:
    - null
    - key: value
      name: test
`,
			configKey:      "test.mixedArray2",
			expectedJSON:   `[null,{"key":"value","name":"test"}]`,
			expectedSimple: "", // Bug: nil would be skipped or cause issues
			shouldBeJSON:   true,
		},
		{
			name: "mixed array with string first element",
			yamlContent: `
test:
  mixedArray3:
    - "simple"
    - key: value
      nested:
        field: data
`,
			configKey:      "test.mixedArray3",
			expectedJSON:   `["simple",{"key":"value","nested":{"field":"data"}}]`,
			expectedSimple: "simple", // Bug: only first element would be serialized
			shouldBeJSON:   true,
		},
		{
			name: "array with complex first element (should work correctly)",
			yamlContent: `
test:
  mixedArray4:
    - key: value
      name: test
    - 1
    - "string"
`,
			configKey:      "test.mixedArray4",
			expectedJSON:   `[{"key":"value","name":"test"},1,"string"]`,
			expectedSimple: "", // This should work correctly (first element is complex)
			shouldBeJSON:   true,
		},
		{
			name: "array with multiple complex elements after simple ones",
			yamlContent: `
test:
  mixedArray5:
    - 1
    - 2
    - key: value1
    - key: value2
`,
			configKey:      "test.mixedArray5",
			expectedJSON:   `[1,2,{"key":"value1"},{"key":"value2"}]`,
			expectedSimple: "1,2", // Bug: only simple elements would be serialized
			shouldBeJSON:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create temporary file
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "test_mixed_array_config.yaml")
			err := os.WriteFile(tmpFile, []byte(tc.yamlContent), 0o600)
			assert.NoError(t, err)

			// Initialize manager with temporary file
			mgr, err := Init(WithFilesSource(&FileInfo{[]string{tmpFile}, -1}))
			assert.NoError(t, err)

			// Get config value
			_, v, err := mgr.GetConfig(tc.configKey)
			assert.NoError(t, err)

			if tc.shouldBeJSON {
				// Should be serialized as JSON (complex array)
				// The bug would cause it to be serialized as simple array (comma-separated)
				// So we check that it's NOT the simple format, and is valid JSON
				assert.NotEqual(t, tc.expectedSimple, v,
					"Bug detected: Array was incorrectly serialized as simple array instead of JSON. "+
						"Expected JSON format but got: %s", v)

				// Verify it's valid JSON by trying to unmarshal
				var result []interface{}
				err = json.Unmarshal([]byte(v), &result)
				assert.NoError(t, err, "Value should be valid JSON, but got: %s", v)

				// Verify it contains the complex element
				hasComplexElement := false
				for _, item := range result {
					switch item.(type) {
					case map[string]interface{}:
						hasComplexElement = true
					}
				}
				assert.True(t, hasComplexElement,
					"JSON should contain at least one complex element (map), but got: %s", v)

				// If we have the expected JSON, verify it matches
				if tc.expectedJSON != "" {
					// Normalize both JSON strings for comparison (handle key ordering)
					var expectedParsed, actualParsed interface{}
					err1 := json.Unmarshal([]byte(tc.expectedJSON), &expectedParsed)
					err2 := json.Unmarshal([]byte(v), &actualParsed)
					if err1 == nil && err2 == nil {
						assert.Equal(t, expectedParsed, actualParsed,
							"JSON content should match expected value")
					}
				}
			} else {
				// Should be serialized as simple array (comma-separated)
				assert.JSONEq(t, tc.expectedSimple, v)
			}
		})
	}
}

func TestConfigFromRemote(t *testing.T) {
	cfg, _ := embed.ConfigFromFile("../../configs/advanced/etcd.yaml")
	cfg.Dir = "/tmp/milvus/test"
	e, err := embed.StartEtcd(cfg)
	assert.NoError(t, err)
	defer e.Close()
	defer os.RemoveAll(cfg.Dir)

	client := v3client.New(e.Server)

	t.Setenv("TMP_KEY", "1")
	t.Setenv("log.level", "info")
	mgr, _ := Init(WithEnvSource(formatKey),
		WithFilesSource(&FileInfo{[]string{"../../configs/milvus.yaml"}, -1}),
		WithEtcdSource(&EtcdInfo{
			Endpoints:       []string{cfg.AdvertiseClientUrls[0].Host},
			KeyPrefix:       "test",
			RefreshInterval: 10 * time.Millisecond,
		}))
	ctx := context.Background()

	t.Run("origin is empty", func(t *testing.T) {
		_, _, err = mgr.GetConfig("test.etcd")
		assert.ErrorIs(t, err, ErrKeyNotFound)

		client.KV.Put(ctx, "test/config/test/etcd", "value")

		time.Sleep(100 * time.Millisecond)

		_, v, err := mgr.GetConfig("test.etcd")
		assert.NoError(t, err)
		assert.Equal(t, "value", v)
		_, v, err = mgr.GetConfig("TEST_ETCD")
		assert.NoError(t, err)
		assert.Equal(t, "value", v)

		client.KV.Delete(ctx, "test/config/test/etcd")
		time.Sleep(100 * time.Millisecond)

		_, _, err = mgr.GetConfig("TEST_ETCD")
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("override origin value", func(t *testing.T) {
		_, v, _ := mgr.GetConfig("tmp.key")
		assert.Equal(t, "1", v)
		client.KV.Put(ctx, "test/config/tmp/key", "2")

		time.Sleep(100 * time.Millisecond)

		_, v, _ = mgr.GetConfig("tmp.key")
		assert.Equal(t, "2", v)

		client.KV.Put(ctx, "test/config/tmp/key", "3")

		time.Sleep(100 * time.Millisecond)

		_, v, _ = mgr.GetConfig("tmp.key")
		assert.Equal(t, "3", v)

		client.KV.Delete(ctx, "test/config/tmp/key")
		time.Sleep(100 * time.Millisecond)

		_, v, _ = mgr.GetConfig("tmp.key")
		assert.Equal(t, "1", v)
	})

	t.Run("multi priority", func(t *testing.T) {
		_, v, _ := mgr.GetConfig("log.level")
		assert.Equal(t, "info", v)
		client.KV.Put(ctx, "test/config/log/level", "error")

		time.Sleep(100 * time.Millisecond)

		_, v, _ = mgr.GetConfig("log.level")
		assert.Equal(t, "error", v)

		client.KV.Delete(ctx, "test/config/log/level")
		time.Sleep(100 * time.Millisecond)

		_, v, _ = mgr.GetConfig("log.level")
		assert.Equal(t, "info", v)
	})

	t.Run("close manager", func(t *testing.T) {
		mgr.Close()

		client.KV.Put(ctx, "test/config/test/etcd", "value2")
		assert.Eventually(t, func() bool {
			_, _, err = mgr.GetConfig("test.etcd")
			return err != nil && errors.Is(err, ErrKeyNotFound)
		}, 300*time.Millisecond, 10*time.Millisecond)
	})
}
