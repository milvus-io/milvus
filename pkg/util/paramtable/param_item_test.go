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

package paramtable

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/config"
)

func TestGetWithRaw_FallbackKeyCacheSuccess(t *testing.T) {
	// When primary key equals DefaultValue and a fallback key has a different value,
	// getWithRaw should return the fallback value as result but the primary key's
	// raw value for CAS, so CASCachedValue succeeds and the cache is populated.
	t.Run("primary_equals_default_fallback_exists", func(t *testing.T) {
		manager := config.NewManager()
		fallbackKey := "test.fallback.key"
		primaryKey := "test.primary.key"
		defaultVal := "100"
		fallbackVal := "200"

		// Set primary key to the default value, and fallback to a different value.
		manager.SetConfig(primaryKey, defaultVal)
		manager.SetConfig(fallbackKey, fallbackVal)

		param := &ParamItem{
			Key:          primaryKey,
			DefaultValue: defaultVal,
			FallbackKeys: []string{fallbackKey},
		}
		param.Init(manager)

		// First call: should return fallback value and cache it via CAS.
		result := param.GetAsInt()
		assert.Equal(t, 200, result)

		// Verify cache was populated (second call should hit cache).
		cached, exist := manager.GetCachedValue(primaryKey)
		assert.True(t, exist)
		assert.Equal(t, 200, cached)

		// Second call should return the same value from cache.
		result2 := param.GetAsInt()
		assert.Equal(t, 200, result2)
	})

	t.Run("primary_not_exist_fallback_exists", func(t *testing.T) {
		manager := config.NewManager()
		fallbackKey := "test.fallback.key2"
		primaryKey := "test.primary.key2"

		manager.SetConfig(fallbackKey, "300")

		param := &ParamItem{
			Key:          primaryKey,
			DefaultValue: "50",
			FallbackKeys: []string{fallbackKey},
		}
		param.Init(manager)

		result := param.GetAsInt()
		assert.Equal(t, 300, result)

		// CAS should succeed (ErrKeyNotFound branch in CASCachedValue).
		cached, exist := manager.GetCachedValue(primaryKey)
		assert.True(t, exist)
		assert.Equal(t, 300, cached)
	})

	t.Run("primary_exists_not_default_no_fallback_used", func(t *testing.T) {
		manager := config.NewManager()
		primaryKey := "test.primary.key3"
		fallbackKey := "test.fallback.key3"

		manager.SetConfig(primaryKey, "500")
		manager.SetConfig(fallbackKey, "600")

		param := &ParamItem{
			Key:          primaryKey,
			DefaultValue: "100",
			FallbackKeys: []string{fallbackKey},
		}
		param.Init(manager)

		// Primary key value != DefaultValue, so fallback is NOT used.
		result := param.GetAsInt()
		assert.Equal(t, 500, result)

		cached, exist := manager.GetCachedValue(primaryKey)
		assert.True(t, exist)
		assert.Equal(t, 500, cached)
	})

	t.Run("nothing_exists_use_default", func(t *testing.T) {
		manager := config.NewManager()

		param := &ParamItem{
			Key:          "test.primary.key4",
			DefaultValue: "42",
			FallbackKeys: []string{"test.fallback.key4"},
		}
		param.Init(manager)

		result := param.GetAsInt()
		assert.Equal(t, 42, result)
	})
}

func TestParamItemGetAsSize(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected int64
	}{
		{name: "bytes", value: "128", expected: 128},
		{name: "integer kilobytes", value: "1KB", expected: 1024},
		{name: "decimal kilobytes", value: "1.5KB", expected: 1536},
		{name: "decimal megabytes", value: "1.25MB", expected: 1280 * 1024},
		{name: "decimal gigabytes", value: "0.5GB", expected: 512 * 1024 * 1024},
		{name: "scientific notation", value: "1.5e3KB", expected: 1500 * 1024},
		{name: "truncate fractional bytes", value: "0.1KB", expected: 102},
		{name: "case insensitive unit", value: "2Mb", expected: 2 * 1024 * 1024},
		{name: "negative decimal", value: "-1.5KB", expected: -1536},
		{name: "invalid", value: "invalid", expected: 0},
		{name: "not a number", value: "NaNMB", expected: 0},
		{name: "infinity", value: "InfMB", expected: 0},
		{name: "decimal overflow", value: "1e100GB", expected: 0},
		{name: "integer multiplication overflow", value: "9223372036854775807KB", expected: 0},
		{name: "negative integer underflow", value: "-9223372036854775809", expected: 0},
		{name: "positive decimal overflow", value: "9223372036854775808.0", expected: 0},
		{name: "negative decimal underflow", value: "-9223372036854775809.0", expected: 0},
		{name: "negative decimal unit underflow", value: "-9007199254740992.1KB", expected: 0},
		{name: "max int64 bytes", value: "9223372036854775807", expected: 9223372036854775807},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := config.NewManager()
			manager.SetConfig("test.size", test.value)
			param := &ParamItem{
				Key:          "test.size",
				DefaultValue: "0",
			}
			param.Init(manager)

			assert.Equal(t, test.expected, param.GetAsSize())
		})
	}
}
