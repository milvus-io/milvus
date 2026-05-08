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
