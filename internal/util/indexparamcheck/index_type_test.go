/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestIsScalarMmapIndex(t *testing.T) {
	t.Run("inverted index", func(t *testing.T) {
		assert.True(t, IsScalarMmapIndex(IndexINVERTED))
	})
}

func TestIsVectorMmapIndex(t *testing.T) {
	t.Run("vector index", func(t *testing.T) {
		assert.True(t, IsVectorMmapIndex("FLAT"))
		assert.False(t, IsVectorMmapIndex(IndexINVERTED))
	})
}

func TestValidateMmapTypeParams(t *testing.T) {
	t.Run("inverted mmap enable", func(t *testing.T) {
		err := ValidateMmapIndexParams(IndexINVERTED, map[string]string{
			common.MmapEnabledKey: "true",
		})
		assert.NoError(t, err)
	})

	t.Run("inverted mmap enable", func(t *testing.T) {
		err := ValidateMmapIndexParams(IndexINVERTED, map[string]string{})
		assert.NoError(t, err)
	})

	t.Run("invalid mmap enable value", func(t *testing.T) {
		err := ValidateMmapIndexParams(IndexINVERTED, map[string]string{
			common.MmapEnabledKey: "invalid",
		})
		assert.Error(t, err)
	})

	t.Run("invalid mmap enable type", func(t *testing.T) {
		err := ValidateMmapIndexParams("GPU_BRUTE_FORCE", map[string]string{
			common.MmapEnabledKey: "true",
		})
		assert.Error(t, err)
	})

	t.Run("stl_mmap mmap enable", func(t *testing.T) {
		err := ValidateMmapIndexParams(IndexNGRAM, map[string]string{
			common.MmapEnabledKey: "true",
		})
		assert.NoError(t, err)
	})

	t.Run("mmap.enabled rejected when MmapUserControlEnabled is false", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapUserControlEnabled.Key, "false")
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapUserControlEnabled.Key)

		params := map[string]string{
			common.MmapEnabledKey: "true",
		}
		err := ValidateMmapIndexParams(IndexINVERTED, params)
		// Error should be returned
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "mmap.enabled property is not allowed")
	})

	t.Run("mmap.enabled allowed when MmapUserControlEnabled is true", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapUserControlEnabled.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapUserControlEnabled.Key)

		params := map[string]string{
			common.MmapEnabledKey: "true",
		}
		err := ValidateMmapIndexParams(IndexINVERTED, params)
		assert.NoError(t, err)
		// Verify mmap.enabled is preserved
		_, exists := params[common.MmapEnabledKey]
		assert.True(t, exists)
	})
}
