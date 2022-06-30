// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package metricsinfo

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"
)

func TestPurgeMemory(t *testing.T) {
	usedMem := metricsinfo.GetUsedMemoryCount()
	assert.True(t, usedMem > 0)
	maxBinsSize := uint64(float64(usedMem) * 0.2)
	_, err := PurgeMemory(maxBinsSize)
	assert.NoError(t, err)

	// do not malloc_trim
	res, err := PurgeMemory(math.MaxUint64)
	assert.NoError(t, err)
	assert.False(t, res)

	// do malloc_trim
	res, err = PurgeMemory(0)
	assert.NoError(t, err)
	assert.True(t, res)
}
