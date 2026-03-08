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

//go:build linux
// +build linux

package hardware

import (
	"os"
	"testing"

	"github.com/containerd/cgroups/v3"
	"github.com/stretchr/testify/assert"
)

func TestGetCgroupStats(t *testing.T) {
	if cgroups.Mode() == cgroups.Unified {
		stats2, err := getCgroupV2Stats()
		assert.NoError(t, err)
		assert.NotNil(t, stats2)

		stats1, err := getCgroupV1Stats()
		assert.Error(t, err)
		assert.Nil(t, stats1)
	} else {
		stats1, err := getCgroupV1Stats()
		assert.NoError(t, err)
		assert.NotNil(t, stats1)

		stats2, err := getCgroupV2Stats()
		assert.Error(t, err)
		assert.Nil(t, stats2)
	}
}

func TestGetContainerMemLimit(t *testing.T) {
	limit, err := getContainerMemLimit()
	assert.NoError(t, err)
	assert.True(t, limit > 0)
	t.Log("limit memory:", limit)

	err = os.Setenv("MEM_LIMIT", "5Gi")
	assert.NoError(t, err)
	defer func() {
		_ = os.Unsetenv("MEM_LIMIT")
		assert.Equal(t, "", os.Getenv("MEM_LIMIT"))
	}()

	limit, err = getContainerMemLimit()
	assert.NoError(t, err)
	assert.Equal(t, limit, 5*1024*1024*1024)
}

func TestGetContainerMemUsed(t *testing.T) {
	used, err := getContainerMemUsed()
	assert.NoError(t, err)
	assert.True(t, used > 0)
	t.Log("used memory:", used)
}
