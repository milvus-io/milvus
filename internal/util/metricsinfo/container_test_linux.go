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

package metricsinfo

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInContainer(t *testing.T) {
	_, err := inContainer()
	assert.NoError(t, err)
}

func TestGetContainerMemLimit(t *testing.T) {
	limit, err := getContainerMemLimit()
	assert.NoError(t, err)
	assert.True(t, limit > 0)
	t.Log("limit memory:", limit)
}

func TestGetContainerMemUsed(t *testing.T) {
	used, err := getContainerMemUsed()
	assert.NoError(t, err)
	assert.True(t, used > 0)
	t.Log("used memory:", used)
}
