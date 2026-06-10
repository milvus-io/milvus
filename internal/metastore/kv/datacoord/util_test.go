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

package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildExternalCollectionRefreshJobKey(t *testing.T) {
	jobID := int64(12345)
	key := buildExternalCollectionRefreshJobKey(jobID)
	assert.NotEmpty(t, key)
	assert.Contains(t, key, "12345")
	assert.True(t, len(key) > 0)
}

func TestBuildExternalCollectionRefreshJobKeyMultiple(t *testing.T) {
	testCases := []int64{1, 100, 12345, 999999999}
	for _, jobID := range testCases {
		key := buildExternalCollectionRefreshJobKey(jobID)
		assert.NotEmpty(t, key)
		assert.Contains(t, key, "external-collection-refresh-job")
	}
}

func TestBuildExternalCollectionRefreshTaskKey(t *testing.T) {
	taskID := int64(54321)
	key := buildExternalCollectionRefreshTaskKey(taskID)
	assert.NotEmpty(t, key)
	assert.Contains(t, key, "54321")
	assert.True(t, len(key) > 0)
}

func TestBuildExternalCollectionRefreshTaskKeyMultiple(t *testing.T) {
	testCases := []int64{1, 100, 54321, 999999999}
	for _, taskID := range testCases {
		key := buildExternalCollectionRefreshTaskKey(taskID)
		assert.NotEmpty(t, key)
		assert.Contains(t, key, "external-collection-refresh-task")
	}
}

func TestBuildExternalCollectionRefreshKeyConsistency(t *testing.T) {
	jobID := int64(12345)
	// Same jobID should produce same key
	key1 := buildExternalCollectionRefreshJobKey(jobID)
	key2 := buildExternalCollectionRefreshJobKey(jobID)
	assert.Equal(t, key1, key2)

	// Different jobIDs should produce different keys
	key3 := buildExternalCollectionRefreshJobKey(int64(54321))
	assert.NotEqual(t, key1, key3)
}
