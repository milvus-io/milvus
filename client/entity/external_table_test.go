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

package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

func TestRefreshExternalCollectionState_String(t *testing.T) {
	testCases := []struct {
		state    RefreshExternalCollectionState
		expected string
	}{
		{RefreshStatePending, "RefreshPending"},
		{RefreshStateInProgress, "RefreshInProgress"},
		{RefreshStateCompleted, "RefreshCompleted"},
		{RefreshStateFailed, "RefreshFailed"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.state.String())
		})
	}
}

func TestRefreshExternalCollectionState_Constants(t *testing.T) {
	// Verify constants match proto values
	assert.Equal(t, RefreshExternalCollectionState(milvuspb.RefreshExternalCollectionState_RefreshPending), RefreshStatePending)
	assert.Equal(t, RefreshExternalCollectionState(milvuspb.RefreshExternalCollectionState_RefreshInProgress), RefreshStateInProgress)
	assert.Equal(t, RefreshExternalCollectionState(milvuspb.RefreshExternalCollectionState_RefreshCompleted), RefreshStateCompleted)
	assert.Equal(t, RefreshExternalCollectionState(milvuspb.RefreshExternalCollectionState_RefreshFailed), RefreshStateFailed)
}

func TestRefreshExternalCollectionJobInfo(t *testing.T) {
	t.Run("struct_fields", func(t *testing.T) {
		info := RefreshExternalCollectionJobInfo{
			JobID:          123,
			CollectionName: "test_collection",
			State:          RefreshStateInProgress,
			Progress:       50,
			Reason:         "",
			ExternalSource: "s3://bucket/path",
			StartTime:      1000,
			EndTime:        0,
		}

		assert.Equal(t, int64(123), info.JobID)
		assert.Equal(t, "test_collection", info.CollectionName)
		assert.Equal(t, RefreshStateInProgress, info.State)
		assert.Equal(t, int64(50), info.Progress)
		assert.Equal(t, "", info.Reason)
		assert.Equal(t, "s3://bucket/path", info.ExternalSource)
		assert.Equal(t, int64(1000), info.StartTime)
		assert.Equal(t, int64(0), info.EndTime)
	})

	t.Run("failed_job", func(t *testing.T) {
		info := RefreshExternalCollectionJobInfo{
			JobID:    456,
			State:    RefreshStateFailed,
			Progress: 30,
			Reason:   "connection timeout",
			EndTime:  2000,
		}

		assert.Equal(t, RefreshStateFailed, info.State)
		assert.Equal(t, "connection timeout", info.Reason)
		assert.Greater(t, info.EndTime, int64(0))
	})
}
