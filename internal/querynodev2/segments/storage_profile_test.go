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

package segments

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storageprofile"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
)

func makeStorageProfileContribution(t testing.TB, nodeID int64, scopeID string, completedBytes uint64) []byte {
	t.Helper()
	profile := storageprofile.NewProfile(storageprofile.Attribution{
		Component:     "querynode",
		WorkloadClass: storageprofile.WorkloadClassRequestPath,
		WorkloadKind:  storageprofile.WorkloadKindSearch,
		StorageRole:   storageprofile.StorageRolePersistent,
	}, time.Now())
	stats := &profile.Operations[storageprofile.StorageOperationRead]
	stats.Count = 1
	stats.Success = 1
	stats.BytesCompleted = completedBytes
	stats.Duration.Observe(time.Millisecond)
	profile.Coverage.GoStorageOperations = storageprofile.CoverageInstrumented
	profile.Coverage.StorageBytes = storageprofile.CoverageInstrumented
	payload, err := storageprofile.MarshalContribution(storageprofile.Contribution{
		Identity: storageprofile.ContributionIdentity{
			ClusterID:   "cluster",
			NodeID:      nodeID,
			ScopeID:     scopeID,
			ExecutionID: "execution",
		},
		Profile: &profile,
	})
	require.NoError(t, err)
	return payload
}

func mergedStorageProfileFromPayload(t testing.TB, payload []byte) *storageprofile.StorageProfile {
	t.Helper()
	contributions, err := storageprofile.UnmarshalContributions(payload)
	require.NoError(t, err)
	profile := storageprofile.MergeContributions(contributions)
	require.NotNil(t, profile)
	return profile
}

func TestObserveRetrieveStorageProfile(t *testing.T) {
	attribution := storageprofile.Attribution{
		Component:     "querynode",
		WorkloadClass: storageprofile.WorkloadClassRequestPath,
		WorkloadKind:  storageprofile.WorkloadKindQuery,
		Phase:         storageprofile.WorkloadPhaseReadSource,
		StorageRole:   storageprofile.StorageRolePersistent,
	}
	recorder := storageprofile.NewRecorder(attribution)
	ctx := storageprofile.WithRecorder(storageprofile.WithAttribution(context.Background(), attribution), recorder)

	observeRetrieveStorageProfile(ctx, &segcorepb.RetrieveResults{
		StorageProfile: &segcorepb.StorageProfileStats{
			ReadDurationNanos:       []uint64{1_000, 2_000},
			ReadCompletedBytes:      128,
			DroppedReadObservations: 3,
		},
	})

	profile := recorder.Snapshot()
	require.NotNil(t, profile)
	stats := profile.Operations[storageprofile.StorageOperationRead]
	assert.Equal(t, uint64(2), stats.Count)
	assert.Equal(t, uint64(128), stats.BytesCompleted)
	assert.Equal(t, uint64(3), profile.OperationBreakdownDropped)
	assert.Equal(t, storageprofile.CoveragePartial, profile.Coverage.CppStorageOperations)
	assert.Equal(t, storageprofile.CoveragePartial, profile.Coverage.StorageBytes)
}

func TestObserveRetrieveStorageProfileOmittedByOldNode(t *testing.T) {
	attribution := storageprofile.Attribution{Component: "querynode", WorkloadKind: storageprofile.WorkloadKindQuery}
	recorder := storageprofile.NewRecorder(attribution)
	ctx := storageprofile.WithRecorder(storageprofile.WithAttribution(context.Background(), attribution), recorder)

	observeRetrieveStorageProfile(ctx, &segcorepb.RetrieveResults{})

	profile := recorder.Snapshot()
	require.NotNil(t, profile)
	assert.Zero(t, profile.Operations[storageprofile.StorageOperationRead].Count)
	assert.Equal(t, storageprofile.CoverageUnavailable, profile.Coverage.CppStorageOperations)
}
