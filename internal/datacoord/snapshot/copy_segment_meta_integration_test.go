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

package snapshot

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/datacoord/copysegment"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

// TestUpdateJobStateAndReleaseRef_UnpinsOnTerminal verifies that transitioning a
// job to Completed with PinId>0 unpins the source snapshot exactly once.
func TestUpdateJobStateAndReleaseRef_UnpinsOnTerminal(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Maybe()

	snapMeta := &SnapshotMeta{}
	unpinCalls := 0
	var unpinPinID int64
	mockUnpin := mockey.Mock((*SnapshotMeta).UnpinSnapshot).To(
		func(_ *SnapshotMeta, _ context.Context, pinID int64) (int64, string, int, error) {
			unpinCalls++
			unpinPinID = pinID
			return 0, "", 0, nil
		}).Build()
	defer mockUnpin.UnPatch()

	copyMeta := &copysegment.Meta{
		Catalog:      catalog,
		SnapshotMeta: snapMeta,
		Jobs:         map[int64]copysegment.CopySegmentJob{},
		Tasks:        copysegment.NewCopySegmentTasks(),
	}

	jobID := int64(777)
	job := &copysegment.Job{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              jobID,
			CollectionId:       1,
			SourceCollectionId: 1,
			SnapshotName:       "snap_pin",
			State:              datapb.CopySegmentJobState_CopySegmentJobExecuting,
			PinId:              42,
		},
		Tr: timerecord.NewTimeRecorder("test"),
	}
	copyMeta.Jobs[jobID] = job

	err := copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), jobID,
		copysegment.UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted))
	assert.NoError(t, err)
	assert.Equal(t, 1, unpinCalls, "UnpinSnapshot must be called exactly once on terminal transition")
	assert.Equal(t, int64(42), unpinPinID, "Unpin must be called with job.PinId")

	// Second terminal call: already terminal → must not Unpin again.
	err = copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), jobID,
		copysegment.UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted))
	assert.NoError(t, err)
	assert.Equal(t, 1, unpinCalls, "Double terminal transition must not double-unpin")
}

// TestUpdateJobStateAndReleaseRef_SkipsUnpinForLegacyJob verifies jobs persisted
// before the pin refactor (PinId=0) skip Unpin and do not panic on nil snapshotMeta.
func TestUpdateJobStateAndReleaseRef_SkipsUnpinForLegacyJob(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Maybe()

	unpinCalled := false
	mockUnpin := mockey.Mock((*SnapshotMeta).UnpinSnapshot).To(
		func(_ *SnapshotMeta, _ context.Context, _ int64) (int64, string, int, error) {
			unpinCalled = true
			return 0, "", 0, nil
		}).Build()
	defer mockUnpin.UnPatch()

	// snapshotMeta=nil is allowed because PinId==0 short-circuits before Unpin.
	copyMeta := &copysegment.Meta{
		Catalog: catalog,
		Jobs:    map[int64]copysegment.CopySegmentJob{},
		Tasks:   copysegment.NewCopySegmentTasks(),
	}

	jobID := int64(888)
	job := &copysegment.Job{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              jobID,
			CollectionId:       1,
			SourceCollectionId: 1,
			SnapshotName:       "snap_legacy",
			State:              datapb.CopySegmentJobState_CopySegmentJobExecuting,
			// PinId intentionally zero (pre-refactor job).
		},
		Tr: timerecord.NewTimeRecorder("test"),
	}
	copyMeta.Jobs[jobID] = job

	err := copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), jobID,
		copysegment.UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed))
	assert.NoError(t, err)
	assert.False(t, unpinCalled, "UnpinSnapshot must not be called for legacy job (PinId=0)")
}

// TestUpdateJobStateAndReleaseRef_UnpinErrorSwallowed verifies that when UnpinSnapshot
// returns an error the state transition is still persisted, and the caller receives nil.
// The pin is expected to self-expire via TTL — failing the state machine would double-drive it.
func TestUpdateJobStateAndReleaseRef_UnpinErrorSwallowed(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveCopySegmentJob(mock.Anything, mock.Anything).Return(nil).Maybe()

	unpinCalls := 0
	mockUnpin := mockey.Mock((*SnapshotMeta).UnpinSnapshot).To(
		func(_ *SnapshotMeta, _ context.Context, _ int64) (int64, string, int, error) {
			unpinCalls++
			return 0, "", 0, errors.New("etcd unavailable")
		}).Build()
	defer mockUnpin.UnPatch()

	copyMeta := &copysegment.Meta{
		Catalog:      catalog,
		SnapshotMeta: &SnapshotMeta{},
		Jobs:         map[int64]copysegment.CopySegmentJob{},
		Tasks:        copysegment.NewCopySegmentTasks(),
	}

	jobID := int64(999)
	copyMeta.Jobs[jobID] = &copysegment.Job{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              jobID,
			CollectionId:       1,
			SourceCollectionId: 1,
			SnapshotName:       "snap_err",
			State:              datapb.CopySegmentJobState_CopySegmentJobExecuting,
			PinId:              101,
		},
		Tr: timerecord.NewTimeRecorder("test"),
	}

	err := copyMeta.UpdateJobStateAndReleaseRef(context.TODO(), jobID,
		copysegment.UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted))

	assert.NoError(t, err, "unpin error must be swallowed — state transition already persisted")
	assert.Equal(t, 1, unpinCalls)
	// State transition happened despite unpin failure.
	assert.Equal(t, datapb.CopySegmentJobState_CopySegmentJobCompleted, copyMeta.Jobs[jobID].GetState())
}
