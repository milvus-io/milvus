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
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestExternalRefreshPublicationRetry(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	job := &datapb.ExternalCollectionRefreshJob{
		JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress,
		TaskIds: []int64{1001}, StartTime: time.Now().Add(-24 * time.Hour).UnixMilli(),
		TransformStartAfterTimeticks: map[string]uint64{"p_1_v100": 500},
	}
	task := &datapb.ExternalCollectionRefreshTask{
		TaskId: 1001, JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFinished, Progress: 100,
	}
	catalog := &stubCatalog{jobs: []*datapb.ExternalCollectionRefreshJob{job}, tasks: []*datapb.ExternalCollectionRefreshTask{task}}
	save := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).To(func(_ *stubCatalog, _ context.Context, current *datapb.ExternalCollectionRefreshJob) error {
		catalog.jobs = []*datapb.ExternalCollectionRefreshJob{proto.Clone(current).(*datapb.ExternalCollectionRefreshJob)}
		return nil
	}).Build()
	defer save.UnPatch()
	mt, err := newExternalCollectionRefreshMeta(ctx, catalog)
	require.NoError(t, err)
	applies, publishes := 0, 0
	publicationErr := error(merr.ErrServiceNotReady)
	checker := newRefreshChecker(ctx, nil, mt, make(chan struct{}), nil, func(context.Context, *datapb.ExternalCollectionRefreshJob) error {
		applies++
		return nil
	}, nil, nil, nil)
	checker.publishJobInfo = func(context.Context, *datapb.ExternalCollectionRefreshJob) error {
		publishes++
		return publicationErr
	}
	checker.aggregateJobState(job)
	pending := mt.GetJob(1)
	require.True(t, pending.GetSegmentsApplied())
	require.Equal(t, indexpb.JobState_JobStateInProgress, pending.GetState())
	require.Less(t, pending.GetProgress(), int64(100))
	require.Equal(t, 1, applies)
	checker.tryTimeoutJob(job) // stale snapshot cannot fail committed publication
	require.Equal(t, indexpb.JobState_JobStateInProgress, mt.GetJob(1).GetState())
	require.NoError(t, mt.UpdateJobProgress(1, 100))
	require.Less(t, mt.GetJob(1).GetProgress(), int64(100))

	// Recovery must reuse the persisted cursor/marker and never reapply results.
	recovered, err := newExternalCollectionRefreshMeta(ctx, catalog)
	require.NoError(t, err)
	checker.refreshMeta = recovered
	checker.aggregateJobState(recovered.GetJob(1))
	require.Equal(t, 1, applies)
	require.Equal(t, 2, publishes)
	require.Equal(t, uint64(500), recovered.GetJob(1).GetTransformStartAfterTimeticks()["p_1_v100"])
	publicationErr = nil
	checker.aggregateJobState(recovered.GetJob(1))
	require.Equal(t, 1, applies)
	require.Equal(t, indexpb.JobState_JobStateFinished, recovered.GetJob(1).GetState())
	require.Equal(t, int64(100), recovered.GetJob(1).GetProgress())
	checker.aggregateJobState(recovered.GetJob(1))
	require.Equal(t, 3, publishes)
}

func TestExternalRefreshPublishWaitsForSchemaAndViews(t *testing.T) {
	ctx := context.Background()
	job := &datapb.ExternalCollectionRefreshJob{JobId: 1, CollectionId: 100, ExternalSource: "new", ExternalSpec: "spec"}
	mt := &meta{collections: newTestCollections(100), segments: NewSegmentsInfo()}
	mt.collections.Insert(100, &collectionInfo{ID: 100, Schema: &schemapb.CollectionSchema{ExternalSource: "old"}})
	mt.segments.SetSegment(10, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 10, CollectionID: 100}})
	manager := &externalCollectionRefreshManager{mt: mt, checker: &externalCollectionRefreshChecker{mt: mt}}
	notify := mockey.Mock((*externalCollectionRefreshManager).handleJobFinished).Return().Build()
	defer notify.UnPatch()
	r := &resource.Resources{}
	get := mockey.Mock(resource.Resource).Return(r).Build()
	defer get.UnPatch()
	refreshErr := error(merr.ErrServiceNotReady)
	calls := 0
	refresh := mockey.Mock((*resource.Resources).Refresh).To(func(_ *resource.Resources, _ context.Context, collectionID int64) error {
		require.Equal(t, int64(100), collectionID)
		calls++
		return refreshErr
	}).Build()
	defer refresh.UnPatch()
	require.ErrorIs(t, manager.publishRefreshJob(ctx, job), merr.ErrServiceNotReady)
	require.Zero(t, calls)
	mt.collections.Insert(100, &collectionInfo{ID: 100, Schema: &schemapb.CollectionSchema{ExternalSource: "new", ExternalSpec: "spec"}})
	require.ErrorIs(t, manager.publishRefreshJob(ctx, job), merr.ErrServiceNotReady)
	refreshErr = nil
	require.NoError(t, manager.publishRefreshJob(ctx, job))
	require.Equal(t, 2, calls)
	get.UnPatch()
	missing := mockey.Mock(resource.Resource).Return(nil).Build()
	defer missing.UnPatch()
	require.ErrorIs(t, manager.publishRefreshJob(ctx, job), merr.ErrServiceNotReady)
}

func TestExternalRefreshPublicationIndexWait(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	require.NoError(t, pt.Save(pt.DataCoordCfg.RefreshWaitForIndex.Key, "true"))
	defer pt.Reset(pt.DataCoordCfg.RefreshWaitForIndex.Key)
	job := &datapb.ExternalCollectionRefreshJob{
		JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress,
		TransformStartAfterTimeticks: map[string]uint64{"v1": 50},
	}
	task := &datapb.ExternalCollectionRefreshTask{TaskId: 1001, JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFinished}
	mt := &meta{indexMeta: &indexMeta{}}
	meta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{job}, []*datapb.ExternalCollectionRefreshTask{task})
	applies := 0
	checker := newRefreshChecker(t.Context(), mt, meta, make(chan struct{}), nil, func(context.Context, *datapb.ExternalCollectionRefreshJob) error {
		applies++
		return nil
	}, nil, nil, nil)
	checker.publishJobInfo = func(context.Context, *datapb.ExternalCollectionRefreshJob) error { return nil }
	indexed := false
	wait := mockey.Mock((*externalCollectionRefreshChecker).indexWaitDone).To(func(_ *externalCollectionRefreshChecker, current *datapb.ExternalCollectionRefreshJob) bool {
		require.True(t, current.GetSegmentsApplied())
		require.NotZero(t, current.GetIndexWaitStartedTime())
		return indexed
	}).Build()
	defer wait.UnPatch()
	checker.aggregateJobState(job)
	require.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetJob(1).GetState())
	checker.aggregateJobState(meta.GetJob(1))
	require.Equal(t, 1, applies)
	indexed = true
	checker.aggregateJobState(meta.GetJob(1))
	require.Equal(t, indexpb.JobState_JobStateFinished, meta.GetJob(1).GetState())
	require.Equal(t, 1, applies)
}

func TestExternalRefreshPublicationApplyFailure(t *testing.T) {
	paramtable.Init()
	job := &datapb.ExternalCollectionRefreshJob{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress}
	meta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{job}, nil)
	checker := newRefreshChecker(t.Context(), nil, meta, make(chan struct{}), nil,
		func(context.Context, *datapb.ExternalCollectionRefreshJob) error { return merr.ErrDataIntegrity }, nil, nil, nil)
	checker.publishJobInfo = func(context.Context, *datapb.ExternalCollectionRefreshJob) error {
		t.Fatal("failed segment apply must not publish")
		return nil
	}
	checker.finishPublishedJob(job)
	require.Equal(t, indexpb.JobState_JobStateFailed, meta.GetJob(1).GetState())
	require.False(t, meta.GetJob(1).GetSegmentsApplied())
	checker.finishPublishedJob(job) // terminal-state guard also covers a stale eager pass
}

func TestExternalRefreshBroadcastFences(t *testing.T) {
	mockey.PatchConvey("all target channels and errors", t, func() {
		api := &embeddedBroadcastAPI{}
		mockey.Mock((*Server).startBroadcastWithCollectionID).Return(api, nil).Build()
		mockey.Mock((*embeddedBroadcastAPI).Close).Return().Build()
		mockey.Mock((*externalCollectionRefreshManager).GetActiveJobByCollectionID).Return(nil).Build()
		mockey.Mock((*ServerHandler).GetCollection).Return(&collectionInfo{VChannelNames: []string{"v1", "v2"}}, nil).Build()
		mockey.Mock((*stubAllocator).AllocID).Return(int64(42), nil).Build()
		broadcastErr := error(nil)
		calls := 0
		mockey.Mock((*embeddedBroadcastAPI).Broadcast).To(func(_ *embeddedBroadcastAPI, _ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			calls++
			require.ElementsMatch(t, []string{"v1", "v2"}, msg.BroadcastHeader().VChannels)
			require.True(t, msg.BroadcastHeader().AckSyncUp)
			return &types.BroadcastAppendResult{}, broadcastErr
		}).Build()
		server := &Server{handler: &ServerHandler{}, allocator: &stubAllocator{}, externalCollectionRefreshManager: &externalCollectionRefreshManager{}}
		server.stateCode.Store(commonpb.StateCode_Healthy)
		request := &datapb.RefreshExternalCollectionRequest{CollectionId: 100}
		response, err := server.RefreshExternalCollection(t.Context(), request)
		require.NoError(t, err)
		require.NoError(t, merr.Error(response.GetStatus()))
		require.Equal(t, int64(42), response.GetJobId())
		broadcastErr = merr.ErrServiceUnavailable
		response, err = server.RefreshExternalCollection(t.Context(), request)
		require.NoError(t, err)
		require.ErrorIs(t, merr.Error(response.GetStatus()), merr.ErrServiceUnavailable)
		require.Equal(t, 2, calls)
	})
}
