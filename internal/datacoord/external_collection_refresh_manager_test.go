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
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	dcTask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ==================== Helper Functions for Manager Tests ====================

func createTestRefreshMeta(t *testing.T) *externalCollectionRefreshMeta {
	catalog := &stubCatalog{}
	meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)
	return meta
}

func createTestRefreshMetaWithJobs(t *testing.T, jobs []*datapb.ExternalCollectionRefreshJob, tasks []*datapb.ExternalCollectionRefreshTask) *externalCollectionRefreshMeta {
	jobsByID := make(map[int64]*datapb.ExternalCollectionRefreshJob, len(jobs))
	for _, job := range jobs {
		jobsByID[job.GetJobId()] = job
	}
	for _, task := range tasks {
		if job := jobsByID[task.GetJobId()]; job != nil {
			job.TaskIds = append(job.TaskIds, task.GetTaskId())
		}
	}
	catalog := &stubCatalog{
		jobs:  jobs,
		tasks: tasks,
	}
	meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)
	return meta
}

func TestExternalCollectionRefreshManager_DropJobTasks(t *testing.T) {
	t.Run("uses worker assignment persisted before Finalize acquires the task lock", func(t *testing.T) {
		refreshMeta := createTestRefreshMetaWithJobs(t,
			[]*datapb.ExternalCollectionRefreshJob{{JobId: 1, CollectionId: 100}},
			[]*datapb.ExternalCollectionRefreshTask{{
				TaskId: 1001, JobId: 1, CollectionId: 100, NodeId: 0,
			}})
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().DropRefreshExternalCollectionTask(int64(11), int64(1001)).
			Return(nil).Once()
		scheduler := dcTask.NewMockGlobalScheduler(t)
		scheduler.EXPECT().Finalize(int64(1001), mock.Anything).
			Run(func(_ int64, fn func()) {
				require.NoError(t, refreshMeta.StartTaskAttempt(1001, 11, nil))
				fn()
			}).Return().Once()
		manager := &externalCollectionRefreshManager{
			refreshMeta: refreshMeta,
			cluster:     cluster,
			scheduler:   scheduler,
		}

		require.NoError(t, manager.dropJobTasks(1))
	})

	t.Run("transient failure keeps the cleanup anchor", func(t *testing.T) {
		refreshMeta := createTestRefreshMetaWithJobs(t,
			[]*datapb.ExternalCollectionRefreshJob{{JobId: 1, CollectionId: 100}},
			[]*datapb.ExternalCollectionRefreshTask{{
				TaskId: 1001, JobId: 1, CollectionId: 100, NodeId: 11,
			}})
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().DropRefreshExternalCollectionTask(int64(11), int64(1001)).
			Return(merr.WrapErrServiceUnavailableMsg("worker temporarily unavailable")).Once()
		scheduler := dcTask.NewMockGlobalScheduler(t)
		scheduler.EXPECT().Finalize(int64(1001), mock.Anything).
			Run(func(_ int64, fn func()) { fn() }).Return().Once()
		manager := &externalCollectionRefreshManager{
			refreshMeta: refreshMeta,
			cluster:     cluster,
			scheduler:   scheduler,
		}

		require.Error(t, manager.dropJobTasks(1))
		assert.NotNil(t, refreshMeta.GetJob(1))
		assert.NotNil(t, refreshMeta.GetTask(1001))
	})

	t.Run("node not found and unassigned tasks are complete", func(t *testing.T) {
		refreshMeta := createTestRefreshMetaWithJobs(t,
			[]*datapb.ExternalCollectionRefreshJob{{JobId: 1, CollectionId: 100}},
			[]*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, CollectionId: 100, NodeId: 11},
				{TaskId: 1002, JobId: 1, CollectionId: 100, NodeId: 0},
			})
		cluster := session.NewMockCluster(t)
		cluster.EXPECT().DropRefreshExternalCollectionTask(int64(11), int64(1001)).
			Return(merr.WrapErrNodeNotFound(11)).Once()
		scheduler := dcTask.NewMockGlobalScheduler(t)
		scheduler.EXPECT().Finalize(int64(1001), mock.Anything).
			Run(func(_ int64, fn func()) { fn() }).Return().Once()
		scheduler.EXPECT().Finalize(int64(1002), mock.Anything).
			Run(func(_ int64, fn func()) { fn() }).Return().Once()
		manager := &externalCollectionRefreshManager{
			refreshMeta: refreshMeta,
			cluster:     cluster,
			scheduler:   scheduler,
		}

		require.NoError(t, manager.dropJobTasks(1))
	})
}

func TestCreateTasksForJobPreservesInputErrors(t *testing.T) {
	mgr := &externalCollectionRefreshManager{}

	tests := []struct {
		name   string
		source string
		spec   string
	}{
		{name: "invalid source", source: "not-a-uri", spec: `{"format":"parquet"}`},
		{name: "invalid spec", source: "s3://bucket/path", spec: "not-json"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := mgr.createTasksForJob(context.Background(), &datapb.ExternalCollectionRefreshJob{
				JobId:          1,
				ExternalSource: test.source,
				ExternalSpec:   test.spec,
			})
			require.Error(t, err)
			assert.Equal(t, merr.InputError, merr.GetErrorType(err))
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}
}

func publishManagerTestTasks(
	t *testing.T,
	refreshMeta *externalCollectionRefreshMeta,
	jobID int64,
	collectionID int64,
	taskIDs ...int64,
) {
	assert.NoError(t, refreshMeta.AddJob(&datapb.ExternalCollectionRefreshJob{
		JobId:        jobID,
		CollectionId: collectionID,
		TaskIds:      taskIDs,
	}))
}

func addManagerOwnershipTask(
	t *testing.T,
	refreshMeta *externalCollectionRefreshMeta,
	task *datapb.ExternalCollectionRefreshTask,
	ownedSegmentIDs ...int64,
) {
	t.Helper()
	task = proto.Clone(task).(*datapb.ExternalCollectionRefreshTask)
	state := task.GetState()
	failReason := task.GetFailReason()
	resultReady := task.GetResultReady()
	keptSegments := append([]int64(nil), task.GetKeptSegments()...)
	updatedSegments := cloneProtoSegments(task.GetUpdatedSegments())

	task.OwnershipPlanVersion = externalRefreshOwnershipPlanVersion
	task.OwnedSegmentIds = append([]int64(nil), ownedSegmentIDs...)
	task.KeptSegments = nil
	task.UpdatedSegments = nil
	task.ResultReady = false
	task.ResultStorageVersion = 0
	task.ResultPath = ""
	task.ResultChecksum = nil
	if resultReady {
		if refreshMeta.resultStore == nil {
			resultStore, _ := createMetaTestRefreshResultStore(t)
			refreshMeta.resultStore = resultStore
		}
		task.State = indexpb.JobState_JobStateInProgress
		task.FailReason = ""
		task.Progress = 0
	}

	require.NoError(t, refreshMeta.AddTask(task))
	if resultReady {
		require.NoError(t, refreshMeta.UpdateTaskResult(
			task.GetTaskId(),
			state,
			failReason,
			keptSegments,
			updatedSegments,
		))
	}
}

func testCollectionGetter(collections *typeutil.ConcurrentMap[UniqueID, *collectionInfo]) func(ctx context.Context, collectionID int64) (*collectionInfo, error) {
	return func(_ context.Context, collectionID int64) (*collectionInfo, error) {
		coll, ok := collections.Get(collectionID)
		if !ok {
			return nil, errors.New("collection not found")
		}
		return coll, nil
	}
}

func testMilvusTableRefreshSchema(externalSource bool) *schemapb.CollectionSchema {
	field := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         "pk",
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
	}
	if externalSource {
		field.ExternalField = "pk"
	}
	return &schemapb.CollectionSchema{
		Name:   "test_collection",
		Fields: []*schemapb.FieldSchema{field},
	}
}

func testMilvusTableTargetRefreshSchema() *schemapb.CollectionSchema {
	schema := testMilvusTableRefreshSchema(true)
	schema.ExternalSource = "s3://bucket/snapshots/1/metadata/10.json"
	schema.ExternalSpec = `{"format":"milvus-table","extfs":{"cloud_provider":"aws","region":"us-west-2","access_key_id":"ak","access_key_value":"sk"}}`
	return schema
}

func TestSubmitRefreshJobWithIDStoresJobMetadata(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	schema := &schemapb.CollectionSchema{
		Name:           "ext",
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   `{"format":"parquet"}`,
		Version:        7,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", ExternalField: "id"},
		},
	}
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	mt := &meta{segments: NewSegmentsInfo()}
	collections.Insert(collectionID, &collectionInfo{
		ID:            collectionID,
		Schema:        schema,
		VChannelNames: []string{"by-dev-rootcoord-dml_0_v1"},
		Partitions:    []int64{1},
	})
	refreshMeta := createTestRefreshMetaWithJobs(t, nil, nil)
	mgr := NewExternalCollectionRefreshManager(
		ctx, mt, newStubScheduler(), &stubAllocator{nextID: 2000},
		refreshMeta, nil, testCollectionGetter(collections), nil, nil)

	mockExplore := mockey.Mock((*externalCollectionRefreshManager).exploreExternalFiles).
		Return([]*datapb.ExternalFileInfo{{FilePath: "s3://bucket/path/a.parquet", NumRows: 10}}, "manifest-path", nil).
		Build()
	defer mockExplore.UnPatch()

	jobID, err := mgr.SubmitRefreshJobWithID(
		ctx, 1001, collectionID, "ext", "", "")
	assert.NoError(t, err)
	assert.Equal(t, int64(1001), jobID)

	job := refreshMeta.GetJob(1001)
	assert.NotNil(t, job)
	assert.Equal(t, collectionID, job.GetCollectionId())
	assert.Equal(t, "ext", job.GetCollectionName())
	assert.Equal(t, "s3://bucket/path", job.GetExternalSource())
	assert.Equal(t, `{"format":"parquet"}`, job.GetExternalSpec())

	mgr.Stop()
}

func TestCreateTasksForJob_PersistedOwnershipDrivesWorkerRequest(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ExternalCollectionFilesPerTask.Key, "2")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ExternalCollectionFilesPerTask.Key)

	collectionID := int64(100)
	schema := &schemapb.CollectionSchema{
		Name:           "ext",
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   `{"format":"parquet"}`,
		Version:        9,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", ExternalField: "id"},
		},
	}
	segments := NewSegmentsInfo()
	segments.SetSegment(10, NewSegmentInfo(&datapb.SegmentInfo{
		ID:           10,
		CollectionID: collectionID,
		State:        commonpb.SegmentState_Flushed,
		ManifestPath: "manifest-10",
	}))
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	mt := &meta{segments: segments}
	collections.Insert(collectionID, &collectionInfo{
		ID:            collectionID,
		Schema:        schema,
		VChannelNames: []string{"by-dev-rootcoord-dml_0_v1"},
		Partitions:    []int64{1},
	})
	refreshMeta := createTestRefreshMetaWithJobs(t, nil, nil)
	mgr := NewExternalCollectionRefreshManager(
		ctx, mt, newStubScheduler(), &stubAllocator{nextID: 2000},
		refreshMeta, nil, testCollectionGetter(collections), nil, nil).(*externalCollectionRefreshManager)

	mockExplore := mockey.Mock((*externalCollectionRefreshManager).exploreExternalFiles).
		Return([]*datapb.ExternalFileInfo{
			{FilePath: "f0.parquet", NumRows: 10},
			{FilePath: "f1.parquet", NumRows: 10},
			{FilePath: "f2.parquet", NumRows: 10},
			{FilePath: "f3.parquet", NumRows: 10},
		}, "manifest-path", nil).
		Build()
	defer mockExplore.UnPatch()
	mockReadFragments := mockey.Mock(packed.ReadFragmentsFromManifest).
		To(func(manifestPath string, _ *indexpb.StorageConfig, _ []string) ([]packed.Fragment, error) {
			switch manifestPath {
			case "manifest-10":
				return []packed.Fragment{{FilePath: "f1.parquet"}, {FilePath: "f2.parquet"}}, nil
			default:
				return nil, errors.New("unexpected manifest path")
			}
		}).Build()
	defer mockReadFragments.UnPatch()

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1001,
		CollectionId:   collectionID,
		CollectionName: "ext",
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   `{"format":"parquet"}`,
		State:          indexpb.JobState_JobStateInit,
	}
	assert.NoError(t, refreshMeta.AddJob(job))

	tasks, err := mgr.createTasksForJob(ctx, job)
	assert.NoError(t, err)
	require.Len(t, tasks, 1)

	committedTasks, err := refreshMeta.GetCommittedTasksByJobID(job.GetJobId())
	assert.NoError(t, err)
	require.Len(t, committedTasks, 1)
	persistedTask := committedTasks[0]
	assert.Equal(t, int64(0), persistedTask.GetFileIndexBegin())
	assert.Equal(t, int64(4), persistedTask.GetFileIndexEnd())
	assert.Equal(t, []int64{10}, persistedTask.GetOwnedSegmentIds())

	cluster := &stubCluster{}
	mgr.wrapTask(persistedTask).CreateTaskOnWorker(1, cluster)
	require.NotNil(t, cluster.refreshReq)
	assert.Equal(t, "manifest-path", cluster.refreshReq.GetExploreManifestPath())
	assert.Equal(t, int64(0), cluster.refreshReq.GetFileIndexBegin())
	assert.Equal(t, int64(4), cluster.refreshReq.GetFileIndexEnd())
	require.Len(t, cluster.refreshReq.GetCurrentSegments(), 1)
	assert.Equal(t, int64(10), cluster.refreshReq.GetCurrentSegments()[0].GetID())
}

func TestExploreExternalFiles_UsesUniqueAttemptDirectories(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collectionID, &collectionInfo{
		ID: collectionID,
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
			FieldID:       100,
			Name:          "id",
			ExternalField: "id",
		}}},
	})
	mgr := &externalCollectionRefreshManager{
		mt:               &meta{},
		allocator:        &stubAllocator{nextID: 300},
		collectionGetter: testCollectionGetter(collections),
	}

	baseDirs := make([]string, 0, 2)
	mockExplore := mockey.Mock(packed.ExploreFilesReturnManifestPath).
		To(func(_ []string, _ string, baseDir, _ string, _ *indexpb.StorageConfig, _ packed.ExternalSpecContext) ([]packed.FileInfo, string, error) {
			baseDirs = append(baseDirs, baseDir)
			return []packed.FileInfo{{FilePath: "f.parquet", NumRows: 1}}, baseDir + "/manifest", nil
		}).Build()
	defer mockExplore.UnPatch()

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          42,
		CollectionId:   collectionID,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   `{"format":"parquet","extfs":{"cloud_provider":"aws","region":"us-west-2","access_key_id":"ak","access_key_value":"sk"}}`,
	}
	for range 2 {
		_, _, err := mgr.exploreExternalFiles(ctx, job)
		assert.NoError(t, err)
	}
	assert.Equal(t, []string{
		"__explore_temp__/coord_42/attempt_301",
		"__explore_temp__/coord_42/attempt_302",
	}, baseDirs)
}

func TestExploreExternalFiles_LoadsMissingCollection(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	collection := &collectionInfo{
		ID: collectionID,
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
			FieldID:       100,
			Name:          "id",
			ExternalField: "id",
		}}},
	}
	getterCalled := false
	mgr := &externalCollectionRefreshManager{
		mt:        &meta{},
		allocator: &stubAllocator{nextID: 300},
		collectionGetter: func(context.Context, int64) (*collectionInfo, error) {
			getterCalled = true
			return collection, nil
		},
	}

	mockExplore := mockey.Mock(packed.ExploreFilesReturnManifestPath).
		Return([]packed.FileInfo{{FilePath: "f.parquet", NumRows: 1}}, "manifest", nil).
		Build()
	defer mockExplore.UnPatch()

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          42,
		CollectionId:   collectionID,
		ExternalSource: "s3://bucket/path",
		ExternalSpec:   `{"format":"parquet","extfs":{"cloud_provider":"aws","region":"us-west-2","access_key_id":"ak","access_key_value":"sk"}}`,
	}
	_, _, err := mgr.exploreExternalFiles(ctx, job)
	require.NoError(t, err)
	assert.True(t, getterCalled)
}

func TestCreateTasksForJob_UnreadableBaselineManifest(t *testing.T) {
	ctx := context.Background()
	collectionID := int64(100)
	jobID := int64(1001)

	segments := NewSegmentsInfo()
	segments.SetSegment(10, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           10,
		CollectionID: collectionID,
		State:        commonpb.SegmentState_Flushed,
		ManifestPath: "baseline-manifest",
	}})
	collections := newTestCollections(collectionID)
	mt := &meta{
		segments: segments,
	}
	refreshMeta := createTestRefreshMetaWithJobs(t, nil, nil)
	alloc := &stubAllocator{nextID: 2000}
	mgr := NewExternalCollectionRefreshManager(
		ctx, mt, newStubScheduler(), alloc,
		refreshMeta, nil, testCollectionGetter(collections), nil, nil).(*externalCollectionRefreshManager)

	mockExplore := mockey.Mock((*externalCollectionRefreshManager).exploreExternalFiles).
		Return([]*datapb.ExternalFileInfo{{FilePath: "s3://bucket/path/a.parquet", NumRows: 10}}, "manifest-path", nil).
		Build()
	defer mockExplore.UnPatch()
	mockRead := mockey.Mock(packed.ReadFragmentsFromManifest).
		Return(nil, errors.New("manifest read failed")).
		Build()
	defer mockRead.UnPatch()

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:        jobID,
		CollectionId: collectionID,
		State:        indexpb.JobState_JobStateInit,
	}
	assert.NoError(t, refreshMeta.AddJob(job))

	tasks, err := mgr.createTasksForJob(ctx, job)
	assert.ErrorContains(t, err, "read external refresh baseline manifests")
	assert.Empty(t, tasks)
	assert.Equal(t, int64(2000), alloc.nextID)
	assert.Empty(t, refreshMeta.GetTasksByJobID(jobID))
	assert.Empty(t, refreshMeta.GetJob(jobID).GetTaskIds())
}

func TestCreateTasksForJob_CompositePersistenceFailureIsUnpublished(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ExternalCollectionFilesPerTask.Key, "1")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ExternalCollectionFilesPerTask.Key)

	const (
		jobID        = int64(1001)
		collectionID = int64(100)
	)
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)
	job := &datapb.ExternalCollectionRefreshJob{
		JobId:        jobID,
		CollectionId: collectionID,
		State:        indexpb.JobState_JobStateInit,
	}
	assert.NoError(t, refreshMeta.AddJob(job))

	collections := newTestCollections(collectionID)
	mt := &meta{
		segments: NewSegmentsInfo(),
	}
	cm := &recordingChunkManager{}
	mgr := NewExternalCollectionRefreshManager(
		ctx,
		mt,
		newStubScheduler(),
		&stubAllocator{nextID: 2000},
		refreshMeta,
		nil,
		testCollectionGetter(collections),
		nil,
		cm,
	).(*externalCollectionRefreshManager)

	mockExplore := mockey.Mock((*externalCollectionRefreshManager).exploreExternalFiles).
		Return([]*datapb.ExternalFileInfo{
			{FilePath: "s3://bucket/path/a.parquet", NumRows: 10},
			{FilePath: "s3://bucket/path/b.parquet", NumRows: 10},
		}, "manifest-path", nil).
		Build()
	defer mockExplore.UnPatch()
	catalog.updateErr = errors.New("save task plan failed")

	assert.Panics(t, func() {
		_, _ = mgr.createTasksForJob(ctx, job)
	})
	assert.Empty(t, refreshMeta.GetTasksByJobID(jobID))
	assert.Empty(t, refreshMeta.GetJob(jobID).GetTaskIds())
	assert.Len(t, catalog.updateActions, 1)
	prefixes, removes := cm.snapshot()
	assert.Empty(t, prefixes)
	assert.Empty(t, removes)
}

func TestCreateTasksForJob_TerminalJobRejectsLatePlanAndCleansExplore(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	const (
		jobID        = int64(1001)
		collectionID = int64(100)
	)
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)
	staleJob := &datapb.ExternalCollectionRefreshJob{
		JobId:        jobID,
		CollectionId: collectionID,
		State:        indexpb.JobState_JobStateInit,
	}
	assert.NoError(t, refreshMeta.AddJob(staleJob))
	applied, err := refreshMeta.UpdateJobState(jobID, indexpb.JobState_JobStateFailed, "timeout")
	assert.NoError(t, err)
	assert.True(t, applied)

	collections := newTestCollections(collectionID)
	mt := &meta{
		segments: NewSegmentsInfo(),
	}
	cm := &recordingChunkManager{}
	mgr := NewExternalCollectionRefreshManager(
		ctx,
		mt,
		newStubScheduler(),
		&stubAllocator{nextID: 2000},
		refreshMeta,
		nil,
		testCollectionGetter(collections),
		nil,
		cm,
	).(*externalCollectionRefreshManager)

	mockExplore := mockey.Mock((*externalCollectionRefreshManager).exploreExternalFiles).
		Return([]*datapb.ExternalFileInfo{{FilePath: "s3://bucket/path/a.parquet", NumRows: 10}}, "manifest-path", nil).
		Build()
	defer mockExplore.UnPatch()

	tasks, err := mgr.createTasksForJob(ctx, staleJob)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, errExternalRefreshTaskPlanNotPublishable))
	assert.Empty(t, tasks)
	assert.Empty(t, catalog.updateActions)
	assert.Empty(t, refreshMeta.GetTasksByJobID(jobID))
	assert.Empty(t, refreshMeta.GetJob(jobID).GetTaskIds())
	prefixes, removes := cm.snapshot()
	assert.Equal(t, []string{"__explore_temp__/coord_1001/"}, prefixes)
	assert.Equal(t, []string{"__explore_temp__/coord_1001"}, removes)
}

func TestCreateTasksForJob_MissingJobRejectsLatePlanAndCleansExplore(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	const (
		jobID        = int64(1001)
		collectionID = int64(100)
	)
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)
	staleJob := &datapb.ExternalCollectionRefreshJob{
		JobId:        jobID,
		CollectionId: collectionID,
		State:        indexpb.JobState_JobStateInit,
	}

	collections := newTestCollections(collectionID)
	mt := &meta{
		segments: NewSegmentsInfo(),
	}
	cm := &recordingChunkManager{}
	mgr := NewExternalCollectionRefreshManager(
		ctx,
		mt,
		newStubScheduler(),
		&stubAllocator{nextID: 2000},
		refreshMeta,
		nil,
		testCollectionGetter(collections),
		nil,
		cm,
	).(*externalCollectionRefreshManager)

	mockExplore := mockey.Mock((*externalCollectionRefreshManager).exploreExternalFiles).
		Return([]*datapb.ExternalFileInfo{{FilePath: "s3://bucket/path/a.parquet", NumRows: 10}}, "manifest-path", nil).
		Build()
	defer mockExplore.UnPatch()

	tasks, err := mgr.createTasksForJob(ctx, staleJob)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, errExternalRefreshTaskPlanNotPublishable))
	assert.Empty(t, tasks)
	assert.Empty(t, catalog.updateActions)
	assert.Empty(t, refreshMeta.GetTasksByJobID(jobID))
	assert.Nil(t, refreshMeta.GetJob(jobID))
	prefixes, removes := cm.snapshot()
	assert.Equal(t, []string{"__explore_temp__/coord_1001/"}, prefixes)
	assert.Equal(t, []string{"__explore_temp__/coord_1001"}, removes)
}

func TestExternalCollectionRefreshManager_ApplyFinishedJobSegmentsMergesTaskResults(t *testing.T) {
	ctx := context.Background()
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	addManagerOwnershipTask(t, refreshMeta, &datapb.ExternalCollectionRefreshTask{
		TaskId:          1001,
		JobId:           1,
		CollectionId:    100,
		State:           indexpb.JobState_JobStateFinished,
		ResultReady:     true,
		KeptSegments:    []int64{1},
		UpdatedSegments: []*datapb.SegmentInfo{newTestExternalRefreshSegment(10, 100, 7)},
	}, 1)
	addManagerOwnershipTask(t, refreshMeta, &datapb.ExternalCollectionRefreshTask{
		TaskId:          1002,
		JobId:           1,
		CollectionId:    100,
		State:           indexpb.JobState_JobStateFinished,
		ResultReady:     true,
		UpdatedSegments: []*datapb.SegmentInfo{newTestExternalRefreshSegment(20, 100, 7)},
	}, 2)
	publishManagerTestTasks(t, refreshMeta, 1, 100, 1001, 1002)

	segments := NewSegmentsInfo()
	segments.SetSegment(1, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           1,
		CollectionID: 100,
		State:        commonpb.SegmentState_Flushed,
		NumOfRows:    5,
	}})
	segments.SetSegment(2, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           2,
		CollectionID: 100,
		State:        commonpb.SegmentState_Flushed,
		NumOfRows:    6,
	}})
	segments.SetSegment(3, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           3,
		CollectionID: 100,
		State:        commonpb.SegmentState_Flushed,
		NumOfRows:    9,
	}})
	mt := &meta{
		catalog:  catalog,
		segments: segments,
	}
	mgr := &externalCollectionRefreshManager{
		mt:               mt,
		refreshMeta:      refreshMeta,
		collectionGetter: testCollectionGetter(newTestCollections(100)),
	}

	err = mgr.applyFinishedJobSegments(ctx, &datapb.ExternalCollectionRefreshJob{
		JobId:        1,
		CollectionId: 100,
	})
	assert.NoError(t, err)
	require.Len(t, catalog.updateActions, 1)

	segmentActionCount := 0
	persistedConsumedTaskIDs := make([]int64, 0, 2)
	for _, action := range catalog.updateActions[0] {
		switch entry := action.Entry.(type) {
		case metastore.SegmentEntry:
			segmentActionCount++
		case metastore.RefreshTaskEntry:
			require.Equal(t, metastore.ActionAdd, action.Type)
			require.NotNil(t, entry.Task)
			assert.True(t, isExternalRefreshTaskResultConsumed(entry.Task))
			persistedConsumedTaskIDs = append(persistedConsumedTaskIDs, entry.Task.GetTaskId())
		}
	}
	assert.NotZero(t, segmentActionCount)
	assert.ElementsMatch(t, []int64{1001, 1002}, persistedConsumedTaskIDs)

	for _, taskID := range []int64{1001, 1002} {
		task := refreshMeta.GetTask(taskID)
		require.NotNil(t, task)
		assert.True(t, task.GetResultReady())
		assert.Empty(t, task.GetKeptSegments())
		assert.Empty(t, task.GetUpdatedSegments())
		assert.Zero(t, task.GetResultStorageVersion())
		assert.Empty(t, task.GetResultPath())
		assert.Empty(t, task.GetResultChecksum())
		assert.True(t, isExternalRefreshTaskResultConsumed(task))
	}

	assert.Equal(t, commonpb.SegmentState_Flushed, mt.segments.GetSegment(1).GetState())
	assert.Equal(t, commonpb.SegmentState_Dropped, mt.segments.GetSegment(2).GetState())
	assert.Equal(t, commonpb.SegmentState_Flushed, mt.segments.GetSegment(3).GetState())
	assert.Equal(t, commonpb.SegmentState_Flushed, mt.segments.GetSegment(10).GetState())
	assert.Equal(t, commonpb.SegmentState_Flushed, mt.segments.GetSegment(20).GetState())
	assert.Equal(t, int64(7), mt.segments.GetSegment(10).GetNumOfRows())
	assert.Equal(t, int64(7), mt.segments.GetSegment(20).GetNumOfRows())
}

func TestExternalCollectionRefreshManager_ApplyFinishedJobSegmentsRejectsCrossTaskResult(t *testing.T) {
	for _, test := range []struct {
		name            string
		keptSegments    []int64
		updatedSegments []*datapb.SegmentInfo
	}{
		{name: "kept", keptSegments: []int64{2}},
		{name: "updated", updatedSegments: []*datapb.SegmentInfo{{ID: 2, CollectionID: 100}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			refreshMeta := createTestRefreshMeta(t)
			addManagerOwnershipTask(t, refreshMeta, &datapb.ExternalCollectionRefreshTask{
				TaskId:          1001,
				JobId:           1,
				CollectionId:    100,
				State:           indexpb.JobState_JobStateFinished,
				ResultReady:     true,
				KeptSegments:    test.keptSegments,
				UpdatedSegments: test.updatedSegments,
			}, 1)
			addManagerOwnershipTask(t, refreshMeta, &datapb.ExternalCollectionRefreshTask{
				TaskId:       1002,
				JobId:        1,
				CollectionId: 100,
				State:        indexpb.JobState_JobStateFinished,
				ResultReady:  true,
			}, 2)
			publishManagerTestTasks(t, refreshMeta, 1, 100, 1001, 1002)

			mgr := &externalCollectionRefreshManager{
				refreshMeta:      refreshMeta,
				collectionGetter: testCollectionGetter(newTestCollections(100)),
			}
			err := mgr.applyFinishedJobSegments(ctx, &datapb.ExternalCollectionRefreshJob{
				JobId:        1,
				CollectionId: 100,
			})
			assert.ErrorContains(t, err, "owned by task 1002")
			assert.ErrorIs(t, err, merr.ErrDataIntegrity)
		})
	}
}

func TestExternalCollectionRefreshManager_ApplyFinishedJobSegmentsRejectsLegacyTask(t *testing.T) {
	ctx := context.Background()
	refreshMeta := createTestRefreshMeta(t)
	assert.NoError(t, refreshMeta.AddTask(&datapb.ExternalCollectionRefreshTask{
		TaskId:       1001,
		JobId:        1,
		CollectionId: 100,
		State:        indexpb.JobState_JobStateFinished,
		ResultReady:  true,
	}))
	publishManagerTestTasks(t, refreshMeta, 1, 100, 1001)

	mgr := &externalCollectionRefreshManager{
		refreshMeta:      refreshMeta,
		collectionGetter: testCollectionGetter(newTestCollections(100)),
	}
	err := mgr.applyFinishedJobSegments(ctx, &datapb.ExternalCollectionRefreshJob{
		JobId:        1,
		CollectionId: 100,
	})
	assert.ErrorContains(t, err, "unsupported ownership plan version 0")
	assert.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestExternalCollectionRefreshManager_ApplyFinishedJobSegmentsWithoutBaseline(t *testing.T) {
	ctx := context.Background()
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)
	addManagerOwnershipTask(t, refreshMeta, &datapb.ExternalCollectionRefreshTask{
		TaskId:          1001,
		JobId:           1,
		CollectionId:    100,
		State:           indexpb.JobState_JobStateFinished,
		ResultReady:     true,
		UpdatedSegments: []*datapb.SegmentInfo{newTestExternalRefreshSegment(10, 100, 7)},
	})
	publishManagerTestTasks(t, refreshMeta, 1, 100, 1001)

	mt := &meta{
		catalog:  catalog,
		segments: NewSegmentsInfo(),
	}
	mgr := &externalCollectionRefreshManager{
		mt:               mt,
		refreshMeta:      refreshMeta,
		collectionGetter: testCollectionGetter(newTestCollections(100)),
	}
	err = mgr.applyFinishedJobSegments(ctx, &datapb.ExternalCollectionRefreshJob{
		JobId:        1,
		CollectionId: 100,
	})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.SegmentState_Flushed, mt.segments.GetSegment(10).GetState())
}

func TestExternalCollectionRefreshManager_ApplyFinishedJobSegmentsLazyLoadsCollection(t *testing.T) {
	ctx := context.Background()
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	require.NoError(t, err)
	addManagerOwnershipTask(t, refreshMeta, &datapb.ExternalCollectionRefreshTask{
		TaskId:          1001,
		JobId:           1,
		CollectionId:    100,
		State:           indexpb.JobState_JobStateFinished,
		ResultReady:     true,
		UpdatedSegments: []*datapb.SegmentInfo{newTestExternalRefreshSegment(10, 100, 7)},
	})
	publishManagerTestTasks(t, refreshMeta, 1, 100, 1001)

	mt := &meta{
		catalog:  catalog,
		segments: NewSegmentsInfo(),
	}
	loadedCollections := newTestCollections(100)
	loaded, ok := loadedCollections.Get(100)
	require.True(t, ok)
	getterCalls := 0
	mgr := &externalCollectionRefreshManager{
		mt:          mt,
		refreshMeta: refreshMeta,
		collectionGetter: func(context.Context, int64) (*collectionInfo, error) {
			getterCalls++
			return loaded, nil
		},
	}

	err = mgr.applyFinishedJobSegments(ctx, &datapb.ExternalCollectionRefreshJob{
		JobId:        1,
		CollectionId: 100,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, getterCalls)
	assert.Equal(t, commonpb.SegmentState_Flushed, mt.segments.GetSegment(10).GetState())
}

func TestExternalCollectionRefreshManager_ApplyFinishedJobSegmentsRejectsNonFinishedTask(t *testing.T) {
	ctx := context.Background()
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	addManagerOwnershipTask(t, refreshMeta, &datapb.ExternalCollectionRefreshTask{
		TaskId:          1001,
		JobId:           1,
		CollectionId:    100,
		State:           indexpb.JobState_JobStateFinished,
		ResultReady:     true,
		UpdatedSegments: []*datapb.SegmentInfo{{ID: 10, CollectionID: 100, NumOfRows: 7}},
	})
	addManagerOwnershipTask(t, refreshMeta, &datapb.ExternalCollectionRefreshTask{
		TaskId:       1002,
		JobId:        1,
		CollectionId: 100,
		State:        indexpb.JobState_JobStateInProgress,
	})
	publishManagerTestTasks(t, refreshMeta, 1, 100, 1001, 1002)

	mt := &meta{
		catalog:  catalog,
		segments: NewSegmentsInfo(),
	}
	updateCalls := 0
	mockUpdate := mockey.Mock((*meta).UpdateSegmentsInfo).To(func(_ *meta, _ context.Context, _ ...UpdateOperator) error {
		updateCalls++
		return nil
	}).Build()
	defer mockUpdate.UnPatch()

	mgr := &externalCollectionRefreshManager{
		mt:               mt,
		refreshMeta:      refreshMeta,
		collectionGetter: testCollectionGetter(newTestCollections(100)),
	}

	err = mgr.applyFinishedJobSegments(ctx, &datapb.ExternalCollectionRefreshJob{
		JobId:        1,
		CollectionId: 100,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be consumed in state JobStateInProgress")
	assert.Equal(t, 0, updateCalls)
}

func TestExternalCollectionRefreshManager_ApplyFinishedJobSegmentsRejectsDuplicateUpdatedSegment(t *testing.T) {
	ctx := context.Background()
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	addManagerOwnershipTask(t, refreshMeta, &datapb.ExternalCollectionRefreshTask{
		TaskId:          1001,
		JobId:           1,
		CollectionId:    100,
		State:           indexpb.JobState_JobStateFinished,
		ResultReady:     true,
		UpdatedSegments: []*datapb.SegmentInfo{{ID: 10, CollectionID: 100, NumOfRows: 7}},
	})
	addManagerOwnershipTask(t, refreshMeta, &datapb.ExternalCollectionRefreshTask{
		TaskId:          1002,
		JobId:           1,
		CollectionId:    100,
		State:           indexpb.JobState_JobStateFinished,
		ResultReady:     true,
		UpdatedSegments: []*datapb.SegmentInfo{{ID: 10, CollectionID: 100, NumOfRows: 8}},
	})
	publishManagerTestTasks(t, refreshMeta, 1, 100, 1001, 1002)

	mt := &meta{
		catalog:  catalog,
		segments: NewSegmentsInfo(),
	}
	updateCalls := 0
	mockUpdate := mockey.Mock((*meta).UpdateSegmentsInfo).To(func(_ *meta, _ context.Context, _ ...UpdateOperator) error {
		updateCalls++
		return nil
	}).Build()
	defer mockUpdate.UnPatch()

	mgr := &externalCollectionRefreshManager{
		mt:               mt,
		refreshMeta:      refreshMeta,
		collectionGetter: testCollectionGetter(newTestCollections(100)),
	}

	err = mgr.applyFinishedJobSegments(ctx, &datapb.ExternalCollectionRefreshJob{
		JobId:        1,
		CollectionId: 100,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate updated segment")
	assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	assert.Equal(t, 0, updateCalls)
}

func TestExternalCollectionRefreshManager_ApplyFinishedJobSegmentsRejectsMissingTaskResult(t *testing.T) {
	ctx := context.Background()
	catalog := &stubCatalog{}
	refreshMeta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	addManagerOwnershipTask(t, refreshMeta, &datapb.ExternalCollectionRefreshTask{
		TaskId:       1001,
		JobId:        1,
		CollectionId: 100,
		State:        indexpb.JobState_JobStateFinished,
	})
	addManagerOwnershipTask(t, refreshMeta, &datapb.ExternalCollectionRefreshTask{
		TaskId:          1002,
		JobId:           1,
		CollectionId:    100,
		State:           indexpb.JobState_JobStateFinished,
		ResultReady:     true,
		KeptSegments:    []int64{1},
		UpdatedSegments: []*datapb.SegmentInfo{{ID: 20, CollectionID: 100, NumOfRows: 7}},
	}, 1)
	publishManagerTestTasks(t, refreshMeta, 1, 100, 1001, 1002)

	mt := &meta{
		catalog:  catalog,
		segments: NewSegmentsInfo(),
	}
	updateCalls := 0
	mockUpdate := mockey.Mock((*meta).UpdateSegmentsInfo).To(func(_ *meta, _ context.Context, _ ...UpdateOperator) error {
		updateCalls++
		return nil
	}).Build()
	defer mockUpdate.UnPatch()

	mgr := &externalCollectionRefreshManager{
		mt:               mt,
		refreshMeta:      refreshMeta,
		collectionGetter: testCollectionGetter(newTestCollections(100)),
	}

	err = mgr.applyFinishedJobSegments(ctx, &datapb.ExternalCollectionRefreshJob{
		JobId:        1,
		CollectionId: 100,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be consumed in state JobStateFinished with result_ready=false")
	assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	assert.Equal(t, 0, updateCalls)
}

func TestExternalCollectionRefreshManager_ApplyFinishedJobSegmentsDroppedCollectionIsNoOp(t *testing.T) {
	mgr := &externalCollectionRefreshManager{
		collectionGetter: func(context.Context, int64) (*collectionInfo, error) {
			return nil, merr.WrapErrCollectionNotFound(100)
		},
	}

	err := mgr.applyFinishedJobSegments(context.Background(), &datapb.ExternalCollectionRefreshJob{
		JobId:        1,
		CollectionId: 100,
	})
	assert.NoError(t, err)
}

func TestExternalCollectionRefreshManager_ApplyFinishedJobSegmentsEmptyLazyLookupRetries(t *testing.T) {
	mgr := &externalCollectionRefreshManager{
		collectionGetter: func(context.Context, int64) (*collectionInfo, error) {
			return nil, nil
		},
	}

	err := mgr.applyFinishedJobSegments(context.Background(), &datapb.ExternalCollectionRefreshJob{
		JobId:        1,
		CollectionId: 100,
	})
	assert.ErrorIs(t, err, merr.ErrServiceNotReady)
	assert.True(t, merr.IsRetryableErr(err))
}

func TestValidateMilvusTableRefreshSchemaErrorClass(t *testing.T) {
	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		ExternalSource: "s3://bucket/snapshots/1/metadata/10.json",
		ExternalSpec:   testMilvusTableTargetRefreshSchema().GetExternalSpec(),
	}

	t.Run("metadata_read_error_is_retryable", func(t *testing.T) {
		readErr := errors.New("temporary metadata read failure")
		mockRead := mockey.Mock(packed.ReadMilvusTableSnapshotMetadata).
			Return(nil, readErr).Build()
		defer mockRead.UnPatch()

		err := validateMilvusTableRefreshSchema(job, testMilvusTableTargetRefreshSchema())
		assert.Error(t, err)
		assert.False(t, errors.Is(err, errMilvusTableRefreshSchemaInvalid))
		assert.ErrorIs(t, err, readErr)
	})

	t.Run("missing_source_schema_is_non_retriable", func(t *testing.T) {
		mockRead := mockey.Mock(packed.ReadMilvusTableSnapshotMetadata).
			Return(&datapb.SnapshotMetadata{
				Collection: &datapb.CollectionDescription{},
			}, nil).Build()
		defer mockRead.UnPatch()

		err := validateMilvusTableRefreshSchema(job, testMilvusTableTargetRefreshSchema())
		assert.Error(t, err)
		assert.ErrorIs(t, err, errMilvusTableRefreshSchemaInvalid)
		assert.Contains(t, err.Error(), "missing collection schema")
	})

	t.Run("external_source_schema_is_non_retriable", func(t *testing.T) {
		mockRead := mockey.Mock(packed.ReadMilvusTableSnapshotMetadata).
			Return(&datapb.SnapshotMetadata{
				Collection: &datapb.CollectionDescription{
					Schema: testMilvusTableRefreshSchema(true),
				},
			}, nil).Build()
		defer mockRead.UnPatch()

		err := validateMilvusTableRefreshSchema(job, testMilvusTableTargetRefreshSchema())
		assert.Error(t, err)
		assert.ErrorIs(t, err, errMilvusTableRefreshSchemaInvalid)
		assert.Contains(t, err.Error(), "source snapshot is an external collection")
	})

	t.Run("schema_mismatch_is_non_retriable", func(t *testing.T) {
		sourceSchema := testMilvusTableRefreshSchema(false)
		sourceSchema.Fields[0].FieldID = 101
		mockRead := mockey.Mock(packed.ReadMilvusTableSnapshotMetadata).
			Return(&datapb.SnapshotMetadata{
				Collection: &datapb.CollectionDescription{
					Schema: sourceSchema,
				},
			}, nil).Build()
		defer mockRead.UnPatch()

		err := validateMilvusTableRefreshSchema(job, testMilvusTableTargetRefreshSchema())
		assert.Error(t, err)
		assert.ErrorIs(t, err, errMilvusTableRefreshSchemaInvalid)
		assert.Contains(t, err.Error(), "source schema does not match target collection schema")
	})

	t.Run("matching_schema_passes", func(t *testing.T) {
		mockRead := mockey.Mock(packed.ReadMilvusTableSnapshotMetadata).
			Return(&datapb.SnapshotMetadata{
				Collection: &datapb.CollectionDescription{
					Schema: testMilvusTableRefreshSchema(false),
				},
			}, nil).Build()
		defer mockRead.UnPatch()

		err := validateMilvusTableRefreshSchema(job, testMilvusTableTargetRefreshSchema())
		assert.NoError(t, err)
	})
}

// ==================== Test Functions ====================

func TestExternalCollectionRefreshManager_NewManager(t *testing.T) {
	ctx := context.Background()
	refreshMeta := createTestRefreshMeta(t)

	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)
	assert.NotNil(t, manager)
}

func TestExternalCollectionRefreshManager_StartStop(t *testing.T) {
	ctx := context.Background()
	refreshMeta := createTestRefreshMeta(t)

	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)
	concreteManager := manager.(*externalCollectionRefreshManager)

	// Mock inspector and checker run methods to avoid actual execution
	mockInspectorRun := mockey.Mock((*externalCollectionRefreshInspector).run).Return().Build()
	defer mockInspectorRun.UnPatch()

	mockCheckerRun := mockey.Mock((*externalCollectionRefreshChecker).run).Return().Build()
	defer mockCheckerRun.UnPatch()

	// Start should not panic
	manager.Start()

	// Stop should not panic and should be idempotent
	manager.Stop()
	manager.Stop() // Call again to verify idempotency
	assert.ErrorIs(t, concreteManager.ctx.Err(), context.Canceled)
}

func TestExternalCollectionRefreshManager_StopCancelsInFlightInitJob(t *testing.T) {
	job := &datapb.ExternalCollectionRefreshJob{
		JobId:        1,
		CollectionId: 100,
		State:        indexpb.JobState_JobStateInit,
	}
	refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{job}, nil)
	manager := NewExternalCollectionRefreshManager(
		context.Background(), nil, newStubScheduler(), &stubAllocator{}, refreshMeta, nil, nil, nil, nil,
	).(*externalCollectionRefreshManager)

	started := make(chan struct{})
	mockCreateTasks := mockey.Mock((*externalCollectionRefreshManager).createTasksForJob).To(func(
		_ *externalCollectionRefreshManager,
		ctx context.Context,
		_ *datapb.ExternalCollectionRefreshJob,
	) ([]*refreshExternalCollectionTask, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}).Build()
	defer mockCreateTasks.UnPatch()

	manager.ensureTasksForInitJob(job.GetJobId())
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("init job did not start")
	}

	stopped := make(chan struct{})
	go func() {
		manager.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not cancel the in-flight init job")
	}
}

func TestExternalCollectionRefreshManager_StopRejectsNewInitJob(t *testing.T) {
	job := &datapb.ExternalCollectionRefreshJob{
		JobId:        1,
		CollectionId: 100,
		State:        indexpb.JobState_JobStateInit,
	}
	refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{job}, nil)
	manager := NewExternalCollectionRefreshManager(
		context.Background(), nil, newStubScheduler(), &stubAllocator{}, refreshMeta, nil, nil, nil, nil,
	).(*externalCollectionRefreshManager)

	called := make(chan struct{})
	mockCreateTasks := mockey.Mock((*externalCollectionRefreshManager).createTasksForJob).To(func(
		_ *externalCollectionRefreshManager,
		_ context.Context,
		_ *datapb.ExternalCollectionRefreshJob,
	) ([]*refreshExternalCollectionTask, error) {
		close(called)
		return nil, nil
	}).Build()
	defer mockCreateTasks.UnPatch()

	manager.Stop()
	manager.ensureTasksForInitJob(job.GetJobId())

	select {
	case <-called:
		t.Fatal("init job started after manager stopped")
	case <-time.After(100 * time.Millisecond):
	}
	manager.initMu.Lock()
	defer manager.initMu.Unlock()
	assert.Empty(t, manager.initJobsInFlight)
}

func TestExternalCollectionRefreshManager_SubmitRefreshJobWithID(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{nextID: 1000}
		scheduler := newStubScheduler()

		// Create a mock meta with external collection. ExternalSpec must be
		// valid JSON now that createTasksForJob → exploreExternalFiles parses
		// it via externalspec.ParseExternalSpec (added in Part 8 cross-bucket).
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name:           "test_collection",
				ExternalSource: "s3://bucket/path",
				ExternalSpec:   `{"format":"parquet"}`,
			},
		})
		mt := &meta{
			segments: NewSegmentsInfo(),
		}

		// Mock IsExternalCollection to return true
		mockIsExternal := mockey.Mock(typeutil.IsExternalCollection).Return(true).Build()
		defer mockIsExternal.UnPatch()

		// Mock exploreExternalFiles so the test does not need real S3 + parquet.
		// Returns one file so createTasksForJob produces a single task chunk.
		mockExplore := mockey.Mock((*externalCollectionRefreshManager).exploreExternalFiles).
			Return([]*datapb.ExternalFileInfo{{FilePath: "s3://bucket/path/file.parquet", NumRows: 100}}, "s3://bucket/path/manifest", nil).Build()
		defer mockExplore.UnPatch()

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(collections), nil, nil)

		jobID, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.NoError(t, err)
		assert.Equal(t, int64(1), jobID)

		// Verify job was created
		job := refreshMeta.GetJob(1)
		assert.NotNil(t, job)
		assert.Equal(t, int64(1), job.GetJobId())
		assert.Equal(t, int64(100), job.GetCollectionId())
		manager.Stop()
	})

	t.Run("idempotent_job_exists", func(t *testing.T) {
		now := time.Now().UnixMilli()
		existingJob := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInProgress,
			StartTime:    now,
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{existingJob}, nil)

		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)

		// Should return without error if job already exists
		jobID, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.NoError(t, err)
		assert.Equal(t, int64(1), jobID)
	})

	t.Run("collection_not_found", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		mt := &meta{}
		collectionGetter := func(context.Context, int64) (*collectionInfo, error) {
			return nil, merr.WrapErrCollectionNotFound(999)
		}
		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, collectionGetter, nil, nil)

		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 999, "test_collection", "", "")
		assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
		assert.True(t, isNonRetriableRefreshError(err))
		assert.Nil(t, refreshMeta.GetJob(1))
	})

	t.Run("transient_collection_lookup_error_is_preserved", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		lookupErr := merr.WrapErrServiceNotReadyMsg("rootcoord metadata is temporarily unavailable")
		collectionGetter := func(context.Context, int64) (*collectionInfo, error) {
			return nil, lookupErr
		}
		manager := NewExternalCollectionRefreshManager(
			ctx, &meta{}, newStubScheduler(), &stubAllocator{}, refreshMeta, nil, collectionGetter, nil, nil,
		)

		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.ErrorIs(t, err, merr.ErrServiceNotReady)
		assert.False(t, isNonRetriableRefreshError(err))
		assert.Nil(t, refreshMeta.GetJob(1))
	})

	t.Run("empty_collection_lookup_is_retriable", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		collectionGetter := func(context.Context, int64) (*collectionInfo, error) {
			return nil, nil
		}
		manager := NewExternalCollectionRefreshManager(
			ctx, &meta{}, newStubScheduler(), &stubAllocator{}, refreshMeta, nil, collectionGetter, nil, nil,
		)

		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.ErrorIs(t, err, merr.ErrServiceNotReady)
		assert.True(t, merr.IsRetryableErr(err))
		assert.False(t, isNonRetriableRefreshError(err))
		assert.Nil(t, refreshMeta.GetJob(1))
	})

	t.Run("not_external_collection", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		// Create a mock meta with non-external collection
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name:           "test_collection",
				ExternalSource: "", // Not external
			},
		})
		mt := &meta{}

		// Mock typeutil.IsExternalCollection to return false
		mockIsExternal := mockey.Mock(typeutil.IsExternalCollection).Return(false).Build()
		defer mockIsExternal.UnPatch()

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(collections), nil, nil)

		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not an external collection")
	})

	t.Run("async_task_creation_failure_leaves_job_in_init", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{}

		// Mock AllocID to return error — triggers createTasksForJob failure
		// in the async Phase B goroutine.
		mockAllocID := mockey.Mock((*stubAllocator).AllocID).Return(int64(0), errors.New("alloc failed")).Build()
		defer mockAllocID.UnPatch()

		scheduler := newStubScheduler()

		// Create a mock meta with external collection
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name:           "test_collection",
				ExternalSource: "s3://bucket/path",
				ExternalSpec:   `{"format":"parquet"}`,
			},
		})
		mt := &meta{
			segments: NewSegmentsInfo(),
		}

		// Mock IsExternalCollection to return true
		mockIsExternal := mockey.Mock(typeutil.IsExternalCollection).Return(true).Build()
		defer mockIsExternal.UnPatch()
		mockExplore := mockey.Mock((*externalCollectionRefreshManager).exploreExternalFiles).
			Return([]*datapb.ExternalFileInfo{{FilePath: "s3://bucket/path/file.parquet", NumRows: 100}}, "manifest", nil).
			Build()
		defer mockExplore.UnPatch()

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(collections), nil, nil)

		// Phase A persists the Init job and returns success. Phase B runs
		// in the background; its failure (AllocID error here) is logged
		// but does NOT unwind the job — that's the whole point of the
		// two-phase split. The job lingers in Init until the checker tick
		// retries or tryTimeoutJob marks it Failed. Stop() waits for the
		// background goroutine to complete so the assertion is stable.
		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.NoError(t, err)

		manager.Stop()

		job := refreshMeta.GetJob(1)
		assert.NotNil(t, job, "job should remain in Init state for retry")
		assert.Equal(t, indexpb.JobState_JobStateInit, job.GetState())
		assert.Empty(t, job.GetTaskIds(), "no tasks should be persisted after async failure")
	})

	t.Run("loon_explore_error_stays_retriable_bounded_by_job_timeout", func(t *testing.T) {
		// Any FFI failure during explore (NoSuchBucket, AccessDenied, DNS
		// NXDOMAIN, S3 throttling, ...) is wrapped by the loon FFI layer as
		// ErrLoonTransient, which by its own contract cannot distinguish a
		// permanent source problem from a transient storage fault. It must
		// therefore stay retriable -- #49233's forever-loop is re-traded for a
		// bound: ExternalCollectionJobTimeout fails the job with the recorded
		// cause ("timeout, last failure: ..."), so the user still gets a clear
		// signal, just after the timeout instead of instantly.
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{nextID: 1000}
		scheduler := newStubScheduler()

		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name:           "test_collection",
				ExternalSource: "s3://bucket-does-not-exist/path",
				ExternalSpec:   `{"format":"parquet"}`,
			},
		})
		mt := &meta{}

		mockIsExternal := mockey.Mock(typeutil.IsExternalCollection).Return(true).Build()
		defer mockIsExternal.UnPatch()

		ffiErr := errors.Wrap(packed.ErrLoonTransient, "FFI operation failed: AWS Error NO_SUCH_BUCKET during ListObjectsV2")
		mockExplore := mockey.Mock((*externalCollectionRefreshManager).exploreExternalFiles).
			Return(nil, "", ffiErr).Build()
		defer mockExplore.UnPatch()

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(collections), nil, nil)

		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.NoError(t, err)

		manager.Stop()

		// ErrLoonTransient does not say the source is permanently broken. packed
		// declares it as "treat all loon failures as retryable": milvus-storage
		// can lose the structured detail and fall back to a generic code, so a
		// throttled read and a missing bucket arrive here as the same sentinel.
		// Failing the job on it turned a recoverable blip into a dead refresh.
		// The job stays retriable and ExternalCollectionJobTimeout bounds it.
		job := refreshMeta.GetJob(1)
		assert.NotNil(t, job)
		assert.Equal(t, indexpb.JobState_JobStateInit, job.GetState(),
			"an indistinguishable loon failure must stay retriable, bounded by the job timeout")
		// The cause is recorded even though the job stays in Init, so the
		// eventual timeout can report it instead of a bare "timeout".
		assert.Contains(t, job.GetFailReason(), "NO_SUCH_BUCKET",
			"underlying error must be surfaced to operators")
	})

	t.Run("system_explore_error_stays_retriable", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{nextID: 1000}
		scheduler := newStubScheduler()

		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name:           "test_collection",
				ExternalSource: "s3://bucket/path",
				ExternalSpec:   `{"format":"parquet"}`,
			},
		})
		mt := &meta{}

		mockIsExternal := mockey.Mock(typeutil.IsExternalCollection).Return(true).Build()
		defer mockIsExternal.UnPatch()

		// A system failure: object storage was briefly unavailable. Whether the
		// job may be retried is merr's Input-vs-System question and nothing
		// else, so this must not end the job the way a bad request does.
		mockExplore := mockey.Mock((*externalCollectionRefreshManager).exploreExternalFiles).
			Return(nil, "", merr.WrapErrIoFailedReason("connection reset by peer")).Build()
		defer mockExplore.UnPatch()

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(collections), nil, nil)

		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.NoError(t, err)

		manager.Stop()

		job := refreshMeta.GetJob(1)
		assert.NotNil(t, job)
		assert.Equal(t, indexpb.JobState_JobStateInit, job.GetState(),
			"a system failure must stay retriable, bounded by the job timeout")
		assert.Contains(t, job.GetFailReason(), "connection reset by peer",
			"the cause is recorded even while the job stays retriable")
	})

	t.Run("empty_explore_result_marks_job_failed_without_eager_cleanup", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{nextID: 1000}
		scheduler := newStubScheduler()
		chunkManager := &recordingChunkManager{}

		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name:           "test_collection",
				ExternalSource: "s3://bucket/path",
				ExternalSpec:   `{"format":"parquet"}`,
			},
		})
		mt := &meta{}

		mockIsExternal := mockey.Mock(typeutil.IsExternalCollection).Return(true).Build()
		defer mockIsExternal.UnPatch()
		mockExplore := mockey.Mock((*externalCollectionRefreshManager).exploreExternalFiles).
			Return([]*datapb.ExternalFileInfo{}, "__explore_temp__/coord_1/attempt_1001/manifest", nil).
			Build()
		defer mockExplore.UnPatch()

		manager := NewExternalCollectionRefreshManager(
			ctx,
			mt,
			scheduler,
			alloc,
			refreshMeta,
			nil,
			testCollectionGetter(collections),
			nil,
			chunkManager,
		)

		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.NoError(t, err)
		manager.Stop()

		job := refreshMeta.GetJob(1)
		require.NotNil(t, job)
		assert.Equal(t, indexpb.JobState_JobStateFailed, job.GetState())
		assert.Contains(t, job.GetFailReason(), "no files found")
		prefixes, removes := chunkManager.snapshot()
		assert.Empty(t, prefixes)
		assert.Empty(t, removes)
	})

	t.Run("input_error_marks_job_failed_non_retriable", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{nextID: 1000}
		scheduler := newStubScheduler()

		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name:           "test_collection",
				ExternalSource: "s3://bucket/path",
				ExternalSpec:   `{"format":"milvus-table"}`,
			},
		})
		mt := &meta{}

		mockIsExternal := mockey.Mock(typeutil.IsExternalCollection).Return(true).Build()
		defer mockIsExternal.UnPatch()

		inputErr := merr.WrapErrParameterInvalidMsg("milvus-table requires external_source to be a snapshot metadata JSON path")
		mockExplore := mockey.Mock((*externalCollectionRefreshManager).exploreExternalFiles).
			Return(nil, "", inputErr).Build()
		defer mockExplore.UnPatch()

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(collections), nil, nil)

		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.NoError(t, err)

		manager.Stop()

		job := refreshMeta.GetJob(1)
		assert.NotNil(t, job)
		assert.Equal(t, indexpb.JobState_JobStateFailed, job.GetState(),
			"input errors must transition the job to Failed, not loop in Init")
		assert.Contains(t, job.GetFailReason(), "snapshot metadata JSON path")
	})

	t.Run("milvus_table_schema_error_marks_job_failed_non_retriable", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{nextID: 1000}
		scheduler := newStubScheduler()

		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID:     100,
			Schema: testMilvusTableTargetRefreshSchema(),
		})
		mt := &meta{}

		mockRead := mockey.Mock(packed.ReadMilvusTableSnapshotMetadata).
			Return(&datapb.SnapshotMetadata{
				Collection: &datapb.CollectionDescription{},
			}, nil).Build()
		defer mockRead.UnPatch()

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(collections), nil, nil)

		_, err := manager.SubmitRefreshJobWithID(ctx, 1, 100, "test_collection", "", "")
		assert.NoError(t, err)

		manager.Stop()

		job := refreshMeta.GetJob(1)
		assert.NotNil(t, job)
		assert.Equal(t, indexpb.JobState_JobStateFailed, job.GetState(),
			"schema validation failure must transition job to Failed, not loop in Init")
		assert.Contains(t, job.GetFailReason(), "explore external files failed")
		assert.Contains(t, job.GetFailReason(), "milvus-table refresh schema invalid")
	})

	t.Run("reject_when_active_job_exists", func(t *testing.T) {
		now := time.Now().UnixMilli()
		existingJob := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInProgress,
			StartTime:    now,
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{existingJob}, nil)

		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		// Create a mock meta with external collection
		collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
		collections.Insert(100, &collectionInfo{
			ID: 100,
			Schema: &schemapb.CollectionSchema{
				Name:           "test_collection",
				ExternalSource: "s3://bucket/path",
				ExternalSpec:   "iceberg",
			},
		})
		mt := &meta{}

		// Mock IsExternalCollection to return true
		mockIsExternal := mockey.Mock(typeutil.IsExternalCollection).Return(true).Build()
		defer mockIsExternal.UnPatch()

		manager := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(collections), nil, nil)

		// Submit a new job with different ID should fail
		_, err := manager.SubmitRefreshJobWithID(ctx, 2, 100, "test_collection", "", "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already in progress")

		// Verify old job was NOT changed
		oldJob := refreshMeta.GetJob(1)
		assert.NotNil(t, oldJob)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, oldJob.GetState())
	})
}

func TestExternalCollectionRefreshManager_GetJobProgress(t *testing.T) {
	ctx := context.Background()

	t.Run("job_exists", func(t *testing.T) {
		now := time.Now().UnixMilli()
		existingJob := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInit,
			StartTime:    now,
		}
		existingTask := &datapb.ExternalCollectionRefreshTask{
			TaskId:   1001,
			JobId:    1,
			State:    indexpb.JobState_JobStateInProgress,
			Progress: 50,
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{existingJob}, []*datapb.ExternalCollectionRefreshTask{existingTask})

		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)

		job, err := manager.GetJobProgress(ctx, 1)
		assert.NoError(t, err)
		assert.NotNil(t, job)
		assert.Equal(t, int64(1), job.GetJobId())
		// State should be aggregated from tasks
		assert.Equal(t, indexpb.JobState_JobStateInProgress, job.GetState())
		assert.Equal(t, int64(50), job.GetProgress())
	})

	t.Run("job_exists_no_tasks_keeps_persisted_state", func(t *testing.T) {
		now := time.Now().UnixMilli()
		existingJob := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInit,
			StartTime:    now,
		}
		// No tasks for this job
		refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{existingJob}, nil)

		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)

		job, err := manager.GetJobProgress(ctx, 1)
		assert.NoError(t, err)
		assert.NotNil(t, job)
		// When no tasks exist, should keep the persisted state (Init), not overwrite to None
		assert.Equal(t, indexpb.JobState_JobStateInit, job.GetState())
	})

	t.Run("index_wait_progress_reaches_the_client", func(t *testing.T) {
		// During the wait every task is Finished, so the task aggregate is a
		// flat 100 and says nothing about it. The job's persisted progress is
		// the indexed fraction - the only signal there is - so a client polling
		// DescribeRefresh must see that, not a constant 99.
		existingJob := &datapb.ExternalCollectionRefreshJob{
			JobId:                1,
			CollectionId:         100,
			State:                indexpb.JobState_JobStateInProgress,
			Progress:             94,
			IndexWaitStartedTime: time.Now().UnixMilli(),
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{existingJob}, tasks)
		manager := NewExternalCollectionRefreshManager(ctx, nil, newStubScheduler(), &stubAllocator{}, refreshMeta, nil, nil, nil, nil)

		job, err := manager.GetJobProgress(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, job.GetState())
		assert.Equal(t, int64(94), job.GetProgress(),
			"the index-wait progress must reach the client, not a flat 99")
	})

	t.Run("a_job_outside_the_index_wait_still_reads_as_good_as_done", func(t *testing.T) {
		// Same shape, no wait marker: the persisted number is just the last
		// ingest progress and must not be mistaken for an indexed fraction.
		existingJob := &datapb.ExternalCollectionRefreshJob{
			JobId: 1, CollectionId: 100,
			State: indexpb.JobState_JobStateInProgress, Progress: 94,
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{existingJob}, tasks)
		manager := NewExternalCollectionRefreshManager(ctx, nil, newStubScheduler(), &stubAllocator{}, refreshMeta, nil, nil, nil, nil)

		job, err := manager.GetJobProgress(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, int64(99), job.GetProgress())
	})

	t.Run("finished_tasks_do_not_expose_finished_before_job_persisted", func(t *testing.T) {
		existingJob := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInProgress,
			Progress:     80,
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
			{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{existingJob}, tasks)

		manager := NewExternalCollectionRefreshManager(ctx, nil, newStubScheduler(), &stubAllocator{}, refreshMeta, nil, nil, nil, nil)

		job, err := manager.GetJobProgress(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, job.GetState())
		assert.Equal(t, int64(99), job.GetProgress())
	})

	t.Run("persisted_failed_job_not_overwritten_by_finished_tasks", func(t *testing.T) {
		existingJob := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateFailed,
			Progress:     80,
			FailReason:   "apply failed",
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
			{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, []*datapb.ExternalCollectionRefreshJob{existingJob}, tasks)

		manager := NewExternalCollectionRefreshManager(ctx, nil, newStubScheduler(), &stubAllocator{}, refreshMeta, nil, nil, nil, nil)

		job, err := manager.GetJobProgress(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, indexpb.JobState_JobStateFailed, job.GetState())
		assert.Equal(t, int64(80), job.GetProgress())
		assert.Equal(t, "apply failed", job.GetFailReason())
	})

	t.Run("terminal_job_with_missing_committed_task_returns_error", func(t *testing.T) {
		existingJob := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateFailed,
			Progress:     40,
			FailReason:   "committed task missing",
			TaskIds:      []int64{1001},
		}
		refreshMeta := createTestRefreshMetaWithJobs(
			t,
			[]*datapb.ExternalCollectionRefreshJob{existingJob},
			nil,
		)
		manager := NewExternalCollectionRefreshManager(
			ctx,
			nil,
			newStubScheduler(),
			&stubAllocator{},
			refreshMeta,
			nil,
			nil,
			nil,
			nil,
		)

		job, err := manager.GetJobProgress(ctx, 1)
		assert.Nil(t, job)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "job 1 references missing task 1001")
	})

	t.Run("job_not_found", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)

		_, err := manager.GetJobProgress(ctx, 999)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestExternalCollectionRefreshManager_ListJobs(t *testing.T) {
	ctx := context.Background()

	t.Run("has_jobs", func(t *testing.T) {
		now := time.Now().UnixMilli()
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit, StartTime: now - 2000},
			{JobId: 2, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, StartTime: now - 1000},
			{JobId: 3, CollectionId: 100, State: indexpb.JobState_JobStateFinished, StartTime: now},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
			{TaskId: 2001, JobId: 2, State: indexpb.JobState_JobStateInProgress, Progress: 50},
			{TaskId: 3001, JobId: 3, State: indexpb.JobState_JobStateFinished, Progress: 100},
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, jobs, tasks)

		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)

		result, err := manager.ListJobs(ctx, 100)
		assert.NoError(t, err)
		assert.Len(t, result, 3)
		// Should be sorted by StartTime descending
		assert.Equal(t, int64(3), result[0].GetJobId())
		assert.Equal(t, int64(2), result[1].GetJobId())
		assert.Equal(t, int64(1), result[2].GetJobId())
	})

	t.Run("jobs_without_tasks_keep_persisted_state", func(t *testing.T) {
		now := time.Now().UnixMilli()
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit, StartTime: now},
		}
		// No tasks
		refreshMeta := createTestRefreshMetaWithJobs(t, jobs, nil)

		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)

		result, err := manager.ListJobs(ctx, 100)
		assert.NoError(t, err)
		assert.Len(t, result, 1)
		// When no tasks exist, should keep the persisted state (Init), not overwrite to None
		assert.Equal(t, indexpb.JobState_JobStateInit, result[0].GetState())
	})

	t.Run("finished_tasks_do_not_expose_finished_before_job_persisted", func(t *testing.T) {
		now := time.Now().UnixMilli()
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, Progress: 80, StartTime: now},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
			{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, jobs, tasks)

		manager := NewExternalCollectionRefreshManager(ctx, nil, newStubScheduler(), &stubAllocator{}, refreshMeta, nil, nil, nil, nil)

		result, err := manager.ListJobs(ctx, 100)
		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, result[0].GetState())
		assert.Equal(t, int64(99), result[0].GetProgress())
	})

	t.Run("broken_job_returns_error", func(t *testing.T) {
		now := time.Now().UnixMilli()
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{
				JobId:        1,
				CollectionId: 100,
				State:        indexpb.JobState_JobStateFailed,
				Progress:     20,
				FailReason:   "terminal task missing",
				TaskIds:      []int64{1001},
				StartTime:    now - 2000,
			},
			{
				JobId:        2,
				CollectionId: 100,
				State:        indexpb.JobState_JobStateInProgress,
				Progress:     30,
				StartTime:    now - 1000,
			},
			{
				JobId:        3,
				CollectionId: 100,
				State:        indexpb.JobState_JobStateFinished,
				Progress:     100,
				StartTime:    now,
			},
		}
		refreshMeta := createTestRefreshMetaWithJobs(t, jobs, nil)
		manager := NewExternalCollectionRefreshManager(
			ctx,
			nil,
			newStubScheduler(),
			&stubAllocator{},
			refreshMeta,
			nil,
			nil,
			nil,
			nil,
		)

		result, err := manager.ListJobs(ctx, 100)
		assert.Nil(t, result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "job 1 references missing task 1001")
	})

	t.Run("empty_list", func(t *testing.T) {
		refreshMeta := createTestRefreshMeta(t)
		alloc := &stubAllocator{}
		scheduler := newStubScheduler()

		manager := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)

		result, err := manager.ListJobs(ctx, 100)
		assert.NoError(t, err)
		assert.Len(t, result, 0)
	})
}

func TestSyncJobSchema_Changed(t *testing.T) {
	ctx := context.Background()

	// Setup: collection with source="s3://old", job with source="s3://new"
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(100, &collectionInfo{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name:           "test_collection",
			ExternalSource: "s3://old-bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
		},
	})
	mt := &meta{}

	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	// Track schemaUpdater calls
	updaterCalled := false
	var updatedCollID int64
	var updatedSource, updatedSpec string
	schemaUpdater := func(_ context.Context, collectionID int64, source, spec string) error {
		updaterCalled = true
		updatedCollID = collectionID
		updatedSource = source
		updatedSpec = spec
		return nil
	}

	mgr := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(collections), schemaUpdater, nil)
	concreteManager := mgr.(*externalCollectionRefreshManager)

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		ExternalSource: "s3://new-bucket/path",
		ExternalSpec:   `{"format":"parquet","version":2}`,
	}
	require.NoError(t, concreteManager.syncJobSchema(ctx, job))

	assert.True(t, updaterCalled, "schemaUpdater should be called when source/spec changed")
	assert.Equal(t, int64(100), updatedCollID)
	assert.Equal(t, "s3://new-bucket/path", updatedSource)
	assert.Equal(t, `{"format":"parquet","version":2}`, updatedSpec)
}

func TestSyncJobSchema_Unchanged(t *testing.T) {
	ctx := context.Background()

	// Setup: collection with same source/spec as job
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(100, &collectionInfo{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name:           "test_collection",
			ExternalSource: "s3://same-bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
		},
	})
	mt := &meta{}

	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	updaterCalled := false
	schemaUpdater := func(_ context.Context, _ int64, _, _ string) error {
		updaterCalled = true
		return nil
	}

	mgr := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(collections), schemaUpdater, nil)
	concreteManager := mgr.(*externalCollectionRefreshManager)

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		ExternalSource: "s3://same-bucket/path",
		ExternalSpec:   `{"format":"parquet"}`,
	}
	require.NoError(t, concreteManager.syncJobSchema(ctx, job))

	assert.False(t, updaterCalled, "schemaUpdater should NOT be called when source/spec unchanged")
}

func TestSyncJobSchema_NilUpdater(t *testing.T) {
	ctx := context.Background()

	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	// Create manager with nil schemaUpdater
	mgr := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, nil)
	concreteManager := mgr.(*externalCollectionRefreshManager)

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		ExternalSource: "s3://new-bucket/path",
		ExternalSpec:   `{"format":"parquet"}`,
	}

	// Should not panic with nil schemaUpdater
	assert.NotPanics(t, func() {
		require.NoError(t, concreteManager.syncJobSchema(ctx, job))
	})
}

func TestSyncJobSchema_CollectionLookupError(t *testing.T) {
	ctx := context.Background()

	// Empty collections - collection not found
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	mt := &meta{}

	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	updaterCalled := false
	schemaUpdater := func(_ context.Context, _ int64, _, _ string) error {
		updaterCalled = true
		return nil
	}

	mgr := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(collections), schemaUpdater, nil)
	concreteManager := mgr.(*externalCollectionRefreshManager)

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   999, // Does not exist
		ExternalSource: "s3://new-bucket/path",
	}

	assert.Error(t, concreteManager.syncJobSchema(ctx, job))
	assert.False(t, updaterCalled, "schemaUpdater should NOT be called when collection not found")
}

func TestSyncJobSchema_DroppedCollectionIsComplete(t *testing.T) {
	ctx := context.Background()
	refreshMeta := createTestRefreshMeta(t)
	manager := NewExternalCollectionRefreshManager(
		ctx,
		nil,
		newStubScheduler(),
		&stubAllocator{},
		refreshMeta,
		nil,
		func(context.Context, int64) (*collectionInfo, error) {
			return nil, merr.WrapErrCollectionNotFound(999)
		},
		func(context.Context, int64, string, string) error {
			t.Fatal("schema updater must not run for a dropped collection")
			return nil
		},
		nil,
	).(*externalCollectionRefreshManager)

	err := manager.syncJobSchema(ctx, &datapb.ExternalCollectionRefreshJob{
		JobId:        1,
		CollectionId: 999,
	})
	require.NoError(t, err)
}

func TestSyncJobSchema_UpdaterErrorCanRetry(t *testing.T) {
	ctx := context.Background()

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(100, &collectionInfo{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name:           "test_collection",
			ExternalSource: "s3://old-bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
		},
	})
	mt := &meta{}

	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	updaterCalls := 0
	schemaUpdater := func(_ context.Context, _ int64, _, _ string) error {
		updaterCalls++
		if updaterCalls == 1 {
			return errors.New("WAL broadcast failed")
		}
		return nil
	}

	mgr := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(collections), schemaUpdater, nil)
	concreteManager := mgr.(*externalCollectionRefreshManager)

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		ExternalSource: "s3://new-bucket/path",
		ExternalSpec:   `{"format":"parquet"}`,
	}

	assert.Error(t, concreteManager.syncJobSchema(ctx, job))
	assert.NoError(t, concreteManager.syncJobSchema(ctx, job))

	// The checker naturally retries while the cached schema still differs.
	// Duplicate same-value broadcasts are harmless, so no in-memory success
	// marker is needed.
	assert.Equal(t, 2, updaterCalls)
}

func TestSyncJobSchema_SourceChangedOnly(t *testing.T) {
	ctx := context.Background()

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(100, &collectionInfo{
		ID: 100,
		Schema: &schemapb.CollectionSchema{
			Name:           "test_collection",
			ExternalSource: "s3://old-bucket/path",
			ExternalSpec:   `{"format":"parquet"}`,
		},
	})
	mt := &meta{}

	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()

	updaterCalled := false
	schemaUpdater := func(_ context.Context, _ int64, _, _ string) error {
		updaterCalled = true
		return nil
	}

	mgr := NewExternalCollectionRefreshManager(ctx, mt, scheduler, alloc, refreshMeta, nil, testCollectionGetter(collections), schemaUpdater, nil)
	concreteManager := mgr.(*externalCollectionRefreshManager)

	// Only source changed, spec unchanged
	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          1,
		CollectionId:   100,
		ExternalSource: "s3://new-bucket/path",
		ExternalSpec:   `{"format":"parquet"}`, // Same as collection
	}
	require.NoError(t, concreteManager.syncJobSchema(ctx, job))

	assert.True(t, updaterCalled, "schemaUpdater should be called when only source changed")
}

// ==================== cleanupExploreTempForJob Tests ====================

// recordingChunkManager captures the prefixes passed to RemoveWithPrefix /
// Remove, and lets each call optionally return a configured error. Used by
// the cleanup-path unit tests without pulling in mockey for interface mocks
// (the manager holds a plain storage.ChunkManager field which this satisfies
// directly via method embedding).
type recordingChunkManager struct {
	storage.ChunkManager
	mu               sync.Mutex
	prefixCalls      []string
	removeCalls      []string
	prefixErr        error
	removeErr        error
	prefixBlockUntil <-chan struct{}
}

func (r *recordingChunkManager) RemoveWithPrefix(ctx context.Context, prefix string) error {
	if r.prefixBlockUntil != nil {
		select {
		case <-r.prefixBlockUntil:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.mu.Lock()
	r.prefixCalls = append(r.prefixCalls, prefix)
	r.mu.Unlock()
	return r.prefixErr
}

func (r *recordingChunkManager) Remove(ctx context.Context, key string) error {
	r.mu.Lock()
	r.removeCalls = append(r.removeCalls, key)
	r.mu.Unlock()
	return r.removeErr
}

func (r *recordingChunkManager) snapshot() (prefixes, removes []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.prefixCalls...), append([]string(nil), r.removeCalls...)
}

func newManagerWithChunkManager(t *testing.T, cm storage.ChunkManager) *externalCollectionRefreshManager {
	ctx := context.Background()
	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()
	mgr := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, cm)
	return mgr.(*externalCollectionRefreshManager)
}

func TestCleanupExploreTempForJob_Success(t *testing.T) {
	cm := &recordingChunkManager{}
	mgr := newManagerWithChunkManager(t, cm)

	assert.NoError(t, mgr.cleanupExploreTempForJob(42))

	prefixes, removes := cm.snapshot()
	assert.Equal(t, []string{"__explore_temp__/coord_42/"}, prefixes)
	assert.Equal(t, []string{"__explore_temp__/coord_42"}, removes)
}

func TestCleanupExploreTempForJob_NilChunkManager(t *testing.T) {
	mgr := newManagerWithChunkManager(t, nil)

	// Nil chunkManager path must be safe and a no-op.
	assert.NoError(t, mgr.cleanupExploreTempForJob(99))
}

func TestCleanupExploreTempForJob_RemoveWithPrefixError(t *testing.T) {
	cm := &recordingChunkManager{prefixErr: errors.New("prefix walk failed")}
	mgr := newManagerWithChunkManager(t, cm)

	assert.Error(t, mgr.cleanupExploreTempForJob(7))
	prefixes, removes := cm.snapshot()
	assert.Equal(t, []string{"__explore_temp__/coord_7/"}, prefixes)
	assert.Empty(t, removes)
}

func TestCleanupExploreTempForJob_RemoveError(t *testing.T) {
	cm := &recordingChunkManager{removeErr: errors.New("delete failed")}
	mgr := newManagerWithChunkManager(t, cm)

	assert.Error(t, mgr.cleanupExploreTempForJob(8))
	prefixes, removes := cm.snapshot()
	assert.Equal(t, []string{"__explore_temp__/coord_8/"}, prefixes)
	assert.Equal(t, []string{"__explore_temp__/coord_8"}, removes)
}

func TestCleanupExploreTempForJob_RespectsManagerCtxCancel(t *testing.T) {
	// Build a chunkManager that blocks inside RemoveWithPrefix until the
	// manager ctx is canceled. If the cleanup derives its ctx from m.ctx
	// (as intended by P2-7), cancellation must abort the call within the
	// deadline instead of hanging for the 30s fallback timeout.
	unblock := make(chan struct{})
	cm := &recordingChunkManager{prefixBlockUntil: unblock}

	ctx, cancel := context.WithCancel(context.Background())
	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()
	mgr := NewExternalCollectionRefreshManager(ctx, nil, scheduler, alloc, refreshMeta, nil, nil, nil, cm).(*externalCollectionRefreshManager)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = mgr.cleanupExploreTempForJob(123)
	}()

	// Cancel the manager ctx; RemoveWithPrefix should unblock via ctx.Done().
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(unblock)
		t.Fatal("cleanupExploreTempForJob did not honor manager ctx cancellation")
	}
	close(unblock)
}

// ==================== syncJobSchema cleanup ownership ====================

func TestSyncJobSchema_DoesNotCleanExploreTemp(t *testing.T) {
	ctx := context.Background()

	// Build a collection whose schema will change, so schemaUpdater is
	// invoked and we exercise the full defer path.
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(200, &collectionInfo{
		ID: 200,
		Schema: &schemapb.CollectionSchema{
			Name:           "coll",
			ExternalSource: "s3://old",
			ExternalSpec:   `{"format":"parquet"}`,
		},
	})
	mt := &meta{}

	cm := &recordingChunkManager{}
	refreshMeta := createTestRefreshMeta(t)
	alloc := &stubAllocator{}
	scheduler := newStubScheduler()
	schemaUpdater := func(_ context.Context, _ int64, _, _ string) error { return nil }

	mgr := NewExternalCollectionRefreshManager(
		ctx, mt, scheduler, alloc, refreshMeta, nil,
		testCollectionGetter(collections), schemaUpdater, cm,
	).(*externalCollectionRefreshManager)

	job := &datapb.ExternalCollectionRefreshJob{
		JobId:          555,
		CollectionId:   200,
		ExternalSource: "s3://new",
		ExternalSpec:   `{"format":"parquet","v":2}`,
	}
	require.NoError(t, mgr.syncJobSchema(ctx, job))

	prefixes, removes := cm.snapshot()
	assert.Empty(t, prefixes, "periodic schema notification must not repeat object cleanup")
	assert.Empty(t, removes)
}

// ==================== handleJobFailed Tests ====================

func TestHandleJobFailed_TriggersCleanup(t *testing.T) {
	cm := &recordingChunkManager{}
	mgr := newManagerWithChunkManager(t, cm)

	mgr.handleJobFailed(777)

	prefixes, removes := cm.snapshot()
	assert.Equal(t, []string{"__explore_temp__/coord_777/"}, prefixes)
	assert.Equal(t, []string{"__explore_temp__/coord_777"}, removes)
}

// ==================== handleJobCleanup Tests ====================

func TestHandleJobCleanup_RetriesCleanup(t *testing.T) {
	cm := &recordingChunkManager{}
	mgr := newManagerWithChunkManager(t, cm)

	assert.NoError(t, mgr.handleJobCleanup(321))

	prefixes, removes := cm.snapshot()
	assert.Equal(t, []string{"__explore_temp__/coord_321/"}, prefixes)
	assert.Equal(t, []string{"__explore_temp__/coord_321"}, removes)
}

func TestHandleJobCleanup_CleansUpWhenNeverHandled(t *testing.T) {
	cm := &recordingChunkManager{}
	mgr := newManagerWithChunkManager(t, cm)

	// Job never entered a terminal-state handler — retention GC is the fallback
	// path (e.g. crash between Failed transition and callback firing).
	assert.NoError(t, mgr.handleJobCleanup(654))

	prefixes, removes := cm.snapshot()
	assert.Equal(t, []string{"__explore_temp__/coord_654/"}, prefixes)
	assert.Equal(t, []string{"__explore_temp__/coord_654"}, removes)
}

func TestHandleJobCleanup_NilChunkManagerSafe(t *testing.T) {
	mgr := newManagerWithChunkManager(t, nil)

	assert.NoError(t, mgr.handleJobCleanup(1))
}

func TestCleanup_FailedAndGCBothUseIdempotentCleanup(t *testing.T) {
	cm := &recordingChunkManager{}
	mgr := newManagerWithChunkManager(t, cm)

	mgr.handleJobFailed(111)
	assert.NoError(t, mgr.handleJobCleanup(111))

	prefixes, removes := cm.snapshot()
	assert.Equal(t, []string{
		"__explore_temp__/coord_111/",
		"__explore_temp__/coord_111/",
	}, prefixes)
	assert.Equal(t, []string{
		"__explore_temp__/coord_111",
		"__explore_temp__/coord_111",
	}, removes)
}
