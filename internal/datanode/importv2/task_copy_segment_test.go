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

package importv2

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	objectstore "github.com/milvus-io/milvus/pkg/v3/objectstorage/store"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	snapshotstorage "github.com/milvus-io/milvus/pkg/v3/snapshotio"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type crossBucketCopyCall struct {
	srcBucket string
	srcObject string
	dstBucket string
	dstObject string
}

type copySegmentChunkManagerTarget struct {
	storage.ChunkManager
}

func copySegmentTaskTestDependencies(
	t *testing.T,
	req *datapb.CopySegmentRequest,
	cm storage.ChunkManager,
	targetStores ...objectstore.ObjectStore,
) (*indexpb.StorageConfig, objectstore.ObjectStore, string) {
	t.Helper()
	storageConfig := req.GetStorageConfig()
	if storageConfig == nil {
		storageConfig = &indexpb.StorageConfig{BucketName: "test-bucket", RootPath: "files"}
		req.StorageConfig = storageConfig
	}
	// AssembleCopySegmentRequest supplies the target storage root in production.
	// Keep explicit empty/dot storage roots as bucket-root configurations.
	for _, target := range req.GetTargets() {
		if target.GetTargetRootPath() == "" {
			target.TargetRootPath = storageConfig.GetRootPath()
		}
	}
	var targetStore objectstore.ObjectStore
	if len(targetStores) > 0 {
		targetStore = targetStores[0]
	} else {
		targetStore = newCopySegmentStoreMock(t, func(ctx context.Context, _, srcObject, _, dstObject string) error {
			return cm.Copy(ctx, srcObject, dstObject)
		})
	}
	return storageConfig, targetStore, storageConfig.GetBucketName()
}

func TestNewCopySegmentTask(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	mockManager := NewTaskManager()

	req := &datapb.CopySegmentRequest{
		JobID:    100,
		TaskID:   200,
		TaskSlot: 1,
		Sources: []*datapb.CopySegmentSource{
			{
				CollectionId: 111,
				PartitionId:  222,
				SegmentId:    333,
			},
		},
		Targets: []*datapb.CopySegmentTarget{
			{
				CollectionId: 444,
				PartitionId:  555,
				SegmentId:    666,
			},
			{
				CollectionId: 444,
				PartitionId:  777,
				SegmentId:    888,
			},
		},
	}

	t.Run("create task", func(t *testing.T) {
		storageConfig := &indexpb.StorageConfig{BucketName: "test-bucket"}
		task := NewCopySegmentTask(
			context.Background(),
			make(chan struct{}, 2),
			req,
			mockManager,
			mockCM,
			mockCM,
			storageConfig,
			newCopySegmentStoreMock(t, func(ctx context.Context, _, srcObject, _, dstObject string) error {
				return mockCM.Copy(ctx, srcObject, dstObject)
			}),
			storageConfig.GetBucketName(),
			storageConfig.GetBucketName(),
		)

		assert.NotNil(t, task)

		copyTask := task.(*CopySegmentTask)
		assert.Equal(t, int64(100), copyTask.GetJobID())
		assert.Equal(t, int64(200), copyTask.GetTaskID())
		assert.Equal(t, int64(444), copyTask.GetCollectionID())
		assert.Equal(t, int64(1), copyTask.GetSlots())
		assert.Equal(t, datapb.ImportTaskStateV2_Pending, copyTask.GetState())
		assert.Equal(t, CopySegmentTaskType, copyTask.GetType())

		// Verify partition IDs contain both unique partitions
		partitionIDs := copyTask.GetPartitionIDs()
		assert.Contains(t, partitionIDs, int64(555))
		assert.Contains(t, partitionIDs, int64(777))

		// Verify segment results map is initialized
		assert.Equal(t, 2, len(copyTask.segmentResults))
		assert.NotNil(t, copyTask.segmentResults[666])
		assert.NotNil(t, copyTask.segmentResults[888])
	})

	t.Run("task methods", func(t *testing.T) {
		storageConfig := &indexpb.StorageConfig{BucketName: "test-bucket"}
		task := NewCopySegmentTask(
			context.Background(),
			make(chan struct{}, 2),
			req,
			mockManager,
			mockCM,
			mockCM,
			storageConfig,
			newCopySegmentStoreMock(t, func(ctx context.Context, _, srcObject, _, dstObject string) error {
				return mockCM.Copy(ctx, srcObject, dstObject)
			}),
			storageConfig.GetBucketName(),
			storageConfig.GetBucketName(),
		)

		copyTask := task.(*CopySegmentTask)

		// Test GetVchannels (should return nil for CopySegmentTask)
		assert.Nil(t, copyTask.GetVchannels())

		// Test GetSchema (should return nil for CopySegmentTask)
		assert.Nil(t, copyTask.GetSchema())

		// Test GetBufferSize (should return 0)
		assert.Equal(t, int64(0), copyTask.GetBufferSize())

		// Test Cancel
		copyTask.Cancel()
		// Verify context is canceled
		select {
		case <-copyTask.ctx.Done():
			// Expected behavior
		default:
			t.Fatal("context should be canceled")
		}
	})

	t.Run("clone task", func(t *testing.T) {
		storageConfig := &indexpb.StorageConfig{BucketName: "test-bucket"}
		task := NewCopySegmentTask(
			context.Background(),
			make(chan struct{}, 2),
			req,
			mockManager,
			mockCM,
			mockCM,
			storageConfig,
			newCopySegmentStoreMock(t, func(ctx context.Context, _, srcObject, _, dstObject string) error {
				return mockCM.Copy(ctx, srcObject, dstObject)
			}),
			storageConfig.GetBucketName(),
			storageConfig.GetBucketName(),
		)

		cloned := task.Clone()
		assert.NotNil(t, cloned)

		copyTask := task.(*CopySegmentTask)
		clonedTask := cloned.(*CopySegmentTask)

		assert.Equal(t, copyTask.copyPermits, clonedTask.copyPermits)
		assert.Equal(t, copyTask.GetJobID(), clonedTask.GetJobID())
		assert.Equal(t, copyTask.GetTaskID(), clonedTask.GetTaskID())
		assert.Equal(t, copyTask.GetCollectionID(), clonedTask.GetCollectionID())
		assert.Equal(t, copyTask.GetState(), clonedTask.GetState())
	})
}

func TestCopySegmentTask_CleanupUsesTargetManager(t *testing.T) {
	sourceCM := &copySegmentChunkManagerTarget{}
	targetCM := &copySegmentChunkManagerTarget{}
	var sourceRemovedFiles, targetRemovedFiles [][]string
	mockRemove := mockey.Mock((*copySegmentChunkManagerTarget).MultiRemove).To(
		func(cm *copySegmentChunkManagerTarget, _ context.Context, filePaths []string) error {
			removed := append([]string(nil), filePaths...)
			if cm == sourceCM {
				sourceRemovedFiles = append(sourceRemovedFiles, removed)
			} else {
				targetRemovedFiles = append(targetRemovedFiles, removed)
			}
			return nil
		},
	).Build()
	defer mockRemove.UnPatch()

	var copyCalls []crossBucketCopyCall
	var copyMu sync.Mutex
	targetStore := newCopySegmentStoreMock(t, func(_ context.Context, srcBucket, srcObject, dstBucket, dstObject string) error {
		copyMu.Lock()
		defer copyMu.Unlock()
		copyCalls = append(copyCalls, crossBucketCopyCall{
			srcBucket: srcBucket,
			srcObject: srcObject,
			dstBucket: dstBucket,
			dstObject: dstObject,
		})
		if len(copyCalls) > 1 {
			return errors.New("copy failed")
		}
		return nil
	})
	sourceStorageConfig := &indexpb.StorageConfig{
		BucketName: "foreign-source",
		RootPath:   "foreign-root",
	}
	manager := NewTaskManager()

	req := &datapb.CopySegmentRequest{
		JobID:    100,
		TaskID:   201,
		TaskSlot: 1,
		Sources: []*datapb.CopySegmentSource{{
			CollectionId:   100,
			PartitionId:    1,
			SegmentId:      10,
			SourceRootPath: "s3://foreign-source/foreign-root",
			StorageVersion: storage.StorageV2,
			InsertBinlogs: []*datapb.FieldBinlog{{
				FieldID: 101,
				Binlogs: []*datapb.Binlog{
					{EntriesNum: 10, LogPath: "foreign-root/files/insert_log/100/1/10/101/1"},
					{EntriesNum: 20, LogPath: "foreign-root/files/insert_log/100/1/10/101/2"},
				},
			}},
		}},
		Targets: []*datapb.CopySegmentTarget{{
			CollectionId:   200,
			PartitionId:    2,
			SegmentId:      20,
			TargetRootPath: "local-root",
		}},
	}

	task := NewCopySegmentTask(
		context.Background(),
		make(chan struct{}, 2),
		req,
		manager,
		sourceCM,
		targetCM,
		sourceStorageConfig,
		targetStore,
		"foreign-source",
		"local-target",
	)

	manager.Add(task)

	futures := task.Execute()
	assert.Len(t, futures, 1)
	_, err := futures[0].Await()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "copy failed")

	assert.Empty(t, task.(*CopySegmentTask).copiedFiles)
	copyTask := manager.Get(task.GetTaskID()).(*CopySegmentTask)
	assert.Len(t, copyTask.copiedFiles, 1)
	copyTask.CleanupCopiedFiles()

	assert.Empty(t, sourceRemovedFiles)
	assert.Len(t, targetRemovedFiles, 1)
	assert.Len(t, targetRemovedFiles[0], 1)
	assert.Len(t, copyCalls, 2)
	assert.Equal(t, copyCalls[0].dstObject, targetRemovedFiles[0][0])
}

func TestCopySegmentTask_CopySegmentsAllowsManifestOnlyStorageV3(t *testing.T) {
	manager := NewTaskManager()
	source := &datapb.CopySegmentSource{SegmentId: 1, StorageVersion: storage.StorageV3, ManifestPath: "manifest"}
	target := &datapb.CopySegmentTarget{SegmentId: 2}
	req := &datapb.CopySegmentRequest{TaskID: 1, Sources: []*datapb.CopySegmentSource{source}, Targets: []*datapb.CopySegmentTarget{target}, TargetIndexes: []*datapb.CopySegmentTargetIndex{{IndexId: 777}}}
	defer mockey.Mock(prepareSegmentCopy).Return(&segmentCopyPlan{mappings: map[string]string{}}, nil).Build().UnPatch()
	var called bool
	defer mockey.Mock(completeSegmentCopy).To(func(_ context.Context, _ *indexpb.StorageConfig, gotSource *datapb.CopySegmentSource, gotTarget *datapb.CopySegmentTarget, _ *segmentCopyPlan) (*datapb.CopySegmentResult, []string, error) {
		called = true
		assert.Same(t, source, gotSource)
		assert.Equal(t, req.TargetIndexes, gotTarget.TargetIndexes)
		assert.Empty(t, target.TargetIndexes)
		return &datapb.CopySegmentResult{SegmentId: 2}, nil, nil
	}).Build().UnPatch()
	task := NewCopySegmentTask(context.Background(), make(chan struct{}, 2), req, manager, nil, nil, nil, newCopySegmentStoreMock(t, nil), "", "")
	manager.Add(task)
	assert.NoError(t, conc.BlockOnAll(task.Execute()...))
	assert.True(t, called)
}

func TestCopySegmentTaskExecute(t *testing.T) {
	for _, scenario := range []string{"no sources", "mismatched targets", "no binlogs", "insert", "multiple segments", "copy failure", "delta only", "many files"} {
		t.Run(scenario, func(t *testing.T) {
			req := copyBatchRequestForTest()
			req.Sources, req.Targets = req.Sources[:1], req.Targets[:1]
			var wantReason string
			wantCopies, wantRows := 1, int64(1)
			switch scenario {
			case "no sources":
				req.Sources, req.Targets = nil, nil
				wantReason = "no source segments"
			case "mismatched targets":
				req.Targets = nil
				wantReason = "does not match"
			case "no binlogs":
				req.Sources[0].InsertBinlogs = nil
				wantReason = "no insert/delete binlogs"
			case "many files":
				wantCopies, wantRows = 100, 10000
				logs := req.Sources[0].InsertBinlogs[0]
				logs.Binlogs = nil
				for i := 0; i < wantCopies; i++ {
					logs.Binlogs = append(logs.Binlogs, &datapb.Binlog{EntriesNum: 100, LogPath: fmt.Sprintf("files/insert_log/100/10/333/1/%d", 500000+i)})
				}
			case "multiple segments", "copy failure":
				wantCopies = 2
				req = copyBatchRequestForTest()
				if scenario == "copy failure" {
					wantReason = "copy failed"
				}
			case "delta only":
				req.Sources[0].InsertBinlogs = nil
				req.Sources[0].DeltaBinlogs = []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogPath: "files/delta_log/100/10/333/1/10001"}}}}
			}
			var copies atomic.Int64
			targetStore := newCopySegmentStoreMock(t, func(_ context.Context, _, src, _, _ string) error {
				copies.Add(1)
				if scenario == "copy failure" && strings.Contains(src, "/444/") {
					return errors.New("copy failed")
				}
				return nil
			})
			task, current := newRunningCopyTaskFixture(t, context.Background(), make(chan struct{}, 2), req, targetStore)
			if scenario == "no sources" {
				assert.Zero(t, task.GetCollectionID())
				assert.Empty(t, task.GetPartitionIDs())
				assert.Empty(t, task.GetSegmentResults())
			}
			futures := task.Execute()
			if scenario == "no sources" || scenario == "mismatched targets" {
				assert.Nil(t, futures)
			} else {
				assert.Len(t, futures, 1)
				err := conc.BlockOnAll(futures...)
				if wantReason != "" {
					assert.ErrorContains(t, err, wantReason)
				} else {
					assert.NoError(t, err)
				}
			}
			latest := current()
			if wantReason != "" {
				assert.Equal(t, datapb.ImportTaskStateV2_Failed, latest.GetState())
				assert.Contains(t, latest.GetReason(), wantReason)
				return
			}
			assert.EqualValues(t, wantCopies, copies.Load())
			assert.Len(t, latest.copiedFiles, wantCopies)
			results := latest.GetSegmentResults()
			assert.Len(t, results, len(req.Targets))
			for _, target := range req.Targets {
				result := results[target.SegmentId]
				assert.Equal(t, target.SegmentId, result.GetSegmentId())
				if scenario == "delta only" {
					assert.Zero(t, result.GetImportedRows())
					assert.Len(t, result.GetDeltalogs(), 1)
				} else {
					assert.Equal(t, wantRows, result.GetImportedRows())
				}
			}
		})
	}
}

func TestCopySegmentTaskExecute_FailureWaitsForAllWorkers(t *testing.T) {
	for _, phase := range []string{"copy", "finalization"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			failure := merr.ErrIoTooManyRequests
			if phase == "finalization" {
				failure = merr.ErrServiceUnavailable
			}
			finish := func(ctx context.Context, fails bool) error {
				if fails {
					<-started
					return failure
				}
				close(started)
				<-ctx.Done()
				close(canceled)
				<-release // A provider/native call may confirm a write after cancellation.
				return nil
			}
			targetStore := newCopySegmentStoreMock(t, func(ctx context.Context, _, src, _, _ string) error {
				if phase == "copy" {
					return finish(ctx, strings.Contains(src, "/333/"))
				}
				return nil
			})
			if phase == "finalization" {
				defer mockey.Mock(completeSegmentCopy).To(func(ctx context.Context, _ *indexpb.StorageConfig, source *datapb.CopySegmentSource, target *datapb.CopySegmentTarget, _ *segmentCopyPlan) (*datapb.CopySegmentResult, []string, error) {
					if err := finish(ctx, source.SegmentId == 333); err != nil {
						return nil, nil, err
					}
					return &datapb.CopySegmentResult{SegmentId: target.SegmentId}, []string{"late/manifest"}, nil
				}).Build().UnPatch()
			}
			task, current := newRunningCopyTaskFixture(t, ctx, make(chan struct{}, 2), copyBatchRequestForTest(), targetStore)
			futures := task.Execute()
			select {
			case <-canceled:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			assert.Equal(t, datapb.ImportTaskStateV2_InProgress, current().GetState())
			close(release)
			assert.ErrorIs(t, conc.BlockOnAll(futures...), failure)
			latest := current()
			assert.Equal(t, datapb.ImportTaskStateV2_Failed, latest.GetState())
			expected := []string{"files/insert_log/200/20/777/1/10002"}
			if phase == "finalization" {
				expected = append(expected, "files/insert_log/200/20/666/1/10001", "late/manifest")
			}
			assert.ElementsMatch(t, expected, latest.copiedFiles)
		})
	}
}

// newRunningCopyTaskFixture registers a real task and reads its persisted clone.
func newRunningCopyTaskFixture(t *testing.T, ctx context.Context, permits chan struct{}, req *datapb.CopySegmentRequest, store objectstore.ObjectStore) (*CopySegmentTask, func() *CopySegmentTask) {
	manager := NewTaskManager()
	task := NewCopySegmentTask(ctx, permits, req, manager, nil, nil, nil, store, "", "").(*CopySegmentTask)
	manager.Add(task)
	t.Cleanup(task.Cancel)
	return task, func() *CopySegmentTask { return manager.Get(req.TaskID).(*CopySegmentTask) }
}

// Each source references one physical file; targets use a different collection.
func copyBatchRequestForTest() *datapb.CopySegmentRequest {
	req := &datapb.CopySegmentRequest{TaskID: 1}
	for i, id := range []int64{333, 444} {
		req.Sources = append(req.Sources, &datapb.CopySegmentSource{CollectionId: 100, PartitionId: 10, SegmentId: id,
			InsertBinlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogPath: fmt.Sprintf("files/insert_log/100/10/%d/1/%d", id, 10001+i), EntriesNum: 1}}}}})
		req.Targets = append(req.Targets, &datapb.CopySegmentTarget{CollectionId: 200, PartitionId: 20, SegmentId: int64(666 + 111*i), TargetRootPath: "files"})
	}
	return req
}

func TestCopySegmentTaskGetSegmentResults(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	mockManager := NewTaskManager()

	req := &datapb.CopySegmentRequest{
		JobID:    100,
		TaskID:   300,
		TaskSlot: 1,
		Targets: []*datapb.CopySegmentTarget{
			{CollectionId: 444, PartitionId: 555, SegmentId: 666},
			{CollectionId: 444, PartitionId: 555, SegmentId: 777},
		},
	}

	storageConfig, targetStore, bucket := copySegmentTaskTestDependencies(t, req, mockCM)
	task := NewCopySegmentTask(context.Background(), make(chan struct{}, 2), req, mockManager, mockCM, mockCM, storageConfig, targetStore, bucket, bucket)
	copyTask := task.(*CopySegmentTask)

	t.Run("initial segment results", func(t *testing.T) {
		results := copyTask.GetSegmentResults()
		assert.Equal(t, 2, len(results))

		// Verify initial state
		assert.Equal(t, int64(666), results[666].SegmentId)
		assert.Equal(t, int64(0), results[666].ImportedRows)
		assert.Equal(t, 0, len(results[666].Binlogs))

		assert.Equal(t, int64(777), results[777].SegmentId)
		assert.Equal(t, int64(0), results[777].ImportedRows)
		assert.Equal(t, 0, len(results[777].Binlogs))
	})

	t.Run("update segment results", func(t *testing.T) {
		// Manually update segment result
		mockManager.Add(task)

		newResult := &datapb.CopySegmentResult{
			SegmentId:    666,
			ImportedRows: 5000,
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 100,
					Binlogs: []*datapb.Binlog{
						{
							EntriesNum: 5000,
							LogPath:    "files/insert_log/444/555/666/100/100001",
						},
					},
				},
			},
		}

		mockManager.Update(task.GetTaskID(), UpdateSegmentResult(newResult))

		// Verify update
		updatedTask := mockManager.Get(task.GetTaskID()).(*CopySegmentTask)
		results := updatedTask.GetSegmentResults()
		assert.Equal(t, int64(5000), results[666].ImportedRows)
		assert.Equal(t, 1, len(results[666].Binlogs))
	})
}

func TestCopySegmentTaskStateManagement(t *testing.T) {
	mockCM := mocks.NewChunkManager(t)
	mockManager := NewTaskManager()

	req := &datapb.CopySegmentRequest{
		JobID:    100,
		TaskID:   400,
		TaskSlot: 1,
		Sources: []*datapb.CopySegmentSource{
			{CollectionId: 111, PartitionId: 222, SegmentId: 333},
		},
		Targets: []*datapb.CopySegmentTarget{
			{CollectionId: 444, PartitionId: 555, SegmentId: 666},
		},
	}

	storageConfig, targetStore, bucket := copySegmentTaskTestDependencies(t, req, mockCM)
	task := NewCopySegmentTask(context.Background(), make(chan struct{}, 2), req, mockManager, mockCM, mockCM, storageConfig, targetStore, bucket, bucket)
	mockManager.Add(task)

	t.Run("initial state", func(t *testing.T) {
		assert.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
		assert.Equal(t, "", task.GetReason())
	})

	t.Run("update state to InProgress", func(t *testing.T) {
		mockManager.Update(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_InProgress))
		updatedTask := mockManager.Get(task.GetTaskID())
		assert.Equal(t, datapb.ImportTaskStateV2_InProgress, updatedTask.GetState())
	})

	t.Run("update state to Failed with reason", func(t *testing.T) {
		reason := "test failure reason"
		mockManager.Update(task.GetTaskID(),
			UpdateState(datapb.ImportTaskStateV2_Failed),
			UpdateReason(reason))

		updatedTask := mockManager.Get(task.GetTaskID())
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, updatedTask.GetState())
		assert.Equal(t, reason, updatedTask.GetReason())
	})

	t.Run("update state to Completed", func(t *testing.T) {
		mockManager.Update(task.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed))
		updatedTask := mockManager.Get(task.GetTaskID())
		assert.Equal(t, datapb.ImportTaskStateV2_Completed, updatedTask.GetState())
	})
}

func TestCopySegmentTaskWithIndexFiles(t *testing.T) {
	req := copyBatchRequestForTest()
	req.Sources, req.Targets = req.Sources[:1], req.Targets[:1]
	source := req.Sources[0]
	binlog := func(kind string) []*datapb.FieldBinlog {
		return []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogPath: "files/" + kind + "/100/10/333/100/20001"}}}}
	}
	source.StatsBinlogs = binlog("stats_log")
	source.DeltaBinlogs = binlog("delta_log")
	source.Bm25Binlogs = binlog("bm25_stats")
	source.IndexFiles = []*indexpb.IndexFilePathInfo{{FieldID: 100, IndexID: 1001, BuildID: 1002,
		IndexFilePaths: metautil.NewIndexPathBuilder("files", indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED, 100, 10, 333, 1002, 1).BuildFilePaths([]string{"index1"})}}
	source.TextIndexFiles = map[int64]*datapb.TextIndexStats{100: {FieldID: 100, Files: []string{"files/text_log/123/1/100/10/333/100/text1"}}}
	source.JsonKeyIndexFiles = map[int64]*datapb.JsonKeyStats{101: {FieldID: 101, Files: []string{"files/json_key_index_log/123/1/100/10/333/101/json1"}}}
	var copies atomic.Int64
	store := newCopySegmentStoreMock(t, func(context.Context, string, string, string, string) error { copies.Add(1); return nil })
	task, current := newRunningCopyTaskFixture(t, context.Background(), make(chan struct{}, 2), req, store)
	assert.NoError(t, conc.BlockOnAll(task.Execute()...))
	latest := current()
	result := latest.GetSegmentResults()[666]
	if !assert.NotNil(t, result) {
		return
	}
	assert.Len(t, result.Binlogs, 1)
	assert.Len(t, result.Statslogs, 1)
	assert.Len(t, result.Deltalogs, 1)
	assert.Len(t, result.Bm25Logs, 1)
	assert.Len(t, result.IndexInfos, 1)
	assert.Len(t, result.TextIndexInfos, 1)
	assert.Len(t, result.JsonKeyIndexInfos, 1)
	assert.EqualValues(t, 7, copies.Load())
	assert.Len(t, latest.copiedFiles, 7)
}

func TestCopySegmentTaskEdgeCases(t *testing.T) {
	req := copyBatchRequestForTest()
	req.TaskSlot = 0
	req.Sources = req.Sources[:1]
	req.Targets = []*datapb.CopySegmentTarget{
		{CollectionId: 200, PartitionId: 20, SegmentId: 666},
		{CollectionId: 200, PartitionId: 20, SegmentId: 777},
		{CollectionId: 200, PartitionId: 20, SegmentId: 888},
	}
	task, _ := newRunningCopyTaskFixture(t, context.Background(), make(chan struct{}, 2), req, nil)
	assert.Zero(t, task.GetSlots())
	assert.Equal(t, []int64{20}, task.GetPartitionIDs())
	assert.Len(t, task.GetSegmentResults(), 3)
}

func TestCopySegmentTask_UpdateCopiedFiles(t *testing.T) {
	newTask := func() (TaskManager, *CopySegmentTask) {
		manager := NewTaskManager()
		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 1000,
			Targets: []*datapb.CopySegmentTarget{{
				CollectionId: 444,
				PartitionId:  555,
				SegmentId:    666,
			}},
		}
		task := NewCopySegmentTask(context.Background(), make(chan struct{}, 2), req, manager, nil, nil, nil, nil, "", "").(*CopySegmentTask)
		manager.Add(task)
		return manager, task
	}

	t.Run("preserves files across manager clones", func(t *testing.T) {
		manager, original := newTask()
		manager.Update(original.GetTaskID(), UpdateCopiedFiles([]string{"10001", "10002"}))
		firstSnapshot := manager.Get(original.GetTaskID()).(*CopySegmentTask)

		assert.Empty(t, original.copiedFiles)
		assert.Equal(t, []string{"10001", "10002"}, firstSnapshot.copiedFiles)

		manager.Update(original.GetTaskID(),
			UpdateCopiedFiles([]string{"10003", "10004"}),
			UpdateState(datapb.ImportTaskStateV2_Failed),
		)
		latest := manager.Get(original.GetTaskID()).(*CopySegmentTask)

		assert.Equal(t, []string{"10001", "10002"}, firstSnapshot.copiedFiles)
		assert.Equal(t, []string{"10001", "10002", "10003", "10004"}, latest.copiedFiles)
		assert.Equal(t, datapb.ImportTaskStateV2_Failed, latest.GetState())
	})

	t.Run("serializes concurrent updates", func(t *testing.T) {
		manager, task := newTask()
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				manager.Update(task.GetTaskID(), UpdateCopiedFiles([]string{fmt.Sprintf("file%d.log", id)}))
			}(i)
		}
		wg.Wait()

		latest := manager.Get(task.GetTaskID()).(*CopySegmentTask)
		assert.Len(t, latest.copiedFiles, 10)
	})
}

func TestCopySegmentTask_CleanupCopiedFiles(t *testing.T) {
	newTask := func(targetCM storage.ChunkManager) (TaskManager, *CopySegmentTask) {
		manager := NewTaskManager()
		req := &datapb.CopySegmentRequest{
			JobID:  100,
			TaskID: 1000,
			Targets: []*datapb.CopySegmentTarget{{
				CollectionId:   444,
				PartitionId:    555,
				SegmentId:      666,
				TargetRootPath: "files",
			}},
		}
		task := NewCopySegmentTask(context.Background(), make(chan struct{}, 2), req, manager, nil, targetCM, nil, nil, "", "").(*CopySegmentTask)
		manager.Add(task)
		return manager, task
	}

	for _, scenario := range []string{"files", "empty", "remove failure", "repeat"} {
		t.Run(scenario, func(t *testing.T) {
			targetCM := &copySegmentChunkManagerTarget{}
			var calls int
			files := []string{
				"files/insert_log/444/555/666/1/10001",
				"files/insert_log/444/555/666/1/10002",
				"files/insert_log/444/555/666/1/10002",
			}
			expected := files[:2]
			if scenario == "empty" {
				files, expected = nil, nil
			}
			defer mockey.Mock((*copySegmentChunkManagerTarget).MultiRemove).To(
				func(_ *copySegmentChunkManagerTarget, _ context.Context, removed []string) error {
					calls++
					assert.Equal(t, expected, removed)
					if scenario == "remove failure" {
						return errors.New("cleanup failed")
					}
					return nil
				}).Build().UnPatch()
			manager, original := newTask(targetCM)
			manager.Update(original.GetTaskID(), UpdateCopiedFiles(files))
			task := manager.Get(original.GetTaskID()).(*CopySegmentTask)
			assert.NotPanics(t, task.CleanupCopiedFiles)
			if scenario == "repeat" {
				task.CleanupCopiedFiles()
				assert.Equal(t, 2, calls)
			} else if scenario == "empty" {
				assert.Zero(t, calls)
			} else {
				assert.Equal(t, 1, calls)
			}
			assert.Equal(t, files, task.copiedFiles, "cleanup keeps the manager-owned inventory")
		})
	}

	t.Run("shared LOB namespaces", func(t *testing.T) {
		for _, root := range []string{"files", "", ".", "/var/lib/milvus", "files/lobs/prefix"} {
			t.Run(root, func(t *testing.T) {
				targetCM := &copySegmentChunkManagerTarget{}
				var removed []string
				defer mockey.Mock((*copySegmentChunkManagerTarget).MultiRemove).To(
					func(_ *copySegmentChunkManagerTarget, _ context.Context, files []string) error {
						removed = append([]string(nil), files...)
						return nil
					}).Build().UnPatch()
				manager, task := newTask(targetCM)
				task.req.Targets[0].TargetRootPath = root
				task.req.Targets = append(task.req.Targets, &datapb.CopySegmentTarget{
					CollectionId: 444, PartitionId: 556, SegmentId: 667, TargetRootPath: root,
				})
				lob := path.Join(root, "insert_log/444/555/lobs/101/_data/shared.vx")
				otherPartitionLOB := path.Join(root, "insert_log/444/556/lobs/101/_data/shared.vx")
				private := []string{
					path.Join(root, "insert_log/444/555/666/manifest.1"),
					path.Join(root, "insert_log/444/555/666/lobs/index-file"),
					path.Join(root, "insert_log/444/555/lobs-extra/101/private"),
					path.Join(root, "index_files/444/555/666/lobs/index-file"),
				}
				manager.Update(task.GetTaskID(), UpdateCopiedFiles(append([]string{lob, otherPartitionLOB, lob}, private...)))
				task = manager.Get(task.GetTaskID()).(*CopySegmentTask)
				task.CleanupCopiedFiles()
				assert.Equal(t, private, removed)
				assert.Len(t, task.copiedFiles, 7, "cleanup must not mutate the manager-owned file list")
			})
		}
	})

	t.Run("only shared LOBs", func(t *testing.T) {
		targetCM := &copySegmentChunkManagerTarget{}
		defer mockey.Mock((*copySegmentChunkManagerTarget).MultiRemove).To(
			func(_ *copySegmentChunkManagerTarget, _ context.Context, _ []string) error {
				t.Fatal("shared LOBs must never enter task deletion")
				return nil
			}).Build().UnPatch()
		manager, task := newTask(targetCM)
		manager.Update(task.GetTaskID(), UpdateCopiedFiles([]string{"files/insert_log/444/555/lobs/101/_data/shared.vx"}))
		manager.Get(task.GetTaskID()).(*CopySegmentTask).CleanupCopiedFiles()
	})

	t.Run("failed task preserves completed task's shared LOB", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
		manager := NewTaskManager()
		var lobPaths []string
		for i := int64(0); i < 2; i++ {
			target := &datapb.CopySegmentTarget{CollectionId: 444, PartitionId: 555, SegmentId: 666 + i, TargetRootPath: root}
			lob, err := generateTargetLOBPath("source/insert_log/111/222/lobs/101/_data/shared.vx",
				&datapb.CopySegmentSource{SourceRootPath: "source"}, target)
			assert.NoError(t, err)
			lobPaths = append(lobPaths, lob)
			req := &datapb.CopySegmentRequest{JobID: 100, TaskID: 1000 + i, Targets: []*datapb.CopySegmentTarget{target}}
			manager.Add(NewCopySegmentTask(ctx, make(chan struct{}, 1), req, manager, nil, cm, nil, nil, "", ""))
		}
		assert.Equal(t, lobPaths[0], lobPaths[1], "different target segments share the same partition LOB")
		private := path.Join(root, "insert_log/444/555/667/manifest.1")
		for _, file := range []string{lobPaths[0], private} {
			assert.NoError(t, cm.Write(ctx, file, []byte("copied")))
		}
		manager.Update(1000, UpdateCopiedFiles(lobPaths[:1]), UpdateState(datapb.ImportTaskStateV2_Completed))
		manager.Update(1001, UpdateCopiedFiles([]string{lobPaths[1], private}), UpdateState(datapb.ImportTaskStateV2_Failed))
		failed := manager.Get(1001).(*CopySegmentTask)
		for i := 0; i < 2; i++ {
			failed.CleanupCopiedFiles()
			exists, err := cm.Exist(ctx, lobPaths[0])
			assert.NoError(t, err)
			assert.True(t, exists)
			exists, err = cm.Exist(ctx, private)
			assert.NoError(t, err)
			assert.False(t, exists)
		}
	})
}

func TestCopySegmentTask_TargetCollision(t *testing.T) {
	defer mockey.Mock(prepareSegmentCopy).Return(&segmentCopyPlan{mappings: map[string]string{"one": "dst", "two": "dst"}}, nil).Build().UnPatch()
	store := newCopySegmentStoreMock(t, func(context.Context, string, string, string, string) error {
		t.Error("conflicting targets must be rejected before copying")
		return nil
	})
	task, current := newRunningCopyTaskFixture(t, context.Background(), make(chan struct{}, 2), copyBatchRequestForTest(), store)
	assert.ErrorIs(t, conc.BlockOnAll(task.Execute()...), merr.ErrDataIntegrity)
	assert.Equal(t, datapb.ImportTaskStateV2_Failed, current().GetState())
	assert.Empty(t, current().copiedFiles)
}

func TestRunCopySegmentStage_CancellationAndPool(t *testing.T) {
	t.Run("canceled before admission", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := runCopySegmentStage(ctx, 2, func(context.Context, int) error { t.Error("unexpected work"); return nil })
		assert.ErrorIs(t, err, context.Canceled)
	})
	t.Run("canceled queued work drains active calls", func(t *testing.T) {
		pool := conc.NewPool[any](1)
		defer pool.Release()
		defer mockey.Mock(GetExecPool).Return(pool).Build().UnPatch()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		var calls atomic.Int64
		go func() {
			done <- runCopySegmentStage(ctx, 3, func(context.Context, int) error {
				calls.Add(1)
				close(started)
				<-release // Native calls may not observe cancellation until they return.
				return nil
			})
		}()
		<-started
		assert.Eventually(t, func() bool { return pool.Waiting() == 1 }, time.Second, time.Millisecond)
		cancel()
		select {
		case <-done:
			t.Error("returned with a native call still active")
		default:
		}
		close(release)
		assert.ErrorIs(t, <-done, context.Canceled)
		assert.EqualValues(t, 1, calls.Load())
	})
	t.Run("pool admission failure", func(t *testing.T) {
		pool := conc.NewPool[any](1)
		pool.Release()
		defer mockey.Mock(GetExecPool).Return(pool).Build().UnPatch()
		assert.Error(t, runCopySegmentStage(context.Background(), 1, func(context.Context, int) error { t.Error("unexpected work"); return nil }))
	})
}

func TestCopySegmentTask_SegmentPipeline(t *testing.T) {
	// Initialize configuration before starting the bounded execution deadline.
	paramtable.Init()
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("late-prepare-failure-%t", fail), func(t *testing.T) {
			pool := conc.NewPool[any](2)
			defer pool.Release()
			defer mockey.Mock(GetExecPool).Return(pool).Build().UnPatch()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			blocked, release, firstCompleted := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			defer mockey.Mock(prepareSegmentCopy).To(func(ctx context.Context, _ snapshotstorage.ChunkManager, _ *indexpb.StorageConfig, source *datapb.CopySegmentSource, _ *datapb.CopySegmentTarget) (*segmentCopyPlan, error) {
				if source.SegmentId == 444 {
					close(blocked)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					if fail {
						return nil, merr.ErrIoKeyNotFound
					}
				}
				return &segmentCopyPlan{mappings: map[string]string{fmt.Sprint(source.SegmentId): fmt.Sprint(source.SegmentId)}}, nil
			}).Build().UnPatch()
			var copies atomic.Int64
			targetStore := newCopySegmentStoreMock(t, func(context.Context, string, string, string, string) error { copies.Add(1); return nil })
			defer mockey.Mock(completeSegmentCopy).To(func(_ context.Context, _ *indexpb.StorageConfig, source *datapb.CopySegmentSource, target *datapb.CopySegmentTarget, _ *segmentCopyPlan) (*datapb.CopySegmentResult, []string, error) {
				if source.SegmentId == 333 {
					close(firstCompleted)
				}
				return &datapb.CopySegmentResult{SegmentId: target.SegmentId}, nil, nil
			}).Build().UnPatch()
			permits := make(chan struct{}, 1)
			task, current := newRunningCopyTaskFixture(t, ctx, permits, copyBatchRequestForTest(), targetStore)
			futures := task.Execute()
			defer func() {
				unblock()
				cancel()
				_ = conc.BlockOnAll(futures...)
			}()
			select {
			case <-blocked:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case <-firstCompleted:
			case <-ctx.Done():
				t.Fatal("first segment waited for all preparations")
			}
			assert.EqualValues(t, 1, copies.Load())
			unblock()
			err := conc.BlockOnAll(futures...)
			latest := current()
			if fail {
				assert.ErrorIs(t, err, merr.ErrIoKeyNotFound)
				assert.Equal(t, []string{"333"}, latest.copiedFiles)
				assert.Equal(t, datapb.ImportTaskStateV2_Failed, latest.GetState())
			} else {
				assert.NoError(t, err)
				assert.ElementsMatch(t, []string{"333", "444"}, latest.copiedFiles)
			}
			assert.Empty(t, permits)
		})
	}
}

func TestCopySegmentTask_RequestConcurrency(t *testing.T) {
	for _, workers := range []int{1, 2} {
		t.Run(fmt.Sprintf("execution-workers-%d", workers), func(t *testing.T) {
			pool := conc.NewPool[any](workers)
			defer pool.Release()
			defer mockey.Mock(GetExecPool).Return(pool).Build().UnPatch()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req := copyBatchRequestForTest()
			req.CopyConcurrency = 1
			permits := snapshotstorage.NewTaskCopyPermits(req.GetCopyConcurrency())
			started, release := make(chan struct{}, 2), make(chan struct{})
			targetStore := newCopySegmentStoreMock(t, func(ctx context.Context, _, _, _, _ string) error {
				started <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			task, current := newRunningCopyTaskFixture(t, ctx, permits, req, targetStore)
			assert.Equal(t, task.copyPermits, task.Clone().(*CopySegmentTask).copyPermits)
			futures := task.Execute()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case <-started:
				t.Error("task exceeded request concurrency")
			case <-time.After(20 * time.Millisecond):
			}
			close(release)
			assert.NoError(t, conc.BlockOnAll(futures...))
			latest := current()
			assert.Len(t, latest.copiedFiles, 2)
			for _, result := range latest.GetSegmentResults() {
				assert.EqualValues(t, 1, result.GetImportedRows())
			}
			assert.Empty(t, permits)
		})
	}
}
