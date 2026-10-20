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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestAssembleCopySegmentRequest_SourceSegmentNotFound(t *testing.T) {
	// Arrange: snapshot data has segment 1, but task maps from segment 999 which doesn't exist
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, PartitionId: 10},
		},
	}

	sm := &snapshotMeta{}
	// Mock ReadSnapshotData to return our test data
	mock1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(snapshotData, nil).Build()
	defer mock1.UnPatch()

	task := &copySegmentTask{
		ctx:          context.Background(),
		snapshotMeta: sm,
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 100,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 999, TargetSegmentId: 2001, PartitionId: 10}, // 999 not in snapshot
		},
	})

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: 100,
			SnapshotName: "test_snapshot",
		},
		tr:            timerecord.NewTimeRecorder("test_job"),
		snapshotCache: &copySegmentSnapshotCache{},
	}

	// Act
	req, err := AssembleCopySegmentRequest(task, job)

	// Assert
	assert.Nil(t, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "source segment 999 not found")
}

func TestAssembleCopySegmentRequest_MarksExternalCollection(t *testing.T) {
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Collection: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "external_pk", ExternalField: "pk"},
				},
			},
		},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:   1,
				PartitionId: 10,
				IndexFiles: []*indexpb.IndexFilePathInfo{{
					IndexFilePaths: []string{
						"source-root/files/files/index_files/1001/2001/3001/index",
					},
				}},
			},
		},
	}

	sm := &snapshotMeta{}
	mockReadSnapshot := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(snapshotData, nil).Build()
	defer mockReadSnapshot.UnPatch()

	mockStorageConfig := mockey.Mock(createStorageConfig).Return(nil).Build()
	defer mockStorageConfig.UnPatch()

	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	task.snapshotMeta = sm
	task.alloc = &embeddedAllocator{}
	mockAlloc := mockey.Mock((*embeddedAllocator).AllocID).Return(typeutil.UniqueID(9001), nil).Build()
	defer mockAlloc.UnPatch()
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: 100,
			SnapshotName: "test_snapshot",
		},
		tr:            timerecord.NewTimeRecorder("test_job"),
		snapshotCache: &copySegmentSnapshotCache{},
	}

	req, err := AssembleCopySegmentRequest(task, job)
	assert.NoError(t, err)
	assert.Len(t, req.GetSources(), 1)
	assert.True(t, req.GetSources()[0].GetIsExternalCollection())
}

func TestAssembleCopySegmentRequest_ExternalSnapshotRootRemap(t *testing.T) {
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Layout: datapb.SnapshotLayout_SnapshotLayoutSelfContained,
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, PartitionId: 10},
		},
	}

	sm := &snapshotMeta{}
	sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
		ForeignBucket: "bucket",
		ForeignCM:     sourceCM,
	}, nil).Build()
	defer mockResolve.UnPatch()
	mockReadExternal := mockey.Mock((*snapshotMeta).ReadExternalSnapshotDataWithChunkManager).To(
		func(_ *snapshotMeta, _ context.Context, gotCM storage.ChunkManager, snapshotS3Location string, includeSegments bool) (*snapshotstorage.SnapshotData, error) {
			assert.Same(t, sourceCM, gotCM)
			assert.Equal(t, "s3://bucket/source-root/snapshots/100/metadata/1.json", snapshotS3Location)
			assert.True(t, includeSegments)
			return snapshotData, nil
		}).Build()
	defer mockReadExternal.UnPatch()
	mockReadLocal := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(nil, errors.New("must not read local snapshot")).Build()
	defer mockReadLocal.UnPatch()
	mockStorage := mockey.Mock(createStorageConfig).Return(&indexpb.StorageConfig{RootPath: "target-root"}).Build()
	defer mockStorage.UnPatch()

	task := &copySegmentTask{
		ctx:          context.Background(),
		snapshotMeta: sm,
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 200,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 20},
		},
	})

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              100,
			CollectionId:       200,
			SnapshotName:       "test_snapshot",
			External:           true,
			SnapshotS3Location: "s3://bucket/source-root/snapshots/100/metadata/1.json",
		},
		tr:            timerecord.NewTimeRecorder("test_job"),
		snapshotCache: &copySegmentSnapshotCache{},
	}

	req, err := AssembleCopySegmentRequest(task, job)

	require.NoError(t, err)
	require.NotNil(t, req)
	assert.Len(t, req.Sources, 1)
	assert.Len(t, req.Targets, 1)
	assert.Equal(t, "s3://bucket/source-root/files", req.Sources[0].GetSourceRootPath())
	assert.Equal(t, "target-root", req.Targets[0].GetTargetRootPath())
}

func TestAssembleCopySegmentRequest_ExternalReferencedSnapshotRootRemap(t *testing.T) {
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Layout: datapb.SnapshotLayout_SnapshotLayoutReferenced,
		Segments: []*datapb.SegmentDescription{
			{SegmentId: 1, PartitionId: 10},
		},
	}

	sm := &snapshotMeta{}
	sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
		ForeignBucket: "bucket",
		ForeignCM:     sourceCM,
	}, nil).Build()
	defer mockResolve.UnPatch()
	mockReadExternal := mockey.Mock((*snapshotMeta).ReadExternalSnapshotDataWithChunkManager).Return(snapshotData, nil).Build()
	defer mockReadExternal.UnPatch()
	mockStorage := mockey.Mock(createStorageConfig).Return(&indexpb.StorageConfig{RootPath: "target-root"}).Build()
	defer mockStorage.UnPatch()

	task := &copySegmentTask{
		ctx:          context.Background(),
		snapshotMeta: sm,
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 200,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 20},
		},
	})

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              100,
			CollectionId:       200,
			SnapshotName:       "test_snapshot",
			External:           true,
			SnapshotS3Location: "s3://bucket/source-root/snapshots/100/metadata/1.json",
		},
		tr:            timerecord.NewTimeRecorder("test_job"),
		snapshotCache: &copySegmentSnapshotCache{},
	}

	req, err := AssembleCopySegmentRequest(task, job)

	require.NoError(t, err)
	require.Len(t, req.GetSources(), 1)
	assert.Equal(t, "s3://bucket/source-root", req.GetSources()[0].GetSourceRootPath())
	assert.Equal(t, "target-root", req.GetTargets()[0].GetTargetRootPath())
}

func TestAssembleCopySegmentRequest_ExternalSnapshotCarriesForeignSpecAndRef(t *testing.T) {
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Layout: datapb.SnapshotLayout_SnapshotLayoutSelfContained,
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:   1,
				PartitionId: 10,
				IndexFiles: []*indexpb.IndexFilePathInfo{{
					IndexFilePaths: []string{
						"source-root/files/files/index_files/1001/2001/3001/index",
					},
				}},
			},
		},
	}

	sm := &snapshotMeta{}
	sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).To(
		func(
			_ context.Context,
			_ *objectstorage.Config,
			direction snapshotstorage.Direction,
			foreignURI string,
			externalSpec string,
		) (*snapshotstorage.ResolvedForeignStorage, error) {
			assert.Equal(t, snapshotstorage.DirectionRestore, direction)
			assert.Equal(t, "s3://bucket/source-root/snapshots/100/metadata/1.json", foreignURI)
			assert.Equal(t, `{"extfs":{"region":"us-west-2"}}`, externalSpec)
			return &snapshotstorage.ResolvedForeignStorage{
				ForeignBucket: "bucket",
				ForeignCM:     sourceCM,
			}, nil
		}).Build()
	defer mockResolve.UnPatch()
	mockReadExternal := mockey.Mock((*snapshotMeta).ReadExternalSnapshotDataWithChunkManager).To(
		func(_ *snapshotMeta, _ context.Context, gotCM storage.ChunkManager, snapshotS3Location string, includeSegments bool) (*snapshotstorage.SnapshotData, error) {
			assert.Same(t, sourceCM, gotCM)
			assert.Equal(t, "s3://bucket/source-root/snapshots/100/metadata/1.json", snapshotS3Location)
			assert.True(t, includeSegments)
			return snapshotData, nil
		}).Build()
	defer mockReadExternal.UnPatch()
	mockStorage := mockey.Mock(createStorageConfig).Return(&indexpb.StorageConfig{RootPath: "target-root"}).Build()
	defer mockStorage.UnPatch()

	nextID := int64(9001)
	alloc := &embeddedAllocator{}
	mockAlloc := mockey.Mock((*embeddedAllocator).AllocID).To(func(ctx context.Context) (typeutil.UniqueID, error) {
		id := nextID
		nextID++
		return id, nil
	}).Build()
	defer mockAlloc.UnPatch()

	task := &copySegmentTask{
		ctx:          context.Background(),
		snapshotMeta: sm,
		alloc:        alloc,
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 200,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 20},
		},
	})

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:              100,
			CollectionId:       200,
			SnapshotName:       "test_snapshot",
			External:           true,
			SnapshotS3Location: "s3://bucket/source-root/snapshots/100/metadata/1.json",
			ExternalSpec:       `{"extfs":{"region":"us-west-2"}}`,
		},
		tr:            timerecord.NewTimeRecorder("test_job"),
		snapshotCache: &copySegmentSnapshotCache{},
	}

	req, err := AssembleCopySegmentRequest(task, job)

	require.NoError(t, err)
	require.NotNil(t, req)
	assert.Equal(t, `{"extfs":{"region":"us-west-2"}}`, req.GetExternalSpec())
	require.Len(t, req.GetSources(), 1)
	assert.Equal(t, "s3://bucket/source-root/files", req.GetSources()[0].GetSourceRootPath())
}

func TestAssembleCopySegmentRequest_ExternalSnapshotFingerprint(t *testing.T) {
	const snapshotLocation = "s3://bucket/source-root/snapshots/100/metadata/1.json"
	newSnapshot := func() *snapshotstorage.SnapshotData {
		return &snapshotstorage.SnapshotData{
			SnapshotInfo: &datapb.SnapshotInfo{
				Id:           1,
				CollectionId: 100,
				Name:         "test_snapshot",
				S3Location:   snapshotLocation,
			},
			Collection: &datapb.CollectionDescription{},
			Layout:     datapb.SnapshotLayout_SnapshotLayoutReferenced,
			Segments: []*datapb.SegmentDescription{{
				SegmentId:   1,
				PartitionId: 10,
			}},
		}
	}
	newTask := func(sm *snapshotMeta) *copySegmentTask {
		task := &copySegmentTask{
			ctx:          context.Background(),
			snapshotMeta: sm,
			tr:           timerecord.NewTimeRecorder("test"),
			times:        taskcommon.NewTimes(),
		}
		task.task.Store(&datapb.CopySegmentTask{
			TaskId:       1001,
			JobId:        100,
			CollectionId: 200,
			IdMappings: []*datapb.CopySegmentIDMapping{{
				SourceSegmentId: 1,
				TargetSegmentId: 2001,
				PartitionId:     20,
			}},
		})
		return task
	}

	t.Run("mismatch is rejected", func(t *testing.T) {
		snapshotData := newSnapshot()
		sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
			ForeignBucket: "bucket",
			ForeignCM:     sourceCM,
		}, nil).Build()
		defer mockResolve.UnPatch()
		mockRead := mockey.Mock((*snapshotMeta).ReadExternalSnapshotDataWithChunkManager).
			Return(snapshotData, nil).Build()
		defer mockRead.UnPatch()

		job := &copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:               100,
				CollectionId:        200,
				SnapshotName:        "test_snapshot",
				External:            true,
				SnapshotS3Location:  snapshotLocation,
				SnapshotFingerprint: "different-fingerprint",
			},
			snapshotCache: &copySegmentSnapshotCache{},
		}

		req, err := AssembleCopySegmentRequest(newTask(&snapshotMeta{}), job)

		require.Error(t, err)
		assert.Nil(t, req)
		assert.True(t, errors.Is(err, merr.ErrDataIntegrity))
	})

	t.Run("matching snapshot is cached per job", func(t *testing.T) {
		snapshotData := newSnapshot()
		fingerprint, err := snapshotstorage.SnapshotFingerprint(snapshotData)
		require.NoError(t, err)
		sourceCM := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
		mockResolve := mockey.Mock(snapshotstorage.ResolveForeignStorage).Return(&snapshotstorage.ResolvedForeignStorage{
			ForeignBucket: "bucket",
			ForeignCM:     sourceCM,
		}, nil).Build()
		defer mockResolve.UnPatch()
		readCalls := 0
		mockRead := mockey.Mock((*snapshotMeta).ReadExternalSnapshotDataWithChunkManager).To(
			func(*snapshotMeta, context.Context, storage.ChunkManager, string, bool) (*snapshotstorage.SnapshotData, error) {
				readCalls++
				return snapshotData, nil
			}).Build()
		defer mockRead.UnPatch()
		mockStorage := mockey.Mock(createStorageConfig).Return(&indexpb.StorageConfig{RootPath: "target-root"}).Build()
		defer mockStorage.UnPatch()

		job := &copySegmentJob{
			CopySegmentJob: &datapb.CopySegmentJob{
				JobId:               100,
				CollectionId:        200,
				SnapshotName:        "test_snapshot",
				External:            true,
				SnapshotS3Location:  snapshotLocation,
				SnapshotFingerprint: fingerprint,
			},
			snapshotCache: &copySegmentSnapshotCache{},
		}
		task := newTask(&snapshotMeta{})

		first, err := AssembleCopySegmentRequest(task, job)
		require.NoError(t, err)
		second, err := AssembleCopySegmentRequest(task, job)
		require.NoError(t, err)
		assert.Equal(t, first, second)
		assert.Equal(t, 1, readCalls)
	})
}

func TestAssembleCopySegmentRequest_AllocatesTextAndJsonBuildIDs(t *testing.T) {
	// Arrange: snapshot data with segment that has vector, text, and JSON key indexes
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:   1,
				PartitionId: 10,
				IndexFiles: []*indexpb.IndexFilePathInfo{
					{BuildID: 3001, FieldID: 100, IndexID: 1001},
				},
				TextIndexFiles: map[int64]*datapb.TextIndexStats{
					200: {FieldID: 200, BuildID: 4001},
				},
				JsonKeyIndexFiles: map[int64]*datapb.JsonKeyStats{
					300: {FieldID: 300, BuildID: 5001},
				},
			},
		},
	}

	sm := &snapshotMeta{}
	mock1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(snapshotData, nil).Build()
	defer mock1.UnPatch()

	// Mock allocator to return sequential IDs starting from 9001
	nextID := int64(9001)
	alloc := &embeddedAllocator{}
	mock2 := mockey.Mock((*embeddedAllocator).AllocID).To(func(ctx context.Context) (typeutil.UniqueID, error) {
		id := nextID
		nextID++
		return id, nil
	}).Build()
	defer mock2.UnPatch()

	// Mock Params access
	mock3 := mockey.Mock(createStorageConfig).Return(nil).Build()
	defer mock3.UnPatch()

	task := &copySegmentTask{
		ctx:          context.Background(),
		snapshotMeta: sm,
		alloc:        alloc,
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 100,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 10},
		},
	})

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: 100,
			SnapshotName: "test_snapshot",
		},
		tr:            timerecord.NewTimeRecorder("test_job"),
		snapshotCache: &copySegmentSnapshotCache{},
	}

	// Act
	req, err := AssembleCopySegmentRequest(task, job)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, req)
	assert.Len(t, req.Targets, 1)

	newBuildIDs := req.Targets[0].GetNewBuildIds()
	// Should have 3 entries: one for vector index (3001), one for text (4001), one for JSON (5001)
	assert.Len(t, newBuildIDs, 3)
	assert.Contains(t, newBuildIDs, int64(3001))
	assert.Contains(t, newBuildIDs, int64(4001))
	assert.Contains(t, newBuildIDs, int64(5001))

	// All new IDs should be distinct and >= 9001
	seenIDs := make(map[int64]bool)
	for _, newID := range newBuildIDs {
		assert.True(t, newID >= 9001, "new build ID should be allocated by allocator")
		assert.False(t, seenIDs[newID], "new build IDs should be unique")
		seenIDs[newID] = true
	}
}

func TestAssembleCopySegmentRequest_RedispatchAllocatesFreshBuildIDs(t *testing.T) {
	// Regression test for re-dispatch idempotency: when a task is reset to
	// Pending after a worker loss and dispatched again, the new attempt must
	// allocate a fresh set of buildIDs. Index files written by the partial
	// earlier attempt (under the old buildIDs) are then never referenced by
	// meta — they stay orphaned and are removed by GC — while the new attempt
	// cannot collide with them.
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			Id:           1,
			CollectionId: 100,
			Name:         "test_snapshot",
		},
		Segments: []*datapb.SegmentDescription{
			{
				SegmentId:   1,
				PartitionId: 10,
				IndexFiles: []*indexpb.IndexFilePathInfo{
					{BuildID: 3001, FieldID: 100, IndexID: 1001},
				},
			},
		},
	}

	sm := &snapshotMeta{}
	mock1 := mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(snapshotData, nil).Build()
	defer mock1.UnPatch()

	nextID := int64(9001)
	alloc := &embeddedAllocator{}
	mock2 := mockey.Mock((*embeddedAllocator).AllocID).To(func(ctx context.Context) (typeutil.UniqueID, error) {
		id := nextID
		nextID++
		return id, nil
	}).Build()
	defer mock2.UnPatch()

	mock3 := mockey.Mock(createStorageConfig).Return(nil).Build()
	defer mock3.UnPatch()

	task := &copySegmentTask{
		snapshotMeta: sm,
		alloc:        alloc,
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 100,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 10},
		},
	})

	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{
			JobId:        100,
			CollectionId: 100,
			SnapshotName: "test_snapshot",
		},
		tr:            timerecord.NewTimeRecorder("test_job"),
		snapshotCache: &copySegmentSnapshotCache{},
	}

	// First dispatch attempt
	firstReq, err := AssembleCopySegmentRequest(task, job)
	assert.NoError(t, err)
	// Second dispatch attempt after a simulated worker loss + reset
	secondReq, err := AssembleCopySegmentRequest(task, job)
	assert.NoError(t, err)

	firstIDs := firstReq.Targets[0].GetNewBuildIds()
	secondIDs := secondReq.Targets[0].GetNewBuildIds()
	assert.Len(t, firstIDs, 1)
	assert.Len(t, secondIDs, 1)
	// The re-dispatched attempt must not reuse any buildID from the first
	// attempt, otherwise its index files could collide with partial files
	// written by the lost worker.
	for srcID, newFirst := range firstIDs {
		newSecond, ok := secondIDs[srcID]
		assert.True(t, ok)
		assert.NotEqual(t, newFirst, newSecond,
			"re-dispatch must allocate a fresh buildID for source buildID %d", srcID)
	}
}

// assembleIndexPrecedenceFixture builds a one-segment copy request whose source
// is a StorageV3 segment carrying the snapshot's own (etcd-derived) index
// metadata, so a test only has to say what its manifest reports.
func assembleIndexPrecedenceFixture(t *testing.T, manifestIndexes []packed.ManifestIndexInfo,
	mutateSegment ...func(*datapb.SegmentDescription),
) (*datapb.CopySegmentRequest, int) {
	mockManifestIndexSubmissions(t)
	t.Helper()

	manifestPath := packed.MarshalManifestPath("files/insert_log/100/10/1", 3)
	segment := &datapb.SegmentDescription{
		SegmentId:      1,
		PartitionId:    10,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifestPath,
		IndexFiles: []*indexpb.IndexFilePathInfo{
			{BuildID: 3001, FieldID: 100, IndexID: 1001, IndexName: "vec_idx"},
		},
	}
	for _, mutate := range mutateSegment {
		mutate(segment)
	}
	snapshotData := &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Id: 1, CollectionId: 100, Name: "test_snapshot"},
		Indexes:      []*indexpb.IndexInfo{{IndexID: 1001, FieldID: 100}},
		Segments:     []*datapb.SegmentDescription{segment},
	}

	defer mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(snapshotData, nil).Build().UnPatch()
	defer mockey.Mock(createStorageConfig).Return(nil).Build().UnPatch()
	manifestReads := 0
	defer mockey.Mock(packed.GetManifestIndexInfosAsync).To(
		func(context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig) ([]packed.ManifestIndexInfo, error) {
			manifestReads++
			return manifestIndexes, nil
		}).Build().UnPatch()

	nextID := int64(9001)
	defer mockey.Mock((*embeddedAllocator).AllocID).To(func(ctx context.Context) (typeutil.UniqueID, error) {
		id := nextID
		nextID++
		return id, nil
	}).Build().UnPatch()

	task := &copySegmentTask{
		ctx:          context.Background(),
		meta:         &meta{},
		snapshotMeta: &snapshotMeta{},
		alloc:        &embeddedAllocator{},
		tr:           timerecord.NewTimeRecorder("test"),
		times:        taskcommon.NewTimes(),
	}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId:       1001,
		JobId:        100,
		CollectionId: 100,
		IdMappings: []*datapb.CopySegmentIDMapping{
			{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 10},
		},
	})
	job := &copySegmentJob{
		CopySegmentJob: &datapb.CopySegmentJob{JobId: 100, CollectionId: 100, SnapshotName: "test_snapshot"},
		tr:             timerecord.NewTimeRecorder("test_job"),
		snapshotCache:  &copySegmentSnapshotCache{},
	}

	req, err := AssembleCopySegmentRequest(task, job)
	require.NoError(t, err)
	require.Len(t, req.GetSources(), 1)
	require.Len(t, req.GetTargets(), 1)
	return req, manifestReads
}

// A manifest written before index publication existed carries no index section.
// The segment still has indexes, recorded in etcd and captured by the snapshot,
// so the manifest must not be treated as the authority on whether they exist.
func TestAssembleCopySegmentRequest_LegacyManifestKeepsSnapshotIndexFiles(t *testing.T) {
	req, manifestReads := assembleIndexPrecedenceFixture(t, nil)

	indexFiles := req.GetSources()[0].GetIndexFiles()
	require.Len(t, indexFiles, 1, "snapshot index metadata must survive an index-less manifest")
	assert.Equal(t, int64(3001), indexFiles[0].GetBuildID())
	assert.Equal(t, "vec_idx", indexFiles[0].GetIndexName())
	assert.Zero(t, manifestReads)
}

// Snapshot-captured index metadata is already complete, so DataCoord does not
// need to read the source manifest. DataNode handles inherited-entry retraction
// against the copied target manifest.
func TestAssembleCopySegmentRequest_SnapshotIndexFilesSkipManifestRead(t *testing.T) {
	req, manifestReads := assembleIndexPrecedenceFixture(t, []packed.ManifestIndexInfo{
		{IndexID: 1001, BuildID: 3001, FieldID: 100, IndexName: "vec_idx"},
		{IndexID: 1002, BuildID: 3002, FieldID: 101, IndexName: "dropped_idx"},
	})

	// The snapshot answered which files to copy, so the manifest did not add to
	// or replace that list.
	indexFiles := req.GetSources()[0].GetIndexFiles()
	require.Len(t, indexFiles, 1)
	assert.Equal(t, int64(3001), indexFiles[0].GetBuildID())
	assert.Zero(t, manifestReads)
}

func TestAssembleCopySegmentRequest_ManifestHasIndexFalseSkipsRead(t *testing.T) {
	req, manifestReads := assembleIndexPrecedenceFixture(t, nil,
		func(segment *datapb.SegmentDescription) {
			segment.IndexFiles = nil
			segment.ManifestHasIndex = proto.Bool(false)
		})

	require.Len(t, req.GetSources(), 1)
	assert.NotNil(t, req.GetSources()[0].ManifestHasIndex)
	assert.False(t, req.GetSources()[0].GetManifestHasIndex())
	assert.Empty(t, req.GetSources()[0].GetIndexFiles())
	assert.Zero(t, manifestReads)
}

func TestAssembleCopySegmentRequest_UnknownMarkerFallsBackToManifestRead(t *testing.T) {
	req, manifestReads := assembleIndexPrecedenceFixture(t, []packed.ManifestIndexInfo{{
		IndexID:               1001,
		BuildID:               3001,
		FieldID:               100,
		IndexName:             "vec_idx",
		IndexType:             "HNSW",
		Path:                  "index_files/3001/0/10/1",
		IndexFileKeys:         []string{"index.bin"},
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
	}}, func(segment *datapb.SegmentDescription) {
		segment.IndexFiles = nil
		segment.ManifestHasIndex = nil
	})

	assert.Equal(t, 1, manifestReads)
	require.Len(t, req.GetSources()[0].GetIndexFiles(), 1)
	assert.Equal(t, int64(3001), req.GetSources()[0].GetIndexFiles()[0].GetBuildID())
}
