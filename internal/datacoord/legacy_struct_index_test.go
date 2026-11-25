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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestLegacyStructArrayScalarIndex_StartupMigration(t *testing.T) {
	ctx := context.Background()
	catalog := catalogmocks.NewDataCoordCatalog(t)
	collID := UniqueID(10)
	segID := UniqueID(20)

	const (
		regularArrayFieldID = UniqueID(101)
		structScalarFieldID = UniqueID(102)
		structVectorFieldID = UniqueID(103)
	)
	const (
		regularIndexID       = UniqueID(1001)
		structScalarIndexID  = UniqueID(1002)
		structVectorIndexID  = UniqueID(1003)
		deletedScalarIndexID = UniqueID(1004)
		regularBuildID       = UniqueID(2001)
		structScalarBuildID  = UniqueID(2002)
		structVectorBuildID  = UniqueID(2003)
		deletedStructBuildID = UniqueID(2004)
	)

	mt := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		indexMeta: &indexMeta{
			ctx:              ctx,
			catalog:          catalog,
			keyLock:          lock.NewKeyLock[UniqueID](),
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {
					regularIndexID:      {CollectionID: collID, FieldID: regularArrayFieldID, IndexID: regularIndexID, IndexName: "regular_idx"},
					structScalarIndexID: {CollectionID: collID, FieldID: structScalarFieldID, IndexID: structScalarIndexID, IndexName: "struct_scalar_idx"},
					structVectorIndexID: {CollectionID: collID, FieldID: structVectorFieldID, IndexID: structVectorIndexID, IndexName: "struct_vec_idx"},
					deletedScalarIndexID: {
						CollectionID: collID,
						FieldID:      structScalarFieldID,
						IndexID:      deletedScalarIndexID,
						IndexName:    "deleted_struct_scalar_idx",
						IsDeleted:    true,
					},
				},
			},
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		},
	}

	mt.AddCollection(&collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: regularArrayFieldID, Name: "arr", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
			},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 200,
					Name:    "items",
					Fields: []*schemapb.FieldSchema{
						{FieldID: structScalarFieldID, Name: "items[tag]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
						{FieldID: structVectorFieldID, Name: "items[vec]", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector},
					},
				},
			},
		},
	})

	mt.indexMeta.updateSegmentIndex(&model.SegmentIndex{
		CollectionID: collID,
		SegmentID:    segID,
		IndexID:      regularIndexID,
		BuildID:      regularBuildID,
		IndexState:   commonpb.IndexState_Finished,
		IndexType:    "INVERTED",
	})
	mt.indexMeta.updateSegmentIndex(&model.SegmentIndex{
		CollectionID: collID,
		SegmentID:    segID,
		IndexID:      structScalarIndexID,
		BuildID:      structScalarBuildID,
		IndexState:   commonpb.IndexState_Finished,
		IndexType:    "HYBRID",
	})
	mt.indexMeta.updateSegmentIndex(&model.SegmentIndex{
		CollectionID: collID,
		SegmentID:    segID,
		IndexID:      structVectorIndexID,
		BuildID:      structVectorBuildID,
		IndexState:   commonpb.IndexState_Finished,
		IndexType:    "HNSW",
	})
	mt.indexMeta.updateSegmentIndex(&model.SegmentIndex{
		CollectionID: collID,
		SegmentID:    segID,
		IndexID:      deletedScalarIndexID,
		BuildID:      deletedStructBuildID,
		IndexState:   commonpb.IndexState_Finished,
		IsDeleted:    true,
		IndexType:    "INVERTED",
	})

	var alteredBuildIDs []UniqueID
	catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, segIdxes []*model.SegmentIndex) error {
			for _, segIdx := range segIdxes {
				alteredBuildIDs = append(alteredBuildIDs, segIdx.BuildID)
			}
			return nil
		}).Once()

	assert.NoError(t, mt.markLegacyStructArrayScalarSegmentIndexes(ctx))
	assert.Equal(t, []UniqueID{structScalarBuildID}, alteredBuildIDs)

	segIdx, ok := mt.indexMeta.segmentBuildInfo.Get(structScalarBuildID)
	assert.True(t, ok)
	assert.True(t, segIdx.LegacyRowDomain)

	regularSegIdx, ok := mt.indexMeta.segmentBuildInfo.Get(regularBuildID)
	assert.True(t, ok)
	assert.False(t, regularSegIdx.LegacyRowDomain)

	vecSegIdx, ok := mt.indexMeta.segmentBuildInfo.Get(structVectorBuildID)
	assert.True(t, ok)
	assert.False(t, vecSegIdx.LegacyRowDomain)

	deletedSegIdx, ok := mt.indexMeta.segmentBuildInfo.Get(deletedStructBuildID)
	assert.True(t, ok)
	assert.False(t, deletedSegIdx.LegacyRowDomain)

	// Idempotent: second run performs zero catalog writes.
	assert.NoError(t, mt.markLegacyStructArrayScalarSegmentIndexes(ctx))
}

func TestLegacyStructArrayScalarIndex_CreateAndGetIndexInfos(t *testing.T) {
	ctx := context.Background()
	notifyChan := make(chan int64, 2)
	scheduler := task.NewMockGlobalScheduler(t)
	alloc := allocator.NewMockAllocator(t)
	handler := NewNMockHandler(t)
	storageCM := mocks.NewChunkManager(t)
	versionManager := newIndexEngineVersionManager()
	catalog := catalogmocks.NewDataCoordCatalog(t)

	collID := UniqueID(11)
	segID := UniqueID(21)
	partID := UniqueID(31)
	const (
		regularArrayFieldID = UniqueID(111)
		structScalarFieldID = UniqueID(112)
		regularIndexID      = UniqueID(1101)
		structScalarIndexID = UniqueID(1102)
	)

	mt := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		indexMeta: &indexMeta{
			ctx:              ctx,
			catalog:          catalog,
			keyLock:          lock.NewKeyLock[UniqueID](),
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {
					regularIndexID: {
						CollectionID: collID,
						FieldID:      regularArrayFieldID,
						IndexID:      regularIndexID,
						IndexName:    "regular_idx",
						IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "INVERTED"}},
					},
					structScalarIndexID: {
						CollectionID: collID,
						FieldID:      structScalarFieldID,
						IndexID:      structScalarIndexID,
						IndexName:    "struct_idx",
						IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "INVERTED"}},
					},
				},
			},
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		},
	}

	mt.AddCollection(&collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: regularArrayFieldID, Name: "arr", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
			},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{
				{
					FieldID: 210,
					Name:    "items",
					Fields: []*schemapb.FieldSchema{
						{FieldID: structScalarFieldID, Name: "items[tag]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
					},
				},
			},
		},
	})

	seg := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           segID,
			CollectionID: collID,
			PartitionID:  partID,
			NumOfRows:    100,
			State:        commonpb.SegmentState_Flushed,
		},
	}
	mt.segments.SetSegment(segID, seg)

	inspector := newIndexInspector(ctx, notifyChan, mt, scheduler, alloc, handler, storageCM, versionManager)
	nextBuildID := UniqueID(3000)
	alloc.EXPECT().AllocID(mock.Anything).RunAndReturn(func(_ context.Context) (UniqueID, error) {
		nextBuildID++
		return nextBuildID, nil
	}).Times(2)
	catalog.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Times(2)
	var enqueued []*model.SegmentIndex
	scheduler.EXPECT().Enqueue(mock.Anything).Run(func(tsk task.Task) {
		enqueued = append(enqueued, tsk.(*indexBuildTask).SegmentIndex)
	}).Times(2)

	assert.NoError(t, inspector.createIndexForSegment(ctx, seg, regularIndexID))
	assert.NoError(t, inspector.createIndexForSegment(ctx, seg, structScalarIndexID))
	assert.Len(t, enqueued, 2)
	for _, segIdx := range enqueued {
		if segIdx.IndexID == structScalarIndexID {
			assert.True(t, segIdx.LegacyRowDomain)
			segIdx.IndexState = commonpb.IndexState_Finished
		} else {
			assert.False(t, segIdx.LegacyRowDomain)
			segIdx.IndexState = commonpb.IndexState_Finished
		}
		mt.indexMeta.updateSegmentIndex(segIdx)
	}

	cmFactory := storage.NewChunkManagerFactoryWithParam(Params)
	cli, err := cmFactory.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	mt.chunkManager = cli

	s := &Server{
		meta: mt,
	}
	s.stateCode.Store(commonpb.StateCode_Healthy)
	resp, err := s.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{
		CollectionID: collID,
		SegmentIDs:   []UniqueID{segID},
	})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	infos := resp.GetSegmentInfo()[segID].GetIndexInfos()
	assert.Len(t, infos, 2)

	findParam := func(params []*commonpb.KeyValuePair, key string) (string, bool) {
		for _, p := range params {
			if p.GetKey() == key {
				return p.GetValue(), true
			}
		}
		return "", false
	}
	for _, info := range infos {
		val, ok := findParam(info.GetIndexParams(), common.LegacyRowDomainKey)
		if info.GetIndexID() == structScalarIndexID {
			assert.True(t, ok)
			assert.Equal(t, "true", val)
		} else {
			assert.False(t, ok)
		}
	}
}

func TestLegacyStructArrayScalarIndex_MigrationStatus(t *testing.T) {
	mt := &meta{}
	s := &Server{meta: mt}

	// Not healthy -> Pending / ready=false.
	s.stateCode.Store(commonpb.StateCode_Initializing)
	status := s.GetLegacyStructIndexMigrationStatus()
	assert.Equal(t, LegacyStructIndexMigrationPending, status.State)
	assert.False(t, status.Ready)

	// Healthy but not started -> Pending / ready=false.
	s.stateCode.Store(commonpb.StateCode_Healthy)
	status = s.GetLegacyStructIndexMigrationStatus()
	assert.Equal(t, LegacyStructIndexMigrationPending, status.State)
	assert.False(t, status.Ready)

	// Running with transient error -> Running / ready=false / last_error populated.
	mt.setLegacyStructIndexMigrationRunning()
	mt.addLegacyStructIndexMarkedCount(3)
	mt.setLegacyStructIndexMigrationError(errors.New("transient etcd error"))
	status = s.GetLegacyStructIndexMigrationStatus()
	assert.Equal(t, LegacyStructIndexMigrationRunning, status.State)
	assert.False(t, status.Ready)
	assert.Equal(t, 3, status.MarkedCount)
	assert.Contains(t, status.LastError, "transient etcd error")

	// Completed -> Completed / ready=true / last_error cleared.
	mt.setLegacyStructIndexMigrationCompleted()
	status = s.GetLegacyStructIndexMigrationStatus()
	assert.Equal(t, LegacyStructIndexMigrationCompleted, status.State)
	assert.True(t, status.Ready)
	assert.Equal(t, 3, status.MarkedCount)
	assert.Empty(t, status.LastError)

	// Failed -> Failed / ready=false.
	mt.setLegacyStructIndexMigrationFailed(errors.New("permanent failure"))
	status = s.GetLegacyStructIndexMigrationStatus()
	assert.Equal(t, LegacyStructIndexMigrationFailed, status.State)
	assert.False(t, status.Ready)
	assert.Contains(t, status.LastError, "permanent failure")
}
