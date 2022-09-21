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

package indexcoord

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/indexcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

func createMetaForHandoff(catalog metastore.IndexCoordCatalog) *metaTable {
	return &metaTable{
		catalog:          catalog,
		segmentIndexLock: sync.RWMutex{},
		indexLock:        sync.RWMutex{},
		collectionIndexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:     "",
					CollectionID: collID,
					FieldID:      fieldID,
					IndexID:      indexID,
					IndexName:    indexName,
					IsDeleted:    false,
					CreateTime:   0,
					TypeParams:   nil,
					IndexParams:  nil,
				},
			},
		},
		segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
			segID: {
				indexID: {
					SegmentID:    segID,
					CollectionID: collID,
					PartitionID:  partID,
					BuildID:      buildID,
					IndexState:   1,
					IsDeleted:    false,
					WriteHandoff: false,
				},
			},
			segID + 1: {
				indexID: {
					SegmentID:    segID + 1,
					CollectionID: collID,
					PartitionID:  partID,
					BuildID:      buildID + 1,
					IndexState:   1,
					IsDeleted:    true,
					WriteHandoff: false,
				},
			},
			segID + 2: {
				indexID: {
					SegmentID:    segID + 2,
					CollectionID: collID,
					PartitionID:  partID,
					BuildID:      buildID + 2,
					IndexState:   1,
					IsDeleted:    false,
					WriteHandoff: true,
				},
			},
		},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:    segID,
				CollectionID: collID,
				PartitionID:  partID,
				BuildID:      buildID,
				IndexID:      indexID,
				IndexState:   1,
				IsDeleted:    false,
				WriteHandoff: false,
			},
			buildID + 1: {
				SegmentID:    segID + 1,
				CollectionID: collID,
				PartitionID:  partID,
				BuildID:      buildID + 1,
				IndexID:      indexID,
				IndexState:   1,
				IsDeleted:    true,
				WriteHandoff: false,
			},
			buildID + 2: {
				SegmentID:    segID + 2,
				CollectionID: collID,
				PartitionID:  partID,
				BuildID:      buildID + 2,
				IndexID:      indexID,
				IndexState:   1,
				IsDeleted:    false,
				WriteHandoff: true,
			},
		},
	}
}

func Test_newHandoff(t *testing.T) {
	ctx := context.Background()
	hd := newHandoff(ctx, createMetaForHandoff(&indexcoord.Catalog{Txn: NewMockEtcdKV()}), NewMockEtcdKV(), &IndexCoord{dataCoordClient: NewDataCoordMock()})
	assert.NotNil(t, hd)
	assert.Equal(t, 1, len(hd.tasks))

	hd.enqueue(segID)
	assert.Equal(t, 1, len(hd.tasks))

	err := hd.meta.AddIndex(&model.SegmentIndex{
		SegmentID:    segID + 3,
		CollectionID: collID,
		PartitionID:  partID,
		NumRows:      0,
		IndexID:      indexID,
		BuildID:      buildID + 3,
	})
	assert.NoError(t, err)
	hd.enqueue(segID + 3)
	assert.Equal(t, 2, len(hd.tasks))

	hd.Start()
	err = hd.meta.FinishTask(&indexpb.IndexTaskInfo{
		BuildID:        buildID,
		State:          commonpb.IndexState_Finished,
		IndexFiles:     []string{"file1", "file2"},
		SerializedSize: 100,
		FailReason:     "",
	})
	assert.NoError(t, err)
	err = hd.meta.FinishTask(&indexpb.IndexTaskInfo{
		BuildID:        buildID + 3,
		State:          commonpb.IndexState_Failed,
		IndexFiles:     nil,
		SerializedSize: 0,
		FailReason:     "failed",
	})
	assert.NoError(t, err)

	// handle ticker
	time.Sleep(time.Second * 2)
	for hd.Len() != 0 {
		time.Sleep(500 * time.Millisecond)
	}

	assert.True(t, hd.taskDone(segID))
	assert.True(t, hd.taskDone(segID+3))

	hd.Stop()
}

func Test_handoff_error(t *testing.T) {
	t.Run("pullSegmentInfo fail", func(t *testing.T) {
		hd := &handoff{
			tasks: map[UniqueID]struct{}{
				segID: {},
			},
			taskMutex: sync.RWMutex{},
			wg:        sync.WaitGroup{},
			meta: &metaTable{
				segmentIndexLock: sync.RWMutex{},
				segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
					segID: {
						indexID: {
							SegmentID:    segID,
							CollectionID: collID,
							PartitionID:  partID,
							IndexID:      indexID,
							BuildID:      buildID,
							IndexState:   commonpb.IndexState_Finished,
							FailReason:   "",
							IsDeleted:    false,
							WriteHandoff: false,
						},
					},
				},
			},
			notifyChan:       make(chan struct{}, 1),
			scheduleDuration: 0,
			kvClient:         nil,
			ic: &IndexCoord{
				dataCoordClient: &DataCoordMock{
					CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
						return nil, errors.New("error")
					},
				},
			},
		}
		hd.process(segID, true)
		assert.Equal(t, 1, hd.Len())

		hd.ic.dataCoordClient = &DataCoordMock{
			CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
				return nil, errSegmentNotFound(segID)
			},
		}
		hd.process(segID, true)
		assert.Equal(t, 0, hd.Len())
	})

	t.Run("is importing", func(t *testing.T) {
		hd := &handoff{
			tasks: map[UniqueID]struct{}{
				segID: {},
			},
			taskMutex: sync.RWMutex{},
			wg:        sync.WaitGroup{},
			meta: &metaTable{
				segmentIndexLock: sync.RWMutex{},
				segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
					segID: {
						indexID: {
							SegmentID:    segID,
							CollectionID: collID,
							PartitionID:  partID,
							IndexID:      indexID,
							BuildID:      buildID,
							IndexState:   commonpb.IndexState_Finished,
							FailReason:   "",
							IsDeleted:    false,
							WriteHandoff: false,
						},
					},
				},
			},
			notifyChan:       make(chan struct{}, 1),
			scheduleDuration: 0,
			kvClient:         nil,
			ic: &IndexCoord{
				dataCoordClient: &DataCoordMock{
					CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
						return &datapb.GetSegmentInfoResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_Success,
							},
							Infos: []*datapb.SegmentInfo{
								{
									ID:            segID,
									CollectionID:  collID,
									PartitionID:   partID,
									InsertChannel: "",
									NumOfRows:     1024,
									State:         commonpb.SegmentState_Flushed,
									IsImporting:   true,
								},
							},
						}, nil
					},
				},
			},
		}

		hd.process(segID, true)
		assert.Equal(t, 1, hd.Len())
	})

	t.Run("get index info fail", func(t *testing.T) {
		hd := &handoff{
			tasks: map[UniqueID]struct{}{
				segID: {},
			},
			taskMutex: sync.RWMutex{},
			wg:        sync.WaitGroup{},
			meta: &metaTable{
				segmentIndexLock: sync.RWMutex{},
				segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
					segID: {
						indexID: {
							SegmentID:    segID,
							CollectionID: collID,
							PartitionID:  partID,
							IndexID:      indexID,
							BuildID:      buildID,
							IndexState:   commonpb.IndexState_Finished,
							FailReason:   "",
							IsDeleted:    true,
							WriteHandoff: false,
						},
					},
				},
			},
			notifyChan:       make(chan struct{}, 1),
			scheduleDuration: 0,
			kvClient:         nil,
			ic: &IndexCoord{
				dataCoordClient: NewDataCoordMock(),
			},
		}

		hd.process(segID, true)
		assert.Equal(t, 0, hd.Len())
	})

	t.Run("write handoff fail", func(t *testing.T) {
		hd := &handoff{
			tasks: map[UniqueID]struct{}{
				segID: {},
			},
			taskMutex: sync.RWMutex{},
			wg:        sync.WaitGroup{},
			meta: &metaTable{
				catalog:          &indexcoord.Catalog{Txn: NewMockEtcdKV()},
				segmentIndexLock: sync.RWMutex{},
				segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
					segID: {
						indexID: {
							SegmentID:    segID,
							CollectionID: collID,
							PartitionID:  partID,
							IndexID:      indexID,
							BuildID:      buildID,
							IndexState:   commonpb.IndexState_Finished,
							FailReason:   "",
							IsDeleted:    false,
							WriteHandoff: false,
						},
					},
				},
			},
			notifyChan:       make(chan struct{}, 1),
			scheduleDuration: 0,
			kvClient: &mockETCDKV{
				save: func(s string, s2 string) error {
					return errors.New("error")
				},
			},
			ic: &IndexCoord{
				dataCoordClient: NewDataCoordMock(),
			},
		}

		hd.process(segID, true)
		assert.Equal(t, 1, hd.Len())
	})

	t.Run("mark meta as write handoff fail", func(t *testing.T) {
		hd := &handoff{
			tasks: map[UniqueID]struct{}{
				segID: {},
			},
			taskMutex: sync.RWMutex{},
			wg:        sync.WaitGroup{},
			meta: &metaTable{
				catalog: &indexcoord.Catalog{Txn: &mockETCDKV{
					multiSave: func(m map[string]string) error {
						return errors.New("error")
					},
				}},
				segmentIndexLock: sync.RWMutex{},
				segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
					segID: {
						indexID: {
							SegmentID:    segID,
							CollectionID: collID,
							PartitionID:  partID,
							IndexID:      indexID,
							BuildID:      buildID,
							IndexState:   commonpb.IndexState_Finished,
							FailReason:   "",
							IsDeleted:    false,
							WriteHandoff: false,
						},
					},
				},
			},
			notifyChan:       make(chan struct{}, 1),
			scheduleDuration: 0,
			kvClient:         NewMockEtcdKV(),
			ic: &IndexCoord{
				dataCoordClient: NewDataCoordMock(),
			},
		}

		hd.process(segID, true)
		assert.Equal(t, 1, hd.Len())
	})
}

func Test_handoff_allParentsDone(t *testing.T) {
	t.Run("done", func(t *testing.T) {
		hd := &handoff{
			tasks: map[UniqueID]struct{}{
				segID: {},
			},
			taskMutex: sync.RWMutex{},
		}

		done := hd.allParentsDone([]UniqueID{segID + 1, segID + 2, segID + 3})
		assert.True(t, done)
	})

	t.Run("not done", func(t *testing.T) {
		hd := &handoff{
			tasks: map[UniqueID]struct{}{
				segID:     {},
				segID + 1: {},
			},
			taskMutex: sync.RWMutex{},
		}

		done := hd.allParentsDone([]UniqueID{segID + 1, segID + 2, segID + 3})
		assert.False(t, done)
	})
}
