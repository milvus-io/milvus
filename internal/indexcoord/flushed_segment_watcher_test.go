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
	"github.com/milvus-io/milvus/internal/metastore/kv/indexcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func Test_flushSegmentWatcher(t *testing.T) {
	ctx := context.Background()

	fsw, err := newFlushSegmentWatcher(ctx,
		&mockETCDKV{
			loadWithRevision: func(key string) ([]string, []string, int64, error) {
				return []string{"1", "2", "3"}, []string{"1", "2", "3"}, 1, nil
			},
			removeWithPrefix: func(key string) error {
				return nil
			},
		},
		&metaTable{
			catalog: &indexcoord.Catalog{
				Txn: NewMockEtcdKV(),
			},
			indexLock:            sync.RWMutex{},
			segmentIndexLock:     sync.RWMutex{},
			collectionIndexes:    map[UniqueID]map[UniqueID]*model.Index{},
			segmentIndexes:       map[UniqueID]map[UniqueID]*model.SegmentIndex{},
			buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{},
		},
		&indexBuilder{}, &handoff{}, &IndexCoord{
			dataCoordClient: NewDataCoordMock(),
		})
	assert.NoError(t, err)
	assert.NotNil(t, fsw)

	fsw.enqueueInternalTask(1)

	fsw.Start()

	// hold ticker.C
	time.Sleep(time.Second * 2)

	for fsw.Len() != 0 {
		time.Sleep(time.Second)
	}

	fsw.Stop()
}

func Test_flushSegmentWatcher_newFlushSegmentWatcher(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		fsw, err := newFlushSegmentWatcher(context.Background(),
			&mockETCDKV{
				loadWithRevision: func(key string) ([]string, []string, int64, error) {
					return []string{"segID1"}, []string{"12345"}, 1, nil
				},
			}, &metaTable{}, &indexBuilder{}, &handoff{}, &IndexCoord{
				dataCoordClient: NewDataCoordMock(),
			})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(fsw.internalTasks))
	})

	t.Run("load fail", func(t *testing.T) {
		fsw, err := newFlushSegmentWatcher(context.Background(),
			&mockETCDKV{
				loadWithRevision: func(key string) ([]string, []string, int64, error) {
					return []string{"segID1"}, []string{"12345"}, 1, errors.New("error")
				},
			}, &metaTable{}, &indexBuilder{}, &handoff{}, &IndexCoord{
				dataCoordClient: NewDataCoordMock(),
			})
		assert.Error(t, err)
		assert.Nil(t, fsw)
	})

	t.Run("parse fail", func(t *testing.T) {
		fsw, err := newFlushSegmentWatcher(context.Background(),
			&mockETCDKV{
				loadWithRevision: func(key string) ([]string, []string, int64, error) {
					return []string{"segID1"}, []string{"segID"}, 1, nil
				},
			}, &metaTable{}, &indexBuilder{}, &handoff{}, &IndexCoord{
				dataCoordClient: NewDataCoordMock(),
			})
		assert.Error(t, err)
		assert.Nil(t, fsw)
	})
}

func Test_flushedSegmentWatcher_internalRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fsw := &flushedSegmentWatcher{
		ctx:               ctx,
		cancel:            cancel,
		kvClient:          NewMockEtcdKV(),
		wg:                sync.WaitGroup{},
		scheduleDuration:  time.Second,
		internalTaskMutex: sync.RWMutex{},
		internalNotify:    make(chan struct{}, 1),
		etcdRevision:      0,
		watchChan:         nil,
		meta:              nil,
		builder:           nil,
		ic: &IndexCoord{
			dataCoordClient: NewDataCoordMock(),
		},
		handoff: nil,
		internalTasks: map[UniqueID]*internalTask{
			segID: {
				state: indexTaskPrepare,
				segmentInfo: &datapb.SegmentInfo{
					CollectionID: collID,
					PartitionID:  partID,
					ID:           segID,
				},
			},
			segID + 1: {
				state:       indexTaskPrepare,
				segmentInfo: nil,
			},
			segID - 1: {
				state:       indexTaskPrepare,
				segmentInfo: nil,
			},
		},
	}

	fsw.internalRun()
	assert.Equal(t, 3, fsw.Len())
}

func Test_flushSegmentWatcher_internalProcess_success(t *testing.T) {
	meta := &metaTable{
		segmentIndexLock: sync.RWMutex{},
		indexLock:        sync.RWMutex{},
		catalog: &indexcoord.Catalog{Txn: &mockETCDKV{
			multiSave: func(m map[string]string) error {
				return nil
			},
		}},
		collectionIndexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:     "",
					CollectionID: collID,
					FieldID:      fieldID,
					IndexID:      indexID,
					IndexName:    indexName,
					IsDeleted:    false,
					CreateTime:   1,
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
					NumRows:      1000,
					IndexID:      indexID,
					BuildID:      buildID,
					IndexState:   commonpb.IndexState_Finished,
				},
			},
		},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:    segID,
				CollectionID: collID,
				PartitionID:  partID,
				NumRows:      1000,
				IndexID:      indexID,
				BuildID:      buildID,
				IndexState:   commonpb.IndexState_Finished,
			},
		},
	}
	task := &internalTask{
		state:       indexTaskPrepare,
		segmentInfo: nil,
	}

	fsw := &flushedSegmentWatcher{
		handoff: &handoff{
			tasks:            map[UniqueID]struct{}{},
			taskMutex:        sync.RWMutex{},
			wg:               sync.WaitGroup{},
			meta:             meta,
			notifyChan:       make(chan struct{}, 1),
			scheduleDuration: time.Second,
			kvClient:         nil,
		},
		ic: &IndexCoord{
			dataCoordClient: &DataCoordMock{
				CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
					return &datapb.GetSegmentInfoResponse{
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
							Reason:    "",
						},
						Infos: []*datapb.SegmentInfo{
							{
								ID:                  segID,
								CollectionID:        collID,
								PartitionID:         partID,
								NumOfRows:           10000,
								State:               commonpb.SegmentState_Flushed,
								CreatedByCompaction: true,
								CompactionFrom:      []int64{},
								StartPosition: &internalpb.MsgPosition{
									ChannelName: "",
									MsgID:       nil,
									MsgGroup:    "",
									Timestamp:   1,
								},
							},
						},
					}, nil
				},
			},
			metaTable: meta,
		},
		internalTasks: map[UniqueID]*internalTask{
			segID: task,
		},
		meta: meta,
		builder: &indexBuilder{
			taskMutex:        sync.RWMutex{},
			scheduleDuration: 0,
			tasks:            map[int64]indexTaskState{},
			notifyChan:       nil,
			meta:             meta,
		},
		kvClient: &mockETCDKV{
			multiSave: func(m map[string]string) error {
				return nil
			},
			save: func(s string, s2 string) error {
				return nil
			},
			removeWithPrefix: func(key string) error {
				return nil
			},
		},
	}
	t.Run("prepare", func(t *testing.T) {
		fsw.internalProcess(segID)
		fsw.internalTaskMutex.RLock()
		assert.Equal(t, indexTaskInit, fsw.internalTasks[segID].state)
		fsw.internalTaskMutex.RUnlock()
	})

	t.Run("init", func(t *testing.T) {
		fsw.internalProcess(segID)
		fsw.internalTaskMutex.RLock()
		assert.Equal(t, indexTaskInProgress, fsw.internalTasks[segID].state)
		fsw.internalTaskMutex.RUnlock()
	})

	fsw.handoff.deleteTask(segID)

	t.Run("inProgress", func(t *testing.T) {
		fsw.internalProcess(segID)
		fsw.internalTaskMutex.RLock()
		assert.Equal(t, indexTaskDone, fsw.internalTasks[segID].state)
		fsw.internalTaskMutex.RUnlock()
	})

	t.Run("done", func(t *testing.T) {
		fsw.internalProcess(segID)
		fsw.internalTaskMutex.RLock()
		_, ok := fsw.internalTasks[segID]
		assert.False(t, ok)
		fsw.internalTaskMutex.RUnlock()
	})
}

func Test_flushSegmentWatcher_internalProcess_error(t *testing.T) {
	task := &internalTask{
		state:       indexTaskPrepare,
		segmentInfo: nil,
	}

	fsw := &flushedSegmentWatcher{
		ic: &IndexCoord{
			dataCoordClient: &DataCoordMock{
				CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
					return &datapb.GetSegmentInfoResponse{
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
							Reason:    "",
						},
						Infos: []*datapb.SegmentInfo{
							{
								ID:                  segID + 100,
								CollectionID:        collID,
								PartitionID:         partID,
								NumOfRows:           10000,
								State:               commonpb.SegmentState_Flushed,
								CreatedByCompaction: true,
								CompactionFrom:      []int64{segID},
							},
						},
					}, nil
				},
			},
			metaTable: &metaTable{},
		},
		internalTasks: map[UniqueID]*internalTask{
			segID: task,
		},
		meta:    &metaTable{},
		builder: &indexBuilder{},
	}

	t.Run("fail", func(t *testing.T) {
		fsw.ic.dataCoordClient = &DataCoordMock{
			CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
				return nil, errors.New("error")
			},
		}
		fsw.internalProcess(segID)

		fsw.ic.dataCoordClient = &DataCoordMock{
			CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
				return &datapb.GetSegmentInfoResponse{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_UnexpectedError,
						Reason:    "fail reason",
					},
				}, nil
			},
		}
		fsw.internalProcess(segID)
		fsw.internalTaskMutex.RLock()
		assert.Equal(t, indexTaskPrepare, fsw.internalTasks[segID].state)
		fsw.internalTaskMutex.RUnlock()
	})

	t.Run("remove flushed segment fail", func(t *testing.T) {
		fsw.kvClient = &mockETCDKV{
			removeWithPrefix: func(key string) error {
				return errors.New("error")
			},
		}
		fsw.internalTasks = map[UniqueID]*internalTask{
			segID: {
				state: indexTaskDone,
				segmentInfo: &datapb.SegmentInfo{
					ID:                  segID,
					CollectionID:        collID,
					PartitionID:         partID,
					InsertChannel:       "",
					NumOfRows:           0,
					State:               0,
					MaxRowNum:           0,
					LastExpireTime:      0,
					StartPosition:       nil,
					DmlPosition:         nil,
					Binlogs:             nil,
					Statslogs:           nil,
					Deltalogs:           nil,
					CreatedByCompaction: false,
					CompactionFrom:      nil,
					DroppedAt:           0,
				},
			},
		}
		fsw.internalProcess(segID)
		fsw.internalTaskMutex.RLock()
		assert.Equal(t, indexTaskDone, fsw.internalTasks[segID].state)
		fsw.internalTaskMutex.RUnlock()
	})

	t.Run("invalid state", func(t *testing.T) {
		fsw.internalTasks = map[UniqueID]*internalTask{
			segID: {
				state:       indexTaskDeleted,
				segmentInfo: nil,
			},
		}
		fsw.internalProcess(segID)
	})
}

func Test_flushSegmentWatcher_prepare_error(t *testing.T) {
	t.Run("segmentInfo already exist", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		fsw := &flushedSegmentWatcher{
			ctx:               ctx,
			cancel:            cancel,
			kvClient:          NewMockEtcdKV(),
			wg:                sync.WaitGroup{},
			scheduleDuration:  time.Second,
			internalTaskMutex: sync.RWMutex{},
			internalNotify:    make(chan struct{}, 1),
			etcdRevision:      0,
			watchChan:         nil,
			meta:              nil,
			builder:           nil,
			ic: &IndexCoord{
				dataCoordClient: NewDataCoordMock(),
			},
			handoff: nil,
			internalTasks: map[UniqueID]*internalTask{
				segID: {
					state: indexTaskPrepare,
					segmentInfo: &datapb.SegmentInfo{
						CollectionID: collID,
						PartitionID:  partID,
						ID:           segID,
					},
				},
			},
		}
		err := fsw.prepare(segID)
		assert.NoError(t, err)
	})

	t.Run("segment is not exist", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		fsw := &flushedSegmentWatcher{
			ctx:               ctx,
			cancel:            cancel,
			kvClient:          NewMockEtcdKV(),
			wg:                sync.WaitGroup{},
			scheduleDuration:  time.Second,
			internalTaskMutex: sync.RWMutex{},
			internalNotify:    make(chan struct{}, 1),
			etcdRevision:      0,
			watchChan:         nil,
			meta:              nil,
			builder:           nil,
			ic: &IndexCoord{
				dataCoordClient: &DataCoordMock{
					CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
						return &datapb.GetSegmentInfoResponse{
							Status: &commonpb.Status{
								ErrorCode: commonpb.ErrorCode_Success,
							},
							Infos: nil,
						}, nil
					},
				},
			},
			handoff: nil,
			internalTasks: map[UniqueID]*internalTask{
				segID: {
					state:       indexTaskPrepare,
					segmentInfo: nil,
				},
			},
		}

		err := fsw.prepare(segID)
		assert.ErrorIs(t, err, ErrSegmentNotFound)
	})
}
