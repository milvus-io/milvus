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

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/metastore/kv/indexcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func Test_flushSegmentWatcher(t *testing.T) {
	ctx := context.Background()

	watcher, err := newFlushSegmentWatcher(ctx,
		&mockETCDKV{
			loadWithRevision: func(key string) ([]string, []string, int64, error) {
				return []string{"seg1"}, []string{"12345"}, 1, nil
			},
		},
		&metaTable{
			catalog: &indexcoord.Catalog{
				Txn: NewMockEtcdKV(),
			},
		},
		&indexBuilder{}, &IndexCoord{
			dataCoordClient: NewDataCoordMock(),
		})
	assert.NoError(t, err)
	assert.NotNil(t, watcher)
}

func Test_flushSegmentWatcher_newFlushSegmentWatcher(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		fsw, err := newFlushSegmentWatcher(context.Background(),
			&mockETCDKV{
				loadWithRevision: func(key string) ([]string, []string, int64, error) {
					return []string{"segID1"}, []string{"12345"}, 1, nil
				},
			}, &metaTable{}, &indexBuilder{}, &IndexCoord{
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
			}, &metaTable{}, &indexBuilder{}, &IndexCoord{
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
			}, &metaTable{}, &indexBuilder{}, &IndexCoord{
				dataCoordClient: NewDataCoordMock(),
			})
		assert.Error(t, err)
		assert.Nil(t, fsw)
	})
}

func Test_flushSegmentWatcher_prepare(t *testing.T) {
	task := &internalTask{
		state:       indexTaskInit,
		segmentInfo: nil,
	}
	t.Run("success", func(t *testing.T) {
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
			},
			internalTasks: map[UniqueID]*internalTask{
				segID:       task,
				segID + 100: {state: indexTaskInit, segmentInfo: nil},
			},
			meta: &metaTable{
				segmentIndexLock: sync.RWMutex{},
				segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
					segID: {
						indexID: {
							SegmentID:    segID,
							CollectionID: collID,
							PartitionID:  partID,
							NumRows:      1000,
							IndexID:      indexID,
							BuildID:      buildID,
						},
					},
				},
			},
			builder: &indexBuilder{
				taskMutex:        sync.RWMutex{},
				scheduleDuration: 0,
				tasks:            nil,
				notifyChan:       nil,
			},
		}
		fsw.prepare(segID + 100)
		// idempotent
		fsw.prepare(segID + 100)
	})
	t.Run("init task get segmentInfo fail", func(t *testing.T) {
		fsw := &flushedSegmentWatcher{
			ic: &IndexCoord{
				dataCoordClient: &DataCoordMock{
					CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
						return nil, errors.New("error")
					},
				},
			},
			internalTasks: map[UniqueID]*internalTask{
				segID: task,
			},
		}
		fsw.prepare(segID)
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
		fsw.prepare(segID)

		fsw.ic.dataCoordClient = &DataCoordMock{
			CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
				return &datapb.GetSegmentInfoResponse{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_UnexpectedError,
						Reason:    msgSegmentNotFound(segID),
					},
				}, nil
			},
		}
		fsw.prepare(segID)
		_, ok := fsw.internalTasks[segID]
		assert.False(t, ok)

		fsw.ic.dataCoordClient = &DataCoordMock{
			CallGetSegmentInfo: func(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
				return &datapb.GetSegmentInfoResponse{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_Success,
						Reason:    "",
					},
					Infos: []*datapb.SegmentInfo{
						{
							ID:            segID + 100,
							CollectionID:  collID,
							PartitionID:   partID,
							InsertChannel: "",
							NumOfRows:     10000,
						},
					},
				}, nil
			},
		}
		fsw.internalTasks = map[UniqueID]*internalTask{
			segID: task,
		}
		fsw.prepare(segID)
	})

	t.Run("done task write handoff event fail", func(t *testing.T) {
		task := &internalTask{
			state: indexTaskDone,
			segmentInfo: &datapb.SegmentInfo{
				ID:                  0,
				CollectionID:        0,
				PartitionID:         0,
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
		}
		fsw := &flushedSegmentWatcher{
			ic: &IndexCoord{
				dataCoordClient: NewDataCoordMock(),
			},
			kvClient: &mockETCDKV{
				save: func(s string, s2 string) error {
					return errors.New("error")
				},
			},
			internalTasks: map[UniqueID]*internalTask{
				segID: task,
			},
		}
		fsw.prepare(segID)
		fsw.internalProcess(segID)
	})

	t.Run("done task remove flush segment fail", func(t *testing.T) {
		task := &internalTask{
			state: indexTaskDone,
			segmentInfo: &datapb.SegmentInfo{
				ID:                  0,
				CollectionID:        0,
				PartitionID:         0,
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
		}
		fsw := &flushedSegmentWatcher{
			ic: &IndexCoord{
				dataCoordClient: NewDataCoordMock(),
			},
			kvClient: &mockETCDKV{
				save: func(s string, s2 string) error {
					return nil
				},
				removeWithPrefix: func(s string) error {
					return errors.New("error")
				},
			},
			internalTasks: map[UniqueID]*internalTask{
				segID: task,
			},
		}

		fsw.prepare(segID)
		fsw.internalProcess(segID)
	})
}

func Test_flushSegmentWatcher_childrenProcess_error(t *testing.T) {
	task := &childrenTask{
		internalTask: internalTask{
			state: indexTaskInProgress,
			segmentInfo: &datapb.SegmentInfo{
				ID:             segID,
				CollectionID:   0,
				PartitionID:    0,
				InsertChannel:  "",
				NumOfRows:      0,
				State:          0,
				MaxRowNum:      0,
				LastExpireTime: 0,
				StartPosition: &internalpb.MsgPosition{
					Timestamp: 1,
				},
			},
		},
		indexInfo: &querypb.FieldIndexInfo{
			FieldID:        0,
			EnableIndex:    true,
			IndexName:      "",
			IndexID:        indexID,
			BuildID:        buildID,
			IndexParams:    nil,
			IndexFilePaths: nil,
			IndexSize:      0,
		},
	}

	t.Run("inProgress task not finish", func(t *testing.T) {
		fsw := &flushedSegmentWatcher{
			ic: &IndexCoord{
				dataCoordClient: NewDataCoordMock(),
			},
			childrenTasks: map[UniqueID]map[UniqueID]*childrenTask{
				segID: {
					indexID: task,
				},
			},
			meta: &metaTable{
				segmentIndexLock: sync.RWMutex{},
				segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
					segID: {
						indexID: &model.SegmentIndex{
							SegmentID:      segID,
							CollectionID:   0,
							PartitionID:    0,
							NumRows:        0,
							IndexID:        indexID,
							BuildID:        buildID,
							NodeID:         1,
							IndexVersion:   1,
							IndexState:     commonpb.IndexState_InProgress,
							FailReason:     "",
							IsDeleted:      false,
							CreateTime:     0,
							IndexFilePaths: nil,
							IndexSize:      0,
						},
					},
				},
			},
		}

		fsw.childrenProcess(task)
	})

	t.Run("inProgress not in meta", func(t *testing.T) {
		fsw := &flushedSegmentWatcher{
			ic: &IndexCoord{
				dataCoordClient: NewDataCoordMock(),
			},
			childrenTasks: map[UniqueID]map[UniqueID]*childrenTask{
				segID: {
					indexID: task,
				},
			},
			meta: &metaTable{
				segmentIndexLock: sync.RWMutex{},
				segmentIndexes:   map[UniqueID]map[UniqueID]*model.SegmentIndex{},
			},
		}

		fsw.childrenProcess(task)
	})
}
