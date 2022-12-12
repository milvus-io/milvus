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
	"errors"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/metautil"
)

type MockedTxnKV struct {
	kv.TxnKV
	multiSave      func(kvs map[string]string) error
	save           func(key, value string) error
	loadWithPrefix func(key string) ([]string, []string, error)
	load           func(key string) (string, error)
	multiRemove    func(keys []string) error
	remove         func(key string) error
}

var (
	logID        = int64(99)
	collectionID = int64(2)
	partitionID  = int64(1)
	segmentID    = int64(11)
	segmentID2   = int64(1)
	fieldID      = int64(1)

	binlogPath   = metautil.BuildInsertLogPath("a", collectionID, partitionID, segmentID, fieldID, logID)
	deltalogPath = metautil.BuildDeltaLogPath("a", collectionID, partitionID, segmentID, logID)
	statslogPath = metautil.BuildStatsLogPath("a", collectionID, partitionID, segmentID, fieldID, logID)

	binlogPath2   = metautil.BuildInsertLogPath("a", collectionID, partitionID, segmentID2, fieldID, logID)
	deltalogPath2 = metautil.BuildDeltaLogPath("a", collectionID, partitionID, segmentID2, logID)
	statslogPath2 = metautil.BuildStatsLogPath("a", collectionID, partitionID, segmentID2, fieldID, logID)

	k1 = buildFieldBinlogPath(collectionID, partitionID, segmentID, fieldID)
	k2 = buildFieldDeltalogPath(collectionID, partitionID, segmentID, fieldID)
	k3 = buildFieldStatslogPath(collectionID, partitionID, segmentID, fieldID)
	k4 = buildSegmentPath(collectionID, partitionID, segmentID2)
	k5 = buildSegmentPath(collectionID, partitionID, segmentID)
	k6 = buildFlushedSegmentPath(collectionID, partitionID, segmentID)
	k7 = buildFieldBinlogPath(collectionID, partitionID, segmentID2, fieldID)
	k8 = buildFieldDeltalogPath(collectionID, partitionID, segmentID2, fieldID)
	k9 = buildFieldStatslogPath(collectionID, partitionID, segmentID2, fieldID)

	keys = map[string]struct{}{
		k1: {},
		k2: {},
		k3: {},
		k4: {},
		k5: {},
		k6: {},
		k7: {},
		k8: {},
		k9: {},
	}

	invalidSegment = &datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		NumOfRows:    100,
		State:        commonpb.SegmentState_Flushed,
		Binlogs: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 5,
						LogPath:    "badpath",
					},
				},
			},
		},
	}

	binlogs = []*datapb.FieldBinlog{
		{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 5,
					LogPath:    binlogPath,
				},
			},
		},
	}

	deltalogs = []*datapb.FieldBinlog{
		{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 5,
					LogPath:    deltalogPath,
				}},
		},
	}
	statslogs = []*datapb.FieldBinlog{
		{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 5,
					LogPath:    statslogPath,
				},
			},
		},
	}

	getlogs = func(logpath string) []*datapb.FieldBinlog {
		return []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 5,
						LogPath:    logpath,
					},
				},
			},
		}
	}

	segment1 = &datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		NumOfRows:    100,
		State:        commonpb.SegmentState_Flushed,
		Binlogs:      binlogs,
		Deltalogs:    deltalogs,
		Statslogs:    statslogs,
	}

	droppedSegment = &datapb.SegmentInfo{
		ID:           segmentID2,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		NumOfRows:    100,
		State:        commonpb.SegmentState_Dropped,
		Binlogs:      getlogs(binlogPath2),
		Deltalogs:    getlogs(deltalogPath2),
		Statslogs:    getlogs(statslogPath2),
	}
)

func (mc *MockedTxnKV) MultiSave(kvs map[string]string) error {
	return mc.multiSave(kvs)
}

func (mc *MockedTxnKV) Save(key, value string) error {
	return mc.save(key, value)
}

func (mc *MockedTxnKV) LoadWithPrefix(key string) ([]string, []string, error) {
	return mc.loadWithPrefix(key)
}

func (mc *MockedTxnKV) MultiRemove(keys []string) error {
	return mc.multiRemove(keys)
}

func (mc *MockedTxnKV) Load(key string) (string, error) {
	return mc.load(key)
}

func (mc *MockedTxnKV) Remove(key string) error {
	return mc.remove(key)
}

func Test_ListSegments(t *testing.T) {
	t.Run("load failed", func(t *testing.T) {
		txn := &MockedTxnKV{}
		txn.loadWithPrefix = func(key string) ([]string, []string, error) {
			return nil, nil, errors.New("error")
		}

		catalog := &Catalog{txn, "a"}
		ret, err := catalog.ListSegments(context.TODO())
		assert.Nil(t, ret)
		assert.Error(t, err)
	})

	verifySegments := func(t *testing.T, logID int64, ret []*datapb.SegmentInfo) {
		assert.Equal(t, 1, len(ret))
		segment := ret[0]
		assert.Equal(t, segmentID, segment.ID)
		assert.Equal(t, collectionID, segment.CollectionID)
		assert.Equal(t, partitionID, segment.PartitionID)

		assert.Equal(t, 1, len(segment.Binlogs))
		assert.Equal(t, fieldID, segment.Binlogs[0].FieldID)
		assert.Equal(t, 1, len(segment.Binlogs[0].Binlogs))
		assert.Equal(t, logID, segment.Binlogs[0].Binlogs[0].LogID)
		assert.Equal(t, binlogPath, segment.Binlogs[0].Binlogs[0].LogPath)

		assert.Equal(t, 1, len(segment.Deltalogs))
		assert.Equal(t, fieldID, segment.Deltalogs[0].FieldID)
		assert.Equal(t, 1, len(segment.Deltalogs[0].Binlogs))
		assert.Equal(t, logID, segment.Deltalogs[0].Binlogs[0].LogID)
		assert.Equal(t, deltalogPath, segment.Deltalogs[0].Binlogs[0].LogPath)

		assert.Equal(t, 1, len(segment.Statslogs))
		assert.Equal(t, fieldID, segment.Statslogs[0].FieldID)
		assert.Equal(t, 1, len(segment.Statslogs[0].Binlogs))
		assert.Equal(t, logID, segment.Statslogs[0].Binlogs[0].LogID)
		assert.Equal(t, statslogPath, segment.Statslogs[0].Binlogs[0].LogPath)
	}

	t.Run("test compatibility", func(t *testing.T) {
		txn := &MockedTxnKV{}
		segBytes, err := proto.Marshal(segment1)
		assert.NoError(t, err)

		txn.loadWithPrefix = func(key string) ([]string, []string, error) {
			return []string{k5}, []string{string(segBytes)}, nil
		}

		catalog := &Catalog{txn, "a"}
		ret, err := catalog.ListSegments(context.TODO())
		assert.NotNil(t, ret)
		assert.NoError(t, err)

		verifySegments(t, int64(0), ret)
	})

	t.Run("list successfully", func(t *testing.T) {
		txn := &MockedTxnKV{}
		var savedKvs map[string]string
		txn.multiSave = func(kvs map[string]string) error {
			savedKvs = kvs
			return nil
		}

		catalog := &Catalog{txn, "a"}
		err := catalog.AddSegment(context.TODO(), segment1)
		assert.Nil(t, err)

		txn.loadWithPrefix = func(key string) ([]string, []string, error) {
			if strings.HasPrefix(k5, key) {
				return []string{k5}, []string{savedKvs[k5]}, nil
			}

			if strings.HasPrefix(k1, key) {
				return []string{k1}, []string{savedKvs[k1]}, nil
			}

			if strings.HasPrefix(k2, key) {
				return []string{k2}, []string{savedKvs[k2]}, nil

			}
			if strings.HasPrefix(k3, key) {
				return []string{k3}, []string{savedKvs[k3]}, nil

			}
			return nil, nil, errors.New("should not reach here")
		}

		ret, err := catalog.ListSegments(context.TODO())
		assert.NotNil(t, ret)
		assert.Nil(t, err)

		verifySegments(t, logID, ret)
	})
}

func Test_AddSegments(t *testing.T) {
	t.Run("generate binlog kvs failed", func(t *testing.T) {
		txn := &MockedTxnKV{}
		txn.multiSave = func(kvs map[string]string) error {
			return errors.New("error")
		}

		catalog := &Catalog{txn, "a"}
		assert.Panics(t, func() {
			catalog.AddSegment(context.TODO(), invalidSegment)
		})
	})

	t.Run("save error", func(t *testing.T) {
		txn := &MockedTxnKV{}
		txn.multiSave = func(kvs map[string]string) error {
			return errors.New("error")
		}

		catalog := &Catalog{txn, "a"}
		err := catalog.AddSegment(context.TODO(), segment1)
		assert.Error(t, err)
	})

	t.Run("save successfully", func(t *testing.T) {
		txn := &MockedTxnKV{}
		savedKvs := make(map[string]string)
		txn.multiSave = func(kvs map[string]string) error {
			savedKvs = kvs
			return nil
		}
		txn.load = func(key string) (string, error) {
			if v, ok := savedKvs[key]; ok {
				return v, nil
			}
			return "", errors.New("key not found")
		}

		catalog := &Catalog{txn, "a"}
		err := catalog.AddSegment(context.TODO(), segment1)
		assert.Nil(t, err)
		adjustedSeg, err := catalog.LoadFromSegmentPath(segment1.CollectionID, segment1.PartitionID, segment1.ID)
		assert.NoError(t, err)
		// Check that num of rows is corrected from 100 to 5.
		assert.Equal(t, int64(100), segment1.GetNumOfRows())
		assert.Equal(t, int64(5), adjustedSeg.GetNumOfRows())

		_, ok := savedKvs[k4]
		assert.False(t, ok)
		assert.Equal(t, 4, len(savedKvs))
		verifySavedKvsForSegment(t, savedKvs)
	})
}

func Test_AlterSegments(t *testing.T) {
	t.Run("generate binlog kvs failed", func(t *testing.T) {
		txn := &MockedTxnKV{}
		txn.multiSave = func(kvs map[string]string) error {
			return errors.New("error")
		}

		catalog := &Catalog{txn, "a"}
		assert.Panics(t, func() {
			catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{invalidSegment})
		})
	})

	t.Run("save error", func(t *testing.T) {
		txn := &MockedTxnKV{}
		txn.multiSave = func(kvs map[string]string) error {
			return errors.New("error")
		}

		catalog := &Catalog{txn, "a"}
		err := catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{segment1})

		assert.Error(t, err)
	})

	t.Run("save successfully", func(t *testing.T) {
		txn := &MockedTxnKV{}
		var savedKvs map[string]string
		txn.multiSave = func(kvs map[string]string) error {
			savedKvs = kvs
			return nil
		}

		catalog := &Catalog{txn, "a"}
		err := catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{})
		assert.Nil(t, err)

		err = catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{segment1})
		assert.Nil(t, err)

		_, ok := savedKvs[k4]
		assert.False(t, ok)
		assert.Equal(t, 4, len(savedKvs))
		verifySavedKvsForSegment(t, savedKvs)
	})

	t.Run("save large ops successfully", func(t *testing.T) {
		txn := &MockedTxnKV{}
		savedKvs := make(map[string]string)
		opGroupCount := 0
		txn.multiSave = func(kvs map[string]string) error {
			var ks []string
			for k := range kvs {
				ks = append(ks, k)
			}
			maps.Copy(savedKvs, kvs)
			opGroupCount++
			return nil
		}
		txn.load = func(key string) (string, error) {
			if v, ok := savedKvs[key]; ok {
				return v, nil
			}
			return "", errors.New("key not found")
		}

		catalog := &Catalog{txn, "a"}
		err := catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{})
		assert.Nil(t, err)

		var binlogXL []*datapb.FieldBinlog
		for i := 0; i < 255; i++ {
			binlogXL = append(binlogXL, &datapb.FieldBinlog{
				FieldID: int64(i),
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 5,
						LogPath:    binlogPath,
					},
				},
			})
		}

		segmentXL := &datapb.SegmentInfo{
			ID:           segmentID,
			CollectionID: collectionID,
			PartitionID:  partitionID,
			NumOfRows:    100,
			State:        commonpb.SegmentState_Flushed,
			Binlogs:      binlogXL,
			Deltalogs:    deltalogs,
			Statslogs:    statslogs,
		}

		err = catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{segmentXL})
		assert.Nil(t, err)
		assert.Equal(t, 255+3, len(savedKvs))
		assert.Equal(t, 3, opGroupCount)

		adjustedSeg, err := catalog.LoadFromSegmentPath(segmentXL.CollectionID, segmentXL.PartitionID, segmentXL.ID)
		assert.NoError(t, err)
		// Check that num of rows is corrected from 100 to 1275.
		assert.Equal(t, int64(100), segmentXL.GetNumOfRows())
		assert.Equal(t, int64(5), adjustedSeg.GetNumOfRows())
	})
}

func Test_AlterSegmentsAndAddNewSegment(t *testing.T) {
	t.Run("save error", func(t *testing.T) {
		txn := &MockedTxnKV{}
		txn.multiSave = func(kvs map[string]string) error {
			return errors.New("error")
		}

		catalog := &Catalog{txn, "a"}
		err := catalog.AlterSegmentsAndAddNewSegment(context.TODO(), []*datapb.SegmentInfo{}, segment1)
		assert.Error(t, err)
	})

	t.Run("get prefix fail", func(t *testing.T) {
		txn := &MockedTxnKV{}
		txn.loadWithPrefix = func(key string) ([]string, []string, error) {
			return nil, nil, errors.New("error")
		}

		catalog := &Catalog{txn, "a"}
		err := catalog.AlterSegmentsAndAddNewSegment(context.TODO(), []*datapb.SegmentInfo{droppedSegment}, nil)
		assert.Error(t, err)
	})

	t.Run("save successfully", func(t *testing.T) {
		txn := &MockedTxnKV{}
		savedKvs := make(map[string]string, 0)
		txn.multiSave = func(kvs map[string]string) error {
			maps.Copy(savedKvs, kvs)
			return nil
		}
		txn.loadWithPrefix = func(key string) ([]string, []string, error) {
			return []string{}, []string{}, nil
		}
		txn.load = func(key string) (string, error) {
			if v, ok := savedKvs[key]; ok {
				return v, nil
			}
			return "", errors.New("key not found")
		}

		catalog := &Catalog{txn, "a"}
		err := catalog.AlterSegmentsAndAddNewSegment(context.TODO(), []*datapb.SegmentInfo{droppedSegment}, segment1)
		assert.NoError(t, err)

		assert.Equal(t, 8, len(savedKvs))
		verifySavedKvsForDroppedSegment(t, savedKvs)
		verifySavedKvsForSegment(t, savedKvs)

		adjustedSeg, err := catalog.LoadFromSegmentPath(droppedSegment.CollectionID, droppedSegment.PartitionID, droppedSegment.ID)
		assert.NoError(t, err)
		// Check that num of rows is corrected from 100 to 5.
		assert.Equal(t, int64(100), droppedSegment.GetNumOfRows())
		assert.Equal(t, int64(5), adjustedSeg.GetNumOfRows())

		adjustedSeg, err = catalog.LoadFromSegmentPath(segment1.CollectionID, segment1.PartitionID, segment1.ID)
		assert.NoError(t, err)
		// Check that num of rows is corrected from 100 to 5.
		assert.Equal(t, int64(100), droppedSegment.GetNumOfRows())
		assert.Equal(t, int64(5), adjustedSeg.GetNumOfRows())
	})
}

func Test_DropSegment(t *testing.T) {
	t.Run("remove failed", func(t *testing.T) {
		txn := &MockedTxnKV{}
		txn.multiRemove = func(keys []string) error {
			return errors.New("error")
		}

		catalog := &Catalog{txn, "a"}
		err := catalog.DropSegment(context.TODO(), segment1)
		assert.Error(t, err)
	})

	t.Run("remove successfully", func(t *testing.T) {
		txn := &MockedTxnKV{}
		removedKvs := make(map[string]struct{}, 0)
		txn.multiRemove = func(keys []string) error {
			for _, key := range keys {
				removedKvs[key] = struct{}{}
			}
			return nil
		}

		catalog := &Catalog{txn, "a"}
		err := catalog.DropSegment(context.TODO(), segment1)
		assert.NoError(t, err)

		assert.Equal(t, 4, len(removedKvs))
		for _, k := range []string{k1, k2, k3, k5} {
			_, ok := removedKvs[k]
			assert.True(t, ok)
		}
	})
}

func Test_SaveDroppedSegmentsInBatch_SaveError(t *testing.T) {
	txn := &mocks.TxnKV{}
	txn.EXPECT().MultiSave(mock.Anything).Return(errors.New("mock error"))

	catalog := &Catalog{txn, ""}
	segments := []*datapb.SegmentInfo{
		{
			ID:           1,
			CollectionID: 1000,
		},
	}
	err := catalog.SaveDroppedSegmentsInBatch(context.TODO(), segments)
	assert.Error(t, err)
}

func Test_SaveDroppedSegmentsInBatch_MultiSave(t *testing.T) {
	var (
		count  = 0
		kvSize = 0
	)
	txn := &mocks.TxnKV{}
	txn.EXPECT().
		MultiSave(mock.Anything).
		Run(func(kvs map[string]string) {
			count++
			kvSize += len(kvs)
		}).
		Return(nil)

	catalog := &Catalog{txn, ""}

	// no segments
	{
		var segments []*datapb.SegmentInfo
		err := catalog.SaveDroppedSegmentsInBatch(context.TODO(), segments)
		assert.Nil(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, 0, kvSize)
	}

	// testing for no splitting
	{
		segments1 := []*datapb.SegmentInfo{
			{
				ID:           1,
				CollectionID: 1000,
				PartitionID:  100,
			},
		}

		err := catalog.SaveDroppedSegmentsInBatch(context.TODO(), segments1)
		assert.Nil(t, err)
		assert.Equal(t, 1, count)
		assert.Equal(t, 1, kvSize)
	}

	// testing for reaching max operation
	{
		segments2 := make([]*datapb.SegmentInfo, 129)
		for i := 0; i < 129; i++ {
			segments2[i] = &datapb.SegmentInfo{
				ID:           int64(i),
				CollectionID: 1000,
				PartitionID:  100,
			}
		}

		count = 0
		kvSize = 0
		err := catalog.SaveDroppedSegmentsInBatch(context.TODO(), segments2)
		assert.Nil(t, err)
		assert.Equal(t, 2, count)
		assert.Equal(t, 129, kvSize)
	}
}

func TestCatalog_RevertAlterSegmentsAndAddNewSegment(t *testing.T) {
	t.Run("save error", func(t *testing.T) {
		txn := &mocks.TxnKV{}
		txn.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything).Return(errors.New("mock error"))

		catalog := &Catalog{txn, ""}
		err := catalog.RevertAlterSegmentsAndAddNewSegment(context.TODO(), []*datapb.SegmentInfo{segment1}, droppedSegment)
		assert.Error(t, err)
	})

	t.Run("revert successfully", func(t *testing.T) {
		txn := &mocks.TxnKV{}
		txn.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything).Return(nil)
		catalog := &Catalog{txn, ""}
		err := catalog.RevertAlterSegmentsAndAddNewSegment(context.TODO(), []*datapb.SegmentInfo{segment1}, droppedSegment)
		assert.NoError(t, err)
	})
}

func TestChannelCP(t *testing.T) {
	mockVChannel := "fake-by-dev-rootcoord-dml-1-testchannelcp-v0"
	mockPChannel := "fake-by-dev-rootcoord-dml-1"

	pos := &internalpb.MsgPosition{
		ChannelName: mockPChannel,
		MsgID:       []byte{},
		Timestamp:   1000,
	}
	k := buildChannelCPKey(mockVChannel)
	v, err := proto.Marshal(pos)
	assert.NoError(t, err)

	t.Run("ListChannelCheckpoint", func(t *testing.T) {
		txn := &mocks.TxnKV{}
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)
		catalog := &Catalog{txn, ""}
		err := catalog.SaveChannelCheckpoint(context.TODO(), mockVChannel, pos)
		assert.NoError(t, err)

		txn.EXPECT().LoadWithPrefix(mock.Anything).Return([]string{k}, []string{string(v)}, nil)
		res, err := catalog.ListChannelCheckpoint(context.TODO())
		assert.NoError(t, err)
		assert.True(t, len(res) > 0)
	})

	t.Run("ListChannelCheckpoint failed", func(t *testing.T) {
		txn := &mocks.TxnKV{}
		catalog := &Catalog{txn, ""}
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, errors.New("mock error"))
		_, err = catalog.ListChannelCheckpoint(context.TODO())
		assert.Error(t, err)
	})

	t.Run("SaveChannelCheckpoint", func(t *testing.T) {
		txn := &mocks.TxnKV{}
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)
		catalog := &Catalog{txn, ""}
		err := catalog.SaveChannelCheckpoint(context.TODO(), mockVChannel, pos)
		assert.NoError(t, err)
	})

	t.Run("SaveChannelCheckpoint failed", func(t *testing.T) {
		txn := &mocks.TxnKV{}
		catalog := &Catalog{txn, ""}
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(errors.New("mock error"))
		err = catalog.SaveChannelCheckpoint(context.TODO(), mockVChannel, &internalpb.MsgPosition{})
		assert.Error(t, err)
	})

	t.Run("DropChannelCheckpoint", func(t *testing.T) {
		txn := &mocks.TxnKV{}
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)
		catalog := &Catalog{txn, ""}
		err := catalog.SaveChannelCheckpoint(context.TODO(), mockVChannel, pos)
		assert.NoError(t, err)

		txn.EXPECT().Remove(mock.Anything).Return(nil)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, nil)
		err = catalog.DropChannelCheckpoint(context.TODO(), mockVChannel)
		assert.NoError(t, err)
		res, err := catalog.ListChannelCheckpoint(context.TODO())
		assert.NoError(t, err)
		assert.True(t, len(res) == 0)
	})

	t.Run("DropChannelCheckpoint failed", func(t *testing.T) {
		txn := &mocks.TxnKV{}
		catalog := &Catalog{txn, ""}
		txn.EXPECT().Remove(mock.Anything).Return(errors.New("mock error"))
		err = catalog.DropChannelCheckpoint(context.TODO(), mockVChannel)
		assert.Error(t, err)
	})
}

func Test_MarkChannelDeleted_SaveError(t *testing.T) {
	txn := &mocks.TxnKV{}
	txn.EXPECT().
		Save(mock.Anything, mock.Anything).
		Return(errors.New("mock error"))

	catalog := &Catalog{txn, ""}
	err := catalog.MarkChannelDeleted(context.TODO(), "test_channel_1")
	assert.Error(t, err)
}

func verifyBinlogs(t *testing.T, binlogBytes []byte) {
	binlogs := &datapb.FieldBinlog{}
	err := proto.Unmarshal([]byte(binlogBytes), binlogs)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(binlogs.Binlogs))
	assert.Equal(t, int64(99), binlogs.Binlogs[0].GetLogID())
	assert.Equal(t, "", binlogs.Binlogs[0].GetLogPath())
}

func verifySegmentInfo(t *testing.T, segmentInfoBytes []byte) {
	segmentInfo := &datapb.SegmentInfo{}
	err := proto.Unmarshal(segmentInfoBytes, segmentInfo)
	assert.NoError(t, err)
	assert.Nil(t, segmentInfo.Binlogs)
	assert.Nil(t, segmentInfo.Binlogs)
	assert.Nil(t, segmentInfo.Deltalogs)
	assert.Equal(t, collectionID, segmentInfo.CollectionID)
	assert.Equal(t, segmentID, segmentInfo.ID)
}

func verifySavedKvsForSegment(t *testing.T, savedKvs map[string]string) {
	for k := range savedKvs {
		_, ok := keys[k]
		assert.True(t, ok)
	}

	for _, k := range []string{k1, k2, k3} {
		ret, ok := savedKvs[k]
		assert.True(t, ok)
		verifyBinlogs(t, []byte(ret))
	}

	ret, ok := savedKvs[k5]
	assert.True(t, ok)
	verifySegmentInfo(t, []byte(ret))
}

func verifySegmentInfo2(t *testing.T, segmentInfoBytes []byte) {
	segmentInfo := &datapb.SegmentInfo{}
	err := proto.Unmarshal(segmentInfoBytes, segmentInfo)
	assert.NoError(t, err)
	assert.Nil(t, segmentInfo.Binlogs)
	assert.Nil(t, segmentInfo.Binlogs)
	assert.Nil(t, segmentInfo.Deltalogs)
	assert.Equal(t, collectionID, segmentInfo.CollectionID)
	assert.Equal(t, segmentID2, segmentInfo.ID)
}

func verifySavedKvsForDroppedSegment(t *testing.T, savedKvs map[string]string) {
	for k := range savedKvs {
		_, ok := keys[k]
		assert.True(t, ok)
	}

	for _, k := range []string{k7, k8, k9} {
		ret, ok := savedKvs[k]
		assert.True(t, ok)
		verifyBinlogs(t, []byte(ret))
	}

	ret, ok := savedKvs[k4]
	assert.True(t, ok)
	verifySegmentInfo2(t, []byte(ret))
}

func TestCatalog_CreateIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		txn := &MockedTxnKV{
			save: func(key, value string) error {
				return nil
			},
		}

		catalog := &Catalog{
			Txn: txn,
		}

		err := catalog.CreateIndex(context.Background(), &model.Index{})
		assert.NoError(t, err)
	})

	t.Run("failed", func(t *testing.T) {
		txn := &MockedTxnKV{
			save: func(key, value string) error {
				return errors.New("error")
			},
		}

		catalog := &Catalog{
			Txn: txn,
		}

		err := catalog.CreateIndex(context.Background(), &model.Index{})
		assert.Error(t, err)
	})
}

func TestCatalog_ListIndexes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		txn := &MockedTxnKV{
			loadWithPrefix: func(key string) ([]string, []string, error) {
				i := &datapb.FieldIndex{
					IndexInfo: &datapb.IndexInfo{
						CollectionID: 0,
						FieldID:      0,
						IndexName:    "",
						IndexID:      0,
						TypeParams:   nil,
						IndexParams:  nil,
					},
					Deleted:    false,
					CreateTime: 0,
				}
				v, err := proto.Marshal(i)
				assert.NoError(t, err)
				return []string{"1"}, []string{string(v)}, nil
			},
		}
		catalog := &Catalog{
			Txn: txn,
		}
		indexes, err := catalog.ListIndexes(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(indexes))
	})

	t.Run("failed", func(t *testing.T) {
		txn := &MockedTxnKV{
			loadWithPrefix: func(key string) ([]string, []string, error) {
				return []string{}, []string{}, errors.New("error")
			},
		}
		catalog := &Catalog{
			Txn: txn,
		}
		_, err := catalog.ListIndexes(context.Background())
		assert.Error(t, err)
	})

	t.Run("unmarshal failed", func(t *testing.T) {
		txn := &MockedTxnKV{
			loadWithPrefix: func(key string) ([]string, []string, error) {
				return []string{"1"}, []string{"invalid"}, nil
			},
		}
		catalog := &Catalog{
			Txn: txn,
		}
		_, err := catalog.ListIndexes(context.Background())
		assert.Error(t, err)
	})
}

func TestCatalog_AlterIndex(t *testing.T) {
	i := &model.Index{
		CollectionID: 0,
		FieldID:      0,
		IndexID:      0,
		IndexName:    "",
		IsDeleted:    false,
		CreateTime:   0,
		TypeParams:   nil,
		IndexParams:  nil,
	}
	t.Run("add", func(t *testing.T) {
		txn := &MockedTxnKV{
			save: func(key, value string) error {
				return nil
			},
		}
		catalog := &Catalog{
			Txn: txn,
		}

		err := catalog.AlterIndex(context.Background(), i)
		assert.NoError(t, err)
	})
}

func TestCatalog_AlterIndexes(t *testing.T) {
	i := &model.Index{
		CollectionID: 0,
		FieldID:      0,
		IndexID:      0,
		IndexName:    "",
		IsDeleted:    false,
		CreateTime:   0,
		TypeParams:   nil,
		IndexParams:  nil,
	}

	txn := &MockedTxnKV{
		multiSave: func(kvs map[string]string) error {
			return nil
		},
	}
	catalog := &Catalog{
		Txn: txn,
	}

	err := catalog.AlterIndexes(context.Background(), []*model.Index{i})
	assert.NoError(t, err)
}

func TestCatalog_DropIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		txn := &MockedTxnKV{
			remove: func(key string) error {
				return nil
			},
		}
		catalog := &Catalog{
			Txn: txn,
		}

		err := catalog.DropIndex(context.Background(), 0, 0)
		assert.NoError(t, err)
	})

	t.Run("failed", func(t *testing.T) {
		txn := &MockedTxnKV{
			remove: func(key string) error {
				return errors.New("error")
			},
		}
		catalog := &Catalog{
			Txn: txn,
		}

		err := catalog.DropIndex(context.Background(), 0, 0)
		assert.Error(t, err)
	})
}

func TestCatalog_CreateSegmentIndex(t *testing.T) {
	segIdx := &model.SegmentIndex{
		SegmentID:     1,
		CollectionID:  2,
		PartitionID:   3,
		NumRows:       1024,
		IndexID:       4,
		BuildID:       5,
		NodeID:        6,
		IndexState:    commonpb.IndexState_Finished,
		FailReason:    "",
		IndexVersion:  0,
		IsDeleted:     false,
		CreateTime:    0,
		IndexFileKeys: nil,
		IndexSize:     0,
	}

	t.Run("success", func(t *testing.T) {
		txn := &MockedTxnKV{
			save: func(key, value string) error {
				return nil
			},
		}
		catalog := &Catalog{
			Txn: txn,
		}

		err := catalog.CreateSegmentIndex(context.Background(), segIdx)
		assert.NoError(t, err)
	})

	t.Run("failed", func(t *testing.T) {
		txn := &MockedTxnKV{
			save: func(key, value string) error {
				return errors.New("error")
			},
		}
		catalog := &Catalog{
			Txn: txn,
		}

		err := catalog.CreateSegmentIndex(context.Background(), segIdx)
		assert.Error(t, err)
	})
}

func TestCatalog_ListSegmentIndexes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		segIdx := &datapb.SegmentIndex{
			CollectionID:  0,
			PartitionID:   0,
			SegmentID:     0,
			NumRows:       0,
			IndexID:       0,
			BuildID:       0,
			NodeID:        0,
			IndexVersion:  0,
			State:         0,
			FailReason:    "",
			IndexFileKeys: nil,
			Deleted:       false,
			CreateTime:    0,
			SerializeSize: 0,
		}
		v, err := proto.Marshal(segIdx)
		assert.NoError(t, err)

		txn := &MockedTxnKV{
			loadWithPrefix: func(key string) ([]string, []string, error) {
				return []string{"key"}, []string{string(v)}, nil
			},
		}
		catalog := &Catalog{
			Txn: txn,
		}

		segIdxes, err := catalog.ListSegmentIndexes(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(segIdxes))
	})

	t.Run("failed", func(t *testing.T) {
		txn := &MockedTxnKV{
			loadWithPrefix: func(key string) ([]string, []string, error) {
				return []string{}, []string{}, errors.New("error")
			},
		}
		catalog := &Catalog{
			Txn: txn,
		}

		_, err := catalog.ListSegmentIndexes(context.Background())
		assert.Error(t, err)
	})

	t.Run("unmarshal failed", func(t *testing.T) {
		txn := &MockedTxnKV{
			loadWithPrefix: func(key string) ([]string, []string, error) {
				return []string{"key"}, []string{"invalid"}, nil
			},
		}
		catalog := &Catalog{
			Txn: txn,
		}

		_, err := catalog.ListSegmentIndexes(context.Background())
		assert.Error(t, err)
	})
}

func TestCatalog_AlterSegmentIndex(t *testing.T) {
	segIdx := &model.SegmentIndex{
		SegmentID:     0,
		CollectionID:  0,
		PartitionID:   0,
		NumRows:       0,
		IndexID:       0,
		BuildID:       0,
		NodeID:        0,
		IndexState:    0,
		FailReason:    "",
		IndexVersion:  0,
		IsDeleted:     false,
		CreateTime:    0,
		IndexFileKeys: nil,
		IndexSize:     0,
	}

	t.Run("add", func(t *testing.T) {
		txn := &MockedTxnKV{
			save: func(key, value string) error {
				return nil
			},
		}
		catalog := &Catalog{
			Txn: txn,
		}

		err := catalog.AlterSegmentIndex(context.Background(), segIdx)
		assert.NoError(t, err)
	})
}

func TestCatalog_AlterSegmentIndexes(t *testing.T) {
	segIdx := &model.SegmentIndex{
		SegmentID:     0,
		CollectionID:  0,
		PartitionID:   0,
		NumRows:       0,
		IndexID:       0,
		BuildID:       0,
		NodeID:        0,
		IndexState:    0,
		FailReason:    "",
		IndexVersion:  0,
		IsDeleted:     false,
		CreateTime:    0,
		IndexFileKeys: nil,
		IndexSize:     0,
	}

	t.Run("add", func(t *testing.T) {
		txn := &MockedTxnKV{
			multiSave: func(kvs map[string]string) error {
				return nil
			},
		}
		catalog := &Catalog{
			Txn: txn,
		}

		err := catalog.AlterSegmentIndexes(context.Background(), []*model.SegmentIndex{segIdx})
		assert.NoError(t, err)
	})
}

func TestCatalog_DropSegmentIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		txn := &MockedTxnKV{
			remove: func(key string) error {
				return nil
			},
		}
		catalog := &Catalog{
			Txn: txn,
		}

		err := catalog.DropSegmentIndex(context.Background(), 0, 0, 0, 0)
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		txn := &MockedTxnKV{
			remove: func(key string) error {
				return errors.New("error")
			},
		}
		catalog := &Catalog{
			Txn: txn,
		}

		err := catalog.DropSegmentIndex(context.Background(), 0, 0, 0, 0)
		assert.Error(t, err)
	})
}
