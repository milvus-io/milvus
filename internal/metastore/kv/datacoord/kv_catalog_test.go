package datacoord

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/metautil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/exp/maps"
)

type MockedTxnKV struct {
	kv.TxnKV
	multiSave      func(kvs map[string]string) error
	save           func(key, value string) error
	loadWithPrefix func(key string) ([]string, []string, error)
	multiRemove    func(keys []string) error
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
		Binlogs:      binlogs,
		Deltalogs:    deltalogs,
		Statslogs:    statslogs,
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
		err := catalog.AddSegment(context.TODO(), invalidSegment)
		assert.Error(t, err)
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
		var savedKvs map[string]string
		txn.multiSave = func(kvs map[string]string) error {
			savedKvs = kvs
			return nil
		}

		catalog := &Catalog{txn, "a"}
		err := catalog.AddSegment(context.TODO(), segment1)
		assert.Nil(t, err)

		_, ok := savedKvs[k4]
		assert.False(t, ok)
		assert.Equal(t, 5, len(savedKvs))
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
		err := catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{invalidSegment})
		assert.Error(t, err)
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
		assert.Equal(t, 5, len(savedKvs))
		verifySavedKvsForSegment(t, savedKvs)
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

		catalog := &Catalog{txn, "a"}
		err := catalog.AlterSegmentsAndAddNewSegment(context.TODO(), []*datapb.SegmentInfo{droppedSegment}, segment1)
		assert.NoError(t, err)

		assert.Equal(t, 8, len(savedKvs))
		verifySavedKvsForDroppedSegment(t, savedKvs)
		verifySavedKvsForSegment(t, savedKvs)
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
		segments2 := make([]*datapb.SegmentInfo, 65)
		for i := 0; i < 65; i++ {
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
		assert.Equal(t, 65, kvSize)
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
