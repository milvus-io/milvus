package datacoord

import (
	"context"
	"errors"
	"math/rand"
	"testing"

	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	segments = []*datapb.SegmentInfo{
		{
			ID:           1,
			CollectionID: 1000,
		},
	}

	newSegment = &datapb.SegmentInfo{
		ID:           2,
		CollectionID: 1000,
	}
)

func TestCatalog_AlterSegmentsAndAddNewSegment(t *testing.T) {
	t.Run("save error", func(t *testing.T) {
		txn := &mocks.TxnKV{}
		txn.EXPECT().MultiSave(mock.Anything).Return(errors.New("mock error"))

		catalog := &Catalog{txn}
		err := catalog.AlterSegmentsAndAddNewSegment(context.TODO(), segments, newSegment)
		assert.Error(t, err)
	})

	t.Run("numRow>0", func(t *testing.T) {
		txn := &mocks.TxnKV{}
		txn.EXPECT().MultiSave(mock.Anything).Return(nil)

		toAlter := []*datapb.SegmentInfo{
			{
				CollectionID: 100,
				PartitionID:  10,
				ID:           1,
			},
		}

		newSeg := &datapb.SegmentInfo{
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{},
				},
			},
			Deltalogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{},
				},
			},
			CollectionID: 100,
			PartitionID:  10,
			ID:           2,
			NumOfRows:    15,
		}

		catalog := &Catalog{txn}
		err := catalog.AlterSegmentsAndAddNewSegment(context.Background(), toAlter, newSeg)
		assert.NoError(t, err)
	})
}

func TestCatalog_RevertAlterSegmentsAndAddNewSegment(t *testing.T) {
	t.Run("save error", func(t *testing.T) {
		txn := &mocks.TxnKV{}
		txn.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything).Return(errors.New("mock error"))

		catalog := &Catalog{txn}
		err := catalog.RevertAlterSegmentsAndAddNewSegment(context.TODO(), segments, newSegment)
		assert.Error(t, err)
	})

	t.Run("numRow>0", func(t *testing.T) {
		txn := &mocks.TxnKV{}
		txn.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything).Return(nil)

		toAlter := []*datapb.SegmentInfo{
			{
				CollectionID: 100,
				PartitionID:  10,
				ID:           1,
			},
		}

		newSeg := &datapb.SegmentInfo{
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{},
				},
			},
			Deltalogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{},
				},
			},
			CollectionID: 100,
			PartitionID:  10,
			ID:           2,
			NumOfRows:    15,
		}

		catalog := &Catalog{txn}
		err := catalog.RevertAlterSegmentsAndAddNewSegment(context.Background(), toAlter, newSeg)
		assert.NoError(t, err)
	})
}

func Test_SaveDroppedSegmentsInBatch_SaveError(t *testing.T) {
	txn := &mocks.TxnKV{}
	txn.EXPECT().MultiSave(mock.Anything).Return(errors.New("mock error"))

	catalog := &Catalog{txn}
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

	catalog := &Catalog{txn}

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

	// testing for reaching max bytes size
	{
		segments3 := []*datapb.SegmentInfo{
			{
				ID:            int64(1),
				CollectionID:  1000,
				PartitionID:   100,
				InsertChannel: randomString(1024 * 1024 * 2),
			},
			{
				ID:            int64(2),
				CollectionID:  1000,
				PartitionID:   100,
				InsertChannel: randomString(1024),
			},
		}

		count = 0
		kvSize = 0
		err := catalog.SaveDroppedSegmentsInBatch(context.TODO(), segments3)
		assert.Nil(t, err)
		assert.Equal(t, 2, count)
		assert.Equal(t, 2, kvSize)
	}
}

func randomString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		bytes[i] = byte(65 + rand.Intn(25))
	}
	return string(bytes)
}

func Test_MarkChannelDeleted_SaveError(t *testing.T) {
	txn := &mocks.TxnKV{}
	txn.EXPECT().Save(mock.Anything, mock.Anything).Return(errors.New("mock error"))

	catalog := &Catalog{txn}
	err := catalog.MarkChannelDeleted(context.TODO(), "test_channel_1")
	assert.Error(t, err)
}
