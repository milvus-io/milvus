package kv

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/stretchr/testify/mock"
)

type MockedTxnKV struct {
	kv.TxnKV
	loadWithPrefixFn func(key string) ([]string, []string, error)
}

func (mc *MockedTxnKV) LoadWithPrefix(key string) ([]string, []string, error) {
	return mc.loadWithPrefixFn(key)
}

type MockedSnapShotKV struct {
	mock.Mock
	kv.SnapShotKV
}

var (
	indexName = "idx"
	IndexID   = 1
	index     = model.Index{
		CollectionID: 1,
		IndexName:    indexName,
		IndexID:      1,
		FieldID:      1,
		IndexParams:  []*commonpb.KeyValuePair{{Key: "index_type", Value: "STL_SORT"}},
		IsDeleted:    true,
		SegmentIndexes: map[int64]model.SegmentIndex{
			1: {
				Segment: model.Segment{
					SegmentID:   1,
					PartitionID: 1,
				},
				BuildID:     1000,
				EnableIndex: false,
				CreateTime:  0,
			},
		},
	}

	segIdx1 = model.SegmentIndex{
		Segment: model.Segment{
			SegmentID:   1,
			PartitionID: 1,
		},
		BuildID:     1000,
		EnableIndex: true,
		CreateTime:  10,
	}

	segIdx2 = model.SegmentIndex{
		Segment: model.Segment{
			SegmentID:   2,
			PartitionID: 1,
		},
		BuildID:     1000,
		EnableIndex: false,
		CreateTime:  10,
	}
)

func getStrIndexPb(t *testing.T) string {
	idxPB := model.MarshalIndexModel(&index)
	msg, err := proto.Marshal(idxPB)
	assert.Nil(t, err)
	return string(msg)
}

func getStrSegIdxPb(idx model.Index, newSegIdx model.SegmentIndex) (string, error) {
	segIdxInfo := &pb.SegmentIndexInfo{
		CollectionID: idx.CollectionID,
		PartitionID:  newSegIdx.PartitionID,
		SegmentID:    newSegIdx.SegmentID,
		BuildID:      newSegIdx.BuildID,
		EnableIndex:  newSegIdx.EnableIndex,
		CreateTime:   newSegIdx.CreateTime,
		FieldID:      idx.FieldID,
		IndexID:      idx.IndexID,
	}
	msg, err := proto.Marshal(segIdxInfo)
	if err != nil {
		return "", err
	}
	return string(msg), nil
}

func getStrSegIdxPbs(t *testing.T) []string {
	msg1, err := getStrSegIdxPb(index, segIdx1)
	assert.Nil(t, err)

	msg2, err := getStrSegIdxPb(index, segIdx2)
	assert.Nil(t, err)

	return []string{msg1, msg2}
}

func Test_ListIndexes(t *testing.T) {
	ctx := context.TODO()

	t.Run("test return err for remote services", func(t *testing.T) {
		tnx := &MockedTxnKV{}
		skv := &MockedSnapShotKV{}
		expectedErr := errors.New("error")
		tnx.loadWithPrefixFn = func(key string) ([]string, []string, error) {
			return []string{}, []string{}, expectedErr
		}
		catalog := &Catalog{tnx, skv}
		ret, err := catalog.ListIndexes(ctx)
		assert.Nil(t, ret)
		assert.ErrorIs(t, err, expectedErr)

		tnx = &MockedTxnKV{}
		skv = &MockedSnapShotKV{}
		tnx.loadWithPrefixFn = func(key string) ([]string, []string, error) {
			if key == SegmentIndexMetaPrefix {
				return []string{}, []string{}, expectedErr
			}
			return []string{}, []string{}, nil
		}

		catalog = &Catalog{tnx, skv}
		ret, err = catalog.ListIndexes(ctx)
		assert.Nil(t, ret)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("test list index successful", func(t *testing.T) {
		tnx := &MockedTxnKV{}
		skv := &MockedSnapShotKV{}
		tnx.loadWithPrefixFn = func(key string) ([]string, []string, error) {
			if key == SegmentIndexMetaPrefix {
				return []string{}, getStrSegIdxPbs(t), nil
			}
			if key == IndexMetaPrefix {
				return []string{}, []string{getStrIndexPb(t)}, nil
			}
			return []string{}, []string{}, nil
		}

		catalog := &Catalog{tnx, skv}
		idxs, err := catalog.ListIndexes(ctx)
		assert.Nil(t, err)

		assert.Equal(t, 1, len(idxs))
		assert.Equal(t, int64(1), idxs[0].IndexID)
		assert.Equal(t, 2, len(idxs[0].SegmentIndexes))
		assert.Equal(t, 2, len(idxs[0].SegmentIndexes))
		assert.Equal(t, true, idxs[0].SegmentIndexes[1].EnableIndex)
		assert.Equal(t, false, idxs[0].SegmentIndexes[2].EnableIndex)
	})

}
