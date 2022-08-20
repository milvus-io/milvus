package datacoord

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

type MockedTxnKV struct {
	kv.TxnKV
	multiSave func(kvs map[string]string) error
	save      func(key, value string) error
}

func (mc *MockedTxnKV) MultiSave(kvs map[string]string) error {
	return mc.multiSave(kvs)
}

func (mc *MockedTxnKV) Save(key, value string) error {
	return mc.save(key, value)
}

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

func Test_AlterSegmentsAndAddNewSegment_SaveError(t *testing.T) {
	txn := &MockedTxnKV{}
	txn.multiSave = func(kvs map[string]string) error {
		return errors.New("error")
	}

	catalog := &Catalog{txn}
	err := catalog.AlterSegmentsAndAddNewSegment(context.TODO(), segments, newSegment)
	assert.Error(t, err)
}

func Test_SaveDroppedSegmentsInBatch_SaveError(t *testing.T) {
	txn := &MockedTxnKV{}
	txn.multiSave = func(kvs map[string]string) error {
		return errors.New("error")
	}

	catalog := &Catalog{txn}
	segments := map[int64]*datapb.SegmentInfo{
		1: {
			ID:           1,
			CollectionID: 1000,
		},
	}
	ids, err := catalog.SaveDroppedSegmentsInBatch(context.TODO(), segments)
	assert.Nil(t, ids)
	assert.Error(t, err)
}

func Test_MarkChannelDeleted_SaveError(t *testing.T) {
	txn := &MockedTxnKV{}
	txn.save = func(key, value string) error {
		return errors.New("error")
	}

	catalog := &Catalog{txn}
	err := catalog.MarkChannelDeleted(context.TODO(), "test_channel_1")
	assert.Error(t, err)
}
