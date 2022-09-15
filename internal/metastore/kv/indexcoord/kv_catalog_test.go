package indexcoord

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

type MockedTxnKV struct {
	kv.TxnKV
	multiSave      func(kvs map[string]string) error
	save           func(key, value string) error
	loadWithPrefix func(key string) ([]string, []string, error)
	remove         func(key string) error
}

func (mc *MockedTxnKV) MultiSave(kvs map[string]string) error {
	return mc.multiSave(kvs)
}

func (mc *MockedTxnKV) Save(key, value string) error {
	return mc.save(key, value)
}

func (mc *MockedTxnKV) LoadWithPrefix(key string) ([]string, []string, error) {
	return mc.loadWithPrefix(key)
}

func (mc *MockedTxnKV) Remove(key string) error {
	return mc.remove(key)
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
				i := &indexpb.FieldIndex{
					IndexInfo: &indexpb.IndexInfo{
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
		SegmentID:      1,
		CollectionID:   2,
		PartitionID:    3,
		NumRows:        1024,
		IndexID:        4,
		BuildID:        5,
		NodeID:         6,
		IndexState:     commonpb.IndexState_Finished,
		FailReason:     "",
		IndexVersion:   0,
		IsDeleted:      false,
		CreateTime:     0,
		IndexFilePaths: nil,
		IndexSize:      0,
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
		segIdx := &indexpb.SegmentIndex{
			CollectionID:    0,
			PartitionID:     0,
			SegmentID:       0,
			NumRows:         0,
			IndexID:         0,
			BuildID:         0,
			NodeID:          0,
			IndexVersion:    0,
			State:           0,
			FailReason:      "",
			IndexFilesPaths: nil,
			Deleted:         false,
			CreateTime:      0,
			SerializeSize:   0,
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

		segIdxes, err := catalog.ListIndexes(context.Background())
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

		_, err := catalog.ListIndexes(context.Background())
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

		_, err := catalog.ListIndexes(context.Background())
		assert.Error(t, err)
	})
}

func TestCatalog_AlterSegmentIndex(t *testing.T) {
	segIdx := &model.SegmentIndex{
		SegmentID:      0,
		CollectionID:   0,
		PartitionID:    0,
		NumRows:        0,
		IndexID:        0,
		BuildID:        0,
		NodeID:         0,
		IndexState:     0,
		FailReason:     "",
		IndexVersion:   0,
		IsDeleted:      false,
		CreateTime:     0,
		IndexFilePaths: nil,
		IndexSize:      0,
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
		SegmentID:      0,
		CollectionID:   0,
		PartitionID:    0,
		NumRows:        0,
		IndexID:        0,
		BuildID:        0,
		NodeID:         0,
		IndexState:     0,
		FailReason:     "",
		IndexVersion:   0,
		IsDeleted:      false,
		CreateTime:     0,
		IndexFilePaths: nil,
		IndexSize:      0,
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
