package rootcoord

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/kv/mocks"

	"github.com/milvus-io/milvus/internal/metastore"

	"github.com/milvus-io/milvus/internal/proto/schemapb"

	"github.com/milvus-io/milvus/internal/util/typeutil"

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
		//EnableIndex:  newSegIdx.EnableIndex,
		CreateTime: newSegIdx.CreateTime,
		FieldID:    idx.FieldID,
		IndexID:    idx.IndexID,
	}
	msg, err := proto.Marshal(segIdxInfo)
	if err != nil {
		return "", err
	}
	return string(msg), nil
}

func TestCatalog_loadCollection(t *testing.T) {
	t.Run("load failed", func(t *testing.T) {
		ctx := context.Background()
		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadFunc = func(key string, ts typeutil.Timestamp) (string, error) {
			return "", errors.New("mock")
		}
		kc := Catalog{Snapshot: snapshot}
		_, err := kc.loadCollection(ctx, 1, 0)
		assert.Error(t, err)
	})

	t.Run("load, not collection info", func(t *testing.T) {
		ctx := context.Background()
		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadFunc = func(key string, ts typeutil.Timestamp) (string, error) {
			return "not in pb format", nil
		}
		kc := Catalog{Snapshot: snapshot}
		_, err := kc.loadCollection(ctx, 1, 0)
		assert.Error(t, err)
	})

	t.Run("load, normal collection info", func(t *testing.T) {
		ctx := context.Background()
		snapshot := kv.NewMockSnapshotKV()
		coll := &pb.CollectionInfo{ID: 1}
		value, err := proto.Marshal(coll)
		assert.NoError(t, err)
		snapshot.LoadFunc = func(key string, ts typeutil.Timestamp) (string, error) {
			return string(value), nil
		}
		kc := Catalog{Snapshot: snapshot}
		got, err := kc.loadCollection(ctx, 1, 0)
		assert.NoError(t, err)
		assert.Equal(t, got.GetID(), coll.GetID())
	})
}

func Test_partitionVersionAfter210(t *testing.T) {
	t.Run("after 210", func(t *testing.T) {
		coll := &pb.CollectionInfo{}
		assert.True(t, partitionVersionAfter210(coll))
	})

	t.Run("before 210", func(t *testing.T) {
		coll := &pb.CollectionInfo{
			PartitionIDs:               []int64{1},
			PartitionNames:             []string{"partition"},
			PartitionCreatedTimestamps: []uint64{2},
		}
		assert.False(t, partitionVersionAfter210(coll))
	})
}

func Test_partitionExist(t *testing.T) {
	t.Run("in partitions ids", func(t *testing.T) {
		coll := &pb.CollectionInfo{
			PartitionIDs: []int64{1},
		}
		assert.True(t, partitionExistByID(coll, 1))
	})

	t.Run("not exist", func(t *testing.T) {
		coll := &pb.CollectionInfo{}
		assert.False(t, partitionExistByID(coll, 1))
	})
}

func Test_partitionExistByName(t *testing.T) {
	t.Run("in partitions ids", func(t *testing.T) {
		coll := &pb.CollectionInfo{
			PartitionNames: []string{"partition"},
		}
		assert.True(t, partitionExistByName(coll, "partition"))
	})

	t.Run("not exist", func(t *testing.T) {
		coll := &pb.CollectionInfo{}
		assert.False(t, partitionExistByName(coll, "partition"))
	})
}

func TestCatalog_CreatePartitionV2(t *testing.T) {
	t.Run("collection not exist", func(t *testing.T) {
		ctx := context.Background()
		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadFunc = func(key string, ts typeutil.Timestamp) (string, error) {
			return "", errors.New("mock")
		}
		kc := Catalog{Snapshot: snapshot}
		err := kc.CreatePartition(ctx, &model.Partition{}, 0)
		assert.Error(t, err)
	})

	t.Run("partition version after 210", func(t *testing.T) {
		ctx := context.Background()

		coll := &pb.CollectionInfo{}
		value, err := proto.Marshal(coll)
		assert.NoError(t, err)

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadFunc = func(key string, ts typeutil.Timestamp) (string, error) {
			return string(value), nil
		}
		snapshot.SaveFunc = func(key string, value string, ts typeutil.Timestamp) error {
			return errors.New("mock")
		}

		kc := Catalog{Snapshot: snapshot}

		err = kc.CreatePartition(ctx, &model.Partition{}, 0)
		assert.Error(t, err)

		snapshot.SaveFunc = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}
		err = kc.CreatePartition(ctx, &model.Partition{}, 0)
		assert.NoError(t, err)
	})

	t.Run("partition version before 210, id exist", func(t *testing.T) {
		ctx := context.Background()

		partID := typeutil.UniqueID(1)
		coll := &pb.CollectionInfo{PartitionIDs: []int64{partID}}
		value, err := proto.Marshal(coll)
		assert.NoError(t, err)

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadFunc = func(key string, ts typeutil.Timestamp) (string, error) {
			return string(value), nil
		}

		kc := Catalog{Snapshot: snapshot}

		err = kc.CreatePartition(ctx, &model.Partition{PartitionID: partID}, 0)
		assert.Error(t, err)
	})

	t.Run("partition version before 210, name exist", func(t *testing.T) {
		ctx := context.Background()

		partition := "partition"
		coll := &pb.CollectionInfo{PartitionNames: []string{partition}}
		value, err := proto.Marshal(coll)
		assert.NoError(t, err)

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadFunc = func(key string, ts typeutil.Timestamp) (string, error) {
			return string(value), nil
		}

		kc := Catalog{Snapshot: snapshot}

		err = kc.CreatePartition(ctx, &model.Partition{PartitionName: partition}, 0)
		assert.Error(t, err)
	})

	t.Run("partition version before 210, not exist", func(t *testing.T) {
		ctx := context.Background()

		coll := &pb.CollectionInfo{
			PartitionNames:             []string{"partition"},
			PartitionIDs:               []int64{111},
			PartitionCreatedTimestamps: []uint64{111111},
		}
		value, err := proto.Marshal(coll)
		assert.NoError(t, err)

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadFunc = func(key string, ts typeutil.Timestamp) (string, error) {
			return string(value), nil
		}
		snapshot.SaveFunc = func(key string, value string, ts typeutil.Timestamp) error {
			return errors.New("mock")
		}

		kc := Catalog{Snapshot: snapshot}

		err = kc.CreatePartition(ctx, &model.Partition{}, 0)
		assert.Error(t, err)

		snapshot.SaveFunc = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}
		err = kc.CreatePartition(ctx, &model.Partition{}, 0)
		assert.NoError(t, err)
	})
}

func TestCatalog_CreateAliasV2(t *testing.T) {
	ctx := context.Background()

	snapshot := kv.NewMockSnapshotKV()
	snapshot.MultiSaveAndRemoveWithPrefixFunc = func(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
		return errors.New("mock")
	}

	kc := Catalog{Snapshot: snapshot}

	err := kc.CreateAlias(ctx, &model.Alias{}, 0)
	assert.Error(t, err)

	snapshot.MultiSaveAndRemoveWithPrefixFunc = func(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
		return nil
	}
	err = kc.CreateAlias(ctx, &model.Alias{}, 0)
	assert.NoError(t, err)
}

func TestCatalog_listPartitionsAfter210(t *testing.T) {
	t.Run("load failed", func(t *testing.T) {
		ctx := context.Background()

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, errors.New("mock")
		}

		kc := Catalog{Snapshot: snapshot}

		_, err := kc.listPartitionsAfter210(ctx, 1, 0)
		assert.Error(t, err)
	})

	t.Run("not in pb format", func(t *testing.T) {
		ctx := context.Background()

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return []string{"key"}, []string{"not in pb format"}, nil
		}

		kc := Catalog{Snapshot: snapshot}

		_, err := kc.listPartitionsAfter210(ctx, 1, 0)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()

		partition := &pb.PartitionInfo{PartitionID: 100}
		value, err := proto.Marshal(partition)
		assert.NoError(t, err)

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return []string{"key"}, []string{string(value)}, nil
		}

		kc := Catalog{Snapshot: snapshot}

		got, err := kc.listPartitionsAfter210(ctx, 1, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(got))
		assert.Equal(t, int64(100), got[0].PartitionID)
	})
}

func Test_fieldVersionAfter210(t *testing.T) {
	coll := &pb.CollectionInfo{Schema: &schemapb.CollectionSchema{}}
	assert.True(t, fieldVersionAfter210(coll))

	coll.Schema.Fields = []*schemapb.FieldSchema{{FieldID: 101}}
	assert.False(t, fieldVersionAfter210(coll))
}

func TestCatalog_listFieldsAfter210(t *testing.T) {
	t.Run("load failed", func(t *testing.T) {
		ctx := context.Background()

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, errors.New("mock")
		}

		kc := Catalog{Snapshot: snapshot}

		_, err := kc.listFieldsAfter210(ctx, 1, 0)
		assert.Error(t, err)
	})

	t.Run("not in pb format", func(t *testing.T) {
		ctx := context.Background()

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return []string{"key"}, []string{"not in pb format"}, nil
		}

		kc := Catalog{Snapshot: snapshot}

		_, err := kc.listFieldsAfter210(ctx, 1, 0)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()

		field := &schemapb.FieldSchema{FieldID: 101}
		value, err := proto.Marshal(field)
		assert.NoError(t, err)

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return []string{"key"}, []string{string(value)}, nil
		}

		kc := Catalog{Snapshot: snapshot}

		got, err := kc.listFieldsAfter210(ctx, 1, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(got))
		assert.Equal(t, int64(101), got[0].FieldID)
	})
}

func TestCatalog_AlterAliasV2(t *testing.T) {
	ctx := context.Background()

	snapshot := kv.NewMockSnapshotKV()
	snapshot.MultiSaveAndRemoveWithPrefixFunc = func(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
		return errors.New("mock")
	}

	kc := Catalog{Snapshot: snapshot}

	err := kc.AlterAlias(ctx, &model.Alias{}, 0)
	assert.Error(t, err)

	snapshot.MultiSaveAndRemoveWithPrefixFunc = func(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
		return nil
	}
	err = kc.AlterAlias(ctx, &model.Alias{}, 0)
	assert.NoError(t, err)
}

func Test_dropPartition(t *testing.T) {
	t.Run("nil, won't panic", func(t *testing.T) {
		dropPartition(nil, 1)
	})

	t.Run("not exist", func(t *testing.T) {
		coll := &pb.CollectionInfo{}
		dropPartition(coll, 100)
	})

	t.Run("in partition ids", func(t *testing.T) {
		coll := &pb.CollectionInfo{
			PartitionIDs:               []int64{100, 101, 102, 103, 104},
			PartitionNames:             []string{"0", "1", "2", "3", "4"},
			PartitionCreatedTimestamps: []uint64{1, 2, 3, 4, 5},
		}
		dropPartition(coll, 100)
		assert.Equal(t, 4, len(coll.GetPartitionIDs()))
		dropPartition(coll, 104)
		assert.Equal(t, 3, len(coll.GetPartitionIDs()))
		dropPartition(coll, 102)
		assert.Equal(t, 2, len(coll.GetPartitionIDs()))
		dropPartition(coll, 103)
		assert.Equal(t, 1, len(coll.GetPartitionIDs()))
		dropPartition(coll, 101)
		assert.Equal(t, 0, len(coll.GetPartitionIDs()))
	})
}

func TestCatalog_DropPartitionV2(t *testing.T) {
	t.Run("failed to load collection", func(t *testing.T) {
		ctx := context.Background()

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadFunc = func(key string, ts typeutil.Timestamp) (string, error) {
			return "", errors.New("mock")
		}

		kc := Catalog{Snapshot: snapshot}

		err := kc.DropPartition(ctx, 100, 101, 0)
		assert.Error(t, err)
	})

	t.Run("partition version after 210", func(t *testing.T) {
		ctx := context.Background()

		coll := &pb.CollectionInfo{}
		value, err := proto.Marshal(coll)
		assert.NoError(t, err)

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadFunc = func(key string, ts typeutil.Timestamp) (string, error) {
			return string(value), nil
		}
		snapshot.MultiSaveAndRemoveWithPrefixFunc = func(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
			return errors.New("mock")
		}

		kc := Catalog{Snapshot: snapshot}

		err = kc.DropPartition(ctx, 100, 101, 0)
		assert.Error(t, err)

		snapshot.MultiSaveAndRemoveWithPrefixFunc = func(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
			return nil
		}
		err = kc.DropPartition(ctx, 100, 101, 0)
		assert.NoError(t, err)
	})

	t.Run("partition before 210", func(t *testing.T) {
		ctx := context.Background()

		coll := &pb.CollectionInfo{
			PartitionIDs:               []int64{101, 102},
			PartitionNames:             []string{"partition1", "partition2"},
			PartitionCreatedTimestamps: []uint64{101, 102},
		}
		value, err := proto.Marshal(coll)
		assert.NoError(t, err)

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadFunc = func(key string, ts typeutil.Timestamp) (string, error) {
			return string(value), nil
		}
		snapshot.SaveFunc = func(key string, value string, ts typeutil.Timestamp) error {
			return errors.New("mock")
		}

		kc := Catalog{Snapshot: snapshot}

		err = kc.DropPartition(ctx, 100, 101, 0)
		assert.Error(t, err)

		snapshot.SaveFunc = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}
		err = kc.DropPartition(ctx, 100, 102, 0)
		assert.NoError(t, err)
	})
}

func TestCatalog_DropAliasV2(t *testing.T) {
	ctx := context.Background()

	snapshot := kv.NewMockSnapshotKV()
	snapshot.MultiSaveAndRemoveWithPrefixFunc = func(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
		return errors.New("mock")
	}

	kc := Catalog{Snapshot: snapshot}

	err := kc.DropAlias(ctx, "alias", 0)
	assert.Error(t, err)

	snapshot.MultiSaveAndRemoveWithPrefixFunc = func(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
		return nil
	}
	err = kc.DropAlias(ctx, "alias", 0)
	assert.NoError(t, err)
}

func TestCatalog_listAliasesBefore210(t *testing.T) {
	t.Run("load failed", func(t *testing.T) {
		ctx := context.Background()

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, errors.New("mock")
		}

		kc := Catalog{Snapshot: snapshot}

		_, err := kc.listAliasesBefore210(ctx, 0)
		assert.Error(t, err)
	})

	t.Run("not in pb format", func(t *testing.T) {
		ctx := context.Background()

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return []string{"key"}, []string{"not in pb format"}, nil
		}

		kc := Catalog{Snapshot: snapshot}

		_, err := kc.listAliasesBefore210(ctx, 0)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()

		coll := &pb.CollectionInfo{ID: 100}
		value, err := proto.Marshal(coll)
		assert.NoError(t, err)

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return []string{"key"}, []string{string(value)}, nil
		}

		kc := Catalog{Snapshot: snapshot}

		got, err := kc.listAliasesBefore210(ctx, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(got))
		assert.Equal(t, int64(100), got[0].CollectionID)
	})
}

func TestCatalog_listAliasesAfter210(t *testing.T) {
	t.Run("load failed", func(t *testing.T) {
		ctx := context.Background()

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return nil, nil, errors.New("mock")
		}

		kc := Catalog{Snapshot: snapshot}

		_, err := kc.listAliasesAfter210(ctx, 0)
		assert.Error(t, err)
	})

	t.Run("not in pb format", func(t *testing.T) {
		ctx := context.Background()

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return []string{"key"}, []string{"not in pb format"}, nil
		}

		kc := Catalog{Snapshot: snapshot}

		_, err := kc.listAliasesAfter210(ctx, 0)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()

		coll := &pb.AliasInfo{CollectionId: 100}
		value, err := proto.Marshal(coll)
		assert.NoError(t, err)

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return []string{"key"}, []string{string(value)}, nil
		}

		kc := Catalog{Snapshot: snapshot}

		got, err := kc.listAliasesAfter210(ctx, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(got))
		assert.Equal(t, int64(100), got[0].CollectionID)
	})
}

func TestCatalog_ListAliasesV2(t *testing.T) {
	t.Run("failed to list aliases before 210", func(t *testing.T) {
		ctx := context.Background()

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return []string{"key"}, []string{"not in pb format"}, nil
		}

		kc := Catalog{Snapshot: snapshot}

		_, err := kc.ListAliases(ctx, 0)
		assert.Error(t, err)
	})

	t.Run("failed to list aliases after 210", func(t *testing.T) {
		ctx := context.Background()

		coll := &pb.CollectionInfo{ID: 100, ShardsNum: 50}
		value, err := proto.Marshal(coll)
		assert.NoError(t, err)

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			if key == AliasMetaPrefix {
				return nil, nil, errors.New("mock")
			}
			return []string{"key"}, []string{string(value)}, nil
		}

		kc := Catalog{Snapshot: snapshot}

		_, err = kc.ListAliases(ctx, 0)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()

		coll := &pb.CollectionInfo{Schema: &schemapb.CollectionSchema{Name: "alias1"}, ID: 100, ShardsNum: 50}
		value, err := proto.Marshal(coll)
		assert.NoError(t, err)

		alias := &pb.AliasInfo{CollectionId: 101, AliasName: "alias2"}
		value2, err := proto.Marshal(alias)
		assert.NoError(t, err)

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			if key == AliasMetaPrefix {
				return []string{"key1"}, []string{string(value2)}, nil
			}
			return []string{"key"}, []string{string(value)}, nil
		}

		kc := Catalog{Snapshot: snapshot}

		got, err := kc.ListAliases(ctx, 0)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(got))
		assert.Equal(t, "alias1", got[0].Name)
		assert.Equal(t, "alias2", got[1].Name)
	})
}

func Test_buildKvs(t *testing.T) {
	t.Run("length not equal", func(t *testing.T) {
		keys := []string{"k1", "k2"}
		values := []string{"v1"}
		_, err := buildKvs(keys, values)
		assert.Error(t, err)
	})

	t.Run("duplicate", func(t *testing.T) {
		keys := []string{"k1", "k1"}
		values := []string{"v1", "v2"}
		_, err := buildKvs(keys, values)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		keys := []string{"k1", "k2"}
		values := []string{"v1", "v2"}
		kvs, err := buildKvs(keys, values)
		assert.NoError(t, err)
		for i, k := range keys {
			v, ok := kvs[k]
			assert.True(t, ok)
			assert.Equal(t, values[i], v)
		}
	})
}

func Test_batchSave(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		snapshot := kv.NewMockSnapshotKV()
		snapshot.MultiSaveFunc = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		kvs := map[string]string{
			"k1": "v1",
			"k2": "v2",
			"k3": "v3",
		}
		maxTxnNum := 2
		err := batchSave(snapshot, maxTxnNum, kvs, 100)
		assert.NoError(t, err)
	})

	t.Run("multi save failed", func(t *testing.T) {
		snapshot := kv.NewMockSnapshotKV()
		snapshot.MultiSaveFunc = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return errors.New("mock")
		}
		kvs := map[string]string{
			"k1": "v1",
			"k2": "v2",
			"k3": "v3",
		}
		maxTxnNum := 2
		err := batchSave(snapshot, maxTxnNum, kvs, 100)
		assert.Error(t, err)
	})
}

func Test_min(t *testing.T) {
	type args struct {
		a int
		b int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			args: args{a: 1, b: 2},
			want: 1,
		},
		{
			args: args{a: 4, b: 3},
			want: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := min(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("min() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_batchMultiSaveAndRemoveWithPrefix(t *testing.T) {
	t.Run("failed to save", func(t *testing.T) {
		snapshot := kv.NewMockSnapshotKV()
		snapshot.MultiSaveFunc = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return errors.New("error mock MultiSave")
		}
		saves := map[string]string{"k": "v"}
		err := batchMultiSaveAndRemoveWithPrefix(snapshot, maxTxnNum, saves, []string{}, 0)
		assert.Error(t, err)
	})
	t.Run("failed to remove", func(t *testing.T) {
		snapshot := kv.NewMockSnapshotKV()
		snapshot.MultiSaveFunc = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		snapshot.MultiSaveAndRemoveWithPrefixFunc = func(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
			return errors.New("error mock MultiSaveAndRemoveWithPrefix")
		}
		saves := map[string]string{"k": "v"}
		removals := []string{"prefix1", "prefix2"}
		err := batchMultiSaveAndRemoveWithPrefix(snapshot, maxTxnNum, saves, removals, 0)
		assert.Error(t, err)
	})
	t.Run("normal case", func(t *testing.T) {
		snapshot := kv.NewMockSnapshotKV()
		snapshot.MultiSaveFunc = func(kvs map[string]string, ts typeutil.Timestamp) error {
			return nil
		}
		snapshot.MultiSaveAndRemoveWithPrefixFunc = func(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
			return nil
		}
		saves := map[string]string{"k": "v"}
		removals := []string{"prefix1", "prefix2"}
		err := batchMultiSaveAndRemoveWithPrefix(snapshot, maxTxnNum, saves, removals, 0)
		assert.NoError(t, err)
	})
}

func TestCatalog_AlterCollection(t *testing.T) {
	t.Run("add", func(t *testing.T) {
		kc := &Catalog{}
		ctx := context.Background()
		err := kc.AlterCollection(ctx, nil, nil, metastore.ADD, 0)
		assert.Error(t, err)
	})

	t.Run("delete", func(t *testing.T) {
		kc := &Catalog{}
		ctx := context.Background()
		err := kc.AlterCollection(ctx, nil, nil, metastore.DELETE, 0)
		assert.Error(t, err)
	})

	t.Run("modify", func(t *testing.T) {
		snapshot := kv.NewMockSnapshotKV()
		kvs := map[string]string{}
		snapshot.SaveFunc = func(key string, value string, ts typeutil.Timestamp) error {
			kvs[key] = value
			return nil
		}
		kc := &Catalog{Snapshot: snapshot}
		ctx := context.Background()
		var collectionID int64 = 1
		oldC := &model.Collection{CollectionID: collectionID, State: pb.CollectionState_CollectionCreating}
		newC := &model.Collection{CollectionID: collectionID, State: pb.CollectionState_CollectionCreated}
		err := kc.AlterCollection(ctx, oldC, newC, metastore.MODIFY, 0)
		assert.NoError(t, err)
		key := BuildCollectionKey(collectionID)
		value, ok := kvs[key]
		assert.True(t, ok)
		var collPb pb.CollectionInfo
		err = proto.Unmarshal([]byte(value), &collPb)
		assert.NoError(t, err)
		got := model.UnmarshalCollectionModel(&collPb)
		assert.Equal(t, pb.CollectionState_CollectionCreated, got.State)
	})

	t.Run("modify, tenant id changed", func(t *testing.T) {
		kc := &Catalog{}
		ctx := context.Background()
		var collectionID int64 = 1
		oldC := &model.Collection{TenantID: "1", CollectionID: collectionID, State: pb.CollectionState_CollectionCreating}
		newC := &model.Collection{TenantID: "2", CollectionID: collectionID, State: pb.CollectionState_CollectionCreated}
		err := kc.AlterCollection(ctx, oldC, newC, metastore.MODIFY, 0)
		assert.Error(t, err)
	})
}

func TestCatalog_AlterPartition(t *testing.T) {
	t.Run("add", func(t *testing.T) {
		kc := &Catalog{}
		ctx := context.Background()
		err := kc.AlterPartition(ctx, nil, nil, metastore.ADD, 0)
		assert.Error(t, err)
	})

	t.Run("delete", func(t *testing.T) {
		kc := &Catalog{}
		ctx := context.Background()
		err := kc.AlterPartition(ctx, nil, nil, metastore.DELETE, 0)
		assert.Error(t, err)
	})

	t.Run("modify", func(t *testing.T) {
		snapshot := kv.NewMockSnapshotKV()
		kvs := map[string]string{}
		snapshot.SaveFunc = func(key string, value string, ts typeutil.Timestamp) error {
			kvs[key] = value
			return nil
		}
		kc := &Catalog{Snapshot: snapshot}
		ctx := context.Background()
		var collectionID int64 = 1
		var partitionID int64 = 2
		oldP := &model.Partition{PartitionID: partitionID, CollectionID: collectionID, State: pb.PartitionState_PartitionCreating}
		newP := &model.Partition{PartitionID: partitionID, CollectionID: collectionID, State: pb.PartitionState_PartitionCreated}
		err := kc.AlterPartition(ctx, oldP, newP, metastore.MODIFY, 0)
		assert.NoError(t, err)
		key := BuildPartitionKey(collectionID, partitionID)
		value, ok := kvs[key]
		assert.True(t, ok)
		var partPb pb.PartitionInfo
		err = proto.Unmarshal([]byte(value), &partPb)
		assert.NoError(t, err)
		got := model.UnmarshalPartitionModel(&partPb)
		assert.Equal(t, pb.PartitionState_PartitionCreated, got.State)
	})

	t.Run("modify, tenant id changed", func(t *testing.T) {
		kc := &Catalog{}
		ctx := context.Background()
		var collectionID int64 = 1
		oldP := &model.Partition{PartitionID: 1, CollectionID: collectionID, State: pb.PartitionState_PartitionCreating}
		newP := &model.Partition{PartitionID: 2, CollectionID: collectionID, State: pb.PartitionState_PartitionCreated}
		err := kc.AlterPartition(ctx, oldP, newP, metastore.MODIFY, 0)
		assert.Error(t, err)
	})
}

type mockSnapshotOpt func(ss *mocks.SnapShotKV)

func newMockSnapshot(t *testing.T, opts ...mockSnapshotOpt) *mocks.SnapShotKV {
	ss := mocks.NewSnapShotKV(t)
	for _, opt := range opts {
		opt(ss)
	}
	return ss
}

func withMockSave(saveErr error) mockSnapshotOpt {
	return func(ss *mocks.SnapShotKV) {
		ss.On(
			"Save",
			mock.AnythingOfType("string"),
			mock.AnythingOfType("string"),
			mock.AnythingOfType("uint64")).
			Return(saveErr)
	}
}

func withMockMultiSave(multiSaveErr error) mockSnapshotOpt {
	return func(ss *mocks.SnapShotKV) {
		ss.On(
			"MultiSave",
			mock.AnythingOfType("map[string]string"),
			mock.AnythingOfType("uint64")).
			Return(multiSaveErr)
	}
}

func withMockMultiSaveAndRemoveWithPrefix(err error) mockSnapshotOpt {
	return func(ss *mocks.SnapShotKV) {
		ss.On(
			"MultiSaveAndRemoveWithPrefix",
			mock.AnythingOfType("map[string]string"),
			mock.AnythingOfType("[]string"),
			mock.AnythingOfType("uint64")).
			Return(err)
	}
}

func TestCatalog_CreateCollection(t *testing.T) {
	t.Run("collection not creating", func(t *testing.T) {
		kc := &Catalog{}
		ctx := context.Background()
		coll := &model.Collection{State: pb.CollectionState_CollectionDropping}
		err := kc.CreateCollection(ctx, coll, 100)
		assert.Error(t, err)
	})

	t.Run("failed to save collection", func(t *testing.T) {
		mockSnapshot := newMockSnapshot(t, withMockSave(errors.New("error mock Save")))
		kc := &Catalog{Snapshot: mockSnapshot}
		ctx := context.Background()
		coll := &model.Collection{State: pb.CollectionState_CollectionCreating}
		err := kc.CreateCollection(ctx, coll, 100)
		assert.Error(t, err)
	})

	t.Run("succeed to save collection but failed to save other keys", func(t *testing.T) {
		mockSnapshot := newMockSnapshot(t, withMockSave(nil), withMockMultiSave(errors.New("error mock MultiSave")))
		kc := &Catalog{Snapshot: mockSnapshot}
		ctx := context.Background()
		coll := &model.Collection{
			Partitions: []*model.Partition{
				{PartitionName: "test"},
			},
			State: pb.CollectionState_CollectionCreating,
		}
		err := kc.CreateCollection(ctx, coll, 100)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		mockSnapshot := newMockSnapshot(t, withMockSave(nil), withMockMultiSave(nil))
		kc := &Catalog{Snapshot: mockSnapshot}
		ctx := context.Background()
		coll := &model.Collection{
			Partitions: []*model.Partition{
				{PartitionName: "test"},
			},
			State: pb.CollectionState_CollectionCreating,
		}
		err := kc.CreateCollection(ctx, coll, 100)
		assert.NoError(t, err)
	})
}

func TestCatalog_DropCollection(t *testing.T) {
	t.Run("failed to remove", func(t *testing.T) {
		mockSnapshot := newMockSnapshot(t, withMockMultiSaveAndRemoveWithPrefix(errors.New("error mock MultiSaveAndRemoveWithPrefix")))
		kc := &Catalog{Snapshot: mockSnapshot}
		ctx := context.Background()
		coll := &model.Collection{
			Partitions: []*model.Partition{
				{PartitionName: "test"},
			},
			State: pb.CollectionState_CollectionDropping,
		}
		err := kc.DropCollection(ctx, coll, 100)
		assert.Error(t, err)
	})

	t.Run("succeed to remove first, but failed to remove twice", func(t *testing.T) {
		mockSnapshot := newMockSnapshot(t)
		removeOtherCalled := false
		removeCollectionCalled := false
		mockSnapshot.On(
			"MultiSaveAndRemoveWithPrefix",
			mock.AnythingOfType("map[string]string"),
			mock.AnythingOfType("[]string"),
			mock.AnythingOfType("uint64")).
			Return(func(map[string]string, []string, typeutil.Timestamp) error {
				removeOtherCalled = true
				return nil
			}).Once()
		mockSnapshot.On(
			"MultiSaveAndRemoveWithPrefix",
			mock.AnythingOfType("map[string]string"),
			mock.AnythingOfType("[]string"),
			mock.AnythingOfType("uint64")).
			Return(func(map[string]string, []string, typeutil.Timestamp) error {
				removeCollectionCalled = true
				return errors.New("error mock MultiSaveAndRemoveWithPrefix")
			}).Once()
		kc := &Catalog{Snapshot: mockSnapshot}
		ctx := context.Background()
		coll := &model.Collection{
			Partitions: []*model.Partition{
				{PartitionName: "test"},
			},
			State: pb.CollectionState_CollectionDropping,
		}
		err := kc.DropCollection(ctx, coll, 100)
		assert.Error(t, err)
		assert.True(t, removeOtherCalled)
		assert.True(t, removeCollectionCalled)
	})

	t.Run("normal case", func(t *testing.T) {
		mockSnapshot := newMockSnapshot(t, withMockMultiSaveAndRemoveWithPrefix(nil))
		kc := &Catalog{Snapshot: mockSnapshot}
		ctx := context.Background()
		coll := &model.Collection{
			Partitions: []*model.Partition{
				{PartitionName: "test"},
			},
			State: pb.CollectionState_CollectionDropping,
		}
		err := kc.DropCollection(ctx, coll, 100)
		assert.NoError(t, err)
	})
}
