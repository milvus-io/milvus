package rootcoord

import (
	"context"
	"errors"
	"testing"

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
