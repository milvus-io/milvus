package rootcoord

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	indexName = "idx"
	IndexID   = 1
	index     = model.Index{
		CollectionID: 1,
		IndexName:    indexName,
		IndexID:      1,
		FieldID:      1,
		IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "STL_SORT"}},
		IsDeleted:    true,
	}
)

const (
	testDb = 1000
)

func TestCatalog_ListCollections(t *testing.T) {
	ctx := context.Background()

	coll1 := &pb.CollectionInfo{
		ID:                         1,
		PartitionIDs:               []int64{100},
		PartitionNames:             []string{"0"},
		PartitionCreatedTimestamps: []uint64{1},
		Schema: &schemapb.CollectionSchema{
			Name: "c1",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID: 1,
					Name:    "f1",
				},
			},
		},
	}

	coll2 := &pb.CollectionInfo{
		ID: 2,
		Schema: &schemapb.CollectionSchema{
			Name: "c1",
			Fields: []*schemapb.FieldSchema{
				{},
			},
		},
		State: pb.CollectionState_CollectionCreated,
	}

	coll3 := &pb.CollectionInfo{
		ID: 3,
		Schema: &schemapb.CollectionSchema{
			Name: "c1",
			Fields: []*schemapb.FieldSchema{
				{},
			},
		},
		State: pb.CollectionState_CollectionDropping,
	}

	targetErr := errors.New("fail")

	t.Run("load collection with prefix fail", func(t *testing.T) {
		kv := mocks.NewSnapShotKV(t)
		ts := uint64(1)
		kv.On("LoadWithPrefix", CollectionMetaPrefix, ts).
			Return(nil, nil, targetErr)

		kc := Catalog{Snapshot: kv}
		ret, err := kc.ListCollections(ctx, util.NonDBID, ts)
		assert.ErrorIs(t, err, targetErr)
		assert.Nil(t, ret)
	})

	t.Run("list partition fail", func(t *testing.T) {
		kv := mocks.NewSnapShotKV(t)
		ts := uint64(1)

		bColl, err := proto.Marshal(coll2)
		assert.NoError(t, err)
		kv.On("LoadWithPrefix", CollectionMetaPrefix, ts).
			Return([]string{"key"}, []string{string(bColl)}, nil)
		kv.On("LoadWithPrefix", mock.MatchedBy(
			func(prefix string) bool {
				return strings.HasPrefix(prefix, PartitionMetaPrefix)
			}), ts).
			Return(nil, nil, targetErr)
		kc := Catalog{Snapshot: kv}

		ret, err := kc.ListCollections(ctx, util.NonDBID, ts)
		assert.ErrorIs(t, err, targetErr)
		assert.Nil(t, ret)
	})

	t.Run("list fields fail", func(t *testing.T) {
		kv := mocks.NewSnapShotKV(t)
		ts := uint64(1)

		bColl, err := proto.Marshal(coll2)
		assert.NoError(t, err)
		kv.On("LoadWithPrefix", CollectionMetaPrefix, ts).
			Return([]string{"key"}, []string{string(bColl)}, nil)

		partitionMeta := &pb.PartitionInfo{}
		pm, err := proto.Marshal(partitionMeta)
		assert.NoError(t, err)

		kv.On("LoadWithPrefix", mock.MatchedBy(
			func(prefix string) bool {
				return strings.HasPrefix(prefix, PartitionMetaPrefix)
			}), ts).
			Return([]string{"key"}, []string{string(pm)}, nil)

		kv.On("LoadWithPrefix", mock.MatchedBy(
			func(prefix string) bool {
				return strings.HasPrefix(prefix, FieldMetaPrefix)
			}), ts).
			Return(nil, nil, targetErr)
		kc := Catalog{Snapshot: kv}

		ret, err := kc.ListCollections(ctx, util.NonDBID, ts)
		assert.ErrorIs(t, err, targetErr)
		assert.Nil(t, ret)
	})

	t.Run("list collection ok for 210 version", func(t *testing.T) {
		kv := mocks.NewSnapShotKV(t)
		ts := uint64(1)

		bColl, err := proto.Marshal(coll1)
		assert.NoError(t, err)
		kv.On("LoadWithPrefix", CollectionMetaPrefix, ts).
			Return([]string{"key"}, []string{string(bColl)}, nil)
		kc := Catalog{Snapshot: kv}

		ret, err := kc.ListCollections(ctx, util.NonDBID, ts)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(ret))
		assert.Equal(t, coll1.ID, ret[0].CollectionID)
	})

	t.Run("list collection with db", func(t *testing.T) {
		kv := mocks.NewSnapShotKV(t)
		ts := uint64(1)

		bColl, err := proto.Marshal(coll2)
		assert.NoError(t, err)
		kv.On("LoadWithPrefix", BuildDatabasePrefixWithDBID(testDb), ts).
			Return([]string{"key"}, []string{string(bColl)}, nil)

		partitionMeta := &pb.PartitionInfo{}
		pm, err := proto.Marshal(partitionMeta)
		assert.NoError(t, err)

		kv.On("LoadWithPrefix", mock.MatchedBy(
			func(prefix string) bool {
				return strings.HasPrefix(prefix, PartitionMetaPrefix)
			}), ts).
			Return([]string{"key"}, []string{string(pm)}, nil)

		fieldMeta := &schemapb.FieldSchema{}
		fm, err := proto.Marshal(fieldMeta)
		assert.NoError(t, err)

		kv.On("LoadWithPrefix", mock.MatchedBy(
			func(prefix string) bool {
				return strings.HasPrefix(prefix, FieldMetaPrefix)
			}), ts).
			Return([]string{"key"}, []string{string(fm)}, nil)
		kc := Catalog{Snapshot: kv}

		ret, err := kc.ListCollections(ctx, testDb, ts)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
		assert.Equal(t, 1, len(ret))
	})

	t.Run("list collection ok for the newest version", func(t *testing.T) {
		kv := mocks.NewSnapShotKV(t)
		ts := uint64(1)

		bColl, err := proto.Marshal(coll2)
		assert.NoError(t, err)

		aColl, err := proto.Marshal(coll3)
		assert.NoError(t, err)

		kv.On("LoadWithPrefix", CollectionMetaPrefix, ts).
			Return([]string{"key", "key2"}, []string{string(bColl), string(aColl)}, nil)

		partitionMeta := &pb.PartitionInfo{}
		pm, err := proto.Marshal(partitionMeta)
		assert.NoError(t, err)

		kv.On("LoadWithPrefix", mock.MatchedBy(
			func(prefix string) bool {
				return strings.HasPrefix(prefix, PartitionMetaPrefix)
			}), ts).
			Return([]string{"key"}, []string{string(pm)}, nil)

		fieldMeta := &schemapb.FieldSchema{}
		fm, err := proto.Marshal(fieldMeta)
		assert.NoError(t, err)

		kv.On("LoadWithPrefix", mock.MatchedBy(
			func(prefix string) bool {
				return strings.HasPrefix(prefix, FieldMetaPrefix)
			}), ts).
			Return([]string{"key"}, []string{string(fm)}, nil)
		kc := Catalog{Snapshot: kv}

		ret, err := kc.ListCollections(ctx, util.NonDBID, ts)
		assert.NoError(t, err)
		assert.NotNil(t, ret)
		assert.Equal(t, 2, len(ret))
		assert.Equal(t, int64(2), ret[0].CollectionID)
		assert.Equal(t, int64(3), ret[1].CollectionID)
	})
}

func TestCatalog_loadCollection(t *testing.T) {
	t.Run("load failed", func(t *testing.T) {
		ctx := context.Background()
		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadFunc = func(key string, ts typeutil.Timestamp) (string, error) {
			return "", errors.New("mock")
		}
		kc := Catalog{Snapshot: snapshot}
		_, err := kc.loadCollection(ctx, testDb, 1, 0)
		assert.Error(t, err)
	})

	t.Run("load, not collection info", func(t *testing.T) {
		ctx := context.Background()
		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadFunc = func(key string, ts typeutil.Timestamp) (string, error) {
			return "not in pb format", nil
		}
		kc := Catalog{Snapshot: snapshot}
		_, err := kc.loadCollection(ctx, testDb, 1, 0)
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
		got, err := kc.loadCollection(ctx, 0, 1, 0)
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

func TestCatalog_GetCollectionByID(t *testing.T) {
	ctx := context.TODO()
	ss := mocks.NewSnapShotKV(t)
	c := Catalog{Snapshot: ss}

	ss.EXPECT().Load(mock.Anything, mock.Anything).Call.Return(
		func(key string, ts typeutil.Timestamp) string {
			if ts > 1000 {
				collByte, err := proto.Marshal(&pb.CollectionInfo{
					ID: 1,
					Schema: &schemapb.CollectionSchema{
						Fields: []*schemapb.FieldSchema{
							{},
						},
					},
					PartitionIDs:               []int64{1, 2, 3},
					PartitionNames:             []string{"1", "2", "3"},
					PartitionCreatedTimestamps: []uint64{1, 2, 3},
				})
				require.NoError(t, err)
				return string(collByte)
			}
			return ""
		},
		func(key string, ts typeutil.Timestamp) error {
			if ts > 1000 {
				return nil
			}

			return errors.New("load error")
		},
	)

	coll, err := c.GetCollectionByID(ctx, 0, 1, 1)
	assert.Error(t, err)
	assert.Nil(t, coll)

	coll, err = c.GetCollectionByID(ctx, 0, 10000, 1)
	assert.NoError(t, err)
	assert.NotNil(t, coll)
}

func TestCatalog_CreatePartitionV2(t *testing.T) {
	t.Run("collection not exist", func(t *testing.T) {
		ctx := context.Background()
		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadFunc = func(key string, ts typeutil.Timestamp) (string, error) {
			return "", errors.New("mock")
		}
		kc := Catalog{Snapshot: snapshot}
		err := kc.CreatePartition(ctx, 0, &model.Partition{}, 0)
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

		err = kc.CreatePartition(ctx, 0, &model.Partition{}, 0)
		assert.Error(t, err)

		snapshot.SaveFunc = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}
		err = kc.CreatePartition(ctx, 0, &model.Partition{}, 0)
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

		err = kc.CreatePartition(ctx, 0, &model.Partition{PartitionID: partID}, 0)
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

		err = kc.CreatePartition(ctx, 0, &model.Partition{PartitionName: partition}, 0)
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

		err = kc.CreatePartition(ctx, 0, &model.Partition{}, 0)
		assert.Error(t, err)

		snapshot.SaveFunc = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}
		err = kc.CreatePartition(ctx, 0, &model.Partition{}, 0)
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

		err := kc.DropPartition(ctx, 0, 100, 101, 0)
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

		err = kc.DropPartition(ctx, 0, 100, 101, 0)
		assert.Error(t, err)

		snapshot.MultiSaveAndRemoveWithPrefixFunc = func(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
			return nil
		}
		err = kc.DropPartition(ctx, 0, 100, 101, 0)
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

		err = kc.DropPartition(ctx, 0, 100, 101, 0)
		assert.Error(t, err)

		snapshot.SaveFunc = func(key string, value string, ts typeutil.Timestamp) error {
			return nil
		}
		err = kc.DropPartition(ctx, 0, 100, 102, 0)
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

	err := kc.DropAlias(ctx, testDb, "alias", 0)
	assert.Error(t, err)

	snapshot.MultiSaveAndRemoveWithPrefixFunc = func(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
		return nil
	}
	err = kc.DropAlias(ctx, testDb, "alias", 0)
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

		_, err := kc.listAliasesAfter210WithDb(ctx, testDb, 0)
		assert.Error(t, err)
	})

	t.Run("not in pb format", func(t *testing.T) {
		ctx := context.Background()

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			return []string{"key"}, []string{"not in pb format"}, nil
		}

		kc := Catalog{Snapshot: snapshot}

		_, err := kc.listAliasesAfter210WithDb(ctx, testDb, 0)
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

		got, err := kc.listAliasesAfter210WithDb(ctx, testDb, 0)
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

		_, err := kc.ListAliases(ctx, testDb, 0)
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

			if strings.Contains(key, DatabaseMetaPrefix) {
				return nil, nil, errors.New("mock")
			}
			return []string{"key"}, []string{string(value)}, nil
		}

		kc := Catalog{Snapshot: snapshot}

		_, err = kc.ListAliases(ctx, util.NonDBID, 0)
		assert.Error(t, err)

		_, err = kc.ListAliases(ctx, testDb, 0)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()

		alias := &pb.AliasInfo{CollectionId: 101, AliasName: "alias2"}
		value2, err := proto.Marshal(alias)
		assert.NoError(t, err)

		snapshot := kv.NewMockSnapshotKV()
		snapshot.LoadWithPrefixFunc = func(key string, ts typeutil.Timestamp) ([]string, []string, error) {
			dbStr := fmt.Sprintf("%d", testDb)
			if strings.Contains(key, dbStr) && strings.Contains(key, Aliases) {
				return []string{"key1"}, []string{string(value2)}, nil
			}
			return []string{}, []string{}, nil
		}

		kc := Catalog{Snapshot: snapshot}

		got, err := kc.ListAliases(ctx, testDb, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(got))
		assert.Equal(t, "alias2", got[0].Name)
	})
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
		key := BuildCollectionKey(0, collectionID)
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
		err := kc.AlterPartition(ctx, testDb, nil, nil, metastore.ADD, 0)
		assert.Error(t, err)
	})

	t.Run("delete", func(t *testing.T) {
		kc := &Catalog{}
		ctx := context.Background()
		err := kc.AlterPartition(ctx, testDb, nil, nil, metastore.DELETE, 0)
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
		err := kc.AlterPartition(ctx, testDb, oldP, newP, metastore.MODIFY, 0)
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
		err := kc.AlterPartition(ctx, testDb, oldP, newP, metastore.MODIFY, 0)
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

func TestRBAC_Credential(t *testing.T) {
	ctx := context.TODO()

	t.Run("test GetCredential", func(t *testing.T) {
		var (
			kvmock = mocks.NewTxnKV(t)
			c      = &Catalog{Txn: kvmock}

			loadFailName    = "invalid"
			loadFailKey     = fmt.Sprintf("%s/%s", CredentialPrefix, loadFailName)
			marshalFailName = "marshal"
			marshalFailkey  = fmt.Sprintf("%s/%s", CredentialPrefix, marshalFailName)
		)

		kvmock.EXPECT().Load(loadFailKey).Return("", errors.New("Mock invalid load"))
		kvmock.EXPECT().Load(marshalFailkey).Return("random", nil)
		kvmock.EXPECT().Load(mock.Anything).Call.Return(
			func(key string) string {
				v, err := json.Marshal(&internalpb.CredentialInfo{EncryptedPassword: key})
				require.NoError(t, err)
				return string(v)
			}, nil)

		tests := []struct {
			description string
			isValid     bool

			user string

			expectedName     string
			expectedPassword string
		}{
			{"valid user1", true, "user1", "user1", "user1"},
			{"valid user2", true, "user2", "user2", "user2"},
			{"invalid user loadFail", false, loadFailName, "", ""},
			{"invalid user unmarshalFail", false, marshalFailName, "", ""},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				cre, err := c.GetCredential(ctx, test.user)
				if test.isValid {
					assert.NoError(t, err)
					assert.Equal(t, test.expectedName, cre.Username)

					expectedPassword := fmt.Sprintf("%s/%s", CredentialPrefix, test.expectedPassword)
					assert.Equal(t, expectedPassword, cre.EncryptedPassword)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})

	t.Run("test CreateCredential", func(t *testing.T) {
		var (
			kvmock      = mocks.NewTxnKV(t)
			c           = &Catalog{Txn: kvmock}
			invalidName = "invalid"
		)

		mockFailPath := fmt.Sprintf("%s/%s", CredentialPrefix, invalidName)
		kvmock.EXPECT().Save(mockFailPath, mock.Anything).Return(errors.New("Mock invalid save"))
		kvmock.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)

		tests := []struct {
			description string
			isValid     bool

			user     string
			password string
		}{
			{"valid user and password", true, "user1", "password"},
			{"valid user and password empty", true, "user2", ""},
			{"invalid user", false, invalidName, "password"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				err := c.CreateCredential(ctx, &model.Credential{
					Username:          test.user,
					EncryptedPassword: test.password,
				})

				if test.isValid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}

				err = c.AlterCredential(ctx, &model.Credential{
					Username:          test.user,
					EncryptedPassword: test.password,
				})

				if test.isValid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})

	t.Run("test DropCredential", func(t *testing.T) {
		var (
			kvmock = mocks.NewTxnKV(t)
			c      = &Catalog{Txn: kvmock}

			dropFailName = "drop-fail"
			dropFailKey  = fmt.Sprintf("%s/%s", CredentialPrefix, dropFailName)
		)

		kvmock.EXPECT().Remove(dropFailKey).Return(errors.New("Mock invalid remove"))
		kvmock.EXPECT().Remove(mock.Anything).Return(nil)

		tests := []struct {
			description string
			isValid     bool

			user string
		}{
			{"valid user1", true, "user1"},
			{"valid user2", true, "user2"},
			{"invalid user drop-fail", false, dropFailName},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				err := c.DropCredential(ctx, test.user)
				if test.isValid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})

	t.Run("test ListCredentials", func(t *testing.T) {
		var (
			kvmock = mocks.NewTxnKV(t)
			c      = &Catalog{Txn: kvmock}

			cmu   sync.RWMutex
			count = 0
		)

		// Return valid keys if count==0
		// return error if count!=0
		kvmock.EXPECT().LoadWithPrefix(mock.Anything).Call.Return(
			func(key string) []string {
				cmu.RLock()
				defer cmu.RUnlock()
				if count == 0 {
					return []string{
						fmt.Sprintf("%s/%s", CredentialPrefix, "user1"),
						fmt.Sprintf("%s/%s", CredentialPrefix, "user2"),
						fmt.Sprintf("%s/%s", CredentialPrefix, "user3"),
						"random",
					}

				}
				return nil
			},
			nil,
			func(key string) error {
				cmu.RLock()
				defer cmu.RUnlock()
				if count == 0 {
					return nil
				}
				return errors.New("Mock load with prefix")
			})

		tests := []struct {
			description string
			isValid     bool

			count       int
			expectedOut []string
		}{
			{"valid list", true, 0, []string{"user1", "user2", "user3"}},
			{"invalid list", false, 1, []string{}},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				cmu.Lock()
				count = test.count
				cmu.Unlock()

				users, err := c.ListCredentials(ctx)
				if test.isValid {
					assert.NoError(t, err)
					assert.ElementsMatch(t, test.expectedOut, users)
				} else {
					assert.Error(t, err)
					assert.Empty(t, users)
				}
			})
		}
	})
}

func TestRBAC_Role(t *testing.T) {
	ctx := context.TODO()
	tenant := "default"

	t.Run("test remove", func(t *testing.T) {
		var (
			kvmock = mocks.NewTxnKV(t)
			c      = &Catalog{Txn: kvmock}

			notExistKey = "not-exist"
			errorKey    = "error"
			otherError  = fmt.Errorf("mock load error")
		)

		kvmock.EXPECT().Load(notExistKey).Return("", common.NewKeyNotExistError(notExistKey)).Once()
		kvmock.EXPECT().Load(errorKey).Return("", otherError).Once()
		kvmock.EXPECT().Load(mock.Anything).Return("", nil).Once()
		kvmock.EXPECT().Remove(mock.Anything).Call.Return(nil).Once()
		tests := []struct {
			description string

			isValid bool
			key     string

			expectedError error
			ignorable     bool
		}{
			{"error key not exists, ignorable", false, notExistKey, nil, true},
			{"error other error", false, errorKey, otherError, false},
			{"no error", true, "key1", nil, false},
		}
		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				err := c.remove(test.key)
				if test.isValid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}

				if test.ignorable {
					_, ok := err.(*common.IgnorableError)
					assert.True(t, ok)
				}
			})
		}
	})

	t.Run("test save", func(t *testing.T) {
		var (
			kvmock = mocks.NewTxnKV(t)
			c      = &Catalog{Txn: kvmock}

			notExistKey = "not-exist"
			errorKey    = "error"
			otherError  = fmt.Errorf("mock load error")
		)

		kvmock.EXPECT().Load(notExistKey).Return("", common.NewKeyNotExistError(notExistKey)).Once()
		kvmock.EXPECT().Load(errorKey).Return("", otherError).Once()
		kvmock.EXPECT().Load(mock.Anything).Return("", nil).Once()
		kvmock.EXPECT().Save(mock.Anything, mock.Anything).Call.Return(nil).Once()

		tests := []struct {
			description string

			isValid bool
			key     string

			expectedError error
			ignorable     bool
		}{
			{"key not exists", true, notExistKey, nil, false},
			{"other error", false, errorKey, otherError, false},
			{"ignorable error", false, "key1", &common.IgnorableError{}, true},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				err := c.save(test.key)
				if test.isValid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}

				if test.ignorable {
					_, ok := err.(*common.IgnorableError)
					assert.True(t, ok)
				}
			})
		}
	})
	t.Run("test CreateRole", func(t *testing.T) {
		var (
			kvmock = mocks.NewTxnKV(t)
			c      = &Catalog{Txn: kvmock}

			notExistName = "not-exist"
			notExistPath = funcutil.HandleTenantForEtcdKey(RolePrefix, tenant, notExistName)
			errorName    = "error"
			errorPath    = funcutil.HandleTenantForEtcdKey(RolePrefix, tenant, errorName)
			otherError   = fmt.Errorf("mock load error")
		)

		kvmock.EXPECT().Load(notExistPath).Return("", common.NewKeyNotExistError(notExistName)).Once()
		kvmock.EXPECT().Load(errorPath).Return("", otherError).Once()
		kvmock.EXPECT().Load(mock.Anything).Return("", nil).Once()
		kvmock.EXPECT().Save(mock.Anything, mock.Anything).Call.Return(nil).Once()

		tests := []struct {
			description string

			isValid bool
			name    string

			expectedError error
		}{
			{"key not exists", true, notExistName, nil},
			{"other error", false, errorName, otherError},
			{"ignorable error", false, "key1", &common.IgnorableError{}},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				err := c.CreateRole(ctx, tenant, &milvuspb.RoleEntity{
					Name: test.name,
				})
				if test.isValid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})
	t.Run("test DropRole", func(t *testing.T) {
		var (
			kvmock = mocks.NewTxnKV(t)
			c      = &Catalog{Txn: kvmock}

			errorName = "error"
			errorPath = funcutil.HandleTenantForEtcdKey(RolePrefix, tenant, errorName)
		)

		kvmock.EXPECT().Remove(errorPath).Return(errors.New("mock remove error")).Once()
		kvmock.EXPECT().Remove(mock.Anything).Return(nil).Once()

		tests := []struct {
			description string
			isValid     bool

			role string
		}{
			{"valid role role1", true, "role1"},
			{"invalid role error", false, errorName},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				err := c.DropRole(ctx, tenant, test.role)
				if test.isValid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})
	t.Run("test AlterUserRole", func(t *testing.T) {
		var (
			kvmock = mocks.NewTxnKV(t)
			c      = &Catalog{Txn: kvmock}

			user = "default-user"

			noErrorRoleSave     = "no-error-role-save"
			noErrorRoleSavepath = funcutil.HandleTenantForEtcdKey(RoleMappingPrefix, tenant, fmt.Sprintf("%s/%s", user, noErrorRoleSave))

			errorRoleSave     = "error-role-save"
			errorRoleSavepath = funcutil.HandleTenantForEtcdKey(RoleMappingPrefix, tenant, fmt.Sprintf("%s/%s", user, errorRoleSave))

			errorRoleRemove     = "error-role-remove"
			errorRoleRemovepath = funcutil.HandleTenantForEtcdKey(RoleMappingPrefix, tenant, fmt.Sprintf("%s/%s", user, errorRoleRemove))
		)
		kvmock.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)
		kvmock.EXPECT().Remove(mock.Anything).Return(nil)

		// Catalog.save() returns error
		kvmock.EXPECT().Load(errorRoleSavepath).Return("", nil)

		// Catalog.save() returns nil
		kvmock.EXPECT().Load(noErrorRoleSavepath).Return("", common.NewKeyNotExistError(noErrorRoleSavepath))

		// Catalog.remove() returns error
		kvmock.EXPECT().Load(errorRoleRemovepath).Return("", errors.New("not exists"))

		// Catalog.remove() returns nil
		kvmock.EXPECT().Load(mock.Anything).Return("", nil)

		tests := []struct {
			description string
			isValid     bool

			role  string
			oType milvuspb.OperateUserRoleType
		}{
			{"valid role role1, AddUserToRole", true, noErrorRoleSave, milvuspb.OperateUserRoleType_AddUserToRole},
			{"invalid role error-role, AddUserToRole", false, errorRoleSave, milvuspb.OperateUserRoleType_AddUserToRole},
			{"valid role role1, RemoveUserFromRole", true, "role", milvuspb.OperateUserRoleType_RemoveUserFromRole},
			{"invalid role error-role, RemoveUserFromRole", false, errorRoleRemove, milvuspb.OperateUserRoleType_RemoveUserFromRole},
			{"invalid operate type 100", false, "role1", milvuspb.OperateUserRoleType(100)},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				err := c.AlterUserRole(ctx, tenant, &milvuspb.UserEntity{Name: user}, &milvuspb.RoleEntity{
					Name: test.role,
				}, test.oType)

				if test.isValid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})
	t.Run("test ListRole", func(t *testing.T) {
		var (
			loadWithPrefixReturn atomic.Bool
		)

		t.Run("test entity!=nil", func(t *testing.T) {
			var (
				kvmock = mocks.NewTxnKV(t)
				c      = &Catalog{Txn: kvmock}

				errorLoad     = "error"
				errorLoadPath = funcutil.HandleTenantForEtcdKey(RolePrefix, tenant, errorLoad)
			)

			kvmock.EXPECT().Load(errorLoadPath).Call.Return("", errors.New("mock load error"))
			kvmock.EXPECT().Load(mock.Anything).Call.Return("", nil)

			// Return valid keys if loadWithPrefixReturn == True
			// return error if loadWithPrefixReturn == False
			kvmock.EXPECT().LoadWithPrefix(mock.Anything).Call.Return(
				func(key string) []string {
					if loadWithPrefixReturn.Load() {
						return []string{
							fmt.Sprintf("%s/%s/%s", RoleMappingPrefix, tenant, "user1/role1"),
							fmt.Sprintf("%s/%s/%s", RoleMappingPrefix, tenant, "user2/role2"),
							fmt.Sprintf("%s/%s/%s", RoleMappingPrefix, tenant, "user3/role3"),
							"random",
						}
					}
					return nil
				},
				nil,
				func(key string) error {
					if loadWithPrefixReturn.Load() {
						return nil
					}
					return fmt.Errorf("Mock load with prefix wrong, key=%s", key)
				})

			tests := []struct {
				description string
				isValid     bool

				includeUserInfo      bool
				loadWithPrefixReturn bool

				entity *milvuspb.RoleEntity
			}{
				{"loadWithPrefix error", false, true, false, &milvuspb.RoleEntity{Name: "role1"}},
				{"entity empty name", false, true, true, &milvuspb.RoleEntity{Name: ""}},
				{"entity load error", false, true, true, &milvuspb.RoleEntity{Name: errorLoad}},
				{"entity role1", true, true, true, &milvuspb.RoleEntity{Name: "role1"}},
				{"entity empty name/includeUserInfo=False", false, false, true, &milvuspb.RoleEntity{Name: ""}},
				{"entity load error/includeUserInfo=False", false, false, true, &milvuspb.RoleEntity{Name: errorLoad}},
				{"entity role1/includeUserInfo=False", true, false, true, &milvuspb.RoleEntity{Name: "role1"}},
			}

			for _, test := range tests {
				t.Run(test.description, func(t *testing.T) {
					loadWithPrefixReturn.Store(test.loadWithPrefixReturn)
					res, err := c.ListRole(ctx, tenant, test.entity, test.includeUserInfo)
					if test.isValid {
						assert.NoError(t, err)

						assert.Equal(t, 1, len(res))
						assert.Equal(t, test.entity.Name, res[0].GetRole().GetName())
						if test.includeUserInfo {
							assert.Equal(t, 1, len(res[0].GetUsers()))
							assert.Equal(t, "user1", res[0].GetUsers()[0].GetName())
						} else {
							for _, r := range res {
								assert.Empty(t, r.GetUsers())
							}
						}
					} else {
						assert.Error(t, err)
					}
				})
			}
		})

		t.Run("test entity is nil", func(t *testing.T) {
			var (
				kvmock = mocks.NewTxnKV(t)
				c      = &Catalog{Txn: kvmock}
			)

			// Return valid keys if loadWithPrefixReturn == True
			// return error if loadWithPrefixReturn == False
			// Mocking the return of kv_catalog.go:L699
			kvmock.EXPECT().LoadWithPrefix(mock.Anything).Call.Return(
				func(key string) []string {
					if loadWithPrefixReturn.Load() {
						return []string{
							fmt.Sprintf("%s/%s/%s", RolePrefix, tenant, "role1"),
							fmt.Sprintf("%s/%s/%s", RolePrefix, tenant, "role2"),
							fmt.Sprintf("%s/%s/%s", RolePrefix, tenant, "role3"),
							"random",
						}
					}
					return nil
				},
				nil,
				func(key string) error {
					if loadWithPrefixReturn.Load() {
						return nil
					}
					return fmt.Errorf("Mock load with prefix wrong, key=%s", key)
				})

			tests := []struct {
				description string
				isValid     bool

				loadWithPrefixReturn bool
			}{
				{"entity nil/loadWithPrefix success", true, true},
				{"entity nil/loadWithPrefix fail", false, false},
			}

			for _, test := range tests {
				t.Run(test.description, func(t *testing.T) {
					loadWithPrefixReturn.Store(test.loadWithPrefixReturn)
					res, err := c.ListRole(ctx, tenant, nil, false)
					if test.isValid {
						assert.NoError(t, err)
						assert.Equal(t, 3, len(res))

						for _, r := range res {
							assert.Empty(t, r.GetUsers())
						}
					} else {
						assert.Error(t, err)
					}
				})
			}
		})
	})
	t.Run("test ListUser", func(t *testing.T) {
		var (
			kvmock = mocks.NewTxnKV(t)
			c      = &Catalog{Txn: kvmock}

			invalidUser    = "invalid-user"
			invalidUserKey = funcutil.HandleTenantForEtcdKey(RoleMappingPrefix, tenant, invalidUser)
		)
		// returns error for invalidUserKey
		kvmock.EXPECT().LoadWithPrefix(invalidUserKey).Call.Return(
			nil, nil, errors.New("Mock load with prefix wrong"))

		// Returns keys for RoleMappingPrefix/tenant/user1
		user1Key := fmt.Sprintf("%s/%s/%s", RoleMappingPrefix, tenant, "user1")
		kvmock.EXPECT().LoadWithPrefix(user1Key).Call.Return(
			func(key string) []string {
				return []string{
					fmt.Sprintf("%s/%s/%s", RoleMappingPrefix, tenant, "user1/role1"),
					fmt.Sprintf("%s/%s/%s", RoleMappingPrefix, tenant, "user1/role2"),
					fmt.Sprintf("%s/%s/%s", RoleMappingPrefix, tenant, "user1/role3/error"),
				}
			}, nil, nil)

		// Returns keys for CredentialPrefix
		var loadCredentialPrefixReturn atomic.Bool
		kvmock.EXPECT().LoadWithPrefix(CredentialPrefix).Call.Return(
			func(key string) []string {
				if loadCredentialPrefixReturn.Load() {
					return []string{
						fmt.Sprintf("%s/%s/%s", CredentialPrefix, UserSubPrefix, "user1"),
					}
				}
				return nil
			}, nil,
			func(key string) error {
				if loadCredentialPrefixReturn.Load() {
					return nil
				}

				return fmt.Errorf("mock load with prefix error, key=%s", key)
			})

		t.Run("test getUserResult", func(t *testing.T) {
			tests := []struct {
				description string
				isValid     bool

				user            string
				includeRoleInfo bool
			}{
				{"valid user1 not include RoleInfo", true, "user1", false},
				{"valid user1 include RoleInfo", true, "user1", true},
				{"invalid user not include RoleInfo", true, invalidUser, false},
				{"invalid user include RoleInfo", false, invalidUser, true},
			}

			for _, test := range tests {
				t.Run(test.description, func(t *testing.T) {
					res, err := c.getUserResult(tenant, test.user, test.includeRoleInfo)

					assert.Equal(t, test.user, res.GetUser().GetName())

					if test.isValid {
						assert.NoError(t, err)

						if test.includeRoleInfo {
							assert.Equal(t, 2, len(res.GetRoles()))
							assert.Equal(t, "role1", res.GetRoles()[0].GetName())
							assert.Equal(t, "role2", res.GetRoles()[1].GetName())
						} else {
							assert.Equal(t, 0, len(res.GetRoles()))
						}
					} else {
						assert.Error(t, err)
					}
				})
			}
		})

		t.Run("test ListUser", func(t *testing.T) {
			var (
				invalidUserLoad    = "invalid-user-load"
				invalidUserLoadKey = fmt.Sprintf("%s/%s", CredentialPrefix, invalidUserLoad)
			)
			// Returns error for invalidUserLoadKey
			kvmock.EXPECT().Load(invalidUserLoadKey).Call.Return("", errors.New("Mock load wrong"))
			kvmock.EXPECT().Load(mock.Anything).Call.Return(
				func(key string) string {
					v, err := json.Marshal(&internalpb.CredentialInfo{EncryptedPassword: key})
					require.NoError(t, err)
					return string(v)
				}, nil)
			tests := []struct {
				isValid                    bool
				loadCredentialPrefixReturn bool

				entity          *milvuspb.UserEntity
				includeRoleInfo bool

				description string
			}{
				{true, true, nil, true, "nil entity include RoleInfo"},
				{true, true, nil, false, "nil entity not include RoleInfo"},
				{false, false, nil, false, "nil entity ListCredentials error"},
				{false, false, nil, true, "nil entity ListCredentials error"},
				{false, true, &milvuspb.UserEntity{Name: ""}, false, "empty entity Name"},
				{false, true, &milvuspb.UserEntity{Name: invalidUserLoad}, false, "invalid entity Name"},
				{true, true, &milvuspb.UserEntity{Name: "user1"}, false, "valid entity user1 not include RoleInfo"},
				{true, true, &milvuspb.UserEntity{Name: "user1"}, true, "valid entity user1 include RoleInfo"},
				{false, true, &milvuspb.UserEntity{Name: invalidUser}, true, "invalid entity invalidUser include RoleInfo"},
			}

			for _, test := range tests {
				t.Run(test.description, func(t *testing.T) {
					loadCredentialPrefixReturn.Store(test.loadCredentialPrefixReturn)
					res, err := c.ListUser(ctx, tenant, test.entity, test.includeRoleInfo)

					if test.isValid {
						assert.NoError(t, err)
						assert.Equal(t, 1, len(res))
						assert.Equal(t, "user1", res[0].GetUser().GetName())
						if test.includeRoleInfo {
							assert.Equal(t, 2, len(res[0].GetRoles()))
						} else {
							assert.Equal(t, 0, len(res[0].GetRoles()))
						}
					} else {
						assert.Error(t, err)
						assert.Empty(t, res)
					}

				})
			}

		})
	})
	t.Run("test ListUserRole", func(t *testing.T) {
		var (
			loadWithPrefixReturn atomic.Bool
			kvmock               = mocks.NewTxnKV(t)
			c                    = &Catalog{Txn: kvmock}
		)

		// Return valid keys if loadWithPrefixReturn == True
		// return error if loadWithPrefixReturn == False
		// Mocking the return of kv_catalog.go:ListUserRole:L982
		kvmock.EXPECT().LoadWithPrefix(mock.Anything).Call.Return(
			func(key string) []string {
				if loadWithPrefixReturn.Load() {
					return []string{
						fmt.Sprintf("%s/%s/%s", RoleMappingPrefix, tenant, "user1/role1"),
						fmt.Sprintf("%s/%s/%s", RoleMappingPrefix, tenant, "user1/role2"),
						fmt.Sprintf("%s/%s/%s", RoleMappingPrefix, tenant, "user3/role3"),
						"random",
					}
				}
				return nil
			},
			nil,
			func(key string) error {
				if loadWithPrefixReturn.Load() {
					return nil
				}
				return fmt.Errorf("Mock load with prefix wrong, key=%s", key)
			})

		tests := []struct {
			isValid              bool
			loadWithPrefixReturn bool

			description string
		}{
			{true, true, "valid loadWithPrefix"},
			{false, false, "error loadWithPrefix"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				loadWithPrefixReturn.Store(test.loadWithPrefixReturn)
				res, err := c.ListUserRole(ctx, tenant)
				if test.isValid {
					assert.NoError(t, err)
					assert.Equal(t, 3, len(res))
					assert.ElementsMatch(t, []string{"user1/role1", "user1/role2", "user3/role3"}, res)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})
}

func TestRBAC_Grant(t *testing.T) {
	var (
		tenant = "default"
		ctx    = context.TODO()

		object  = "Collection"
		objName = "insert"

		validRole       = "role1"
		invalidRole     = "role2"
		keyNotExistRole = "role3"
		errorSaveRole   = "role100"

		validUser   = "user1"
		invalidUser = "user2"

		validPrivilege        = "write"
		invalidPrivilege      = "wal"
		keyNotExistPrivilege  = "read"
		keyNotExistPrivilege2 = "read2"
	)
	t.Run("test AlterGrant", func(t *testing.T) {
		var (
			kvmock = mocks.NewTxnKV(t)
			c      = &Catalog{Txn: kvmock}
		)

		validRoleKey := funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant, fmt.Sprintf("%s/%s/%s", validRole, object, objName))
		validRoleValue := crypto.MD5(validRoleKey)

		invalidRoleKey := funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant, fmt.Sprintf("%s/%s/%s", invalidRole, object, objName))
		invalidRoleKeyWithDb := funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant, fmt.Sprintf("%s/%s/%s", invalidRole, object, funcutil.CombineObjectName(util.DefaultDBName, objName)))

		keyNotExistRoleKey := funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant, fmt.Sprintf("%s/%s/%s", keyNotExistRole, object, objName))
		keyNotExistRoleKeyWithDb := funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant, fmt.Sprintf("%s/%s/%s", keyNotExistRole, object, funcutil.CombineObjectName(util.DefaultDBName, objName)))
		keyNotExistRoleValueWithDb := crypto.MD5(keyNotExistRoleKeyWithDb)

		errorSaveRoleKey := funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant, fmt.Sprintf("%s/%s/%s", errorSaveRole, object, objName))
		errorSaveRoleKeyWithDb := funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant, fmt.Sprintf("%s/%s/%s", errorSaveRole, object, funcutil.CombineObjectName(util.DefaultDBName, objName)))

		// Mock return in kv_catalog.go:AlterGrant:L815
		kvmock.EXPECT().Load(validRoleKey).Call.
			Return(func(key string) string { return validRoleValue }, nil)

		kvmock.EXPECT().Load(invalidRoleKey).Call.
			Return("", func(key string) error {
				return fmt.Errorf("mock load error, key=%s", key)
			})
		kvmock.EXPECT().Load(invalidRoleKeyWithDb).Call.
			Return("", func(key string) error {
				return fmt.Errorf("mock load error, key=%s", key)
			})
		kvmock.EXPECT().Load(keyNotExistRoleKey).Call.
			Return("", func(key string) error {
				return common.NewKeyNotExistError(key)
			})
		kvmock.EXPECT().Load(keyNotExistRoleKeyWithDb).Call.
			Return("", func(key string) error {
				return common.NewKeyNotExistError(key)
			})
		kvmock.EXPECT().Load(errorSaveRoleKey).Call.
			Return("", func(key string) error {
				return common.NewKeyNotExistError(key)
			})
		kvmock.EXPECT().Load(errorSaveRoleKeyWithDb).Call.
			Return("", func(key string) error {
				return common.NewKeyNotExistError(key)
			})
		kvmock.EXPECT().Save(keyNotExistRoleKeyWithDb, mock.Anything).Return(nil)
		kvmock.EXPECT().Save(errorSaveRoleKeyWithDb, mock.Anything).Return(errors.New("mock save error role"))

		validPrivilegeKey := funcutil.HandleTenantForEtcdKey(GranteeIDPrefix, tenant, fmt.Sprintf("%s/%s", validRoleValue, validPrivilege))
		invalidPrivilegeKey := funcutil.HandleTenantForEtcdKey(GranteeIDPrefix, tenant, fmt.Sprintf("%s/%s", validRoleValue, invalidPrivilege))
		keyNotExistPrivilegeKey := funcutil.HandleTenantForEtcdKey(GranteeIDPrefix, tenant, fmt.Sprintf("%s/%s", validRoleValue, keyNotExistPrivilege))
		keyNotExistPrivilegeKey2WithDb := funcutil.HandleTenantForEtcdKey(GranteeIDPrefix, tenant, fmt.Sprintf("%s/%s", keyNotExistRoleValueWithDb, keyNotExistPrivilege2))

		// Mock return in kv_catalog.go:AlterGrant:L838
		kvmock.EXPECT().Load(validPrivilegeKey).Call.Return("", nil)
		kvmock.EXPECT().Load(invalidPrivilegeKey).Call.
			Return("", func(key string) error {
				return fmt.Errorf("mock load error, key=%s", key)
			})
		kvmock.EXPECT().Load(keyNotExistPrivilegeKey).Call.
			Return("", func(key string) error {
				return common.NewKeyNotExistError(key)
			})
		kvmock.EXPECT().Load(keyNotExistPrivilegeKey2WithDb).Call.
			Return("", func(key string) error {
				return common.NewKeyNotExistError(key)
			})
		kvmock.EXPECT().Load(mock.Anything).Call.Return("", nil)

		t.Run("test Grant", func(t *testing.T) {
			kvmock.EXPECT().Save(mock.Anything, validUser).Return(nil)
			kvmock.EXPECT().Save(mock.Anything, invalidUser).Return(errors.New("mock save invalid user"))

			tests := []struct {
				isValid bool

				userName      string
				roleName      string
				privilegeName string

				ignorable   bool
				description string
			}{
				// exist role
				{false, validUser, validRole, invalidPrivilege, false, "grant exist Role with error Privilege"},
				{false, validUser, validRole, validPrivilege, true, "grant exist Role with exist Privilege, ignorable"},
				{false, invalidUser, validRole, keyNotExistPrivilege, false, "grant exist Role with not exist Privilege with invalid user"},
				{true, validUser, validRole, keyNotExistPrivilege, true, "grant exist Role with not exist Privilege with valid user"},
				// error role
				{false, validUser, invalidRole, invalidPrivilege, false, "grant invalid role with invalid privilege"},
				{false, validUser, invalidRole, validPrivilege, false, "grant invalid role with valid privilege"},
				{false, validUser, invalidRole, keyNotExistPrivilege, false, "grant invalid role with not exist privilege"},
				{false, validUser, errorSaveRole, keyNotExistPrivilege, false, "grant error role with not exist privilege"},
				// not exist role
				{false, validUser, keyNotExistRole, validPrivilege, true, "grant not exist role with exist privilege"},
				{true, validUser, keyNotExistRole, keyNotExistPrivilege2, false, "grant not exist role with not exist privilege"},
			}

			for _, test := range tests {
				t.Run(test.description, func(t *testing.T) {
					err := c.AlterGrant(ctx, tenant, &milvuspb.GrantEntity{
						Role:       &milvuspb.RoleEntity{Name: test.roleName},
						Object:     &milvuspb.ObjectEntity{Name: object},
						ObjectName: objName,
						DbName:     util.DefaultDBName,
						Grantor: &milvuspb.GrantorEntity{
							User:      &milvuspb.UserEntity{Name: test.userName},
							Privilege: &milvuspb.PrivilegeEntity{Name: test.privilegeName}},
					}, milvuspb.OperatePrivilegeType_Grant)

					if test.isValid {
						assert.NoError(t, err)
					} else {
						assert.Error(t, err)
						_, ok := err.(*common.IgnorableError)
						if test.ignorable {
							assert.True(t, ok)
						} else {
							assert.False(t, ok)
						}
					}
				})
			}
		})

		t.Run("test Revoke", func(t *testing.T) {
			var invalidPrivilegeRemove = "p-remove"
			invalidPrivilegeRemoveKey := funcutil.HandleTenantForEtcdKey(GranteeIDPrefix, tenant, fmt.Sprintf("%s/%s", validRoleValue, invalidPrivilegeRemove))

			kvmock.EXPECT().Load(invalidPrivilegeRemoveKey).Call.Return("", nil)
			kvmock.EXPECT().Remove(invalidPrivilegeRemoveKey).Return(errors.New("mock remove error"))
			kvmock.EXPECT().Remove(mock.Anything).Return(nil)
			tests := []struct {
				isValid bool

				userName      string
				roleName      string
				privilegeName string

				ignorable   bool
				description string
			}{
				// invalid role
				{false, validUser, invalidRole, validPrivilege, false, "invalid role"},
				{false, validUser, invalidRole, invalidPrivilege, false, "invalid role, invalid privilege"},
				{false, validUser, invalidRole, keyNotExistPrivilege, false, "invalid role, not exist privilege"},
				// not exist role
				{false, validUser, keyNotExistRole, validPrivilege, true, "not exist role with exist privilege"},
				// exist role
				{false, validUser, validRole, invalidPrivilege, false, "exist role with invalid privilege"},
				{false, validUser, validRole, keyNotExistPrivilege, true, "exist role with not exist privilege"},
				{true, validUser, validRole, validPrivilege, false, "exist role with exist privilege success to remove"},
				{false, validUser, validRole, invalidPrivilegeRemove, false, "exist role with exist privilege fail to remove"},
			}

			for _, test := range tests {
				t.Run(test.description, func(t *testing.T) {
					err := c.AlterGrant(ctx, tenant, &milvuspb.GrantEntity{
						Role:       &milvuspb.RoleEntity{Name: test.roleName},
						Object:     &milvuspb.ObjectEntity{Name: object},
						ObjectName: objName,
						DbName:     util.DefaultDBName,
						Grantor: &milvuspb.GrantorEntity{
							User:      &milvuspb.UserEntity{Name: test.userName},
							Privilege: &milvuspb.PrivilegeEntity{Name: test.privilegeName}},
					}, milvuspb.OperatePrivilegeType_Revoke)

					if test.isValid {
						assert.NoError(t, err)
					} else {
						assert.Error(t, err)
						_, ok := err.(*common.IgnorableError)
						if test.ignorable {
							assert.True(t, ok)
						} else {
							assert.False(t, ok)
						}
					}
				})
			}
		})
	})
	t.Run("test DeleteGrant", func(t *testing.T) {
		var (
			kvmock = mocks.NewTxnKV(t)
			c      = &Catalog{Txn: kvmock}

			errorRole       = "error-role"
			errorRolePrefix = funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant, errorRole+"/")
		)

		kvmock.EXPECT().RemoveWithPrefix(errorRolePrefix).Call.Return(errors.New("mock removeWithPrefix error"))
		kvmock.EXPECT().RemoveWithPrefix(mock.Anything).Call.Return(nil)

		tests := []struct {
			isValid bool
			role    string

			description string
		}{
			{true, "role1", "valid role1"},
			{false, errorRole, "invalid errorRole"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				err := c.DeleteGrant(ctx, tenant, &milvuspb.RoleEntity{Name: test.role})
				if test.isValid {
					assert.NoError(t, err)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})
	t.Run("test ListGrant", func(t *testing.T) {
		var (
			kvmock = mocks.NewTxnKV(t)
			c      = &Catalog{Txn: kvmock}
		)

		// Mock Load in kv_catalog.go:L901
		validGranteeKey := funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant,
			fmt.Sprintf("%s/%s/%s", "role1", "obj1", "obj_name1"))
		kvmock.EXPECT().Load(validGranteeKey).Call.
			Return(func(key string) string { return crypto.MD5(key) }, nil)
		validGranteeKey2 := funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant,
			fmt.Sprintf("%s/%s/%s", "role1", "obj1", "foo.obj_name2"))
		kvmock.EXPECT().Load(validGranteeKey2).Call.
			Return(func(key string) string { return crypto.MD5(key) }, nil)
		kvmock.EXPECT().Load(mock.Anything).Call.
			Return("", errors.New("mock Load error"))

		invalidRoleKey := funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant, invalidRole)
		kvmock.EXPECT().LoadWithPrefix(invalidRoleKey).Call.Return(nil, nil, errors.New("mock loadWithPrefix error"))
		kvmock.EXPECT().LoadWithPrefix(mock.Anything).Call.Return(
			func(key string) []string {

				// Mock kv_catalog.go:ListGrant:L871
				if strings.Contains(key, GranteeIDPrefix) {
					return []string{
						fmt.Sprintf("%s/%s", key, "PrivilegeLoad"),
						fmt.Sprintf("%s/%s", key, "PrivilegeRelease"),
					}
				}

				// Mock kv_catalog.go:ListGrant:L912
				return []string{
					fmt.Sprintf("%s/%s", key, "obj1/obj_name1"),
					fmt.Sprintf("%s/%s", key, "obj2/obj_name2"),
				}
			},
			func(key string) []string {
				if strings.Contains(key, GranteeIDPrefix) {
					return []string{"user1", "user2"}
				}
				return []string{
					crypto.MD5(fmt.Sprintf("%s/%s", key, "obj1/obj_name1")),
					crypto.MD5(fmt.Sprintf("%s/%s", key, "obj2/obj_name2")),
				}
			},
			nil,
		)

		tests := []struct {
			isValid bool

			entity      *milvuspb.GrantEntity
			description string
		}{
			{true, &milvuspb.GrantEntity{Role: &milvuspb.RoleEntity{Name: "role1"}}, "valid role role1 with empty entity"},
			{false, &milvuspb.GrantEntity{Role: &milvuspb.RoleEntity{Name: invalidRole}}, "invalid role with empty entity"},
			{false, &milvuspb.GrantEntity{
				Object:     &milvuspb.ObjectEntity{Name: "random"},
				ObjectName: "random2",
				Role:       &milvuspb.RoleEntity{Name: "role1"}}, "valid role with not exist entity"},
			{true, &milvuspb.GrantEntity{
				Object:     &milvuspb.ObjectEntity{Name: "obj1"},
				ObjectName: "obj_name1",
				Role:       &milvuspb.RoleEntity{Name: "role1"}}, "valid role with valid entity"},
			{true, &milvuspb.GrantEntity{
				Object:     &milvuspb.ObjectEntity{Name: "obj1"},
				ObjectName: "obj_name2",
				DbName:     "foo",
				Role:       &milvuspb.RoleEntity{Name: "role1"}}, "valid role and dbName with valid entity"},
			{false, &milvuspb.GrantEntity{
				Object:     &milvuspb.ObjectEntity{Name: "obj1"},
				ObjectName: "obj_name2",
				DbName:     "foo2",
				Role:       &milvuspb.RoleEntity{Name: "role1"}}, "valid role and invalid dbName with valid entity"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				if test.entity.DbName == "" {
					test.entity.DbName = util.DefaultDBName
				}
				grants, err := c.ListGrant(ctx, tenant, test.entity)
				if test.isValid {
					assert.NoError(t, err)
					for _, g := range grants {
						assert.Equal(t, test.entity.GetRole().GetName(), g.GetRole().GetName())
					}
				} else {
					assert.Error(t, err)
				}

			})
		}
	})
	t.Run("test ListPolicy", func(t *testing.T) {
		var (
			kvmock = mocks.NewTxnKV(t)
			c      = &Catalog{Txn: kvmock}

			firstLoadWithPrefixReturn  atomic.Bool
			secondLoadWithPrefixReturn atomic.Bool
		)

		kvmock.EXPECT().LoadWithPrefix(mock.Anything).Call.Return(
			func(key string) []string {
				contains := strings.Contains(key, GranteeIDPrefix)
				if contains {
					if secondLoadWithPrefixReturn.Load() {
						return []string{
							fmt.Sprintf("%s/%s", key, "PrivilegeLoad"),
							fmt.Sprintf("%s/%s", key, "PrivilegeRelease"),
							fmt.Sprintf("%s/%s", key, "random/a/b/c"),
						}
					}
					return nil
				}

				if firstLoadWithPrefixReturn.Load() {
					return []string{
						fmt.Sprintf("%s/%s", key, "role1/obj1/obj_name1"),
						fmt.Sprintf("%s/%s", key, "role2/obj2/obj_name2"),
						"random",
					}
				}
				return nil
			},

			func(key string) []string {
				if firstLoadWithPrefixReturn.Load() {
					return []string{
						crypto.MD5(fmt.Sprintf("%s/%s", key, "obj1/obj_name1")),
						crypto.MD5(fmt.Sprintf("%s/%s", key, "obj2/obj_name2")),
					}
				}
				return nil
			},

			func(key string) error {
				contains := strings.Contains(key, GranteeIDPrefix)
				if contains {
					if secondLoadWithPrefixReturn.Load() {
						return nil
					}
					return errors.New("mock loadwithprefix error")
				}

				if firstLoadWithPrefixReturn.Load() {
					return nil
				}
				return errors.New("mock loadWithPrefix error")
			},
		)

		tests := []struct {
			isValid      bool
			firstReturn  bool
			secondReturn bool

			description string
		}{
			{true, true, true, "valid both load with prefix success"},
			{false, true, false, "invalid first loadWithPrefix success second fail"},
			{false, false, true, "invalid first loadWithPrefix fail and second success"},
			{false, false, false, "invalid first loadWithPrefix fail and second fail"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				firstLoadWithPrefixReturn.Store(test.firstReturn)
				secondLoadWithPrefixReturn.Store(test.secondReturn)

				policy, err := c.ListPolicy(ctx, tenant)
				if test.isValid {
					assert.NoError(t, err)
					assert.Equal(t, 4, len(policy))
					ps := []string{
						funcutil.PolicyForPrivilege("role1", "obj1", "obj_name1", "PrivilegeLoad", "default"),
						funcutil.PolicyForPrivilege("role1", "obj1", "obj_name1", "PrivilegeRelease", "default"),
						funcutil.PolicyForPrivilege("role2", "obj2", "obj_name2", "PrivilegeLoad", "default"),
						funcutil.PolicyForPrivilege("role2", "obj2", "obj_name2", "PrivilegeRelease", "default"),
					}
					assert.ElementsMatch(t, ps, policy)
				} else {
					assert.Error(t, err)
				}
			})
		}
	})
}
