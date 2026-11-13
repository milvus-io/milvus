package querycoord

import (
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type CatalogTestSuite struct {
	suite.Suite

	kv      kv.MetaKv
	catalog Catalog
}

func (suite *CatalogTestSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *CatalogTestSuite) SetupTest() {
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath.GetValue())
	suite.catalog = NewCatalog(suite.kv)
}

func (suite *CatalogTestSuite) TearDownTest() {
	if suite.kv != nil {
		suite.kv.Close()
	}
}

func (suite *CatalogTestSuite) TestCollection() {
	ctx := context.Background()
	suite.catalog.SaveCollection(ctx, &querypb.CollectionLoadInfo{
		CollectionID: 1,
	})

	suite.catalog.SaveCollection(ctx, &querypb.CollectionLoadInfo{
		CollectionID: 2,
	})

	suite.catalog.SaveCollection(ctx, &querypb.CollectionLoadInfo{
		CollectionID: 3,
	})

	suite.catalog.ReleaseCollection(ctx, 1)
	suite.catalog.ReleaseCollection(ctx, 2)

	collections, err := suite.catalog.GetCollections(ctx)
	suite.NoError(err)
	suite.Len(collections, 1)
}

func (suite *CatalogTestSuite) TestCollectionWithPartition() {
	ctx := context.Background()
	suite.catalog.SaveCollection(ctx, &querypb.CollectionLoadInfo{
		CollectionID: 1,
	})

	suite.catalog.SaveCollection(ctx, &querypb.CollectionLoadInfo{
		CollectionID: 2,
	}, &querypb.PartitionLoadInfo{
		CollectionID: 2,
		PartitionID:  102,
	})

	suite.catalog.SaveCollection(ctx, &querypb.CollectionLoadInfo{
		CollectionID: 3,
	}, &querypb.PartitionLoadInfo{
		CollectionID: 3,
		PartitionID:  103,
	})

	suite.catalog.ReleaseCollection(ctx, 1)
	suite.catalog.ReleaseCollection(ctx, 2)

	collections, err := suite.catalog.GetCollections(ctx)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(int64(3), collections[0].GetCollectionID())
	partitions, err := suite.catalog.GetPartitions(ctx, lo.Map(collections, func(collection *querypb.CollectionLoadInfo, _ int) int64 {
		return collection.GetCollectionID()
	}))
	suite.NoError(err)
	suite.Len(partitions, 1)
	suite.Len(partitions[int64(3)], 1)
	suite.Equal(int64(103), partitions[int64(3)][0].GetPartitionID())
}

func (suite *CatalogTestSuite) TestSaveCollectionInOneTxn() {
	ctx := context.Background()
	store := mocks.NewMetaKv(suite.T())
	catalog := NewCatalog(store)

	store.EXPECT().MaxTxnOps().Return(3)
	store.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, saves map[string]string, removals []string, _ ...predicates.Predicate) error {
			suite.ElementsMatch([]string{
				EncodeCollectionLoadInfoKey(1),
				EncodePartitionLoadInfoKey(1, 100),
				EncodePartitionLoadInfoKey(1, 101),
			}, lo.Keys(saves))
			suite.Empty(removals)
			return nil
		}).Once()

	err := catalog.SaveCollection(ctx, &querypb.CollectionLoadInfo{CollectionID: 1},
		&querypb.PartitionLoadInfo{CollectionID: 1, PartitionID: 100},
		&querypb.PartitionLoadInfo{CollectionID: 1, PartitionID: 101},
	)
	suite.NoError(err)

	mockErr := errors.New("failed to access etcd")
	store.EXPECT().MultiSaveAndRemove(mock.Anything, mock.Anything, mock.Anything).Return(mockErr).Once()
	err = catalog.SaveCollection(ctx, &querypb.CollectionLoadInfo{CollectionID: 1})
	suite.ErrorIs(err, mockErr)
}

func (suite *CatalogTestSuite) TestSaveCollectionAcrossTxnLimit() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.Key, "3")
	defer paramtable.Get().Reset(paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.Key)

	partitionsOf := func(collectionID int64, num int) []*querypb.PartitionLoadInfo {
		partitions := make([]*querypb.PartitionLoadInfo, 0, num)
		for i := 0; i < num; i++ {
			partitions = append(partitions, &querypb.PartitionLoadInfo{CollectionID: collectionID, PartitionID: collectionID*100 + int64(i)})
		}
		return partitions
	}
	// Collection 1 and its partitions fit in one transaction, collection 2's do not.
	suite.NoError(suite.catalog.SaveCollection(ctx, &querypb.CollectionLoadInfo{CollectionID: 1}, partitionsOf(1, 2)...))
	suite.NoError(suite.catalog.SaveCollection(ctx, &querypb.CollectionLoadInfo{CollectionID: 2}, partitionsOf(2, 5)...))

	collections, err := suite.catalog.GetCollections(ctx)
	suite.NoError(err)
	suite.ElementsMatch([]int64{1, 2}, lo.Map(collections, func(collection *querypb.CollectionLoadInfo, _ int) int64 {
		return collection.GetCollectionID()
	}))
	partitions, err := suite.catalog.GetPartitions(ctx, []int64{1, 2})
	suite.NoError(err)
	suite.Len(partitions[1], 2)
	suite.Len(partitions[2], 5)
}

func (suite *CatalogTestSuite) TestPartition() {
	ctx := context.Background()
	suite.catalog.SavePartition(ctx, &querypb.PartitionLoadInfo{
		PartitionID: 1,
	})

	suite.catalog.SavePartition(ctx, &querypb.PartitionLoadInfo{
		PartitionID: 2,
	})

	suite.catalog.SavePartition(ctx, &querypb.PartitionLoadInfo{
		PartitionID: 3,
	})

	suite.catalog.ReleasePartition(ctx, 1)
	suite.catalog.ReleasePartition(ctx, 2)

	partitions, err := suite.catalog.GetPartitions(ctx, []int64{0})
	suite.NoError(err)
	suite.Len(partitions, 1)
}

func (suite *CatalogTestSuite) TestGetPartitions() {
	ctx := context.Background()
	suite.catalog.SaveCollection(ctx, &querypb.CollectionLoadInfo{
		CollectionID: 1,
	})
	suite.catalog.SavePartition(ctx, &querypb.PartitionLoadInfo{
		CollectionID: 1,
		PartitionID:  100,
	})
	suite.catalog.SaveCollection(ctx, &querypb.CollectionLoadInfo{
		CollectionID: 2,
	})
	suite.catalog.SavePartition(ctx, &querypb.PartitionLoadInfo{
		CollectionID: 2,
		PartitionID:  200,
	})
	suite.catalog.SaveCollection(ctx, &querypb.CollectionLoadInfo{
		CollectionID: 3,
	})
	suite.catalog.SavePartition(ctx, &querypb.PartitionLoadInfo{
		CollectionID: 3,
		PartitionID:  300,
	})

	partitions, err := suite.catalog.GetPartitions(ctx, []int64{1, 2, 3})
	suite.NoError(err)
	suite.Len(partitions, 3)
	suite.Len(partitions[int64(1)], 1)
	suite.Len(partitions[int64(2)], 1)
	suite.Len(partitions[int64(3)], 1)
	partitions, err = suite.catalog.GetPartitions(ctx, []int64{2, 3})
	suite.NoError(err)
	suite.Len(partitions, 2)
	suite.Len(partitions[int64(2)], 1)
	suite.Len(partitions[int64(3)], 1)
	partitions, err = suite.catalog.GetPartitions(ctx, []int64{3})
	suite.NoError(err)
	suite.Len(partitions, 1)
	suite.Len(partitions[int64(3)], 1)
	suite.Equal(int64(300), partitions[int64(3)][0].GetPartitionID())
	partitions, err = suite.catalog.GetPartitions(ctx, []int64{})
	suite.NoError(err)
	suite.Len(partitions, 0)
}

func (suite *CatalogTestSuite) TestReleaseManyPartitions() {
	ctx := context.Background()
	partitionIDs := make([]int64, 0)
	for i := 1; i <= 150; i++ {
		suite.catalog.SavePartition(ctx, &querypb.PartitionLoadInfo{
			CollectionID: 1,
			PartitionID:  int64(i),
		})
		partitionIDs = append(partitionIDs, int64(i))
	}

	err := suite.catalog.ReleasePartition(ctx, 1, partitionIDs...)
	suite.NoError(err)
	partitions, err := suite.catalog.GetPartitions(ctx, []int64{1})
	suite.NoError(err)
	suite.Len(partitions, 1)
	suite.Len(partitions[int64(1)], 0)
}

func (suite *CatalogTestSuite) TestReplica() {
	ctx := context.Background()
	suite.catalog.SaveReplica(ctx, &querypb.Replica{
		CollectionID: 1,
		ID:           1,
	})

	suite.catalog.SaveReplica(ctx, &querypb.Replica{
		CollectionID: 1,
		ID:           2,
	})

	suite.catalog.SaveReplica(ctx, &querypb.Replica{
		CollectionID: 1,
		ID:           3,
	})

	suite.catalog.ReleaseReplica(ctx, 1, 1)
	suite.catalog.ReleaseReplica(ctx, 1, 2)

	replicas, err := suite.catalog.GetReplicas(ctx)
	suite.NoError(err)
	suite.Len(replicas, 1)
}

func (suite *CatalogTestSuite) TestResourceGroup() {
	ctx := context.Background()
	suite.catalog.SaveResourceGroup(ctx, &querypb.ResourceGroup{
		Name:     "rg1",
		Capacity: 3,
		Nodes:    []int64{1, 2, 3},
	})
	suite.catalog.SaveResourceGroup(ctx, &querypb.ResourceGroup{
		Name:     "rg2",
		Capacity: 3,
		Nodes:    []int64{4, 5},
	})

	suite.catalog.SaveResourceGroup(ctx, &querypb.ResourceGroup{
		Name:     "rg3",
		Capacity: 0,
		Nodes:    []int64{},
	})

	suite.catalog.RemoveResourceGroup(ctx, "rg3")

	groups, err := suite.catalog.GetResourceGroups(ctx)
	suite.NoError(err)
	suite.Len(groups, 2)

	sort.Slice(groups, func(i, j int) bool {
		return groups[i].GetName() < groups[j].GetName()
	})

	suite.Equal("rg1", groups[0].GetName())
	suite.Equal(int32(3), groups[0].GetCapacity())
	suite.Equal([]int64{1, 2, 3}, groups[0].GetNodes())

	suite.Equal("rg2", groups[1].GetName())
	suite.Equal(int32(3), groups[1].GetCapacity())
	suite.Equal([]int64{4, 5}, groups[1].GetNodes())
}

func (suite *CatalogTestSuite) TestCollectionTarget() {
	ctx := context.Background()
	suite.catalog.SaveCollectionTargets(ctx, &querypb.CollectionTarget{
		CollectionID: 1,
		Version:      1,
	},
		&querypb.CollectionTarget{
			CollectionID: 2,
			Version:      2,
		},
		&querypb.CollectionTarget{
			CollectionID: 3,
			Version:      3,
		},
		&querypb.CollectionTarget{
			CollectionID: 1,
			Version:      4,
		})
	suite.catalog.RemoveCollectionTarget(ctx, 2)

	targets, err := suite.catalog.GetCollectionTargets(ctx)
	suite.NoError(err)
	suite.Len(targets, 2)
	suite.Equal(int64(4), targets[1].Version)
	suite.Equal(int64(3), targets[3].Version)

	// test access meta store failed
	mockStore := mocks.NewMetaKv(suite.T())
	mockErr := errors.New("failed to access etcd")
	mockStore.EXPECT().MultiSave(mock.Anything, mock.Anything).Return(mockErr)
	mockStore.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockErr)

	suite.catalog.cli = mockStore
	err = suite.catalog.SaveCollectionTargets(ctx, &querypb.CollectionTarget{})
	suite.ErrorIs(err, mockErr)

	_, err = suite.catalog.GetCollectionTargets(ctx)
	suite.ErrorIs(err, mockErr)

	// test invalid message
	err = suite.catalog.SaveCollectionTargets(ctx)
	suite.Error(err)
}

func (suite *CatalogTestSuite) TestRemoveCollectionTargets() {
	ctx := context.Background()
	// save 5 targets
	suite.catalog.SaveCollectionTargets(ctx,
		&querypb.CollectionTarget{CollectionID: 1, Version: 1},
		&querypb.CollectionTarget{CollectionID: 2, Version: 2},
		&querypb.CollectionTarget{CollectionID: 3, Version: 3},
		&querypb.CollectionTarget{CollectionID: 4, Version: 4},
		&querypb.CollectionTarget{CollectionID: 5, Version: 5},
	)

	// remove all targets via prefix delete
	err := suite.catalog.RemoveCollectionTargets(ctx)
	suite.NoError(err)

	targets, err := suite.catalog.GetCollectionTargets(ctx)
	suite.NoError(err)
	suite.Len(targets, 0)

	// remove when no targets exist should be no-op
	err = suite.catalog.RemoveCollectionTargets(ctx)
	suite.NoError(err)

	// test error from meta store
	mockStore := mocks.NewMetaKv(suite.T())
	mockErr := errors.New("failed to access etcd")
	mockStore.EXPECT().RemoveWithPrefix(mock.Anything, CollectionTargetPrefix).Return(mockErr)

	suite.catalog.cli = mockStore
	err = suite.catalog.RemoveCollectionTargets(ctx)
	suite.ErrorIs(err, mockErr)
}

func (suite *CatalogTestSuite) TestLoadRelease() {
	// TODO(sunby): add ut
}

func TestCatalogSuite(t *testing.T) {
	suite.Run(t, new(CatalogTestSuite))
}

// getPartitionsByPrefix is the former GetPartitions: one prefix read per
// requested collection.
func getPartitionsByPrefix(ctx context.Context, cli kv.MetaKv, collectionIDs []int64) (map[int64][]*querypb.PartitionLoadInfo, error) {
	result := make(map[int64][]*querypb.PartitionLoadInfo, len(collectionIDs))
	for _, collectionID := range collectionIDs {
		_, values, err := cli.LoadWithPrefix(ctx, EncodePartitionLoadInfoPrefix(collectionID))
		if err != nil {
			return nil, err
		}
		partitions := make([]*querypb.PartitionLoadInfo, 0, len(values))
		for _, value := range values {
			info := &querypb.PartitionLoadInfo{}
			if err := proto.Unmarshal([]byte(value), info); err != nil {
				return nil, err
			}
			partitions = append(partitions, info)
		}
		result[collectionID] = partitions
	}
	return result, nil
}

func (suite *CatalogTestSuite) TestGetPartitionsMatchesPrefixReads() {
	ctx := context.Background()
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	suite.Require().NoError(err)
	storage := etcdkv.NewEtcdKV(cli, "get-partitions-"+uuid.New().String())
	defer storage.Close()
	defer storage.RemoveWithPrefix(ctx, "")
	catalog := NewCatalog(storage)
	for _, collectionID := range []int64{1, 12, 123, 1234, 7, 99, -5} {
		for partitionID := int64(0); partitionID < collectionID%5+1; partitionID++ {
			suite.Require().NoError(catalog.SavePartition(ctx, &querypb.PartitionLoadInfo{CollectionID: collectionID, PartitionID: collectionID*100 + partitionID, RecoverTimes: int32(partitionID)}))
		}
	}
	// Keys no collection prefix matches, including non-canonical IDs.
	for _, key := range []string{"0123/1", "+123/1", "12", "123x/1", "/1"} {
		suite.Require().NoError(storage.Save(ctx, PartitionLoadInfoPrefix+"/"+key, "invalid protobuf"))
	}
	// An orphan's value is never decoded.
	suite.Require().NoError(storage.Save(ctx, EncodePartitionLoadInfoKey(5, 1), "invalid protobuf"))

	for _, collectionIDs := range [][]int64{{1, 12, 123, 1234}, {123}, {1234, 7, 1, 1}, {99, -5, 8}, {}} {
		want, err := getPartitionsByPrefix(ctx, storage, collectionIDs)
		suite.Require().NoError(err)
		got, err := catalog.GetPartitions(ctx, collectionIDs)
		suite.Require().NoError(err)
		suite.Require().Equal(len(want), len(got))
		for collectionID, partitions := range want {
			suite.Require().Contains(got, collectionID)
			suite.Require().NotNil(got[collectionID])
			suite.Require().Len(got[collectionID], len(partitions))
			for i := range partitions {
				suite.True(proto.Equal(partitions[i], got[collectionID][i]))
			}
		}
	}

	suite.Require().NoError(storage.Save(ctx, EncodePartitionLoadInfoKey(7, 999), "invalid protobuf"))
	_, wantErr := getPartitionsByPrefix(ctx, storage, []int64{1, 7})
	suite.Require().Error(wantErr)
	partitions, err := catalog.GetPartitions(ctx, []int64{1, 7})
	suite.EqualError(err, wantErr.Error())
	suite.Nil(partitions)
}
