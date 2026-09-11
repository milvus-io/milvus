package querycoord

import (
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

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

// releaseFailKV wraps a real MetaKv and fails selected removal ops to
// simulate a crash between the two legacy release steps.
type releaseFailKV struct {
	kv.MetaKv
	removeWithPrefixErr error
	mixedErr            error
}

func (f *releaseFailKV) RemoveWithPrefix(ctx context.Context, key string) error {
	if f.removeWithPrefixErr != nil {
		return f.removeWithPrefixErr
	}
	return f.MetaKv.RemoveWithPrefix(ctx, key)
}

func (f *releaseFailKV) MultiSaveAndRemoveMixed(ctx context.Context, saves map[string]string, removals []string, prefixRemovals []string, preds ...predicates.Predicate) error {
	if f.mixedErr != nil {
		return f.mixedErr
	}
	return f.MetaKv.MultiSaveAndRemoveMixed(ctx, saves, removals, prefixRemovals, preds...)
}

func (suite *CatalogTestSuite) saveCollectionWithPartitions(ctx context.Context, collection int64, partitions ...int64) {
	err := suite.catalog.SaveCollection(ctx, &querypb.CollectionLoadInfo{
		CollectionID: collection,
	}, lo.Map(partitions, func(partition int64, _ int) *querypb.PartitionLoadInfo {
		return &querypb.PartitionLoadInfo{
			CollectionID: collection,
			PartitionID:  partition,
		}
	})...)
	suite.Require().NoError(err)
}

func (suite *CatalogTestSuite) TestReleaseCollectionRemovesAllKeys() {
	ctx := context.Background()
	suite.saveCollectionWithPartitions(ctx, 1, 100, 101)
	suite.saveCollectionWithPartitions(ctx, 10, 110)

	otherCollectionValue, err := suite.kv.Load(ctx, EncodeCollectionLoadInfoKey(10))
	suite.NoError(err)
	otherPartitionValue, err := suite.kv.Load(ctx, EncodePartitionLoadInfoKey(10, 110))
	suite.NoError(err)

	suite.NoError(suite.catalog.ReleaseCollection(ctx, 1))

	_, err = suite.kv.Load(ctx, EncodeCollectionLoadInfoKey(1))
	suite.Error(err)
	keys, _, err := suite.kv.LoadWithPrefix(ctx, EncodePartitionLoadInfoPrefix(1))
	suite.NoError(err)
	suite.Empty(keys)

	// collection 10 shares the "1" digit prefix and must be untouched, byte for byte
	value, err := suite.kv.Load(ctx, EncodeCollectionLoadInfoKey(10))
	suite.NoError(err)
	suite.Equal(otherCollectionValue, value)
	value, err = suite.kv.Load(ctx, EncodePartitionLoadInfoKey(10, 110))
	suite.NoError(err)
	suite.Equal(otherPartitionValue, value)
}

func (suite *CatalogTestSuite) TestReleaseCollectionAtomicUnderFailure() {
	ctx := context.Background()
	injected := errors.New("injected failure")

	assertIntact := func(collection int64, partitions ...int64) {
		_, err := suite.kv.Load(ctx, EncodeCollectionLoadInfoKey(collection))
		suite.NoError(err)
		keys, _, err := suite.kv.LoadWithPrefix(ctx, EncodePartitionLoadInfoPrefix(collection))
		suite.NoError(err)
		suite.Len(keys, len(partitions))
	}

	// prefix removal fails while exact removal would succeed: the release must
	// not leave partitions behind without the collection load info
	suite.saveCollectionWithPartitions(ctx, 1, 100, 101)
	suite.catalog.cli = &releaseFailKV{MetaKv: suite.kv, removeWithPrefixErr: injected}
	err := suite.catalog.ReleaseCollection(ctx, 1)
	if err != nil {
		assertIntact(1, 100, 101)
	} else {
		_, err = suite.kv.Load(ctx, EncodeCollectionLoadInfoKey(1))
		suite.Error(err)
		keys, _, err := suite.kv.LoadWithPrefix(ctx, EncodePartitionLoadInfoPrefix(1))
		suite.NoError(err)
		suite.Empty(keys)
	}

	// whole transaction fails: nothing may be removed
	suite.catalog.cli = suite.kv
	suite.saveCollectionWithPartitions(ctx, 2, 200, 201)
	suite.catalog.cli = &releaseFailKV{MetaKv: suite.kv, removeWithPrefixErr: injected, mixedErr: injected}
	err = suite.catalog.ReleaseCollection(ctx, 2)
	suite.ErrorIs(err, injected)
	assertIntact(2, 200, 201)
}

func (suite *CatalogTestSuite) TestReleaseCollectionSingleTxn() {
	ctx := context.Background()
	mockStore := mocks.NewMetaKv(suite.T())
	mockStore.EXPECT().MultiSaveAndRemoveMixed(mock.Anything, mock.Anything,
		[]string{"querycoord-collection-loadinfo/1"},
		[]string{"querycoord-partition-loadinfo/1/"}).Return(nil).Once()

	suite.catalog.cli = mockStore
	suite.NoError(suite.catalog.ReleaseCollection(ctx, 1))

	mockErr := errors.New("failed to access etcd")
	mockStore.EXPECT().MultiSaveAndRemoveMixed(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockErr).Once()
	suite.ErrorIs(suite.catalog.ReleaseCollection(ctx, 1), mockErr)
}

func TestCatalogSuite(t *testing.T) {
	suite.Run(t, new(CatalogTestSuite))
}
