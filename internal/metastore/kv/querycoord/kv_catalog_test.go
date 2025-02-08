package querycoord

import (
	"sort"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
	suite.catalog.SaveCollection(&querypb.CollectionLoadInfo{
		CollectionID: 1,
	})

	suite.catalog.SaveCollection(&querypb.CollectionLoadInfo{
		CollectionID: 2,
	})

	suite.catalog.SaveCollection(&querypb.CollectionLoadInfo{
		CollectionID: 3,
	})

	suite.catalog.ReleaseCollection(1)
	suite.catalog.ReleaseCollection(2)

	collections, err := suite.catalog.GetCollections()
	suite.NoError(err)
	suite.Len(collections, 1)
}

func (suite *CatalogTestSuite) TestCollectionWithPartition() {
	suite.catalog.SaveCollection(&querypb.CollectionLoadInfo{
		CollectionID: 1,
	})

	suite.catalog.SaveCollection(&querypb.CollectionLoadInfo{
		CollectionID: 2,
	}, &querypb.PartitionLoadInfo{
		CollectionID: 2,
		PartitionID:  102,
	})

	suite.catalog.SaveCollection(&querypb.CollectionLoadInfo{
		CollectionID: 3,
	}, &querypb.PartitionLoadInfo{
		CollectionID: 3,
		PartitionID:  103,
	})

	suite.catalog.ReleaseCollection(1)
	suite.catalog.ReleaseCollection(2)

	collections, err := suite.catalog.GetCollections()
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(int64(3), collections[0].GetCollectionID())
	partitions, err := suite.catalog.GetPartitions()
	suite.NoError(err)
	suite.Len(partitions, 1)
	suite.Len(partitions[int64(3)], 1)
	suite.Equal(int64(103), partitions[int64(3)][0].GetPartitionID())
}

func (suite *CatalogTestSuite) TestPartition() {
	suite.catalog.SavePartition(&querypb.PartitionLoadInfo{
		PartitionID: 1,
	})

	suite.catalog.SavePartition(&querypb.PartitionLoadInfo{
		PartitionID: 2,
	})

	suite.catalog.SavePartition(&querypb.PartitionLoadInfo{
		PartitionID: 3,
	})

	suite.catalog.ReleasePartition(1)
	suite.catalog.ReleasePartition(2)

	partitions, err := suite.catalog.GetPartitions()
	suite.NoError(err)
	suite.Len(partitions, 1)
}

func (suite *CatalogTestSuite) TestReleaseManyPartitions() {
	partitionIDs := make([]int64, 0)
	for i := 1; i <= 150; i++ {
		suite.catalog.SavePartition(&querypb.PartitionLoadInfo{
			CollectionID: 1,
			PartitionID:  int64(i),
		})
		partitionIDs = append(partitionIDs, int64(i))
	}

	err := suite.catalog.ReleasePartition(1, partitionIDs...)
	suite.NoError(err)
	partitions, err := suite.catalog.GetPartitions()
	suite.NoError(err)
	suite.Len(partitions, 0)
}

func (suite *CatalogTestSuite) TestReplica() {
	suite.catalog.SaveReplica(&querypb.Replica{
		CollectionID: 1,
		ID:           1,
	})

	suite.catalog.SaveReplica(&querypb.Replica{
		CollectionID: 1,
		ID:           2,
	})

	suite.catalog.SaveReplica(&querypb.Replica{
		CollectionID: 1,
		ID:           3,
	})

	suite.catalog.ReleaseReplica(1, 1)
	suite.catalog.ReleaseReplica(1, 2)

	replicas, err := suite.catalog.GetReplicas()
	suite.NoError(err)
	suite.Len(replicas, 1)
}

func (suite *CatalogTestSuite) TestResourceGroup() {
	suite.catalog.SaveResourceGroup(&querypb.ResourceGroup{
		Name:     "rg1",
		Capacity: 3,
		Nodes:    []int64{1, 2, 3},
	})
	suite.catalog.SaveResourceGroup(&querypb.ResourceGroup{
		Name:     "rg2",
		Capacity: 3,
		Nodes:    []int64{4, 5},
	})

	suite.catalog.SaveResourceGroup(&querypb.ResourceGroup{
		Name:     "rg3",
		Capacity: 0,
		Nodes:    []int64{},
	})

	suite.catalog.RemoveResourceGroup("rg3")

	groups, err := suite.catalog.GetResourceGroups()
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
	suite.catalog.SaveCollectionTargets(&querypb.CollectionTarget{
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
	suite.catalog.RemoveCollectionTarget(2)

	targets, err := suite.catalog.GetCollectionTargets()
	suite.NoError(err)
	suite.Len(targets, 2)
	suite.Equal(int64(4), targets[1].Version)
	suite.Equal(int64(3), targets[3].Version)

	// test access meta store failed
	mockStore := mocks.NewMetaKv(suite.T())
	mockErr := errors.New("failed to access etcd")
	mockStore.EXPECT().MultiSave(mock.Anything).Return(mockErr)
	mockStore.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything).Return(mockErr)

	suite.catalog.cli = mockStore
	err = suite.catalog.SaveCollectionTargets(&querypb.CollectionTarget{})
	suite.ErrorIs(err, mockErr)

	_, err = suite.catalog.GetCollectionTargets()
	suite.ErrorIs(err, mockErr)

	// test invalid message
	err = suite.catalog.SaveCollectionTargets(nil)
	suite.Error(err)
}

func (suite *CatalogTestSuite) TestLoadRelease() {
	// TODO(sunby): add ut
}

func TestCatalogSuite(t *testing.T) {
	suite.Run(t, new(CatalogTestSuite))
}
