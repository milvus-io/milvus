package rg

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	DefaultResourceGroup = "__default_resource_group"
	RecycleResourceGroup = "__recycle_resource_group"
)

type collectionConfig struct {
	resourceGroups []string
	createCfg      *integration.CreateCollectionConfig
}

type resourceGroupConfig struct {
	expectedNodeNum int
	rgCfg           *rgpb.ResourceGroupConfig
}

type ResourceGroupTestSuite struct {
	integration.MiniClusterSuite
	rgs         map[string]*resourceGroupConfig
	collections map[string]*collectionConfig
}

func (s *ResourceGroupTestSuite) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "1000")
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.CheckNodeInReplicaInterval.Key, "1")
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")

	s.MiniClusterSuite.SetupSuite()
}

func (s *ResourceGroupTestSuite) TestResourceGroup() {
	ctx := context.Background()

	s.rgs = map[string]*resourceGroupConfig{
		DefaultResourceGroup: {
			expectedNodeNum: 1,
			rgCfg:           newRGConfig(1, 1),
		},
		RecycleResourceGroup: {
			expectedNodeNum: 0,
			rgCfg: &rgpb.ResourceGroupConfig{
				Requests: &rgpb.ResourceGroupLimit{
					NodeNum: 0,
				},
				Limits: &rgpb.ResourceGroupLimit{
					NodeNum: 10000,
				},
			},
		},
		"rg1": {
			expectedNodeNum: 0,
			rgCfg:           newRGConfig(0, 0),
		},
		"rg2": {
			expectedNodeNum: 0,
			rgCfg:           newRGConfig(0, 0),
		},
	}

	s.initResourceGroup(ctx)

	s.assertResourceGroup(ctx)

	// only one node in rg
	s.rgs[DefaultResourceGroup].rgCfg.Requests.NodeNum = 2
	s.rgs[DefaultResourceGroup].rgCfg.Limits.NodeNum = 2
	s.syncResourceConfig(ctx)
	s.assertResourceGroup(ctx)

	s.rgs[DefaultResourceGroup].expectedNodeNum = 2
	s.Cluster.AddQueryNode()
	s.syncResourceConfig(ctx)
	s.assertResourceGroup(ctx)

	s.rgs[RecycleResourceGroup].expectedNodeNum = 3
	s.Cluster.AddQueryNodes(3)
	s.syncResourceConfig(ctx)
	s.assertResourceGroup(ctx)

	// node in recycle rg should be balanced to rg1 and rg2
	s.rgs["rg1"].rgCfg.Requests.NodeNum = 1
	s.rgs["rg1"].rgCfg.Limits.NodeNum = 1
	s.rgs["rg1"].expectedNodeNum = 1
	s.rgs["rg2"].rgCfg.Requests.NodeNum = 2
	s.rgs["rg2"].rgCfg.Limits.NodeNum = 2
	s.rgs["rg2"].expectedNodeNum = 2
	s.rgs[RecycleResourceGroup].expectedNodeNum = 0
	s.syncResourceConfig(ctx)
	s.assertResourceGroup(ctx)

	s.rgs[DefaultResourceGroup].rgCfg.Requests.NodeNum = 1
	s.rgs[DefaultResourceGroup].rgCfg.Limits.NodeNum = 2
	s.rgs[DefaultResourceGroup].expectedNodeNum = 2
	s.syncResourceConfig(ctx)
	s.assertResourceGroup(ctx)

	// redundant node in default rg should be balanced to recycle rg
	s.rgs[DefaultResourceGroup].rgCfg.Limits.NodeNum = 1
	s.rgs[DefaultResourceGroup].expectedNodeNum = 1
	s.rgs[RecycleResourceGroup].expectedNodeNum = 1
	s.syncResourceConfig(ctx)
	s.assertResourceGroup(ctx)
}

func (s *ResourceGroupTestSuite) TestWithReplica() {
	ctx := context.Background()

	s.rgs = map[string]*resourceGroupConfig{
		DefaultResourceGroup: {
			expectedNodeNum: 1,
			rgCfg:           newRGConfig(1, 1),
		},
		RecycleResourceGroup: {
			expectedNodeNum: 0,
			rgCfg: &rgpb.ResourceGroupConfig{
				Requests: &rgpb.ResourceGroupLimit{
					NodeNum: 0,
				},
				Limits: &rgpb.ResourceGroupLimit{
					NodeNum: 10000,
				},
			},
		},
		"rg1": {
			expectedNodeNum: 1,
			rgCfg:           newRGConfig(1, 1),
		},
		"rg2": {
			expectedNodeNum: 2,
			rgCfg:           newRGConfig(2, 2),
		},
	}
	s.collections = map[string]*collectionConfig{
		"c1": {
			resourceGroups: []string{DefaultResourceGroup},
			createCfg:      newCreateCollectionConfig("c1"),
		},
		"c2": {
			resourceGroups: []string{"rg1"},
			createCfg:      newCreateCollectionConfig("c2"),
		},
		"c3": {
			resourceGroups: []string{"rg2"},
			createCfg:      newCreateCollectionConfig("c3"),
		},
	}

	// create resource group
	s.initResourceGroup(ctx)
	s.Cluster.AddQueryNodes(3)
	time.Sleep(100 * time.Millisecond)
	s.assertResourceGroup(ctx)

	// create and load replicas for testing.
	s.createAndLoadCollections(ctx)
	s.assertReplica(ctx)

	// TODO: current balancer is not working well on move segment between nodes, open following test after fix it.
	// // test transfer replica and nodes.
	// // transfer one of replica in c3 from rg2 into DEFAULT rg.
	// s.collections["c3"].resourceGroups = []string{DefaultResourceGroup, "rg2"}
	//
	//	status, err := s.Cluster.Proxy.TransferReplica(ctx, &milvuspb.TransferReplicaRequest{
	//		DbName:              s.collections["c3"].createCfg.DBName,
	//		CollectionName:      s.collections["c3"].createCfg.CollectionName,
	//		SourceResourceGroup: "rg2",
	//		TargetResourceGroup: DefaultResourceGroup,
	//		NumReplica:          1,
	//	})
	//
	// s.NoError(err)
	// s.True(merr.Ok(status))
	//
	// // test transfer node from rg2 into DEFAULT_RESOURCE_GROUP
	// s.rgs[DefaultResourceGroup].rgCfg.Requests.NodeNum = 2
	// s.rgs[DefaultResourceGroup].rgCfg.Limits.NodeNum = 2
	// s.rgs[DefaultResourceGroup].expectedNodeNum = 2
	// s.rgs["rg2"].rgCfg.Requests.NodeNum = 1
	// s.rgs["rg2"].rgCfg.Limits.NodeNum = 1
	// s.rgs["rg2"].expectedNodeNum = 1
	// s.syncResourceConfig(ctx)
	//
	//	s.Eventually(func() bool {
	//		return s.assertReplica(ctx)
	//	}, 10*time.Minute, 30*time.Second)
}

func (s *ResourceGroupTestSuite) syncResourceConfig(ctx context.Context) {
	req := &milvuspb.UpdateResourceGroupsRequest{
		ResourceGroups: make(map[string]*rgpb.ResourceGroupConfig),
	}
	for rgName, cfg := range s.rgs {
		req.ResourceGroups[rgName] = cfg.rgCfg
	}
	status, err := s.Cluster.Proxy.UpdateResourceGroups(ctx, req)
	s.NoError(err)
	s.True(merr.Ok(status))

	// wait for recovery.
	time.Sleep(100 * time.Millisecond)
}

func (s *ResourceGroupTestSuite) assertResourceGroup(ctx context.Context) {
	resp, err := s.Cluster.Proxy.ListResourceGroups(ctx, &milvuspb.ListResourceGroupsRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp.Status))
	s.ElementsMatch(resp.ResourceGroups, lo.Keys(s.rgs))

	for _, rg := range resp.ResourceGroups {
		resp, err := s.Cluster.Proxy.DescribeResourceGroup(ctx, &milvuspb.DescribeResourceGroupRequest{
			ResourceGroup: rg,
		})
		s.NoError(err)
		s.True(merr.Ok(resp.Status))

		s.Equal(s.rgs[rg].expectedNodeNum, len(resp.ResourceGroup.Nodes))
		s.True(proto.Equal(s.rgs[rg].rgCfg, resp.ResourceGroup.Config))
	}
}

func (s *ResourceGroupTestSuite) initResourceGroup(ctx context.Context) {
	status, err := s.Cluster.Proxy.CreateResourceGroup(ctx, &milvuspb.CreateResourceGroupRequest{
		ResourceGroup: RecycleResourceGroup,
		Config:        s.rgs[RecycleResourceGroup].rgCfg,
	})
	s.NoError(err)
	s.True(merr.Ok(status))

	for rgName, cfg := range s.rgs {
		if rgName == RecycleResourceGroup || rgName == DefaultResourceGroup {
			continue
		}
		status, err := s.Cluster.Proxy.CreateResourceGroup(ctx, &milvuspb.CreateResourceGroupRequest{
			ResourceGroup: rgName,
			Config:        cfg.rgCfg,
		})
		s.NoError(err)
		s.True(merr.Ok(status))
	}

	status, err = s.Cluster.Proxy.UpdateResourceGroups(ctx, &milvuspb.UpdateResourceGroupsRequest{
		ResourceGroups: map[string]*rgpb.ResourceGroupConfig{
			DefaultResourceGroup: s.rgs[DefaultResourceGroup].rgCfg,
		},
	})
	s.NoError(err)
	s.True(merr.Ok(status))
}

func (s *ResourceGroupTestSuite) createAndLoadCollections(ctx context.Context) {
	wg := &sync.WaitGroup{}
	for _, cfg := range s.collections {
		cfg := cfg
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.CreateCollectionWithConfiguration(ctx, cfg.createCfg)
			loadStatus, err := s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
				DbName:         cfg.createCfg.DBName,
				CollectionName: cfg.createCfg.CollectionName,
				ReplicaNumber:  int32(len(cfg.resourceGroups)),
				ResourceGroups: cfg.resourceGroups,
			})
			s.NoError(err)
			s.True(merr.Ok(loadStatus))
			s.WaitForLoad(ctx, cfg.createCfg.CollectionName)
		}()
	}
	wg.Wait()
}

func (s *ResourceGroupTestSuite) assertReplica(ctx context.Context) bool {
	for _, cfg := range s.collections {
		resp, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			CollectionName: cfg.createCfg.CollectionName,
			DbName:         cfg.createCfg.DBName,
		})
		s.NoError(err)
		s.True(merr.Ok(resp.Status))
		rgs := make(map[string]int)
		for _, rg := range cfg.resourceGroups {
			rgs[rg]++
		}
		for _, replica := range resp.GetReplicas() {
			s.True(rgs[replica.ResourceGroupName] > 0)
			rgs[replica.ResourceGroupName]--
			s.NotZero(len(replica.NodeIds))
			if len(replica.NumOutboundNode) > 0 {
				return false
			}
		}
		for _, v := range rgs {
			s.Zero(v)
		}
	}
	return true
}

func newRGConfig(request int, limit int) *rgpb.ResourceGroupConfig {
	return &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{
			NodeNum: int32(request),
		},
		Limits: &rgpb.ResourceGroupLimit{
			NodeNum: int32(limit),
		},
		TransferFrom: []*rgpb.ResourceGroupTransfer{
			{
				ResourceGroup: RecycleResourceGroup,
			},
		},
		TransferTo: []*rgpb.ResourceGroupTransfer{
			{
				ResourceGroup: RecycleResourceGroup,
			},
		},
	}
}

func newCreateCollectionConfig(collectionName string) *integration.CreateCollectionConfig {
	return &integration.CreateCollectionConfig{
		DBName:           "",
		CollectionName:   collectionName,
		ChannelNum:       2,
		SegmentNum:       2,
		RowNumPerSegment: 100,
		Dim:              128,
	}
}

func TestResourceGroup(t *testing.T) {
	suite.Run(t, new(ResourceGroupTestSuite))
}
