package meta

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type ReplicaSuite struct {
	suite.Suite

	replicaPB *querypb.Replica
}

func (suite *ReplicaSuite) SetupSuite() {
	paramtable.Init()
	suite.replicaPB = &querypb.Replica{
		ID:            1,
		CollectionID:  2,
		Nodes:         []int64{1, 2, 3},
		ResourceGroup: DefaultResourceGroupName,
		RoNodes:       []int64{4},
	}
}

func (suite *ReplicaSuite) TestSNNodes() {
	replicaPB := &querypb.Replica{
		ID:            1,
		CollectionID:  2,
		Nodes:         []int64{1, 2, 3},
		ResourceGroup: DefaultResourceGroupName,
		RoNodes:       []int64{4},
		RwSqNodes:     []int64{6, 7, 8, 2},
		RoSqNodes:     []int64{5},
	}
	r := newReplica(replicaPB)
	suite.Len(r.GetNodes(), 8)
	suite.Len(r.GetROSQNodes(), r.ROSQNodesCount())
	suite.Len(r.GetRWSQNodes(), r.RWSQNodesCount())
	cnt := 0
	r.RangeOverRWSQNodes(func(nodeID int64) bool {
		cnt++
		return true
	})
	suite.Equal(r.RWSQNodesCount(), cnt)

	cnt = 0
	r.RangeOverROSQNodes(func(nodeID int64) bool {
		cnt++
		return true
	})
	suite.Equal(r.RONodesCount(), cnt)

	suite.Len(r.GetChannelRWNodes("channel1"), 0)

	copiedR := r.CopyForWrite()
	copiedR.AddRWSQNode(9, 5)
	r2 := copiedR.IntoReplica()
	suite.Equal(6, r2.RWSQNodesCount())
	suite.Equal(0, r2.ROSQNodesCount())

	copiedR = r.CopyForWrite()
	copiedR.AddROSQNode(7, 8)
	r2 = copiedR.IntoReplica()
	suite.Equal(2, r2.RWSQNodesCount())
	suite.Equal(3, r2.ROSQNodesCount())

	copiedR = r.CopyForWrite()
	copiedR.RemoveSQNode(5, 8)
	r2 = copiedR.IntoReplica()
	suite.Equal(3, r2.RWSQNodesCount())
	suite.Equal(0, r2.ROSQNodesCount())
}

func (suite *ReplicaSuite) TestReadOperations() {
	r := newReplica(suite.replicaPB)
	suite.testRead(r)
	// keep same after clone.
	mutableReplica := r.CopyForWrite()
	suite.testRead(mutableReplica.IntoReplica())
}

func (suite *ReplicaSuite) TestClone() {
	r := newReplica(suite.replicaPB)
	r2 := r.CopyForWrite()
	suite.testRead(r)

	// after apply write operation on copy, the original should not be affected.
	r2.AddRWNode(5, 6)
	r2.AddRONode(1, 2)
	r2.RemoveNode(3)
	suite.testRead(r)
}

func (suite *ReplicaSuite) TestRange() {
	count := 0
	r := newReplica(suite.replicaPB)
	r.RangeOverRWNodes(func(nodeID int64) bool {
		count++
		return true
	})
	suite.Equal(3, count)
	count = 0
	r.RangeOverRONodes(func(nodeID int64) bool {
		count++
		return true
	})
	suite.Equal(1, count)

	count = 0
	r.RangeOverRWNodes(func(nodeID int64) bool {
		count++
		return false
	})
	suite.Equal(1, count)

	mr := r.CopyForWrite()
	mr.AddRONode(1)

	count = 0
	mr.RangeOverRWNodes(func(nodeID int64) bool {
		count++
		return false
	})
	suite.Equal(1, count)
}

func (suite *ReplicaSuite) TestWriteOperation() {
	r := newReplica(suite.replicaPB)
	mr := r.CopyForWrite()

	// test add available node.
	suite.False(mr.Contains(5))
	suite.False(mr.Contains(6))
	mr.AddRWNode(5, 6)
	suite.Equal(3, r.RWNodesCount())
	suite.Equal(1, r.RONodesCount())
	suite.Equal(4, r.NodesCount())
	suite.Equal(5, mr.RWNodesCount())
	suite.Equal(1, mr.RONodesCount())
	suite.Equal(6, mr.NodesCount())
	suite.True(mr.Contains(5))
	suite.True(mr.Contains(5))
	suite.True(mr.Contains(6))

	// test add ro node.
	suite.False(mr.ContainRWNode(4))
	suite.False(mr.ContainRWNode(7))
	mr.AddRWNode(4, 7)
	suite.Equal(3, r.RWNodesCount())
	suite.Equal(1, r.RONodesCount())
	suite.Equal(4, r.NodesCount())
	suite.Equal(7, mr.RWNodesCount())
	suite.Equal(0, mr.RONodesCount())
	suite.Equal(7, mr.NodesCount())
	suite.True(mr.Contains(4))
	suite.True(mr.Contains(7))

	// test remove node to ro.
	mr.AddRONode(4, 7)
	suite.Equal(3, r.RWNodesCount())
	suite.Equal(1, r.RONodesCount())
	suite.Equal(4, r.NodesCount())
	suite.Equal(5, mr.RWNodesCount())
	suite.Equal(2, mr.RONodesCount())
	suite.Equal(7, mr.NodesCount())
	suite.False(mr.ContainRWNode(4))
	suite.False(mr.ContainRWNode(7))
	suite.True(mr.ContainRONode(4))
	suite.True(mr.ContainRONode(7))

	// test remove node.
	mr.RemoveNode(4, 5, 7, 8)
	suite.Equal(3, r.RWNodesCount())
	suite.Equal(1, r.RONodesCount())
	suite.Equal(4, r.NodesCount())
	suite.Equal(4, mr.RWNodesCount())
	suite.Equal(0, mr.RONodesCount())
	suite.Equal(4, mr.NodesCount())
	suite.False(mr.Contains(4))
	suite.False(mr.Contains(5))
	suite.False(mr.Contains(7))

	// test set resource group.
	mr.SetResourceGroup("rg1")
	suite.Equal(r.GetResourceGroup(), DefaultResourceGroupName)
	suite.Equal("rg1", mr.GetResourceGroup())

	// should panic after IntoReplica.
	mr.IntoReplica()
	suite.Panics(func() {
		mr.SetResourceGroup("newResourceGroup")
	})
}

func (suite *ReplicaSuite) testRead(r *Replica) {
	// Test GetID()
	suite.Equal(suite.replicaPB.GetID(), r.GetID())

	// Test GetCollectionID()
	suite.Equal(suite.replicaPB.GetCollectionID(), r.GetCollectionID())

	// Test GetResourceGroup()
	suite.Equal(suite.replicaPB.GetResourceGroup(), r.GetResourceGroup())

	// Test GetNodes()
	suite.ElementsMatch(suite.replicaPB.GetNodes(), r.GetRWNodes())

	// Test GetRONodes()
	suite.ElementsMatch(suite.replicaPB.GetRoNodes(), r.GetRONodes())

	// Test AvailableNodesCount()
	suite.Equal(len(suite.replicaPB.GetNodes()), r.RWNodesCount())

	// Test Contains()
	suite.True(r.Contains(1))
	suite.True(r.Contains(4))

	// Test ContainRONode()
	suite.False(r.ContainRONode(1))
	suite.True(r.ContainRONode(4))

	// Test ContainsRWNode()
	suite.True(r.ContainRWNode(1))
	suite.False(r.ContainRWNode(4))
}

func (suite *ReplicaSuite) TestChannelExclusiveMode() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.Balancer.Key, ChannelLevelScoreBalancerName)
	defer paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.Balancer.Key)

	r := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  2,
		ResourceGroup: DefaultResourceGroupName,
		ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{
			"channel1": {},
			"channel2": {},
			"channel3": {},
			"channel4": {},
		},
	})

	mutableReplica := r.CopyForWrite()
	// add 10 rw nodes, exclusive mode is false.
	for i := 0; i < 10; i++ {
		mutableReplica.AddRWNode(int64(i))
	}
	r = mutableReplica.IntoReplica()
	for _, channelNodeInfo := range r.replicaPB.GetChannelNodeInfos() {
		suite.Equal(0, len(channelNodeInfo.GetRwNodes()))
	}

	mutableReplica = r.CopyForWrite()
	// add 10 rw nodes, exclusive mode is true.
	for i := 10; i < 20; i++ {
		mutableReplica.AddRWNode(int64(i))
	}
	r = mutableReplica.IntoReplica()
	for _, channelNodeInfo := range r.replicaPB.GetChannelNodeInfos() {
		suite.Equal(5, len(channelNodeInfo.GetRwNodes()))
	}

	// 4 node become read only, exclusive mode still be true
	mutableReplica = r.CopyForWrite()
	for i := 0; i < 4; i++ {
		mutableReplica.AddRONode(int64(i))
	}
	r = mutableReplica.IntoReplica()
	for _, channelNodeInfo := range r.replicaPB.GetChannelNodeInfos() {
		suite.Equal(4, len(channelNodeInfo.GetRwNodes()))
	}

	// 4 node has been removed, exclusive mode back to false
	mutableReplica = r.CopyForWrite()
	for i := 4; i < 8; i++ {
		mutableReplica.RemoveNode(int64(i))
	}
	r = mutableReplica.IntoReplica()
	for _, channelNodeInfo := range r.replicaPB.GetChannelNodeInfos() {
		suite.Equal(0, len(channelNodeInfo.GetRwNodes()))
	}
}

// TestTryBalanceNodeForChannelEmptyChannels tests behavior when no channels exist
func (suite *ReplicaSuite) TestTryBalanceNodeForChannelEmptyChannels() {
	r := newReplica(&querypb.Replica{
		ID:               1,
		CollectionID:     2,
		ResourceGroup:    DefaultResourceGroupName,
		Nodes:            []int64{1, 2, 3, 4},
		ChannelNodeInfos: make(map[string]*querypb.ChannelNodeInfo),
	})

	mutableReplica := r.CopyForWrite()
	// Should not panic and should return early
	mutableReplica.tryBalanceNodeForChannel()

	// Verify no changes were made
	newR := mutableReplica.IntoReplica()
	suite.Equal(0, len(newR.replicaPB.GetChannelNodeInfos()))
}

// TestTryBalanceNodeForChannelDisabledMode tests when channel exclusive mode is disabled
func (suite *ReplicaSuite) TestTryBalanceNodeForChannelDisabledMode() {
	// Set balance policy to non-ChannelLevelScoreBalancer
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.Balancer.Key, RoundRobinBalancerName)
	defer paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.Balancer.Key)

	r := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  2,
		ResourceGroup: DefaultResourceGroupName,
		Nodes:         []int64{1, 2, 3, 4},
		ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{
			"channel1": {RwNodes: []int64{1, 2}},
			"channel2": {RwNodes: []int64{3, 4}},
		},
	})

	mutableReplica := r.CopyForWrite()
	mutableReplica.tryBalanceNodeForChannel()

	newR := mutableReplica.IntoReplica()
	// Channel node infos should be cleared when exclusive mode is disabled
	for _, channelNodeInfo := range newR.replicaPB.GetChannelNodeInfos() {
		suite.Equal(0, len(channelNodeInfo.GetRwNodes()))
	}
	// exclusiveRWNodeToChannel should be reset
	suite.Equal(0, len(mutableReplica.exclusiveRWNodeToChannel))
}

// TestTryBalanceNodeForChannelInsufficientNodes tests when there are not enough nodes
func (suite *ReplicaSuite) TestTryBalanceNodeForChannelInsufficientNodes() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.Balancer.Key, ChannelLevelScoreBalancerName)
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ChannelExclusiveNodeFactor.Key, "2")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.Balancer.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ChannelExclusiveNodeFactor.Key)
	}()

	// 2 nodes for 2 channels, but factor is 2, so need 4 nodes minimum
	r := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  2,
		ResourceGroup: DefaultResourceGroupName,
		Nodes:         []int64{1, 2},
		ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{
			"channel1": {RwNodes: []int64{1}},
			"channel2": {RwNodes: []int64{2}},
		},
	})

	mutableReplica := r.CopyForWrite()
	mutableReplica.tryBalanceNodeForChannel()

	newR := mutableReplica.IntoReplica()
	// Should clear channel node infos due to insufficient nodes
	for _, channelNodeInfo := range newR.replicaPB.GetChannelNodeInfos() {
		suite.Equal(0, len(channelNodeInfo.GetRwNodes()))
	}
}

// TestTryBalanceNodeForChannelPerfectBalance tests perfect node distribution
func (suite *ReplicaSuite) TestTryBalanceNodeForChannelPerfectBalance() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.Balancer.Key, ChannelLevelScoreBalancerName)
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ChannelExclusiveNodeFactor.Key, "1")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.Balancer.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ChannelExclusiveNodeFactor.Key)
	}()

	// 6 nodes for 3 channels = 2 nodes per channel
	r := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  2,
		ResourceGroup: DefaultResourceGroupName,
		Nodes:         []int64{1, 2, 3, 4, 5, 6},
		ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{
			"channel1": {},
			"channel2": {},
			"channel3": {},
		},
	})

	mutableReplica := r.CopyForWrite()
	mutableReplica.tryBalanceNodeForChannel()

	newR := mutableReplica.IntoReplica()

	// Each channel should have exactly 2 nodes
	totalAssignedNodes := 0
	for _, channelNodeInfo := range newR.replicaPB.GetChannelNodeInfos() {
		suite.Equal(2, len(channelNodeInfo.GetRwNodes()))
		totalAssignedNodes += len(channelNodeInfo.GetRwNodes())
	}
	suite.Equal(6, totalAssignedNodes)

	// All nodes should be assigned exclusively
	suite.Equal(6, len(mutableReplica.exclusiveRWNodeToChannel))
}

// TestTryBalanceNodeForChannelUnbalancedToBalanced tests rebalancing from unbalanced state
func (suite *ReplicaSuite) TestTryBalanceNodeForChannelUnbalancedToBalanced() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.Balancer.Key, ChannelLevelScoreBalancerName)
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ChannelExclusiveNodeFactor.Key, "1")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.Balancer.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ChannelExclusiveNodeFactor.Key)
	}()

	// Start with unbalanced distribution: channel1 has 4 nodes, channel2 has 1 node, channel3 has 0 nodes
	r := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  2,
		ResourceGroup: DefaultResourceGroupName,
		Nodes:         []int64{1, 2, 3, 4, 5},
		ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{
			"channel1": {RwNodes: []int64{1, 2, 3, 4}},
			"channel2": {RwNodes: []int64{5}},
			"channel3": {},
		},
	})

	mutableReplica := r.CopyForWrite()
	// Initialize exclusiveRWNodeToChannel to simulate existing assignments
	mutableReplica.exclusiveRWNodeToChannel = map[int64]string{
		1: "channel1", 2: "channel1", 3: "channel1", 4: "channel1", 5: "channel2",
	}

	mutableReplica.tryBalanceNodeForChannel()

	newR := mutableReplica.IntoReplica()

	// Should be rebalanced: 5 nodes / 3 channels = 1 node each, with 2 channels getting 2 nodes
	nodeCountPerChannel := make(map[string]int)
	totalNodes := 0
	for channelName, channelNodeInfo := range newR.replicaPB.GetChannelNodeInfos() {
		nodeCount := len(channelNodeInfo.GetRwNodes())
		nodeCountPerChannel[channelName] = nodeCount
		totalNodes += nodeCount
		// Each channel should have 1 or 2 nodes
		suite.True(nodeCount >= 1 && nodeCount <= 2, "Channel %s has %d nodes", channelName, nodeCount)
	}

	suite.Equal(5, totalNodes)
	// Two channels should have 2 nodes, one should have 1 node
	countOfChannelsWith2Nodes := 0
	countOfChannelsWith1Node := 0
	for _, count := range nodeCountPerChannel {
		if count == 2 {
			countOfChannelsWith2Nodes++
		} else if count == 1 {
			countOfChannelsWith1Node++
		}
	}
	suite.Equal(2, countOfChannelsWith2Nodes)
	suite.Equal(1, countOfChannelsWith1Node)
}

// TestTryBalanceNodeForChannelWithExtraNodes tests distribution with extra nodes
func (suite *ReplicaSuite) TestTryBalanceNodeForChannelWithExtraNodes() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.Balancer.Key, ChannelLevelScoreBalancerName)
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ChannelExclusiveNodeFactor.Key, "1")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.Balancer.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ChannelExclusiveNodeFactor.Key)
	}()

	// 7 nodes for 3 channels = 2 nodes per channel + 1 extra
	r := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  2,
		ResourceGroup: DefaultResourceGroupName,
		Nodes:         []int64{1, 2, 3, 4, 5, 6, 7},
		ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{
			"channel1": {},
			"channel2": {},
			"channel3": {},
		},
	})

	mutableReplica := r.CopyForWrite()
	mutableReplica.tryBalanceNodeForChannel()

	newR := mutableReplica.IntoReplica()

	// Should distribute extra node: 2 channels get 3 nodes, 1 channel gets 2 nodes
	// Or: 1 channel gets 3 nodes, 2 channels get 2 nodes
	nodeCountPerChannel := make([]int, 0, 3)
	totalNodes := 0
	for _, channelNodeInfo := range newR.replicaPB.GetChannelNodeInfos() {
		nodeCount := len(channelNodeInfo.GetRwNodes())
		nodeCountPerChannel = append(nodeCountPerChannel, nodeCount)
		totalNodes += nodeCount
		suite.True(nodeCount >= 2 && nodeCount <= 3, "Each channel should have 2 or 3 nodes, got %d", nodeCount)
	}

	suite.Equal(7, totalNodes)

	// Sum should be 7 (2+2+3 or 2+3+2 or 3+2+2)
	sum := 0
	for _, count := range nodeCountPerChannel {
		sum += count
	}
	suite.Equal(7, sum)
}

// TestShouldEnableChannelExclusiveMode tests the condition checking function
func (suite *ReplicaSuite) TestShouldEnableChannelExclusiveMode() {
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.Balancer.Key, ChannelLevelScoreBalancerName)
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ChannelExclusiveNodeFactor.Key, "2")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.Balancer.Key)
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.ChannelExclusiveNodeFactor.Key)
	}()

	r := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  2,
		ResourceGroup: DefaultResourceGroupName,
		Nodes:         []int64{1, 2, 3, 4},
	})
	mutableReplica := r.CopyForWrite()

	// Test with sufficient nodes (4 nodes, 2 channels, factor 2: 4 >= 2*2)
	channelInfos := map[string]*querypb.ChannelNodeInfo{
		"channel1": {},
		"channel2": {},
	}
	suite.True(mutableReplica.shouldEnableChannelExclusiveMode(channelInfos))

	// Test with insufficient nodes (4 nodes, 3 channels, factor 2: 4 < 3*2)
	channelInfos = map[string]*querypb.ChannelNodeInfo{
		"channel1": {},
		"channel2": {},
		"channel3": {},
	}
	suite.False(mutableReplica.shouldEnableChannelExclusiveMode(channelInfos))

	// Test with disabled balancer
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.Balancer.Key, RoundRobinBalancerName)
	channelInfos = map[string]*querypb.ChannelNodeInfo{
		"channel1": {},
		"channel2": {},
	}
	suite.False(mutableReplica.shouldEnableChannelExclusiveMode(channelInfos))
}

// TestClearChannelNodeInfos tests the channel clearing function
func (suite *ReplicaSuite) TestClearChannelNodeInfos() {
	r := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  2,
		ResourceGroup: DefaultResourceGroupName,
		Nodes:         []int64{1, 2, 3, 4},
		ChannelNodeInfos: map[string]*querypb.ChannelNodeInfo{
			"channel1": {RwNodes: []int64{1, 2}},
			"channel2": {RwNodes: []int64{3, 4}},
		},
	})

	mutableReplica := r.CopyForWrite()
	mutableReplica.exclusiveRWNodeToChannel = map[int64]string{
		1: "channel1", 2: "channel1", 3: "channel2", 4: "channel2",
	}

	mutableReplica.DisableChannelExclusiveMode()

	// All channel node infos should be cleared
	for _, channelNodeInfo := range mutableReplica.replicaPB.GetChannelNodeInfos() {
		suite.Equal(0, len(channelNodeInfo.GetRwNodes()))
	}

	// exclusiveRWNodeToChannel should be reset
	suite.Equal(0, len(mutableReplica.exclusiveRWNodeToChannel))
}

// TestGetAvailableNodes tests the available nodes retrieval function
func (suite *ReplicaSuite) TestGetAvailableNodes() {
	r := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  2,
		ResourceGroup: DefaultResourceGroupName,
		Nodes:         []int64{1, 2, 3, 4, 5},
	})

	mutableReplica := r.CopyForWrite()

	// Initially all nodes should be available
	availableNodes := mutableReplica.getAvailableNodes()
	suite.ElementsMatch([]int64{1, 2, 3, 4, 5}, availableNodes)

	// Mark some nodes as exclusively assigned
	mutableReplica.exclusiveRWNodeToChannel = map[int64]string{
		1: "channel1",
		3: "channel2",
	}

	availableNodes = mutableReplica.getAvailableNodes()
	suite.ElementsMatch([]int64{2, 4, 5}, availableNodes)
}

// TestAllocateNodesFromPool tests the node allocation function
func (suite *ReplicaSuite) TestAllocateNodesFromPool() {
	r := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  2,
		ResourceGroup: DefaultResourceGroupName,
		Nodes:         []int64{1, 2, 3, 4, 5},
	})

	mutableReplica := r.CopyForWrite()
	mutableReplica.exclusiveRWNodeToChannel = make(map[int64]string)

	// Test allocating 3 nodes from pool of 5
	availableNodes := []int64{1, 2, 3, 4, 5}
	allocatedNodes := mutableReplica.allocateNodesFromPool(availableNodes, 3, "channel1")

	suite.Equal(3, len(allocatedNodes))
	for _, nodeID := range allocatedNodes {
		suite.Equal("channel1", mutableReplica.exclusiveRWNodeToChannel[nodeID])
	}

	// Test allocating more nodes than available
	availableNodes = []int64{6, 7}
	allocatedNodes = mutableReplica.allocateNodesFromPool(availableNodes, 5, "channel2")

	suite.Equal(2, len(allocatedNodes))
	suite.ElementsMatch([]int64{6, 7}, allocatedNodes)
	for _, nodeID := range allocatedNodes {
		suite.Equal("channel2", mutableReplica.exclusiveRWNodeToChannel[nodeID])
	}

	// Test allocating from empty pool
	availableNodes = []int64{}
	allocatedNodes = mutableReplica.allocateNodesFromPool(availableNodes, 2, "channel3")

	suite.Equal(0, len(allocatedNodes))
}

// TestGetSortedChannelsByNodeCount tests the channel sorting function
func (suite *ReplicaSuite) TestGetSortedChannelsByNodeCount() {
	r := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  2,
		ResourceGroup: DefaultResourceGroupName,
	})

	mutableReplica := r.CopyForWrite()

	channelInfos := map[string]*querypb.ChannelNodeInfo{
		"channel1": {RwNodes: []int64{1}},       // 1 node
		"channel2": {RwNodes: []int64{2, 3, 4}}, // 3 nodes
		"channel3": {RwNodes: []int64{5, 6}},    // 2 nodes
		"channel4": {RwNodes: []int64{}},        // 0 nodes
	}

	sortedChannels := mutableReplica.getSortedChannelsByNodeCount(channelInfos)

	// Should be sorted by node count descending: channel2(3), channel3(2), channel1(1), channel4(0)
	suite.Equal(4, len(sortedChannels))
	suite.Equal("channel2", sortedChannels[0])
	suite.Equal("channel3", sortedChannels[1])
	suite.Equal("channel1", sortedChannels[2])
	suite.Equal("channel4", sortedChannels[3])
}

// TestCalculateOptimalAssignments tests the assignment calculation function
func (suite *ReplicaSuite) TestCalculateOptimalAssignments() {
	r := newReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  2,
		ResourceGroup: DefaultResourceGroupName,
		Nodes:         []int64{1, 2, 3, 4, 5, 6, 7},
	})

	mutableReplica := r.CopyForWrite()

	// Test perfect division: 6 nodes, 3 channels = 2 nodes each
	channelInfos := map[string]*querypb.ChannelNodeInfo{
		"channel1": {RwNodes: []int64{1, 2, 3}},
		"channel2": {RwNodes: []int64{4}},
		"channel3": {RwNodes: []int64{}},
	}

	// Mock RWNodesCount to return 6 for this test
	originalNodes := mutableReplica.rwNodes
	mutableReplica.rwNodes.Clear()
	mutableReplica.rwNodes.Insert(1, 2, 3, 4, 5, 6)

	assignments := mutableReplica.calculateOptimalAssignments(channelInfos)

	suite.Equal(3, len(assignments))
	totalAssigned := 0
	for _, count := range assignments {
		totalAssigned += count
		suite.True(count >= 2 && count <= 2, "Each channel should get exactly 2 nodes")
	}
	suite.Equal(6, totalAssigned)

	// Restore original nodes
	mutableReplica.rwNodes = originalNodes

	// Test with remainder: 7 nodes, 3 channels = 2 nodes each + 1 extra
	mutableReplica.rwNodes.Clear()
	mutableReplica.rwNodes.Insert(1, 2, 3, 4, 5, 6, 7)

	assignments = mutableReplica.calculateOptimalAssignments(channelInfos)

	suite.Equal(3, len(assignments))
	totalAssigned = 0
	countsOfTwo := 0
	countsOfThree := 0
	for _, count := range assignments {
		totalAssigned += count
		if count == 2 {
			countsOfTwo++
		} else if count == 3 {
			countsOfThree++
		}
	}
	suite.Equal(7, totalAssigned)
	suite.Equal(2, countsOfTwo)   // 2 channels get 2 nodes
	suite.Equal(1, countsOfThree) // 1 channel gets 3 nodes
}

func TestReplica(t *testing.T) {
	suite.Run(t, new(ReplicaSuite))
}
