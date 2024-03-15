package meta

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/stretchr/testify/suite"
)

type ReplicaSuite struct {
	suite.Suite

	replicaPB *querypb.Replica
}

func (suite *ReplicaSuite) SetupSuite() {
	suite.replicaPB = &querypb.Replica{
		ID:            1,
		CollectionID:  2,
		Nodes:         []int64{1, 2, 3},
		ResourceGroup: DefaultResourceGroupName,
		OutboundNodes: []int64{4},
	}
}

func (suite *ReplicaSuite) TestReadOperations() {
	r := newReplica(suite.replicaPB)
	suite.testRead(r)
	// keep same after clone.
	mutableReplica := r.copyForWrite()
	suite.testRead(mutableReplica.IntoReplica())
}

func (suite *ReplicaSuite) TestClone() {
	r := newReplica(suite.replicaPB)
	r2 := r.copyForWrite()
	suite.testRead(r)

	// after apply write operation on copy, the original should not be affected.
	r2.AddNode(5, 6)
	r2.MoveNodeToOutbound(1, 2)
	r2.RemoveNode(3)
	suite.testRead(r)
}

func (suite *ReplicaSuite) TestRange() {
	count := 0
	r := newReplica(suite.replicaPB)
	r.RangeOverAvailableNodes(func(nodeID int64) bool {
		count++
		return true
	})
	suite.Equal(3, count)
	count = 0
	r.RangeOverOutboundNodes(func(nodeID int64) bool {
		count++
		return true
	})
	suite.Equal(1, count)

	count = 0
	r.RangeOverAvailableNodes(func(nodeID int64) bool {
		count++
		return false
	})
	suite.Equal(1, count)

	count = 0
	mr := r.copyForWrite()
	mr.MoveNodeToOutbound(1)
	mr.RangeOverAvailableNodes(func(nodeID int64) bool {
		count++
		return false
	})
	suite.Equal(1, count)
}

func (suite *ReplicaSuite) TestWriteOperation() {
	r := newReplica(suite.replicaPB)
	mr := r.copyForWrite()

	// test add node.
	suite.False(mr.Contains(5))
	suite.False(mr.InUseNode(5))
	suite.False(mr.Contains(6))
	suite.False(mr.InUseNode(6))
	mr.AddNode(5, 6)
	suite.Equal(3, r.AvailableNodesCount())
	suite.Equal(1, r.OutboundNodesCount())
	suite.Equal(4, r.NodesCount())
	suite.Equal(5, mr.AvailableNodesCount())
	suite.Equal(1, mr.OutboundNodesCount())
	suite.Equal(6, mr.NodesCount())
	suite.True(mr.Contains(5))
	suite.True(mr.Contains(5))
	suite.True(mr.InUseNode(5))
	suite.True(mr.Contains(6))
	suite.True(mr.InUseNode(6))

	// test add outbound node.
	suite.False(mr.Contains(4))
	suite.True(mr.InUseNode(4))
	suite.False(mr.Contains(7))
	suite.False(mr.InUseNode(7))
	mr.AddNode(4, 7)
	suite.Equal(3, r.AvailableNodesCount())
	suite.Equal(1, r.OutboundNodesCount())
	suite.Equal(4, r.NodesCount())
	suite.Equal(7, mr.AvailableNodesCount())
	suite.Equal(0, mr.OutboundNodesCount())
	suite.Equal(7, mr.NodesCount())
	suite.True(mr.Contains(4))
	suite.True(mr.InUseNode(4))
	suite.True(mr.Contains(7))
	suite.True(mr.InUseNode(7))

	// test remove node to outbound.
	mr.MoveNodeToOutbound(4, 7)
	suite.Equal(3, r.AvailableNodesCount())
	suite.Equal(1, r.OutboundNodesCount())
	suite.Equal(4, r.NodesCount())
	suite.Equal(5, mr.AvailableNodesCount())
	suite.Equal(2, mr.OutboundNodesCount())
	suite.Equal(7, mr.NodesCount())
	suite.False(mr.Contains(4))
	suite.True(mr.InUseNode(4))
	suite.False(mr.Contains(7))
	suite.True(mr.InUseNode(7))

	// test remove node.
	mr.RemoveNode(4, 5, 7)
	suite.Equal(3, r.AvailableNodesCount())
	suite.Equal(1, r.OutboundNodesCount())
	suite.Equal(4, r.NodesCount())
	suite.Equal(4, mr.AvailableNodesCount())
	suite.Equal(0, mr.OutboundNodesCount())
	suite.Equal(4, mr.NodesCount())
	suite.False(mr.Contains(4))
	suite.False(mr.Contains(5))
	suite.False(mr.Contains(7))
	suite.False(mr.InUseNode(4))
	suite.False(mr.InUseNode(5))
	suite.False(mr.InUseNode(7))

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
	suite.ElementsMatch(suite.replicaPB.GetNodes(), r.GetNodes())

	// Test GetOutboundNodes()
	suite.ElementsMatch(suite.replicaPB.GetOutboundNodes(), r.GetOutboundNodes())

	// Test AvailableNodesCount()
	suite.Equal(len(suite.replicaPB.GetNodes()), r.AvailableNodesCount())

	// Test Contains()
	suite.True(r.Contains(1))
	suite.False(r.Contains(4))

	// Test ContainOutboundNode()
	suite.True(r.ContainOutboundNode(4))

	// Test InUseNode()
	suite.True(r.InUseNode(4))
	suite.True(r.InUseNode(1))
	suite.False(r.InUseNode(5))
}

func TestReplica(t *testing.T) {
	suite.Run(t, new(ReplicaSuite))
}
