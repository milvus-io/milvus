package meta

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/proto/querypb"
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
		RoNodes:       []int64{4},
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

	mr := r.copyForWrite()
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
	mr := r.copyForWrite()

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
	suite.ElementsMatch(suite.replicaPB.GetNodes(), r.GetNodes())

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

func TestReplica(t *testing.T) {
	suite.Run(t, new(ReplicaSuite))
}
