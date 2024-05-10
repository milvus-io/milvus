package meta

import (
	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// NilReplica is used to represent a nil replica.
var NilReplica = newReplica(&querypb.Replica{
	ID: -1,
})

// Replica is a immutable type for manipulating replica meta info for replica manager.
// Performed a copy-on-write strategy to keep the consistency of the replica manager.
// So only read only operations are allowed on these type.
type Replica struct {
	replicaPB *querypb.Replica
	rwNodes   typeutil.UniqueSet // a helper field for manipulating replica's Available Nodes slice field.
	// always keep consistent with replicaPB.Nodes.
	// mutual exclusive with roNodes.
	roNodes typeutil.UniqueSet // a helper field for manipulating replica's RO Nodes slice field.
	// always keep consistent with replicaPB.RoNodes.
	// node used by replica but cannot add more channel or segment ont it.
	// include rebalance node or node out of resource group.
}

// Deprecated: may break the consistency of ReplicaManager, use `Spawn` of `ReplicaManager` or `newReplica` instead.
func NewReplica(replica *querypb.Replica, nodes ...typeutil.UniqueSet) *Replica {
	r := proto.Clone(replica).(*querypb.Replica)
	// TODO: nodes is a bad parameter, break the consistency, should be removed in future.
	// keep it for old unittest.
	if len(nodes) > 0 && len(replica.Nodes) == 0 && nodes[0].Len() > 0 {
		r.Nodes = nodes[0].Collect()
	}
	return newReplica(r)
}

// newReplica creates a new replica from pb.
func newReplica(replica *querypb.Replica) *Replica {
	return &Replica{
		replicaPB: proto.Clone(replica).(*querypb.Replica),
		rwNodes:   typeutil.NewUniqueSet(replica.Nodes...),
		roNodes:   typeutil.NewUniqueSet(replica.RoNodes...),
	}
}

// GetID returns the id of the replica.
func (replica *Replica) GetID() typeutil.UniqueID {
	return replica.replicaPB.GetID()
}

// GetCollectionID returns the collection id of the replica.
func (replica *Replica) GetCollectionID() typeutil.UniqueID {
	return replica.replicaPB.GetCollectionID()
}

// GetResourceGroup returns the resource group name of the replica.
func (replica *Replica) GetResourceGroup() string {
	return replica.replicaPB.GetResourceGroup()
}

// GetNodes returns the rw nodes of the replica.
// readonly, don't modify the returned slice.
func (replica *Replica) GetNodes() []int64 {
	return replica.replicaPB.GetNodes()
}

// GetRONodes returns the ro nodes of the replica.
// readonly, don't modify the returned slice.
func (replica *Replica) GetRONodes() []int64 {
	return replica.replicaPB.GetRoNodes()
}

// RangeOverRWNodes iterates over the read and write nodes of the replica.
func (replica *Replica) RangeOverRWNodes(f func(node int64) bool) {
	replica.rwNodes.Range(f)
}

// RangeOverRONodes iterates over the ro nodes of the replica.
func (replica *Replica) RangeOverRONodes(f func(node int64) bool) {
	replica.roNodes.Range(f)
}

// RWNodesCount returns the count of rw nodes of the replica.
func (replica *Replica) RWNodesCount() int {
	return replica.rwNodes.Len()
}

// RONodesCount returns the count of ro nodes of the replica.
func (replica *Replica) RONodesCount() int {
	return replica.roNodes.Len()
}

// NodesCount returns the count of rw nodes and ro nodes of the replica.
func (replica *Replica) NodesCount() int {
	return replica.rwNodes.Len() + replica.roNodes.Len()
}

// Contains checks if the node is in rw nodes of the replica.
func (replica *Replica) Contains(node int64) bool {
	return replica.ContainRONode(node) || replica.ContainRWNode(node)
}

// ContainRONode checks if the node is in ro nodes of the replica.
func (replica *Replica) ContainRONode(node int64) bool {
	return replica.roNodes.Contain(node)
}

// ContainRONode checks if the node is in ro nodes of the replica.
func (replica *Replica) ContainRWNode(node int64) bool {
	return replica.rwNodes.Contain(node)
}

// Deprecated: Warning, break the consistency of ReplicaManager, use `SetAvailableNodesInSameCollectionAndRG` in ReplicaManager instead.
// TODO: removed in future, only for old unittest now.
func (replica *Replica) AddRWNode(nodes ...int64) {
	replica.roNodes.Remove(nodes...)
	replica.replicaPB.RoNodes = replica.roNodes.Collect()
	replica.rwNodes.Insert(nodes...)
	replica.replicaPB.Nodes = replica.rwNodes.Collect()
}

// copyForWrite returns a mutable replica for write operations.
func (replica *Replica) copyForWrite() *mutableReplica {
	return &mutableReplica{
		&Replica{
			replicaPB: proto.Clone(replica.replicaPB).(*querypb.Replica),
			rwNodes:   typeutil.NewUniqueSet(replica.replicaPB.Nodes...),
			roNodes:   typeutil.NewUniqueSet(replica.replicaPB.RoNodes...),
		},
	}
}

// mutableReplica is a mutable type (COW) for manipulating replica meta info for replica manager.
type mutableReplica struct {
	*Replica
}

// SetResourceGroup sets the resource group name of the replica.
func (replica *mutableReplica) SetResourceGroup(resourceGroup string) {
	replica.replicaPB.ResourceGroup = resourceGroup
}

// AddRWNode adds the node to rw nodes of the replica.
func (replica *mutableReplica) AddRWNode(nodes ...int64) {
	replica.Replica.AddRWNode(nodes...)
}

// AddRONode moves the node from rw nodes to ro nodes of the replica.
// only used in replica manager.
func (replica *mutableReplica) AddRONode(nodes ...int64) {
	replica.rwNodes.Remove(nodes...)
	replica.replicaPB.Nodes = replica.rwNodes.Collect()
	replica.roNodes.Insert(nodes...)
	replica.replicaPB.RoNodes = replica.roNodes.Collect()
}

// RemoveNode removes the node from rw nodes and ro nodes of the replica.
// only used in replica manager.
func (replica *mutableReplica) RemoveNode(nodes ...int64) {
	replica.roNodes.Remove(nodes...)
	replica.replicaPB.RoNodes = replica.roNodes.Collect()
	replica.rwNodes.Remove(nodes...)
	replica.replicaPB.Nodes = replica.rwNodes.Collect()
}

// IntoReplica returns the immutable replica, After calling this method, the mutable replica should not be used again.
func (replica *mutableReplica) IntoReplica() *Replica {
	r := replica.Replica
	replica.Replica = nil
	return r
}
