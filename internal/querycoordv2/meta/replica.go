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
	replicaPB      *querypb.Replica
	availableNodes typeutil.UniqueSet // a helper field for manipulating replica's Available Nodes slice field.
	// always keep consistent with replicaPB.Nodes.
	// mutual exclusive with outboundNodes.
	outboundNodes typeutil.UniqueSet // a helper field for manipulating replica's Outbound Nodes slice field.
	// always keep consistent with replicaPB.OutboundNodes.
	// node used by replica but not in resource group will be in outboundNodes.
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
		replicaPB:      proto.Clone(replica).(*querypb.Replica),
		availableNodes: typeutil.NewUniqueSet(replica.Nodes...),
		outboundNodes:  typeutil.NewUniqueSet(replica.OutboundNodes...),
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

// GetNodes returns the available nodes of the replica.
// readonly, don't modify the returned slice.
func (replica *Replica) GetNodes() []int64 {
	return replica.replicaPB.GetNodes()
}

// GetOutboundNodes returns the outbound nodes of the replica.
// readonly, don't modify the returned slice.
func (replica *Replica) GetOutboundNodes() []int64 {
	return replica.replicaPB.GetOutboundNodes()
}

// RangeOverAvailableNodes iterates over the available nodes of the replica.
func (replica *Replica) RangeOverAvailableNodes(f func(node int64) bool) {
	replica.availableNodes.Range(f)
}

// RangeOverOutboundNodes iterates over the outbound nodes of the replica.
func (replica *Replica) RangeOverOutboundNodes(f func(node int64) bool) {
	replica.outboundNodes.Range(f)
}

// AvailableNodesCount returns the count of available nodes of the replica.
func (replica *Replica) AvailableNodesCount() int {
	return replica.availableNodes.Len()
}

// OutboundNodesCount returns the count of outbound nodes of the replica.
func (replica *Replica) OutboundNodesCount() int {
	return replica.outboundNodes.Len()
}

// NodesCount returns the count of available nodes and outbound nodes of the replica.
func (replica *Replica) NodesCount() int {
	return replica.availableNodes.Len() + replica.outboundNodes.Len()
}

// Contains checks if the node is in available nodes of the replica.
func (replica *Replica) Contains(node int64) bool {
	return replica.availableNodes.Contain(node)
}

// ContainOutboundNode checks if the node is in outbound nodes of the replica.
func (replica *Replica) ContainOutboundNode(node int64) bool {
	return replica.outboundNodes.Contain(node)
}

// Deprecated: Warning, break the consistency of ReplicaManager, use `SetAvailableNodesInSameCollectionAndRG` in ReplicaManager instead.
// TODO: removed in future, only for old unittest now.
func (replica *Replica) AddAvailableNode(nodes ...int64) {
	replica.outboundNodes.Remove(nodes...)
	replica.replicaPB.OutboundNodes = replica.outboundNodes.Collect()
	replica.availableNodes.Insert(nodes...)
	replica.replicaPB.Nodes = replica.availableNodes.Collect()
}

// copyForWrite returns a mutable replica for write operations.
func (replica *Replica) copyForWrite() *mutableReplica {
	return &mutableReplica{
		&Replica{
			replicaPB:      proto.Clone(replica.replicaPB).(*querypb.Replica),
			availableNodes: typeutil.NewUniqueSet(replica.replicaPB.Nodes...),
			outboundNodes:  typeutil.NewUniqueSet(replica.replicaPB.OutboundNodes...),
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

// AddAvailableNode adds the node to available nodes of the replica.
func (replica *mutableReplica) AddAvailableNode(nodes ...int64) {
	replica.Replica.AddAvailableNode(nodes...)
}

// AddOutboundNode moves the node from available nodes to outbound nodes of the replica.
// only used in replica manager.
func (replica *mutableReplica) AddOutboundNode(nodes ...int64) {
	replica.availableNodes.Remove(nodes...)
	replica.replicaPB.Nodes = replica.availableNodes.Collect()
	replica.outboundNodes.Insert(nodes...)
	replica.replicaPB.OutboundNodes = replica.outboundNodes.Collect()
}

// RemoveNode removes the node from available nodes and outbound nodes of the replica.
// only used in replica manager.
func (replica *mutableReplica) RemoveNode(nodes ...int64) {
	replica.outboundNodes.Remove(nodes...)
	replica.replicaPB.OutboundNodes = replica.outboundNodes.Collect()
	replica.availableNodes.Remove(nodes...)
	replica.replicaPB.Nodes = replica.availableNodes.Collect()
}

// IntoReplica returns the immutable replica, After calling this method, the mutable replica should not be used again.
func (replica *mutableReplica) IntoReplica() *Replica {
	r := replica.Replica
	replica.Replica = nil
	return r
}
