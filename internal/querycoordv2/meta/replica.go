package meta

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
	nodes := make([]int64, 0)
	nodes = append(nodes, replica.replicaPB.GetRoNodes()...)
	nodes = append(nodes, replica.replicaPB.GetNodes()...)
	return nodes
}

// GetRONodes returns the ro nodes of the replica.
// readonly, don't modify the returned slice.
func (replica *Replica) GetRONodes() []int64 {
	return replica.replicaPB.GetRoNodes()
}

// GetRONodes returns the rw nodes of the replica.
// readonly, don't modify the returned slice.
func (replica *Replica) GetRWNodes() []int64 {
	return replica.replicaPB.GetNodes()
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

func (replica *Replica) GetChannelRWNodes(channelName string) []int64 {
	channelNodeInfos := replica.replicaPB.GetChannelNodeInfos()
	if channelNodeInfos[channelName] == nil || len(channelNodeInfos[channelName].GetRwNodes()) == 0 {
		return nil
	}
	return replica.replicaPB.ChannelNodeInfos[channelName].GetRwNodes()
}

// CopyForWrite returns a mutable replica for write operations.
func (replica *Replica) CopyForWrite() *mutableReplica {
	exclusiveRWNodeToChannel := make(map[int64]string)
	for name, channelNodeInfo := range replica.replicaPB.GetChannelNodeInfos() {
		for _, nodeID := range channelNodeInfo.GetRwNodes() {
			exclusiveRWNodeToChannel[nodeID] = name
		}
	}

	return &mutableReplica{
		Replica: &Replica{
			replicaPB: proto.Clone(replica.replicaPB).(*querypb.Replica),
			rwNodes:   typeutil.NewUniqueSet(replica.replicaPB.Nodes...),
			roNodes:   typeutil.NewUniqueSet(replica.replicaPB.RoNodes...),
		},
		exclusiveRWNodeToChannel: exclusiveRWNodeToChannel,
	}
}

// mutableReplica is a mutable type (COW) for manipulating replica meta info for replica manager.
type mutableReplica struct {
	*Replica

	exclusiveRWNodeToChannel map[int64]string
}

// SetResourceGroup sets the resource group name of the replica.
func (replica *mutableReplica) SetResourceGroup(resourceGroup string) {
	replica.replicaPB.ResourceGroup = resourceGroup
}

// AddRWNode adds the node to rw nodes of the replica.
func (replica *mutableReplica) AddRWNode(nodes ...int64) {
	replica.Replica.AddRWNode(nodes...)

	// try to update node's assignment between channels
	replica.tryBalanceNodeForChannel()
}

// AddRONode moves the node from rw nodes to ro nodes of the replica.
// only used in replica manager.
func (replica *mutableReplica) AddRONode(nodes ...int64) {
	replica.rwNodes.Remove(nodes...)
	replica.replicaPB.Nodes = replica.rwNodes.Collect()
	replica.roNodes.Insert(nodes...)
	replica.replicaPB.RoNodes = replica.roNodes.Collect()

	// remove node from channel's exclusive list
	replica.removeChannelExclusiveNodes(nodes...)

	// try to update node's assignment between channels
	replica.tryBalanceNodeForChannel()
}

// RemoveNode removes the node from rw nodes and ro nodes of the replica.
// only used in replica manager.
func (replica *mutableReplica) RemoveNode(nodes ...int64) {
	replica.roNodes.Remove(nodes...)
	replica.replicaPB.RoNodes = replica.roNodes.Collect()
	replica.rwNodes.Remove(nodes...)
	replica.replicaPB.Nodes = replica.rwNodes.Collect()

	// remove node from channel's exclusive list
	replica.removeChannelExclusiveNodes(nodes...)

	// try to update node's assignment between channels
	replica.tryBalanceNodeForChannel()
}

func (replica *mutableReplica) removeChannelExclusiveNodes(nodes ...int64) {
	channelNodeMap := make(map[string][]int64)
	for _, nodeID := range nodes {
		channelName, ok := replica.exclusiveRWNodeToChannel[nodeID]
		if ok {
			if channelNodeMap[channelName] == nil {
				channelNodeMap[channelName] = make([]int64, 0)
			}
			channelNodeMap[channelName] = append(channelNodeMap[channelName], nodeID)
		}
		delete(replica.exclusiveRWNodeToChannel, nodeID)
	}

	for channelName, nodeIDs := range channelNodeMap {
		channelNodeInfo, ok := replica.replicaPB.ChannelNodeInfos[channelName]
		if ok {
			channelUsedNodes := typeutil.NewUniqueSet()
			channelUsedNodes.Insert(channelNodeInfo.GetRwNodes()...)
			channelUsedNodes.Remove(nodeIDs...)
			replica.replicaPB.ChannelNodeInfos[channelName].RwNodes = channelUsedNodes.Collect()
		}
	}
}

func (replica *mutableReplica) tryBalanceNodeForChannel() {
	channelNodeInfos := replica.replicaPB.GetChannelNodeInfos()
	if len(channelNodeInfos) == 0 {
		return
	}

	balancePolicy := paramtable.Get().QueryCoordCfg.Balancer.GetValue()
	enableChannelExclusiveMode := balancePolicy == ChannelLevelScoreBalancerName
	channelExclusiveFactor := paramtable.Get().QueryCoordCfg.ChannelExclusiveNodeFactor.GetAsInt()
	// if balance policy or node count doesn't match condition, clean up channel node info
	if !enableChannelExclusiveMode || len(replica.rwNodes) < len(channelNodeInfos)*channelExclusiveFactor {
		for name := range replica.replicaPB.GetChannelNodeInfos() {
			replica.replicaPB.ChannelNodeInfos[name] = &querypb.ChannelNodeInfo{}
		}
		return
	}

	if channelNodeInfos != nil {
		average := replica.RWNodesCount() / len(channelNodeInfos)

		// release node in channel
		for channelName, channelNodeInfo := range channelNodeInfos {
			currentNodes := channelNodeInfo.GetRwNodes()
			if len(currentNodes) > average {
				replica.replicaPB.ChannelNodeInfos[channelName].RwNodes = currentNodes[:average]
				for _, nodeID := range currentNodes[average:] {
					delete(replica.exclusiveRWNodeToChannel, nodeID)
				}
			}
		}

		// acquire node in channel
		for channelName, channelNodeInfo := range channelNodeInfos {
			currentNodes := channelNodeInfo.GetRwNodes()
			if len(currentNodes) < average {
				for _, nodeID := range replica.rwNodes.Collect() {
					if _, ok := replica.exclusiveRWNodeToChannel[nodeID]; !ok {
						currentNodes = append(currentNodes, nodeID)
						replica.exclusiveRWNodeToChannel[nodeID] = channelName
						if len(currentNodes) == average {
							break
						}
					}
				}
				replica.replicaPB.ChannelNodeInfos[channelName].RwNodes = currentNodes
			}
		}
	}
}

// IntoReplica returns the immutable replica, After calling this method, the mutable replica should not be used again.
func (replica *mutableReplica) IntoReplica() *Replica {
	r := replica.Replica
	replica.Replica = nil
	return r
}
