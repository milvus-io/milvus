package meta

import (
	"sort"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ReplicaInterface defines read operations for replica metadata
type ReplicaInterface interface {
	// Basic information
	GetID() typeutil.UniqueID
	GetCollectionID() typeutil.UniqueID
	GetResourceGroup() string

	// Node access
	GetNodes() []int64
	GetRONodes() []int64
	GetRWNodes() []int64
	GetROSQNodes() []int64
	GetRWSQNodes() []int64

	// Node iteration
	RangeOverRWNodes(f func(node int64) bool)
	RangeOverRONodes(f func(node int64) bool)
	RangeOverRWSQNodes(f func(node int64) bool)
	RangeOverROSQNodes(f func(node int64) bool)

	// Node counting
	RWNodesCount() int
	RONodesCount() int
	RWSQNodesCount() int
	ROSQNodesCount() int
	NodesCount() int

	// Node existence checks
	Contains(node int64) bool
	ContainRONode(node int64) bool
	ContainRWNode(node int64) bool
	ContainSQNode(node int64) bool
	ContainROSQNode(node int64) bool
	ContainRWSQNode(node int64) bool
}

// NilReplica is used to represent a nil replica.
var NilReplica = newReplica(&querypb.Replica{
	ID: -1,
})

// Replica is a immutable type for manipulating replica meta info for replica manager.
// Performed a copy-on-write strategy to keep the consistency of the replica manager.
// So only read only operations are allowed on these type.
type Replica struct {
	replicaPB *querypb.Replica
	// Nodes is the legacy querynode that is not embedded in the streamingnode, which can only load sealed segment.
	rwNodes typeutil.UniqueSet // a helper field for manipulating replica's Available Nodes slice field.
	// always keep consistent with replicaPB.Nodes.
	// mutual exclusive with roNodes.
	roNodes typeutil.UniqueSet // a helper field for manipulating replica's RO Nodes slice field.
	// always keep consistent with replicaPB.RoNodes.
	// node used by replica but cannot add segment on it.
	// include rebalance node or node out of resource group.

	// SQNodes is the querynode that is embedded in the streamingnode, which can only watch channel and load growing segment.
	rwSQNodes typeutil.UniqueSet // a helper field for manipulating replica's RW SQ Nodes slice field.
	// always keep consistent with replicaPB.RwSqNodes.
	// mutable exclusive with roSQNodes.
	roSQNodes typeutil.UniqueSet // a helper field for manipulating replica's RO SQ Nodes slice field.
	// always keep consistent with replicaPB.RoSqNodes.
	// node used by replica but cannot add more channel on it.
	// include the rebalance node.
	loadPriority commonpb.LoadPriority
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
		replicaPB:    proto.Clone(replica).(*querypb.Replica),
		rwNodes:      typeutil.NewUniqueSet(replica.Nodes...),
		roNodes:      typeutil.NewUniqueSet(replica.RoNodes...),
		rwSQNodes:    typeutil.NewUniqueSet(replica.RwSqNodes...),
		roSQNodes:    typeutil.NewUniqueSet(replica.RoSqNodes...),
		loadPriority: commonpb.LoadPriority_HIGH,
	}
}

func NewReplicaWithPriority(replica *querypb.Replica, priority commonpb.LoadPriority) *Replica {
	return &Replica{
		replicaPB:    proto.Clone(replica).(*querypb.Replica),
		rwNodes:      typeutil.NewUniqueSet(replica.Nodes...),
		roNodes:      typeutil.NewUniqueSet(replica.RoNodes...),
		rwSQNodes:    typeutil.NewUniqueSet(replica.RwSqNodes...),
		roSQNodes:    typeutil.NewUniqueSet(replica.RoSqNodes...),
		loadPriority: priority,
	}
}

func (replica *Replica) LoadPriority() commonpb.LoadPriority {
	return replica.loadPriority
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
	nodes := typeutil.NewUniqueSet()
	nodes.Insert(replica.replicaPB.GetRoNodes()...)
	nodes.Insert(replica.replicaPB.GetNodes()...)
	nodes.Insert(replica.replicaPB.GetRwSqNodes()...)
	nodes.Insert(replica.replicaPB.GetRoSqNodes()...)
	return nodes.Collect()
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

// GetROSQNodes returns the ro sq nodes of the replica.
// readonly, don't modify the returned slice.
func (replica *Replica) GetROSQNodes() []int64 {
	return replica.replicaPB.GetRoSqNodes()
}

// GetRWSQNodes returns the rw sq nodes of the replica.
// readonly, don't modify the returned slice.
func (replica *Replica) GetRWSQNodes() []int64 {
	return replica.replicaPB.GetRwSqNodes()
}

// RangeOverRWNodes iterates over the read and write nodes of the replica.
func (replica *Replica) RangeOverRWNodes(f func(node int64) bool) {
	replica.rwNodes.Range(f)
}

// RangeOverRONodes iterates over the ro nodes of the replica.
func (replica *Replica) RangeOverRONodes(f func(node int64) bool) {
	replica.roNodes.Range(f)
}

// RangeOverRWSQNodes iterates over the read and write streaming query nodes of the replica.
func (replica *Replica) RangeOverRWSQNodes(f func(node int64) bool) {
	replica.rwSQNodes.Range(f)
}

// RangeOverROSQNodes iterates over the ro streaming query nodes of the replica.
func (replica *Replica) RangeOverROSQNodes(f func(node int64) bool) {
	replica.roSQNodes.Range(f)
}

// RWNodesCount returns the count of rw nodes of the replica.
func (replica *Replica) RWNodesCount() int {
	return replica.rwNodes.Len()
}

// RONodesCount returns the count of ro nodes of the replica.
func (replica *Replica) RONodesCount() int {
	return replica.roNodes.Len()
}

// RWSQNodesCount returns the count of rw nodes of the replica.
func (replica *Replica) RWSQNodesCount() int {
	return replica.rwSQNodes.Len()
}

// ROSQNodesCount returns the count of ro nodes of the replica.
func (replica *Replica) ROSQNodesCount() int {
	return replica.roSQNodes.Len()
}

// NodesCount returns the count of rw nodes and ro nodes of the replica.
func (replica *Replica) NodesCount() int {
	return replica.rwNodes.Len() + replica.roNodes.Len()
}

// Contains checks if the node is in rw nodes of the replica.
func (replica *Replica) Contains(node int64) bool {
	return replica.ContainRONode(node) || replica.ContainRWNode(node) || replica.ContainSQNode(node)
}

// ContainRONode checks if the node is in ro nodes of the replica.
func (replica *Replica) ContainRONode(node int64) bool {
	return replica.roNodes.Contain(node)
}

// ContainRONode checks if the node is in ro nodes of the replica.
func (replica *Replica) ContainRWNode(node int64) bool {
	return replica.rwNodes.Contain(node)
}

// ContainSQNode checks if the node is in rw sq nodes of the replica.
func (replica *Replica) ContainSQNode(node int64) bool {
	return replica.ContainROSQNode(node) || replica.ContainRWSQNode(node)
}

// ContainRWSQNode checks if the node is in rw sq nodes of the replica.
func (replica *Replica) ContainROSQNode(node int64) bool {
	return replica.roSQNodes.Contain(node)
}

// ContainRWSQNode checks if the node is in rw sq nodes of the replica.
func (replica *Replica) ContainRWSQNode(node int64) bool {
	return replica.rwSQNodes.Contain(node)
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
			replicaPB:    proto.Clone(replica.replicaPB).(*querypb.Replica),
			rwNodes:      typeutil.NewUniqueSet(replica.replicaPB.Nodes...),
			roNodes:      typeutil.NewUniqueSet(replica.replicaPB.RoNodes...),
			rwSQNodes:    typeutil.NewUniqueSet(replica.replicaPB.RwSqNodes...),
			roSQNodes:    typeutil.NewUniqueSet(replica.replicaPB.RoSqNodes...),
			loadPriority: replica.LoadPriority(),
		},
		exclusiveRWNodeToChannel: exclusiveRWNodeToChannel,
	}
}

func (replica *Replica) IsChannelExclusiveModeEnabled() bool {
	return replica.replicaPB.ChannelNodeInfos != nil && len(replica.replicaPB.ChannelNodeInfos) > 0
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

// AddRWSQNode adds the node to rw sq nodes of the replica.
func (replica *mutableReplica) AddRWSQNode(nodes ...int64) {
	replica.roSQNodes.Remove(nodes...)
	replica.replicaPB.RoSqNodes = replica.roSQNodes.Collect()
	replica.rwSQNodes.Insert(nodes...)
	replica.replicaPB.RwSqNodes = replica.rwSQNodes.Collect()
}

// AddROSQNode add the node to ro sq nodes of the replica.
func (replica *mutableReplica) AddROSQNode(nodes ...int64) {
	replica.rwSQNodes.Remove(nodes...)
	replica.replicaPB.RwSqNodes = replica.rwSQNodes.Collect()
	replica.roSQNodes.Insert(nodes...)
	replica.replicaPB.RoSqNodes = replica.roSQNodes.Collect()
}

// RemoveSQNode removes the node from rw sq nodes and ro sq nodes of the replica.
func (replica *mutableReplica) RemoveSQNode(nodes ...int64) {
	replica.rwSQNodes.Remove(nodes...)
	replica.replicaPB.RwSqNodes = replica.rwSQNodes.Collect()
	replica.roSQNodes.Remove(nodes...)
	replica.replicaPB.RoSqNodes = replica.roSQNodes.Collect()
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

func (replica *mutableReplica) TryEnableChannelExclusiveMode(channelNames ...string) {
	if replica.replicaPB.ChannelNodeInfos == nil {
		replica.replicaPB.ChannelNodeInfos = make(map[string]*querypb.ChannelNodeInfo)
		for _, channelName := range channelNames {
			replica.replicaPB.ChannelNodeInfos[channelName] = &querypb.ChannelNodeInfo{}
		}
	}
	if replica.exclusiveRWNodeToChannel == nil {
		replica.exclusiveRWNodeToChannel = make(map[int64]string)
	}
}

func (replica *mutableReplica) DisableChannelExclusiveMode() {
	if replica.replicaPB.ChannelNodeInfos != nil {
		channelNodeInfos := make(map[string]*querypb.ChannelNodeInfo)
		for channelName := range replica.replicaPB.ChannelNodeInfos {
			channelNodeInfos[channelName] = &querypb.ChannelNodeInfo{}
		}
		replica.replicaPB.ChannelNodeInfos = channelNodeInfos
	}
	replica.exclusiveRWNodeToChannel = make(map[int64]string)
}

// tryBalanceNodeForChannel attempts to balance nodes across channels using an improved algorithm
func (replica *mutableReplica) tryBalanceNodeForChannel() {
	channelNodeInfos := replica.replicaPB.GetChannelNodeInfos()
	if len(channelNodeInfos) == 0 {
		return
	}

	// Check if channel exclusive mode should be enabled
	if !replica.shouldEnableChannelExclusiveMode(channelNodeInfos) {
		replica.DisableChannelExclusiveMode()
		return
	}

	// Calculate optimal node assignments
	targetAssignments := replica.calculateOptimalAssignments(channelNodeInfos)

	// Apply the rebalancing with minimal node movement
	replica.rebalanceChannelNodes(channelNodeInfos, targetAssignments)
}

// shouldEnableChannelExclusiveMode determines if channel exclusive mode should be enabled
func (replica *mutableReplica) shouldEnableChannelExclusiveMode(channelInfos map[string]*querypb.ChannelNodeInfo) bool {
	balancePolicy := paramtable.Get().QueryCoordCfg.Balancer.GetValue()
	channelExclusiveFactor := paramtable.Get().QueryCoordCfg.ChannelExclusiveNodeFactor.GetAsInt()
	return balancePolicy == ChannelLevelScoreBalancerName &&
		replica.RWNodesCount() >= len(channelInfos)*channelExclusiveFactor
}

// calculateOptimalAssignments calculates the optimal node count for each channel
func (replica *mutableReplica) calculateOptimalAssignments(channelInfos map[string]*querypb.ChannelNodeInfo) map[string]int {
	channelCount := len(channelInfos)
	totalNodes := replica.RWNodesCount()

	// Get channels sorted by current node count (descending)
	sortedChannels := replica.getSortedChannelsByNodeCount(channelInfos)
	// Calculate base assignment: average nodes per channel
	assignments := make(map[string]int, channelCount)
	baseNodes := totalNodes / channelCount
	extraNodes := totalNodes % channelCount

	// Distribute extra nodes to channels with fewer current nodes first
	for i, channel := range sortedChannels {
		nodeCount := baseNodes
		if i < extraNodes {
			nodeCount++
		}
		assignments[channel] = nodeCount
	}

	return assignments
}

// getSortedChannelsByNodeCount returns channels sorted by current node count (descending)
func (replica *mutableReplica) getSortedChannelsByNodeCount(channelInfos map[string]*querypb.ChannelNodeInfo) []string {
	// channelNodeAssignment represents a channel's node assignment
	type channelNodeAssignment struct {
		name  string
		nodes []int64
	}

	assignments := make([]channelNodeAssignment, 0, len(channelInfos))
	for name, channelNodeInfo := range channelInfos {
		assignments = append(assignments, channelNodeAssignment{
			name:  name,
			nodes: channelNodeInfo.GetRwNodes(),
		})
	}

	// Sort by node count (descending) to prioritize channels with more nodes for reduction
	sort.Slice(assignments, func(i, j int) bool {
		return len(assignments[i].nodes) > len(assignments[j].nodes)
	})

	channels := make([]string, len(assignments))
	for i, assignment := range assignments {
		channels[i] = assignment.name
	}

	return channels
}

// rebalanceChannelNodes performs the actual node rebalancing
func (replica *mutableReplica) rebalanceChannelNodes(channelInfos map[string]*querypb.ChannelNodeInfo, targetAssignments map[string]int) {
	// Phase 1: Release excess nodes from over-allocated channels
	replica.releaseExcessNodes(channelInfos, targetAssignments)

	// Phase 2: Allocate nodes to under-allocated channels
	replica.allocateInsufficientNodes(channelInfos, targetAssignments)
}

// releaseExcessNodes releases nodes from channels that have more than their target allocation
func (replica *mutableReplica) releaseExcessNodes(channelInfos map[string]*querypb.ChannelNodeInfo, targetAssignments map[string]int) {
	for channelName, channelNodeInfo := range channelInfos {
		currentNodes := channelNodeInfo.GetRwNodes()
		targetCount := targetAssignments[channelName]

		if len(currentNodes) > targetCount {
			// Keep the first targetCount nodes, release the rest
			replica.replicaPB.ChannelNodeInfos[channelName].RwNodes = currentNodes[:targetCount]

			// Remove released nodes from the exclusive mapping
			for _, nodeID := range currentNodes[targetCount:] {
				delete(replica.exclusiveRWNodeToChannel, nodeID)
			}
		}
	}
}

// allocateInsufficientNodes allocates nodes to channels that need more nodes
func (replica *mutableReplica) allocateInsufficientNodes(channelInfos map[string]*querypb.ChannelNodeInfo, targetAssignments map[string]int) {
	// Get available nodes (not exclusively assigned to any channel)
	availableNodes := replica.getAvailableNodes()

	for channelName, channelNodeInfo := range channelInfos {
		currentNodes := channelNodeInfo.GetRwNodes()
		targetCount := targetAssignments[channelName]

		if len(currentNodes) < targetCount {
			neededCount := targetCount - len(currentNodes)
			allocatedNodes := replica.allocateNodesFromPool(availableNodes, neededCount, channelName)

			// Update channel's node list
			updatedNodes := make([]int64, 0, len(currentNodes)+len(allocatedNodes))
			updatedNodes = append(updatedNodes, currentNodes...)
			updatedNodes = append(updatedNodes, allocatedNodes...)
			replica.replicaPB.ChannelNodeInfos[channelName].RwNodes = updatedNodes
		}
		log.Info("channel exclusive node list",
			zap.String("channelName", channelName),
			zap.Int64s("nodes", replica.replicaPB.ChannelNodeInfos[channelName].RwNodes))
	}
}

// getAvailableNodes returns nodes that are not exclusively assigned to any channel
func (replica *mutableReplica) getAvailableNodes() []int64 {
	allNodes := replica.rwNodes.Collect()
	availableNodes := make([]int64, 0, len(allNodes))

	for _, nodeID := range allNodes {
		if _, isExclusive := replica.exclusiveRWNodeToChannel[nodeID]; !isExclusive {
			availableNodes = append(availableNodes, nodeID)
		}
	}

	return availableNodes
}

// allocateNodesFromPool allocates nodes from the available pool to a channel
func (replica *mutableReplica) allocateNodesFromPool(availableNodes []int64, neededCount int, channelName string) []int64 {
	allocatedCount := 0
	allocatedNodes := make([]int64, 0, neededCount)

	for _, nodeID := range availableNodes {
		if allocatedCount >= neededCount {
			break
		}

		// Check if node is still available (not assigned since we got the list)
		if _, isExclusive := replica.exclusiveRWNodeToChannel[nodeID]; !isExclusive {
			allocatedNodes = append(allocatedNodes, nodeID)
			replica.exclusiveRWNodeToChannel[nodeID] = channelName
			allocatedCount++
		}
	}

	return allocatedNodes
}

// IntoReplica returns the immutable replica, After calling this method, the mutable replica should not be used again.
func (replica *mutableReplica) IntoReplica() *Replica {
	r := replica.Replica
	replica.Replica = nil
	return r
}
