package meta

import (
	"sort"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// collectionAssignmentHelper is a helper to manage the replica assignment in same collection.
type collectionAssignmentHelper struct {
	collectionID            typeutil.UniqueID
	resourceGroupToReplicas map[string]*replicaAssignmentHelper
}

// newCollectionAssignmentHelper creates a new collectionAssignmentHelper.
func newCollectionAssignmentHelper(
	collectionID typeutil.UniqueID,
	rgToReplicas map[string][]*Replica,
	rgs map[string]typeutil.UniqueSet,
) *collectionAssignmentHelper {
	resourceGroupToReplicas := make(map[string]*replicaAssignmentHelper)
	for rgName, replicas := range rgToReplicas {
		resourceGroupToReplicas[rgName] = newReplicaAssignmentHelper(rgName, replicas, rgs[rgName])
	}

	helper := &collectionAssignmentHelper{
		collectionID:            collectionID,
		resourceGroupToReplicas: resourceGroupToReplicas,
	}
	helper.updateIncomingNodesAndExpectedNode()
	return helper
}

// updateIncomingNodesAndExpectedNode updates the incoming nodes for all resource groups.
// An incoming node is a node that not used by current collection but in resource group.
func (h *collectionAssignmentHelper) updateIncomingNodesAndExpectedNode() {
	// incoming nodes should be compared with all node of replica in same collection, even not in same resource group.
	for _, helper := range h.resourceGroupToReplicas {
		// some node in current resource group may load other replica data of same collection in other resource group.
		// those node cannot be used right now.
		newIncomingNodes := helper.allAvailableNodes.Clone()
		currentUsedNodeCount := newIncomingNodes.Len()
		h.RangeOverReplicas(func(rgName string, assignment *replicaAssignmentInfo) {
			assignment.RangeOverAllNodes(func(nodeID int64) {
				if newIncomingNodes.Contain(nodeID) {
					newIncomingNodes.Remove(nodeID)
					if rgName != helper.rgName {
						// Node is still used by other replica of same collection in other resource group, cannot be used right now.
						// filter it out to calculate the expected node count to avoid node starve of some replica in same resource group.
						currentUsedNodeCount--
					}
				}
			})
		})
		helper.incomingNodes = newIncomingNodes
		helper.updateExpectedNodeCountForReplicas(currentUsedNodeCount)
	}
}

// RangeOverResourceGroup iterate resource groups
func (h *collectionAssignmentHelper) RangeOverResourceGroup(f func(helper *replicaAssignmentHelper)) {
	for _, helper := range h.resourceGroupToReplicas {
		f(helper)
	}
}

// RangeOverReplicas iterate replicas
func (h *collectionAssignmentHelper) RangeOverReplicas(f func(rgName string, assignment *replicaAssignmentInfo)) {
	for _, helper := range h.resourceGroupToReplicas {
		for _, assignment := range helper.replicas {
			f(helper.rgName, assignment)
		}
	}
}

// newReplicaAssignmentHelper creates a new replicaAssignmentHelper.
func newReplicaAssignmentHelper(rgName string, replicas []*Replica, nodeInRG typeutil.UniqueSet) *replicaAssignmentHelper {
	assignmentInfos := make([]*replicaAssignmentInfo, 0, len(replicas))
	for _, replica := range replicas {
		assignmentInfos = append(assignmentInfos, newReplicaAssignmentInfo(replica, nodeInRG))
	}
	h := &replicaAssignmentHelper{
		rgName:            rgName,
		allAvailableNodes: nodeInRG,
		replicas:          assignmentInfos,
	}
	return h
}

// replicaAssignmentHelper is a helper to manage the replica assignment in same rg.
type replicaAssignmentHelper struct {
	rgName            string
	allAvailableNodes typeutil.UniqueSet
	incomingNodes     typeutil.UniqueSet
	replicas          []*replicaAssignmentInfo
}

func (h *replicaAssignmentHelper) AllocateIncomingNodes(n int) []int64 {
	nodeIDs := make([]int64, 0, n)
	h.incomingNodes.Range(func(nodeID int64) bool {
		if n > 0 {
			nodeIDs = append(nodeIDs, nodeID)
			n--
		} else {
			return false
		}
		return true
	})
	h.incomingNodes.Remove(nodeIDs...)
	return nodeIDs
}

// RangeOverReplicas iterate replicas.
func (h *replicaAssignmentHelper) RangeOverReplicas(f func(*replicaAssignmentInfo)) {
	for _, info := range h.replicas {
		f(info)
	}
}

// updateExpectedNodeCountForReplicas updates the expected node count for all replicas in same resource group.
func (h *replicaAssignmentHelper) updateExpectedNodeCountForReplicas(currentUsageNodesCount int) {
	minimumNodeCount := currentUsageNodesCount / len(h.replicas)
	maximumNodeCount := minimumNodeCount
	remainder := currentUsageNodesCount % len(h.replicas)
	if remainder > 0 {
		maximumNodeCount += 1
	}

	// rule:
	// 1. make minimumNodeCount <= expectedNodeCount <= maximumNodeCount
	// 2. expectedNodeCount should be closed to len(assignedNodes) for each replica as much as possible to avoid unnecessary node transfer.
	sorter := make(replicaAssignmentInfoSorter, 0, len(h.replicas))
	for _, info := range h.replicas {
		sorter = append(sorter, info)
	}
	sort.Sort(sort.Reverse(replicaAssignmentInfoSortByAvailableAndRecoverable{sorter}))
	for _, info := range sorter {
		if remainder > 0 {
			info.expectedNodeCount = maximumNodeCount
			remainder--
		} else {
			info.expectedNodeCount = minimumNodeCount
		}
	}
}

// newReplicaAssignmentInfo creates a new replicaAssignmentInfo.
func newReplicaAssignmentInfo(replica *Replica, nodeInRG typeutil.UniqueSet) *replicaAssignmentInfo {
	// node in replica can be split into 3 part.
	availableNodes := make(typeutil.UniqueSet, replica.AvailableNodesCount())
	newOutBoundNodes := make(typeutil.UniqueSet, replica.OutboundNodesCount())
	unrecoverableOutboundNodes := make(typeutil.UniqueSet, replica.OutboundNodesCount())
	recoverableOutboundNodes := make(typeutil.UniqueSet, replica.OutboundNodesCount())

	replica.RangeOverAvailableNodes(func(nodeID int64) bool {
		if nodeInRG.Contain(nodeID) {
			availableNodes.Insert(nodeID)
		} else {
			newOutBoundNodes.Insert(nodeID)
		}
		return true
	})

	replica.RangeOverOutboundNodes(func(nodeID int64) bool {
		if nodeInRG.Contain(nodeID) {
			recoverableOutboundNodes.Insert(nodeID)
		} else {
			unrecoverableOutboundNodes.Insert(nodeID)
		}
		return true
	})
	return &replicaAssignmentInfo{
		replicaID:                  replica.GetID(),
		expectedNodeCount:          0,
		availableNodes:             availableNodes,
		newOutBoundNodes:           newOutBoundNodes,
		recoverableOutboundNodes:   recoverableOutboundNodes,
		unrecoverableOutboundNodes: unrecoverableOutboundNodes,
	}
}

type replicaAssignmentInfo struct {
	replicaID                  typeutil.UniqueID
	expectedNodeCount          int                // expected node count for each replica.
	availableNodes             typeutil.UniqueSet // nodes is used by current replica and not outbound at current resource group.
	newOutBoundNodes           typeutil.UniqueSet // new outbound nodes for these replica.
	recoverableOutboundNodes   typeutil.UniqueSet // recoverable outbound nodes for these replica (outbound node can be put back to available node if it's in current resource group).
	unrecoverableOutboundNodes typeutil.UniqueSet // unrecoverable outbound nodes for these replica (outbound node can't be put back to available node if it's not in current resource group).
}

// GetReplicaID returns the replica id for these replica.
func (s *replicaAssignmentInfo) GetReplicaID() typeutil.UniqueID {
	return s.replicaID
}

// GetNewOutboundNodes returns the new outbound nodes for these replica.
func (s *replicaAssignmentInfo) GetNewOutboundNodes() []int64 {
	newOutBoundNodes := make([]int64, 0, s.newOutBoundNodes.Len())
	// not in current resource group must be set outbound.
	for nodeID := range s.newOutBoundNodes {
		newOutBoundNodes = append(newOutBoundNodes, nodeID)
	}

	// too much node is occupied by current replica, then set some node to outbound.
	if s.availableNodes.Len() > s.expectedNodeCount {
		cnt := s.availableNodes.Len() - s.expectedNodeCount
		s.availableNodes.Range(func(node int64) bool {
			if cnt > 0 {
				newOutBoundNodes = append(newOutBoundNodes, node)
				cnt--
			} else {
				return false
			}
			return true
		})
	}
	return newOutBoundNodes
}

// GetRecoverNodesAndIncomingNodeCount returns the recoverable outbound nodes and incoming node count for these replica.
func (s *replicaAssignmentInfo) GetRecoverNodesAndIncomingNodeCount() (recoverNodes []int64, incomingNodeCount int) {
	recoverNodes = make([]int64, 0, s.recoverableOutboundNodes.Len())
	incomingNodeCount = 0
	if s.availableNodes.Len() < s.expectedNodeCount {
		incomingNodeCount = s.expectedNodeCount - s.availableNodes.Len()
		s.recoverableOutboundNodes.Range(func(node int64) bool {
			if incomingNodeCount > 0 {
				recoverNodes = append(recoverNodes, node)
				incomingNodeCount--
			} else {
				return false
			}
			return true
		})
	}
	return recoverNodes, incomingNodeCount
}

// RangeOverAllNodes iterate all nodes in replica.
func (s *replicaAssignmentInfo) RangeOverAllNodes(f func(nodeID int64)) {
	ff := func(nodeID int64) bool {
		f(nodeID)
		return true
	}
	s.availableNodes.Range(ff)
	s.newOutBoundNodes.Range(ff)
	s.recoverableOutboundNodes.Range(ff)
	s.unrecoverableOutboundNodes.Range(ff)
}

type replicaAssignmentInfoSorter []*replicaAssignmentInfo

func (s replicaAssignmentInfoSorter) Len() int {
	return len(s)
}

func (s replicaAssignmentInfoSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type replicaAssignmentInfoSortByAvailableAndRecoverable struct {
	replicaAssignmentInfoSorter
}

func (s replicaAssignmentInfoSortByAvailableAndRecoverable) Less(i, j int) bool {
	left := s.replicaAssignmentInfoSorter[i].availableNodes.Len() + s.replicaAssignmentInfoSorter[i].recoverableOutboundNodes.Len()
	right := s.replicaAssignmentInfoSorter[j].availableNodes.Len() + s.replicaAssignmentInfoSorter[j].recoverableOutboundNodes.Len()

	// Reach stable sort result by replica id.
	// Otherwise unstable assignment may cause unnecessary node transfer.
	return left < right || (left == right && s.replicaAssignmentInfoSorter[i].replicaID < s.replicaAssignmentInfoSorter[j].replicaID)
}
