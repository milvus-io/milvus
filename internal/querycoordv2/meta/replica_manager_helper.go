package meta

import (
	"sort"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// collectionAssignmentHelper is a helper to manage the replica assignment in same collection.
type collectionAssignmentHelper struct {
	collectionID            typeutil.UniqueID
	resourceGroupToReplicas map[string]*replicasInSameRGAssignmentHelper
}

// newCollectionAssignmentHelper creates a new collectionAssignmentHelper.
func newCollectionAssignmentHelper(
	collectionID typeutil.UniqueID,
	rgToReplicas map[string][]*Replica,
	rgs map[string]typeutil.UniqueSet,
) *collectionAssignmentHelper {
	resourceGroupToReplicas := make(map[string]*replicasInSameRGAssignmentHelper)
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
		newIncomingNodes := helper.nodesInRG.Clone()
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
func (h *collectionAssignmentHelper) RangeOverResourceGroup(f func(helper *replicasInSameRGAssignmentHelper)) {
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
func newReplicaAssignmentHelper(rgName string, replicas []*Replica, nodeInRG typeutil.UniqueSet) *replicasInSameRGAssignmentHelper {
	assignmentInfos := make([]*replicaAssignmentInfo, 0, len(replicas))
	for _, replica := range replicas {
		assignmentInfos = append(assignmentInfos, newReplicaAssignmentInfo(replica, nodeInRG))
	}
	h := &replicasInSameRGAssignmentHelper{
		rgName:    rgName,
		nodesInRG: nodeInRG,
		replicas:  assignmentInfos,
	}
	return h
}

// replicasInSameRGAssignmentHelper is a helper to manage the replica assignment in same rg.
type replicasInSameRGAssignmentHelper struct {
	rgName        string
	nodesInRG     typeutil.UniqueSet
	incomingNodes typeutil.UniqueSet // nodes that not used by current replicas in resource group.
	replicas      []*replicaAssignmentInfo
}

func (h *replicasInSameRGAssignmentHelper) AllocateIncomingNodes(n int) []int64 {
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
func (h *replicasInSameRGAssignmentHelper) RangeOverReplicas(f func(*replicaAssignmentInfo)) {
	for _, info := range h.replicas {
		f(info)
	}
}

// updateExpectedNodeCountForReplicas updates the expected node count for all replicas in same resource group.
func (h *replicasInSameRGAssignmentHelper) updateExpectedNodeCountForReplicas(currentUsageNodesCount int) {
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
	rwNodes := make(typeutil.UniqueSet, replica.RWNodesCount())
	newRONodes := make(typeutil.UniqueSet, replica.RONodesCount())
	unrecoverableRONodes := make(typeutil.UniqueSet, replica.RONodesCount())
	recoverableRONodes := make(typeutil.UniqueSet, replica.RONodesCount())

	replica.RangeOverRWNodes(func(nodeID int64) bool {
		if nodeInRG.Contain(nodeID) {
			rwNodes.Insert(nodeID)
		} else {
			newRONodes.Insert(nodeID)
		}
		return true
	})

	replica.RangeOverRONodes(func(nodeID int64) bool {
		if nodeInRG.Contain(nodeID) {
			recoverableRONodes.Insert(nodeID)
		} else {
			unrecoverableRONodes.Insert(nodeID)
		}
		return true
	})
	return &replicaAssignmentInfo{
		replicaID:            replica.GetID(),
		expectedNodeCount:    0,
		rwNodes:              rwNodes,
		newRONodes:           newRONodes,
		recoverableRONodes:   recoverableRONodes,
		unrecoverableRONodes: unrecoverableRONodes,
	}
}

type replicaAssignmentInfo struct {
	replicaID            typeutil.UniqueID
	expectedNodeCount    int                // expected node count for each replica.
	rwNodes              typeutil.UniqueSet // rw nodes is used by current replica. (rw -> rw)
	newRONodes           typeutil.UniqueSet // new ro nodes for these replica. (rw -> ro)
	recoverableRONodes   typeutil.UniqueSet // recoverable ro nodes for these replica (ro node can be put back to rw node if it's in current resource group). (may ro -> rw)
	unrecoverableRONodes typeutil.UniqueSet // unrecoverable ro nodes for these replica (ro node can't be put back to rw node if it's not in current resource group). (ro -> ro)
}

// GetReplicaID returns the replica id for these replica.
func (s *replicaAssignmentInfo) GetReplicaID() typeutil.UniqueID {
	return s.replicaID
}

// GetNewRONodes returns the new ro nodes for these replica.
func (s *replicaAssignmentInfo) GetNewRONodes() []int64 {
	newRONodes := make([]int64, 0, s.newRONodes.Len())
	// not in current resource group must be set ro.
	for nodeID := range s.newRONodes {
		newRONodes = append(newRONodes, nodeID)
	}

	// too much node is occupied by current replica, then set some node to ro.
	if s.rwNodes.Len() > s.expectedNodeCount {
		cnt := s.rwNodes.Len() - s.expectedNodeCount
		s.rwNodes.Range(func(node int64) bool {
			if cnt > 0 {
				newRONodes = append(newRONodes, node)
				cnt--
			} else {
				return false
			}
			return true
		})
	}
	return newRONodes
}

// GetRecoverNodesAndIncomingNodeCount returns the recoverable ro nodes and incoming node count for these replica.
func (s *replicaAssignmentInfo) GetRecoverNodesAndIncomingNodeCount() (recoverNodes []int64, incomingNodeCount int) {
	recoverNodes = make([]int64, 0, s.recoverableRONodes.Len())
	incomingNodeCount = 0
	if s.rwNodes.Len() < s.expectedNodeCount {
		incomingNodeCount = s.expectedNodeCount - s.rwNodes.Len()
		s.recoverableRONodes.Range(func(node int64) bool {
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
	s.rwNodes.Range(ff)
	s.newRONodes.Range(ff)
	s.recoverableRONodes.Range(ff)
	s.unrecoverableRONodes.Range(ff)
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
	left := s.replicaAssignmentInfoSorter[i].rwNodes.Len() + s.replicaAssignmentInfoSorter[i].recoverableRONodes.Len()
	right := s.replicaAssignmentInfoSorter[j].rwNodes.Len() + s.replicaAssignmentInfoSorter[j].recoverableRONodes.Len()

	// Reach stable sort result by replica id.
	// Otherwise unstable assignment may cause unnecessary node transfer.
	return left < right || (left == right && s.replicaAssignmentInfoSorter[i].replicaID < s.replicaAssignmentInfoSorter[j].replicaID)
}
