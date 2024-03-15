package meta

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type expectedReplicaPlan struct {
	newOutboundNodes  int
	recoverNodes      int
	incomingNodeCount int
	expectedNodeCount int
}
type testCase struct {
	collectionID             typeutil.UniqueID                         // collection id
	rgToReplicas             map[string][]*Replica                     // from resource group to replicas
	rgs                      map[string]typeutil.UniqueSet             // from resource group to nodes
	expectedPlan             map[typeutil.UniqueID]expectedReplicaPlan // from replica id to expected plan
	expectedNewIncomingNodes map[string]typeutil.UniqueSet             // from resource group to incoming nodes
}

type CollectionAssignmentHelperSuite struct {
	suite.Suite
}

func (s *CollectionAssignmentHelperSuite) TestNoModificationCase() {
	s.runCase(testCase{
		collectionID: 1,
		rgToReplicas: map[string][]*Replica{
			"rg1": {
				newReplica(&querypb.Replica{
					ID:            1,
					CollectionID:  1,
					Nodes:         []int64{1, 2, 3, 4},
					OutboundNodes: []int64{},
				}),
			},
			"rg2": {
				newReplica(&querypb.Replica{
					ID:            2,
					CollectionID:  1,
					Nodes:         []int64{5, 6},
					OutboundNodes: []int64{},
				}),
				newReplica(&querypb.Replica{
					ID:            3,
					CollectionID:  1,
					Nodes:         []int64{7, 8},
					OutboundNodes: []int64{},
				}),
			},
		},
		rgs: map[string]typeutil.UniqueSet{
			"rg1": typeutil.NewUniqueSet(1, 2, 3, 4),
			"rg2": typeutil.NewUniqueSet(5, 6, 7, 8),
		},
		expectedPlan: map[typeutil.UniqueID]expectedReplicaPlan{
			1: {
				newOutboundNodes:  0,
				recoverNodes:      0,
				incomingNodeCount: 0,
				expectedNodeCount: 4,
			},
			2: {
				newOutboundNodes:  0,
				recoverNodes:      0,
				incomingNodeCount: 0,
				expectedNodeCount: 2,
			},
			3: {
				newOutboundNodes:  0,
				recoverNodes:      0,
				incomingNodeCount: 0,
				expectedNodeCount: 2,
			},
		},
		expectedNewIncomingNodes: map[string]typeutil.UniqueSet{
			"rg1": typeutil.NewUniqueSet(),
			"rg2": typeutil.NewUniqueSet(),
		},
	})

	s.runCase(testCase{
		collectionID: 1,
		rgToReplicas: map[string][]*Replica{
			"rg1": {
				newReplica(&querypb.Replica{
					ID:            1,
					CollectionID:  1,
					Nodes:         []int64{1, 2, 3, 4},
					OutboundNodes: []int64{},
				}),
			},
			"rg2": {
				newReplica(&querypb.Replica{
					ID:            2,
					CollectionID:  1,
					Nodes:         []int64{5},
					OutboundNodes: []int64{},
				}),
				newReplica(&querypb.Replica{
					ID:            3,
					CollectionID:  1,
					Nodes:         []int64{6, 7},
					OutboundNodes: []int64{},
				}),
			},
		},
		rgs: map[string]typeutil.UniqueSet{
			"rg1": typeutil.NewUniqueSet(1, 2, 3, 4),
			"rg2": typeutil.NewUniqueSet(5, 6, 7),
		},
		expectedPlan: map[typeutil.UniqueID]expectedReplicaPlan{
			1: {
				newOutboundNodes:  0,
				recoverNodes:      0,
				incomingNodeCount: 0,
				expectedNodeCount: 4,
			},
			2: {
				newOutboundNodes:  0,
				recoverNodes:      0,
				incomingNodeCount: 0,
				expectedNodeCount: 1,
			},
			3: {
				newOutboundNodes:  0,
				recoverNodes:      0,
				incomingNodeCount: 0,
				expectedNodeCount: 2,
			},
		},
		expectedNewIncomingNodes: map[string]typeutil.UniqueSet{
			"rg1": typeutil.NewUniqueSet(),
			"rg2": typeutil.NewUniqueSet(),
		},
	})
}

func (s *CollectionAssignmentHelperSuite) TestOutbound() {
	s.runCase(testCase{
		collectionID: 1,
		rgToReplicas: map[string][]*Replica{
			"rg1": {
				newReplica(&querypb.Replica{
					ID:            1,
					CollectionID:  1,
					Nodes:         []int64{1, 2, 3, 4, 5},
					OutboundNodes: []int64{},
				}),
			},
			"rg2": {
				newReplica(&querypb.Replica{
					ID:            2,
					CollectionID:  1,
					Nodes:         []int64{6},
					OutboundNodes: []int64{},
				}),
				newReplica(&querypb.Replica{
					ID:            3,
					CollectionID:  1,
					Nodes:         []int64{7, 8},
					OutboundNodes: []int64{},
				}),
			},
		},
		rgs: map[string]typeutil.UniqueSet{
			"rg1": typeutil.NewUniqueSet(1, 2, 3, 4),
			"rg2": typeutil.NewUniqueSet(5, 6, 7, 8),
		},
		expectedPlan: map[typeutil.UniqueID]expectedReplicaPlan{
			1: {
				newOutboundNodes:  1,
				recoverNodes:      0,
				incomingNodeCount: 0,
				expectedNodeCount: 4,
			},
			2: {
				newOutboundNodes:  0,
				recoverNodes:      0,
				incomingNodeCount: 0,
				expectedNodeCount: 1,
			},
			3: {
				newOutboundNodes:  0,
				recoverNodes:      0,
				incomingNodeCount: 0,
				expectedNodeCount: 2,
			},
		},
		expectedNewIncomingNodes: map[string]typeutil.UniqueSet{
			"rg1": typeutil.NewUniqueSet(),
			"rg2": typeutil.NewUniqueSet(), // 5 is still used rg1 of replica 1.
		},
	})

	s.runCase(testCase{
		collectionID: 1,
		rgToReplicas: map[string][]*Replica{
			"rg1": {
				newReplica(&querypb.Replica{
					ID:            1,
					CollectionID:  1,
					Nodes:         []int64{1, 2, 3, 4, 5},
					OutboundNodes: []int64{},
				}),
			},
			"rg2": {
				newReplica(&querypb.Replica{
					ID:            2,
					CollectionID:  1,
					Nodes:         []int64{6},
					OutboundNodes: []int64{},
				}),
				newReplica(&querypb.Replica{
					ID:            3,
					CollectionID:  1,
					Nodes:         []int64{7, 8},
					OutboundNodes: []int64{},
				}),
			},
		},
		rgs: map[string]typeutil.UniqueSet{
			"rg1": typeutil.NewUniqueSet(1, 2, 3, 4),
			"rg2": typeutil.NewUniqueSet(5, 7, 8),
		},
		expectedPlan: map[typeutil.UniqueID]expectedReplicaPlan{
			1: {
				newOutboundNodes:  1,
				recoverNodes:      0,
				incomingNodeCount: 0,
				expectedNodeCount: 4,
			},
			2: {
				newOutboundNodes:  1,
				recoverNodes:      0,
				incomingNodeCount: 1,
				expectedNodeCount: 1,
			},
			3: {
				newOutboundNodes:  1,
				recoverNodes:      0,
				incomingNodeCount: 0,
				expectedNodeCount: 1,
			},
		},
		expectedNewIncomingNodes: map[string]typeutil.UniqueSet{
			"rg1": typeutil.NewUniqueSet(),
			"rg2": typeutil.NewUniqueSet(), // 5 is still used rg1 of replica 1.
		},
	})
}

func (s *CollectionAssignmentHelperSuite) TestIncomingNode() {
	s.runCase(testCase{
		collectionID: 1,
		rgToReplicas: map[string][]*Replica{
			"rg1": {
				newReplica(&querypb.Replica{
					ID:            1,
					CollectionID:  1,
					Nodes:         []int64{1, 2},
					OutboundNodes: []int64{5},
				}),
			},
			"rg2": {
				newReplica(&querypb.Replica{
					ID:            2,
					CollectionID:  1,
					Nodes:         []int64{6},
					OutboundNodes: []int64{},
				}),
				newReplica(&querypb.Replica{
					ID:            3,
					CollectionID:  1,
					Nodes:         []int64{7},
					OutboundNodes: []int64{},
				}),
			},
		},
		rgs: map[string]typeutil.UniqueSet{
			"rg1": typeutil.NewUniqueSet(1, 2, 3, 4),
			"rg2": typeutil.NewUniqueSet(5, 6, 7, 8),
		},
		expectedPlan: map[typeutil.UniqueID]expectedReplicaPlan{
			1: {
				newOutboundNodes:  0,
				recoverNodes:      0,
				incomingNodeCount: 2,
				expectedNodeCount: 4,
			},
			2: {
				newOutboundNodes:  0,
				recoverNodes:      0,
				incomingNodeCount: 0,
				expectedNodeCount: 1,
			},
			3: {
				newOutboundNodes:  0,
				recoverNodes:      0,
				incomingNodeCount: 1,
				expectedNodeCount: 2,
			},
		},
		expectedNewIncomingNodes: map[string]typeutil.UniqueSet{
			"rg1": typeutil.NewUniqueSet(3, 4),
			"rg2": typeutil.NewUniqueSet(8),
		},
	})
}

func (s *CollectionAssignmentHelperSuite) TestRecoverNode() {
	s.runCase(testCase{
		collectionID: 1,
		rgToReplicas: map[string][]*Replica{
			"rg1": {
				newReplica(&querypb.Replica{
					ID:            1,
					CollectionID:  1,
					Nodes:         []int64{1, 2},
					OutboundNodes: []int64{3},
				}),
			},
			"rg2": {
				newReplica(&querypb.Replica{
					ID:            2,
					CollectionID:  1,
					Nodes:         []int64{6},
					OutboundNodes: []int64{7},
				}),
				newReplica(&querypb.Replica{
					ID:            3,
					CollectionID:  1,
					Nodes:         []int64{8},
					OutboundNodes: []int64{},
				}),
			},
		},
		rgs: map[string]typeutil.UniqueSet{
			"rg1": typeutil.NewUniqueSet(1, 2, 3, 4),
			"rg2": typeutil.NewUniqueSet(5, 6, 7, 8),
		},
		expectedPlan: map[typeutil.UniqueID]expectedReplicaPlan{
			1: {
				newOutboundNodes:  0,
				recoverNodes:      1,
				incomingNodeCount: 1,
				expectedNodeCount: 4,
			},
			2: {
				newOutboundNodes:  0,
				recoverNodes:      1,
				incomingNodeCount: 0,
				expectedNodeCount: 2,
			},
			3: {
				newOutboundNodes:  0,
				recoverNodes:      0,
				incomingNodeCount: 1,
				expectedNodeCount: 2,
			},
		},
		expectedNewIncomingNodes: map[string]typeutil.UniqueSet{
			"rg1": typeutil.NewUniqueSet(4),
			"rg2": typeutil.NewUniqueSet(5),
		},
	})
}

func (s *CollectionAssignmentHelperSuite) TestMixRecoverNode() {
	s.runCase(testCase{
		collectionID: 1,
		rgToReplicas: map[string][]*Replica{
			"rg1": {
				newReplica(&querypb.Replica{
					ID:            1,
					CollectionID:  1,
					Nodes:         []int64{1, 2},
					OutboundNodes: []int64{3},
				}),
			},
			"rg2": {
				newReplica(&querypb.Replica{
					ID:            2,
					CollectionID:  1,
					Nodes:         []int64{6},
					OutboundNodes: []int64{7},
				}),
				newReplica(&querypb.Replica{
					ID:            3,
					CollectionID:  1,
					Nodes:         []int64{8},
					OutboundNodes: []int64{},
				}),
			},
			"rg3": {
				newReplica(&querypb.Replica{
					ID:            4,
					CollectionID:  1,
					Nodes:         []int64{9},
					OutboundNodes: []int64{},
				}),
				newReplica(&querypb.Replica{
					ID:            5,
					CollectionID:  1,
					Nodes:         []int64{10},
					OutboundNodes: []int64{},
				}),
			},
		},
		rgs: map[string]typeutil.UniqueSet{
			"rg1": typeutil.NewUniqueSet(1, 2, 3, 4),
			"rg2": typeutil.NewUniqueSet(5, 6, 7),
			"rg3": typeutil.NewUniqueSet(8, 9, 10),
		},
		expectedPlan: map[typeutil.UniqueID]expectedReplicaPlan{
			1: {
				newOutboundNodes:  0,
				recoverNodes:      1,
				incomingNodeCount: 1,
				expectedNodeCount: 4,
			},
			2: {
				newOutboundNodes:  0,
				recoverNodes:      1,
				incomingNodeCount: 0,
				expectedNodeCount: 2,
			},
			3: {
				newOutboundNodes:  1,
				recoverNodes:      0,
				incomingNodeCount: 1,
				expectedNodeCount: 1,
			},
			4: {
				newOutboundNodes:  0,
				recoverNodes:      0,
				incomingNodeCount: 0,
				expectedNodeCount: 1,
			},
			5: {
				newOutboundNodes:  0,
				recoverNodes:      0,
				incomingNodeCount: 0,
				expectedNodeCount: 1,
			},
		},
		expectedNewIncomingNodes: map[string]typeutil.UniqueSet{
			"rg1": typeutil.NewUniqueSet(4),
			"rg2": typeutil.NewUniqueSet(5),
			"rg3": typeutil.NewUniqueSet(),
		},
	})
}

func (s *CollectionAssignmentHelperSuite) runCase(c testCase) {
	cHelper := newCollectionAssignmentHelper(c.collectionID, c.rgToReplicas, c.rgs)
	cHelper.RangeOverResourceGroup(func(rHelper *replicaAssignmentHelper) {
		s.ElementsMatch(c.expectedNewIncomingNodes[rHelper.rgName].Collect(), rHelper.incomingNodes.Collect())
		rHelper.RangeOverReplicas(func(assignment *replicaAssignmentInfo) {
			outboundNodes := assignment.GetNewOutboundNodes()
			recoverNodes, incomingNodes := assignment.GetRecoverNodesAndIncomingNodeCount()
			plan := c.expectedPlan[assignment.GetReplicaID()]
			s.Equal(
				plan.newOutboundNodes,
				len(outboundNodes),
			)
			s.Equal(
				plan.incomingNodeCount,
				incomingNodes,
			)
			s.Equal(
				plan.recoverNodes,
				len(recoverNodes),
			)
			s.Equal(
				plan.expectedNodeCount,
				assignment.expectedNodeCount,
			)
		})
	})
}

func TestCollectionAssignmentHelper(t *testing.T) {
	suite.Run(t, new(CollectionAssignmentHelperSuite))
}
