// multi_target_balance.go implements the MultiTargetBalancer which uses multiple optimization
// strategies to achieve comprehensive load balancing across query nodes.
package balance

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// rowCountCostModel calculates the cost based on row count distribution across nodes.
// A lower cost indicates a more balanced distribution of rows.
type rowCountCostModel struct {
	nodeSegments map[int64][]*meta.Segment
}

// cost calculates the normalized cost of the current row distribution.
// Returns a value between 0 (best case - perfectly balanced) and 1 (worst case - all on one node).
func (m *rowCountCostModel) cost() float64 {
	nodeCount := len(m.nodeSegments)
	if nodeCount == 0 {
		return 0
	}
	totalRowCount := 0
	nodesRowCount := make(map[int64]int)
	for node, segments := range m.nodeSegments {
		rowCount := 0
		for _, segment := range segments {
			rowCount += int(segment.GetNumOfRows())
		}
		totalRowCount += rowCount
		nodesRowCount[node] = rowCount
	}
	expectAvg := float64(totalRowCount) / float64(nodeCount)

	// calculate worst case, all rows are allocated to only one node
	worst := float64(nodeCount-1)*expectAvg + float64(totalRowCount) - expectAvg
	// calculate best case, all rows are allocated meanly
	nodeWithMoreRows := totalRowCount % nodeCount
	best := float64(nodeWithMoreRows)*(math.Ceil(expectAvg)-expectAvg) + float64(nodeCount-nodeWithMoreRows)*(expectAvg-math.Floor(expectAvg))

	if worst == best {
		return 0
	}
	var currCost float64
	for _, rowCount := range nodesRowCount {
		currCost += math.Abs(float64(rowCount) - expectAvg)
	}

	// normalization
	return (currCost - best) / (worst - best)
}

// segmentCountCostModel calculates the cost based on segment count distribution across nodes.
// A lower cost indicates a more balanced distribution of segments.
type segmentCountCostModel struct {
	nodeSegments map[int64][]*meta.Segment
}

// cost calculates the normalized cost of the current segment distribution.
// Returns a value between 0 (best case - perfectly balanced) and 1 (worst case - all on one node).
func (m *segmentCountCostModel) cost() float64 {
	nodeCount := len(m.nodeSegments)
	if nodeCount == 0 {
		return 0
	}
	totalSegmentCount := 0
	nodeSegmentCount := make(map[int64]int)
	for node, segments := range m.nodeSegments {
		totalSegmentCount += len(segments)
		nodeSegmentCount[node] = len(segments)
	}
	expectAvg := float64(totalSegmentCount) / float64(nodeCount)
	// calculate worst case, all segments are allocated to only one node
	worst := float64(nodeCount-1)*expectAvg + float64(totalSegmentCount) - expectAvg
	// calculate best case, all segments are allocated meanly
	nodeWithMoreRows := totalSegmentCount % nodeCount
	best := float64(nodeWithMoreRows)*(math.Ceil(expectAvg)-expectAvg) + float64(nodeCount-nodeWithMoreRows)*(expectAvg-math.Floor(expectAvg))

	var currCost float64
	for _, count := range nodeSegmentCount {
		currCost += math.Abs(float64(count) - expectAvg)
	}

	if worst == best {
		return 0
	}
	// normalization
	return (currCost - best) / (worst - best)
}

// cmpCost compares two cost values with a threshold for equality.
// Returns -1 if f1 < f2, 0 if they're approximately equal, 1 if f1 > f2.
func cmpCost(f1, f2 float64) int {
	if math.Abs(f1-f2) < params.Params.QueryCoordCfg.BalanceCostThreshold.GetAsFloat() {
		return 0
	}
	if f1 < f2 {
		return -1
	}
	return 1
}

// generator defines the interface for balance plan generators.
// Each generator uses a different optimization strategy to generate segment assignment plans.
type generator interface {
	setPlans(plans []assign.SegmentAssignPlan)
	setReplicaNodeSegments(replicaNodeSegments map[int64][]*meta.Segment)
	setGlobalNodeSegments(globalNodeSegments map[int64][]*meta.Segment)
	setCost(cost float64)
	getReplicaNodeSegments() map[int64][]*meta.Segment
	getGlobalNodeSegments() map[int64][]*meta.Segment
	getCost() float64
	generatePlans() []assign.SegmentAssignPlan
}

// basePlanGenerator provides common functionality for all plan generators.
// It manages segment distributions and calculates cluster costs using weighted factors.
type basePlanGenerator struct {
	plans                        []assign.SegmentAssignPlan
	currClusterCost              float64
	replicaNodeSegments          map[int64][]*meta.Segment
	globalNodeSegments           map[int64][]*meta.Segment
	rowCountCostWeight           float64
	globalRowCountCostWeight     float64
	segmentCountCostWeight       float64
	globalSegmentCountCostWeight float64
}

// newBasePlanGenerator creates a new basePlanGenerator with cost weights from configuration.
func newBasePlanGenerator() *basePlanGenerator {
	return &basePlanGenerator{
		rowCountCostWeight:           params.Params.QueryCoordCfg.RowCountFactor.GetAsFloat(),
		globalRowCountCostWeight:     params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat(),
		segmentCountCostWeight:       params.Params.QueryCoordCfg.SegmentCountFactor.GetAsFloat(),
		globalSegmentCountCostWeight: params.Params.QueryCoordCfg.GlobalSegmentCountFactor.GetAsFloat(),
	}
}

func (g *basePlanGenerator) setPlans(plans []assign.SegmentAssignPlan) {
	g.plans = plans
}

func (g *basePlanGenerator) setReplicaNodeSegments(replicaNodeSegments map[int64][]*meta.Segment) {
	g.replicaNodeSegments = replicaNodeSegments
}

func (g *basePlanGenerator) setGlobalNodeSegments(globalNodeSegments map[int64][]*meta.Segment) {
	g.globalNodeSegments = globalNodeSegments
}

func (g *basePlanGenerator) setCost(cost float64) {
	g.currClusterCost = cost
}

func (g *basePlanGenerator) getReplicaNodeSegments() map[int64][]*meta.Segment {
	return g.replicaNodeSegments
}

func (g *basePlanGenerator) getGlobalNodeSegments() map[int64][]*meta.Segment {
	return g.globalNodeSegments
}

func (g *basePlanGenerator) getCost() float64 {
	return g.currClusterCost
}

// applyPlans applies the given segment assignment plans to a node-segments map,
// returning a new map with the updated distribution.
func (g *basePlanGenerator) applyPlans(nodeSegments map[int64][]*meta.Segment, plans []assign.SegmentAssignPlan) map[int64][]*meta.Segment {
	newCluster := make(map[int64][]*meta.Segment)
	for k, v := range nodeSegments {
		newCluster[k] = append(newCluster[k], v...)
	}
	for _, p := range plans {
		for i, s := range newCluster[p.From] {
			if s.GetID() == p.Segment.ID {
				newCluster[p.From] = append(newCluster[p.From][:i], newCluster[p.From][i+1:]...)
				break
			}
		}
		newCluster[p.To] = append(newCluster[p.To], p.Segment)
	}
	return newCluster
}

// calClusterCost calculates the total weighted cost of the cluster based on both
// replica-level and global-level segment distributions.
func (g *basePlanGenerator) calClusterCost(replicaNodeSegments, globalNodeSegments map[int64][]*meta.Segment) float64 {
	replicaRowCountCostModel, replicaSegmentCountCostModel := &rowCountCostModel{replicaNodeSegments}, &segmentCountCostModel{replicaNodeSegments}
	globalRowCountCostModel, globalSegmentCountCostModel := &rowCountCostModel{globalNodeSegments}, &segmentCountCostModel{globalNodeSegments}
	replicaCost1, replicaCost2 := replicaRowCountCostModel.cost(), replicaSegmentCountCostModel.cost()
	globalCost1, globalCost2 := globalRowCountCostModel.cost(), globalSegmentCountCostModel.cost()

	return replicaCost1*g.rowCountCostWeight + replicaCost2*g.segmentCountCostWeight +
		globalCost1*g.globalRowCountCostWeight + globalCost2*g.globalSegmentCountCostWeight
}

// mergePlans merges incremental plans with existing plans, combining movements of the same segment.
// For example, if plan1 moves segment1 from node1 to node2, and plan2 moves segment1 from node2 to node3,
// they are merged into a single plan moving segment1 from node1 to node3.
// Plans that result in no movement (from == to) are filtered out.
func (g *basePlanGenerator) mergePlans(curr []assign.SegmentAssignPlan, inc []assign.SegmentAssignPlan) []assign.SegmentAssignPlan {
	result := make([]assign.SegmentAssignPlan, 0, len(curr)+len(inc))
	processed := typeutil.NewSet[int]()
	for _, p := range curr {
		newPlan, idx, has := lo.FindIndexOf(inc, func(newPlan assign.SegmentAssignPlan) bool {
			return newPlan.Segment.GetID() == p.Segment.GetID() && newPlan.From == p.To
		})

		if has {
			processed.Insert(idx)
			p.To = newPlan.To
		}
		// in case of generator 1 move segment from node 1 to node 2 and generator 2 move segment back
		if p.From != p.To {
			result = append(result, p)
		}
	}

	// add not merged inc plans
	result = append(result, lo.Filter(inc, func(_ assign.SegmentAssignPlan, idx int) bool {
		return !processed.Contain(idx)
	})...)

	return result
}

// rowCountBasedPlanGenerator generates balance plans by moving segments from nodes
// with higher row counts to nodes with lower row counts. It uses a greedy approach,
// iteratively selecting segments to move until the cost no longer decreases.
type rowCountBasedPlanGenerator struct {
	*basePlanGenerator
	maxSteps int
	isGlobal bool // if true, considers global distribution; otherwise replica-level
}

// newRowCountBasedPlanGenerator creates a new row count based plan generator.
// maxSteps limits the number of optimization iterations.
// isGlobal determines whether to optimize for global or replica-level balance.
func newRowCountBasedPlanGenerator(maxSteps int, isGlobal bool) *rowCountBasedPlanGenerator {
	return &rowCountBasedPlanGenerator{
		basePlanGenerator: newBasePlanGenerator(),
		maxSteps:          maxSteps,
		isGlobal:          isGlobal,
	}
}

// generatePlans generates segment assignment plans using row count optimization.
// It iteratively moves segments from the node with highest row count to the node
// with lowest row count, as long as it reduces the overall cluster cost.
func (g *rowCountBasedPlanGenerator) generatePlans() []assign.SegmentAssignPlan {
	type nodeWithRowCount struct {
		id       int64
		count    int
		segments []*meta.Segment
	}

	if g.currClusterCost == 0 {
		g.currClusterCost = g.calClusterCost(g.replicaNodeSegments, g.globalNodeSegments)
	}
	nodeSegments := g.replicaNodeSegments
	if g.isGlobal {
		nodeSegments = g.globalNodeSegments
	}
	nodesWithRowCount := make([]*nodeWithRowCount, 0)
	for node, segments := range g.replicaNodeSegments {
		rowCount := 0
		for _, segment := range nodeSegments[node] {
			rowCount += int(segment.GetNumOfRows())
		}
		nodesWithRowCount = append(nodesWithRowCount, &nodeWithRowCount{
			id:       node,
			count:    rowCount,
			segments: segments,
		})
	}

	modified := true
	for i := 0; i < g.maxSteps; i++ {
		if modified {
			sort.Slice(nodesWithRowCount, func(i, j int) bool {
				return nodesWithRowCount[i].count < nodesWithRowCount[j].count
			})
		}
		maxNode, minNode := nodesWithRowCount[len(nodesWithRowCount)-1], nodesWithRowCount[0]
		if len(maxNode.segments) == 0 {
			break
		}
		segment := maxNode.segments[rand.Intn(len(maxNode.segments))]
		plan := assign.SegmentAssignPlan{
			Segment: segment,
			From:    maxNode.id,
			To:      minNode.id,
		}
		newCluster := g.applyPlans(g.replicaNodeSegments, []assign.SegmentAssignPlan{plan})
		newGlobalCluster := g.applyPlans(g.globalNodeSegments, []assign.SegmentAssignPlan{plan})
		newCost := g.calClusterCost(newCluster, newGlobalCluster)
		if cmpCost(newCost, g.currClusterCost) < 0 {
			g.currClusterCost = newCost
			g.replicaNodeSegments = newCluster
			g.globalNodeSegments = newGlobalCluster
			maxNode.count -= int(segment.GetNumOfRows())
			minNode.count += int(segment.GetNumOfRows())
			for n, segment := range maxNode.segments {
				if segment.GetID() == plan.Segment.ID {
					maxNode.segments = append(maxNode.segments[:n], maxNode.segments[n+1:]...)
					break
				}
			}
			minNode.segments = append(minNode.segments, segment)
			g.plans = g.mergePlans(g.plans, []assign.SegmentAssignPlan{plan})
			modified = true
		} else {
			modified = false
		}
	}
	return g.plans
}

// segmentCountBasedPlanGenerator generates balance plans by moving segments from nodes
// with higher segment counts to nodes with lower segment counts. It uses a greedy approach,
// iteratively selecting segments to move until the cost no longer decreases.
type segmentCountBasedPlanGenerator struct {
	*basePlanGenerator
	maxSteps int
	isGlobal bool // if true, considers global distribution; otherwise replica-level
}

// newSegmentCountBasedPlanGenerator creates a new segment count based plan generator.
// maxSteps limits the number of optimization iterations.
// isGlobal determines whether to optimize for global or replica-level balance.
func newSegmentCountBasedPlanGenerator(maxSteps int, isGlobal bool) *segmentCountBasedPlanGenerator {
	return &segmentCountBasedPlanGenerator{
		basePlanGenerator: newBasePlanGenerator(),
		maxSteps:          maxSteps,
		isGlobal:          isGlobal,
	}
}

// generatePlans generates segment assignment plans using segment count optimization.
// It iteratively moves segments from the node with highest segment count to the node
// with lowest segment count, as long as it reduces the overall cluster cost.
func (g *segmentCountBasedPlanGenerator) generatePlans() []assign.SegmentAssignPlan {
	type nodeWithSegmentCount struct {
		id       int64
		count    int
		segments []*meta.Segment
	}

	if g.currClusterCost == 0 {
		g.currClusterCost = g.calClusterCost(g.replicaNodeSegments, g.globalNodeSegments)
	}

	nodeSegments := g.replicaNodeSegments
	if g.isGlobal {
		nodeSegments = g.globalNodeSegments
	}
	nodesWithSegmentCount := make([]*nodeWithSegmentCount, 0)
	for node, segments := range g.replicaNodeSegments {
		nodesWithSegmentCount = append(nodesWithSegmentCount, &nodeWithSegmentCount{
			id:       node,
			count:    len(nodeSegments[node]),
			segments: segments,
		})
	}

	modified := true
	for i := 0; i < g.maxSteps; i++ {
		if modified {
			sort.Slice(nodesWithSegmentCount, func(i, j int) bool {
				return nodesWithSegmentCount[i].count < nodesWithSegmentCount[j].count
			})
		}
		maxNode, minNode := nodesWithSegmentCount[len(nodesWithSegmentCount)-1], nodesWithSegmentCount[0]
		if len(maxNode.segments) == 0 {
			break
		}
		segment := maxNode.segments[rand.Intn(len(maxNode.segments))]
		plan := assign.SegmentAssignPlan{
			Segment: segment,
			From:    maxNode.id,
			To:      minNode.id,
		}
		newCluster := g.applyPlans(g.replicaNodeSegments, []assign.SegmentAssignPlan{plan})
		newGlobalCluster := g.applyPlans(g.globalNodeSegments, []assign.SegmentAssignPlan{plan})
		newCost := g.calClusterCost(newCluster, newGlobalCluster)
		if cmpCost(newCost, g.currClusterCost) < 0 {
			g.currClusterCost = newCost
			g.replicaNodeSegments = newCluster
			g.globalNodeSegments = newGlobalCluster
			maxNode.count -= 1
			minNode.count += 1
			for n, segment := range maxNode.segments {
				if segment.GetID() == plan.Segment.ID {
					maxNode.segments = append(maxNode.segments[:n], maxNode.segments[n+1:]...)
					break
				}
			}
			minNode.segments = append(minNode.segments, segment)
			g.plans = g.mergePlans(g.plans, []assign.SegmentAssignPlan{plan})
			modified = true
		} else {
			modified = false
		}
	}
	return g.plans
}

// planType represents the type of balance plan operation.
type planType int

const (
	movePlan planType = iota + 1 // move a segment from one node to another
	swapPlan                     // swap segments between two nodes
)

// randomPlanGenerator generates balance plans by randomly selecting segments and nodes,
// then applying moves or swaps if they reduce the overall cluster cost.
// This stochastic approach helps escape local minima that greedy algorithms might get stuck in.
type randomPlanGenerator struct {
	*basePlanGenerator
	maxSteps int
}

// newRandomPlanGenerator creates a new random plan generator.
// maxSteps limits the number of random operations to try.
func newRandomPlanGenerator(maxSteps int) *randomPlanGenerator {
	return &randomPlanGenerator{
		basePlanGenerator: newBasePlanGenerator(),
		maxSteps:          maxSteps,
	}
}

// generatePlans generates segment assignment plans using random optimization.
// It randomly selects two nodes and tries either moving a segment or swapping segments,
// accepting the change only if it reduces the cluster cost.
func (g *randomPlanGenerator) generatePlans() []assign.SegmentAssignPlan {
	g.currClusterCost = g.calClusterCost(g.replicaNodeSegments, g.globalNodeSegments)
	nodes := lo.Keys(g.replicaNodeSegments)
	if len(nodes) == 0 {
		return g.plans
	}
	for i := 0; i < g.maxSteps; i++ {
		// random select two nodes and two segments
		node1 := nodes[rand.Intn(len(nodes))]
		node2 := nodes[rand.Intn(len(nodes))]
		if node1 == node2 {
			continue
		}
		segments1 := g.replicaNodeSegments[node1]
		segments2 := g.replicaNodeSegments[node2]
		if len(segments1) == 0 || len(segments2) == 0 {
			continue
		}
		segment1 := segments1[rand.Intn(len(segments1))]
		segment2 := segments2[rand.Intn(len(segments2))]

		// random select plan type, for move type, we move segment1 to node2; for swap type, we swap segment1 and segment2
		plans := make([]assign.SegmentAssignPlan, 0)
		planType := planType(rand.Intn(2) + 1)
		if planType == movePlan {
			plan := assign.SegmentAssignPlan{
				From:    node1,
				To:      node2,
				Segment: segment1,
			}
			plans = append(plans, plan)
		} else {
			plan1 := assign.SegmentAssignPlan{
				From:    node1,
				To:      node2,
				Segment: segment1,
			}
			plan2 := assign.SegmentAssignPlan{
				From:    node2,
				To:      node1,
				Segment: segment2,
			}
			plans = append(plans, plan1, plan2)
		}

		// validate the plan, if the plan is valid, we apply the plan and update the cluster cost
		newCluster := g.applyPlans(g.replicaNodeSegments, plans)
		newGlobalCluster := g.applyPlans(g.globalNodeSegments, plans)
		newCost := g.calClusterCost(newCluster, newGlobalCluster)
		if cmpCost(newCost, g.currClusterCost) < 0 {
			g.currClusterCost = newCost
			g.replicaNodeSegments = newCluster
			g.globalNodeSegments = newGlobalCluster
			g.plans = g.mergePlans(g.plans, plans)
		}
	}
	return g.plans
}

// MultiTargetBalancer implements a multi-objective optimization balancer.
// It combines multiple optimization strategies (row count, segment count, and random)
// to achieve comprehensive load balancing. The generators run sequentially, each
// improving upon the previous results, allowing the balancer to escape local minima
// and find better global solutions.
type MultiTargetBalancer struct {
	*ScoreBasedBalancer
	dist      *meta.DistributionManager
	targetMgr meta.TargetManagerInterface
}

// BalanceReplica balances segments and channels across nodes using multi-target optimization.
// It first attempts to balance channels if AutoBalanceChannel is enabled, then balances segments
// using multiple optimization strategies in sequence.
func (b *MultiTargetBalancer) BalanceReplica(ctx context.Context, replica *meta.Replica) (segmentPlans []assign.SegmentAssignPlan, channelPlans []assign.ChannelAssignPlan) {
	log := log.With(
		zap.Int64("collection", replica.GetCollectionID()),
		zap.Int64("replica id", replica.GetID()),
		zap.String("replica group", replica.GetResourceGroup()),
	)
	br := NewBalanceReport()
	defer func() {
		if len(segmentPlans) == 0 && len(channelPlans) == 0 {
			log.WithRateGroup(fmt.Sprintf("scorebasedbalance-noplan-%d", replica.GetID()), 1, 60).
				RatedDebug(60, "no plan generated, balance report", zap.Stringers("records", br.detailRecords))
		} else {
			log.Info("balance plan generated", zap.Stringers("report details", br.records))
		}
	}()

	if paramtable.Get().QueryCoordCfg.AutoBalanceChannel.GetAsBool() {
		channelPlans = b.balanceChannels(ctx, br, replica)
	}
	if len(channelPlans) == 0 {
		segmentPlans = b.balanceSegments(ctx, br, replica)
	}
	return
}

// balanceChannels generates channel balance plans for a replica.
// It requires at least 2 RW nodes to perform balancing.
func (b *MultiTargetBalancer) balanceChannels(ctx context.Context, br *balanceReport, replica *meta.Replica) []assign.ChannelAssignPlan {
	var rwNodes []int64
	if streamingutil.IsStreamingServiceEnabled() {
		rwNodes, _ = utils.GetChannelRWAndRONodesFor260(replica, b.nodeManager)
	} else {
		rwNodes = replica.GetRWNodes()
	}

	if len(rwNodes) < 2 {
		br.AddRecord(StrRecord("no enough rwNodes to balance channels"))
		return nil
	}

	return b.genChannelPlan(ctx, br, replica, rwNodes)
}

// balanceSegments generates segment balance plans for a replica.
// It requires at least 2 RW nodes to perform balancing.
func (b *MultiTargetBalancer) balanceSegments(ctx context.Context, br *balanceReport, replica *meta.Replica) []assign.SegmentAssignPlan {
	rwNodes := replica.GetRWNodes()
	if len(rwNodes) < 2 {
		br.AddRecord(StrRecord("no enough rwNodes to balance segments"))
		return nil
	}

	return b.genSegmentPlan(ctx, replica, rwNodes)
}

// genSegmentPlan generates segment balance plans using multi-target optimization.
// It collects segment distributions at both replica and global levels, then applies
// multiple optimization strategies sequentially to find an improved distribution.
func (b *MultiTargetBalancer) genSegmentPlan(ctx context.Context, replica *meta.Replica, rwNodes []int64) []assign.SegmentAssignPlan {
	// get segments distribution on replica level and global level
	nodeSegments := make(map[int64][]*meta.Segment)
	globalNodeSegments := make(map[int64][]*meta.Segment)
	for _, node := range rwNodes {
		dist := b.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(replica.GetCollectionID()), meta.WithNodeID(node))
		segments := lo.Filter(dist, func(segment *meta.Segment, _ int) bool {
			return b.targetMgr.CanSegmentBeMoved(ctx, segment.GetCollectionID(), segment.GetID())
		})
		nodeSegments[node] = segments
		globalNodeSegments[node] = b.dist.SegmentDistManager.GetByFilter(meta.WithNodeID(node))
	}

	plans := b.genPlanByDistributions(nodeSegments, globalNodeSegments)
	for i := range plans {
		plans[i].Replica = replica
	}
	return plans
}

// genPlanByDistributions generates segment assignment plans using multiple optimization generators.
// It creates 5 generators: row count (replica), row count (global), segment count (replica),
// segment count (global), and random. These generators run sequentially, each building upon
// the previous results to progressively improve the distribution.
func (b *MultiTargetBalancer) genPlanByDistributions(nodeSegments, globalNodeSegments map[int64][]*meta.Segment) []assign.SegmentAssignPlan {
	// create generators
	// we have 3 types of generators: row count, segment count, random
	// for row count based and segment count based generator, we have 2 types of generators: replica level and global level
	generators := make([]generator, 0)
	generators = append(generators,
		newRowCountBasedPlanGenerator(params.Params.QueryCoordCfg.RowCountMaxSteps.GetAsInt(), false),
		newRowCountBasedPlanGenerator(params.Params.QueryCoordCfg.RowCountMaxSteps.GetAsInt(), true),
		newSegmentCountBasedPlanGenerator(params.Params.QueryCoordCfg.SegmentCountMaxSteps.GetAsInt(), false),
		newSegmentCountBasedPlanGenerator(params.Params.QueryCoordCfg.SegmentCountMaxSteps.GetAsInt(), true),
		newRandomPlanGenerator(params.Params.QueryCoordCfg.RandomMaxSteps.GetAsInt()),
	)

	// run generators sequentially to generate plans
	var cost float64
	var plans []assign.SegmentAssignPlan
	for _, generator := range generators {
		generator.setCost(cost)
		generator.setPlans(plans)
		generator.setReplicaNodeSegments(nodeSegments)
		generator.setGlobalNodeSegments(globalNodeSegments)
		plans = generator.generatePlans()
		cost = generator.getCost()
		nodeSegments = generator.getReplicaNodeSegments()
		globalNodeSegments = generator.getGlobalNodeSegments()
	}
	return plans
}

// NewMultiTargetBalancer creates a new MultiTargetBalancer instance.
// It embeds a ScoreBasedBalancer and adds multi-objective optimization capabilities.
func NewMultiTargetBalancer(scheduler task.Scheduler, nodeManager *session.NodeManager, dist *meta.DistributionManager, meta *meta.Meta, targetMgr meta.TargetManagerInterface) *MultiTargetBalancer {
	return &MultiTargetBalancer{
		ScoreBasedBalancer: NewScoreBasedBalancer(scheduler, nodeManager, dist, meta, targetMgr),
		dist:               dist,
		targetMgr:          targetMgr,
	}
}
