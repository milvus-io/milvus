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

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type rowCountCostModel struct {
	nodeSegments map[int64][]*meta.Segment
}

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

type segmentCountCostModel struct {
	nodeSegments map[int64][]*meta.Segment
}

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

func cmpCost(f1, f2 float64) int {
	if math.Abs(f1-f2) < params.Params.QueryCoordCfg.BalanceCostThreshold.GetAsFloat() {
		return 0
	}
	if f1 < f2 {
		return -1
	}
	return 1
}

type generator interface {
	setPlans(plans []SegmentAssignPlan)
	setReplicaNodeSegments(replicaNodeSegments map[int64][]*meta.Segment)
	setGlobalNodeSegments(globalNodeSegments map[int64][]*meta.Segment)
	setCost(cost float64)
	getReplicaNodeSegments() map[int64][]*meta.Segment
	getGlobalNodeSegments() map[int64][]*meta.Segment
	getCost() float64
	generatePlans() []SegmentAssignPlan
}

type basePlanGenerator struct {
	plans                        []SegmentAssignPlan
	currClusterCost              float64
	replicaNodeSegments          map[int64][]*meta.Segment
	globalNodeSegments           map[int64][]*meta.Segment
	rowCountCostWeight           float64
	globalRowCountCostWeight     float64
	segmentCountCostWeight       float64
	globalSegmentCountCostWeight float64
}

func newBasePlanGenerator() *basePlanGenerator {
	return &basePlanGenerator{
		rowCountCostWeight:           params.Params.QueryCoordCfg.RowCountFactor.GetAsFloat(),
		globalRowCountCostWeight:     params.Params.QueryCoordCfg.GlobalRowCountFactor.GetAsFloat(),
		segmentCountCostWeight:       params.Params.QueryCoordCfg.SegmentCountFactor.GetAsFloat(),
		globalSegmentCountCostWeight: params.Params.QueryCoordCfg.GlobalSegmentCountFactor.GetAsFloat(),
	}
}

func (g *basePlanGenerator) setPlans(plans []SegmentAssignPlan) {
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

func (g *basePlanGenerator) applyPlans(nodeSegments map[int64][]*meta.Segment, plans []SegmentAssignPlan) map[int64][]*meta.Segment {
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

func (g *basePlanGenerator) calClusterCost(replicaNodeSegments, globalNodeSegments map[int64][]*meta.Segment) float64 {
	replicaRowCountCostModel, replicaSegmentCountCostModel := &rowCountCostModel{replicaNodeSegments}, &segmentCountCostModel{replicaNodeSegments}
	globalRowCountCostModel, globalSegmentCountCostModel := &rowCountCostModel{globalNodeSegments}, &segmentCountCostModel{globalNodeSegments}
	replicaCost1, replicaCost2 := replicaRowCountCostModel.cost(), replicaSegmentCountCostModel.cost()
	globalCost1, globalCost2 := globalRowCountCostModel.cost(), globalSegmentCountCostModel.cost()

	return replicaCost1*g.rowCountCostWeight + replicaCost2*g.segmentCountCostWeight +
		globalCost1*g.globalRowCountCostWeight + globalCost2*g.globalSegmentCountCostWeight
}

func (g *basePlanGenerator) mergePlans(curr []SegmentAssignPlan, inc []SegmentAssignPlan) []SegmentAssignPlan {
	// merge plans with the same segment
	// eg, plan1 is move segment1 from node1 to node2, plan2 is move segment1 from node2 to node3
	// we should merge plan1 and plan2 to one plan, which is move segment1 from node1 to node2
	result := make([]SegmentAssignPlan, 0, len(curr)+len(inc))
	processed := typeutil.NewSet[int]()
	for _, p := range curr {
		newPlan, idx, has := lo.FindIndexOf(inc, func(newPlan SegmentAssignPlan) bool {
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
	result = append(result, lo.Filter(inc, func(_ SegmentAssignPlan, idx int) bool {
		return !processed.Contain(idx)
	})...)

	return result
}

type rowCountBasedPlanGenerator struct {
	*basePlanGenerator
	maxSteps int
	isGlobal bool
}

func newRowCountBasedPlanGenerator(maxSteps int, isGlobal bool) *rowCountBasedPlanGenerator {
	return &rowCountBasedPlanGenerator{
		basePlanGenerator: newBasePlanGenerator(),
		maxSteps:          maxSteps,
		isGlobal:          isGlobal,
	}
}

func (g *rowCountBasedPlanGenerator) generatePlans() []SegmentAssignPlan {
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
		plan := SegmentAssignPlan{
			Segment: segment,
			From:    maxNode.id,
			To:      minNode.id,
		}
		newCluster := g.applyPlans(g.replicaNodeSegments, []SegmentAssignPlan{plan})
		newGlobalCluster := g.applyPlans(g.globalNodeSegments, []SegmentAssignPlan{plan})
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
			g.plans = g.mergePlans(g.plans, []SegmentAssignPlan{plan})
			modified = true
		} else {
			modified = false
		}
	}
	return g.plans
}

type segmentCountBasedPlanGenerator struct {
	*basePlanGenerator
	maxSteps int
	isGlobal bool
}

func newSegmentCountBasedPlanGenerator(maxSteps int, isGlobal bool) *segmentCountBasedPlanGenerator {
	return &segmentCountBasedPlanGenerator{
		basePlanGenerator: newBasePlanGenerator(),
		maxSteps:          maxSteps,
		isGlobal:          isGlobal,
	}
}

func (g *segmentCountBasedPlanGenerator) generatePlans() []SegmentAssignPlan {
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
		plan := SegmentAssignPlan{
			Segment: segment,
			From:    maxNode.id,
			To:      minNode.id,
		}
		newCluster := g.applyPlans(g.replicaNodeSegments, []SegmentAssignPlan{plan})
		newGlobalCluster := g.applyPlans(g.globalNodeSegments, []SegmentAssignPlan{plan})
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
			g.plans = g.mergePlans(g.plans, []SegmentAssignPlan{plan})
			modified = true
		} else {
			modified = false
		}
	}
	return g.plans
}

type planType int

const (
	movePlan planType = iota + 1
	swapPlan
)

type randomPlanGenerator struct {
	*basePlanGenerator
	maxSteps int
}

func newRandomPlanGenerator(maxSteps int) *randomPlanGenerator {
	return &randomPlanGenerator{
		basePlanGenerator: newBasePlanGenerator(),
		maxSteps:          maxSteps,
	}
}

func (g *randomPlanGenerator) generatePlans() []SegmentAssignPlan {
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
		plans := make([]SegmentAssignPlan, 0)
		planType := planType(rand.Intn(2) + 1)
		if planType == movePlan {
			plan := SegmentAssignPlan{
				From:    node1,
				To:      node2,
				Segment: segment1,
			}
			plans = append(plans, plan)
		} else {
			plan1 := SegmentAssignPlan{
				From:    node1,
				To:      node2,
				Segment: segment1,
			}
			plan2 := SegmentAssignPlan{
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

type MultiTargetBalancer struct {
	*ScoreBasedBalancer
	dist      *meta.DistributionManager
	targetMgr meta.TargetManagerInterface
}

func (b *MultiTargetBalancer) BalanceReplica(ctx context.Context, replica *meta.Replica) (segmentPlans []SegmentAssignPlan, channelPlans []ChannelAssignPlan) {
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

	if replica.NodesCount() == 0 {
		return nil, nil
	}

	rwNodes := replica.GetRWNodes()
	roNodes := replica.GetRONodes()

	if len(rwNodes) == 0 {
		// no available nodes to balance
		return nil, nil
	}

	// print current distribution before generating plans
	segmentPlans, channelPlans = make([]SegmentAssignPlan, 0), make([]ChannelAssignPlan, 0)
	if len(roNodes) != 0 {
		if !paramtable.Get().QueryCoordCfg.EnableStoppingBalance.GetAsBool() {
			log.RatedInfo(10, "stopping balance is disabled!", zap.Int64s("stoppingNode", roNodes))
			return nil, nil
		}

		log.Info("Handle stopping nodes",
			zap.Any("stopping nodes", roNodes),
			zap.Any("available nodes", rwNodes),
		)
		// handle stopped nodes here, have to assign segments on stopping nodes to nodes with the smallest score
		if b.permitBalanceChannel(replica.GetCollectionID()) {
			channelPlans = append(channelPlans, b.genStoppingChannelPlan(ctx, replica, rwNodes, roNodes)...)
		}
		if len(channelPlans) == 0 && b.permitBalanceSegment(replica.GetCollectionID()) {
			segmentPlans = append(segmentPlans, b.genStoppingSegmentPlan(ctx, replica, rwNodes, roNodes)...)
		}
	} else {
		if paramtable.Get().QueryCoordCfg.AutoBalanceChannel.GetAsBool() && b.permitBalanceChannel(replica.GetCollectionID()) {
			channelPlans = append(channelPlans, b.genChannelPlan(ctx, br, replica, rwNodes)...)
		}

		if len(channelPlans) == 0 && b.permitBalanceSegment(replica.GetCollectionID()) {
			segmentPlans = b.genSegmentPlan(ctx, replica, rwNodes)
		}
	}

	return segmentPlans, channelPlans
}

func (b *MultiTargetBalancer) genSegmentPlan(ctx context.Context, replica *meta.Replica, rwNodes []int64) []SegmentAssignPlan {
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

	return b.genPlanByDistributions(nodeSegments, globalNodeSegments)
}

func (b *MultiTargetBalancer) genPlanByDistributions(nodeSegments, globalNodeSegments map[int64][]*meta.Segment) []SegmentAssignPlan {
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
	var plans []SegmentAssignPlan
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

func NewMultiTargetBalancer(scheduler task.Scheduler, nodeManager *session.NodeManager, dist *meta.DistributionManager, meta *meta.Meta, targetMgr meta.TargetManagerInterface) *MultiTargetBalancer {
	return &MultiTargetBalancer{
		ScoreBasedBalancer: NewScoreBasedBalancer(scheduler, nodeManager, dist, meta, targetMgr),
		dist:               dist,
		targetMgr:          targetMgr,
	}
}
