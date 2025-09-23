// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkers

import (
	"context"
	"strings"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// balanceConfig holds all configuration parameters for balance operations.
// This configuration controls how balance tasks are generated and executed.
type balanceConfig struct {
	// segmentBatchSize specifies the maximum number of segment balance tasks to generate in one round
	segmentBatchSize int
	// channelBatchSize specifies the maximum number of channel balance tasks to generate in one round
	channelBatchSize int
	// balanceOnMultipleCollections determines whether to balance multiple collections in one round.
	// If false, only balance one collection at a time to avoid resource contention
	balanceOnMultipleCollections bool
	// maxCheckCollectionCount limits the maximum number of collections to check in one round
	// to prevent long-running balance operations
	maxCheckCollectionCount int
	// autoBalanceInterval controls how frequently automatic balance operations are triggered
	autoBalanceInterval time.Duration
	// segmentTaskTimeout specifies the timeout for segment balance tasks
	segmentTaskTimeout time.Duration
	// channelTaskTimeout specifies the timeout for channel balance tasks
	channelTaskTimeout time.Duration
}

// This method fetches all balance-related configuration parameters from the global
// parameter table and returns a balanceConfig struct for use in balance operations.
func (b *BalanceChecker) loadBalanceConfig() balanceConfig {
	return balanceConfig{
		segmentBatchSize:             paramtable.Get().QueryCoordCfg.BalanceSegmentBatchSize.GetAsInt(),
		channelBatchSize:             paramtable.Get().QueryCoordCfg.BalanceChannelBatchSize.GetAsInt(),
		balanceOnMultipleCollections: paramtable.Get().QueryCoordCfg.EnableBalanceOnMultipleCollections.GetAsBool(),
		maxCheckCollectionCount:      paramtable.Get().QueryCoordCfg.BalanceCheckCollectionMaxCount.GetAsInt(),
		autoBalanceInterval:          paramtable.Get().QueryCoordCfg.AutoBalanceInterval.GetAsDuration(time.Millisecond),
		segmentTaskTimeout:           paramtable.Get().QueryCoordCfg.SegmentTaskTimeout.GetAsDuration(time.Millisecond),
		channelTaskTimeout:           paramtable.Get().QueryCoordCfg.ChannelTaskTimeout.GetAsDuration(time.Millisecond),
	}
}

// collectionBalanceItem represents a collection in the balance priority queue.
// Each item contains collection metadata and is used to determine the order
// in which collections should be processed for balance operations.
type collectionBalanceItem struct {
	*balance.BaseItem
	balancePriority int

	// collectionID and rowCount are used to calculate the priority
	collectionID int64
	rowCount     int
	sortOrder    string
}

// The priority is determined by the BalanceTriggerOrder configuration:
//   - "byrowcount": Higher row count collections get higher priority (processed first)
//   - "bycollectionid": Collections with smaller IDs get higher priority
func newCollectionBalanceItem(collectionID int64, rowCount int, sortOrder string) *collectionBalanceItem {
	priority := 0
	if sortOrder == "bycollectionid" {
		priority = int(collectionID)
	} else {
		priority = -rowCount
	}

	return &collectionBalanceItem{
		BaseItem:        &balance.BaseItem{},
		collectionID:    collectionID,
		rowCount:        rowCount,
		sortOrder:       sortOrder,
		balancePriority: priority,
	}
}

func (c *collectionBalanceItem) getPriority() int {
	return c.balancePriority
}

func (c *collectionBalanceItem) setPriority(priority int) {
	c.balancePriority = priority
}

// BalanceChecker checks the cluster distribution and generates balance tasks.
// It is responsible for monitoring the load distribution across query nodes and
// generating segment/channel move tasks to maintain optimal balance.
//
// The BalanceChecker operates in two modes:
//  1. Stopping Balance: High-priority balance for nodes that are being stopped or read-only nodes
//  2. Normal Balance: Regular automatic balance operations to optimize cluster performance
//
// Both modes use priority queues to determine the order in which collections are processed.
type BalanceChecker struct {
	*checkerActivation
	meta        *meta.Meta
	nodeManager *session.NodeManager
	scheduler   task.Scheduler
	targetMgr   meta.TargetManagerInterface
	// getBalancerFunc returns the appropriate balancer for generating balance plans
	getBalancerFunc GetBalancerFunc

	// normalBalanceQueue maintains collections pending normal balance operations,
	// ordered by priority (row count or collection ID)
	normalBalanceQueue *balance.PriorityQueue
	// stoppingBalanceQueue maintains collections pending stopping balance operations,
	// used when nodes are being gracefully stopped
	stoppingBalanceQueue *balance.PriorityQueue

	// autoBalanceTs records the timestamp of the last auto balance operation
	// to ensure balance operations don't happen too frequently
	autoBalanceTs time.Time
}

func NewBalanceChecker(meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	nodeMgr *session.NodeManager,
	scheduler task.Scheduler,
	getBalancerFunc GetBalancerFunc,
) *BalanceChecker {
	return &BalanceChecker{
		checkerActivation:    newCheckerActivation(),
		meta:                 meta,
		targetMgr:            targetMgr,
		nodeManager:          nodeMgr,
		normalBalanceQueue:   balance.NewPriorityQueuePtr(),
		stoppingBalanceQueue: balance.NewPriorityQueuePtr(),
		scheduler:            scheduler,
		getBalancerFunc:      getBalancerFunc,
	}
}

func (b *BalanceChecker) ID() utils.CheckerType {
	return utils.BalanceChecker
}

func (b *BalanceChecker) Description() string {
	return "BalanceChecker checks the cluster distribution and generates balance tasks"
}

// readyToCheck determines if a collection is ready for balance operations.
// A collection is considered ready if:
//  1. It exists in the metadata
//  2. It has either a current target or next target defined
//
// Returns true if the collection is ready for balance operations.
func (b *BalanceChecker) readyToCheck(ctx context.Context, collectionID int64) bool {
	metaExist := (b.meta.GetCollection(ctx, collectionID) != nil)
	targetExist := b.targetMgr.IsNextTargetExist(ctx, collectionID) || b.targetMgr.IsCurrentTargetExist(ctx, collectionID, common.AllPartitionsID)

	return metaExist && targetExist
}

type ReadyForBalanceFilter func(ctx context.Context, collectionID int64) bool

// filterCollectionForBalance filters all collections using the provided filter functions.
// Only collections that pass ALL filter criteria will be included in the result.
// This is used to select collections eligible for balance operations based on
// various conditions like load status, target readiness, etc.
// Returns a slice of collection IDs that pass all filter criteria.
func (b *BalanceChecker) filterCollectionForBalance(ctx context.Context, filter ...ReadyForBalanceFilter) []int64 {
	ids := b.meta.GetAll(ctx)
	ret := make([]int64, 0)
	for _, cid := range ids {
		shouldInclude := true
		for _, f := range filter {
			if !f(ctx, cid) {
				shouldInclude = false
				break
			}
		}
		if shouldInclude {
			ret = append(ret, cid)
		}
	}
	return ret
}

// constructStoppingBalanceQueue creates and populates the stopping balance priority queue.
// This queue contains collections that need balance operations due to nodes being stopped.
// Collections are ordered by priority (row count or collection ID based on configuration).
//
// Returns a new priority queue with all eligible collections for stopping balance.
// Note: cause stopping balance need to move out all data from the node, so we need to check all collections.
func (b *BalanceChecker) constructStoppingBalanceQueue(ctx context.Context) *balance.PriorityQueue {
	sortOrder := strings.ToLower(Params.QueryCoordCfg.BalanceTriggerOrder.GetValue())
	if sortOrder == "" {
		sortOrder = "byrowcount" // Default to ByRowCount
	}

	ret := b.filterCollectionForBalance(ctx, b.readyToCheck)
	pq := balance.NewPriorityQueuePtr()
	for _, cid := range ret {
		rowCount := b.targetMgr.GetCollectionRowCount(ctx, cid, meta.CurrentTargetFirst)
		item := newCollectionBalanceItem(cid, int(rowCount), sortOrder)
		pq.Push(item)
	}
	b.stoppingBalanceQueue = pq
	return pq
}

// constructNormalBalanceQueue creates and populates the normal balance priority queue.
// This queue contains loaded collections that are ready for regular balance operations.
// Collections must meet multiple criteria:
//  1. Be ready for balance operations (metadata and target exist)
//  2. Have loaded status (actively serving queries)
//  3. Have current target ready (consistent state)
//
// Returns a new priority queue with all eligible collections for normal balance.
func (b *BalanceChecker) constructNormalBalanceQueue(ctx context.Context) *balance.PriorityQueue {
	filterLoadedCollections := func(ctx context.Context, cid int64) bool {
		collection := b.meta.GetCollection(ctx, cid)
		return collection != nil && collection.GetStatus() == querypb.LoadStatus_Loaded
	}

	filterTargetReadyCollections := func(ctx context.Context, cid int64) bool {
		return b.targetMgr.IsCurrentTargetReady(ctx, cid)
	}

	sortOrder := strings.ToLower(Params.QueryCoordCfg.BalanceTriggerOrder.GetValue())
	if sortOrder == "" {
		sortOrder = "byrowcount" // Default to ByRowCount
	}

	ret := b.filterCollectionForBalance(ctx, b.readyToCheck, filterLoadedCollections, filterTargetReadyCollections)
	pq := balance.NewPriorityQueuePtr()
	for _, cid := range ret {
		rowCount := b.targetMgr.GetCollectionRowCount(ctx, cid, meta.CurrentTargetFirst)
		item := newCollectionBalanceItem(cid, int(rowCount), sortOrder)
		pq.Push(item)
	}
	b.normalBalanceQueue = pq
	return pq
}

// getReplicaForStoppingBalance returns replicas that need stopping balance operations.
// A replica needs stopping balance if it has:
//  1. Read-only (RO) nodes that need to be drained
//  2. Read-only streaming query (ROSQ) nodes that need to be drained
//  3. Channel read-only nodes when streaming service is enabled
//
// These replicas need immediate attention to move data off nodes that are being stopped.
//
// Returns a slice of replica IDs that need stopping balance operations.
func (b *BalanceChecker) getReplicaForStoppingBalance(ctx context.Context, collectionID int64) []int64 {
	filterReplicaWithRONodes := func(replica *meta.Replica, _ int) bool {
		channelRONodes := make([]int64, 0)
		if streamingutil.IsStreamingServiceEnabled() {
			_, channelRONodes = utils.GetChannelRWAndRONodesFor260(replica, b.nodeManager)
		}
		return replica.RONodesCount()+replica.ROSQNodesCount() > 0 || len(channelRONodes) > 0
	}

	// filter replicas with RONodes or channelRONodes
	replicas := b.meta.ReplicaManager.GetByCollection(ctx, collectionID)
	ret := make([]int64, 0)
	for _, replica := range replicas {
		if filterReplicaWithRONodes(replica, 0) {
			ret = append(ret, replica.GetID())
		}
	}
	return ret
}

// getReplicaForNormalBalance returns all replicas for a collection for normal balance operations.
// Unlike stopping balance, normal balance considers all replicas regardless of their node status.
// This allows for comprehensive load balancing across the entire collection.
//
// Returns a slice of all replica IDs for the collection.
func (b *BalanceChecker) getReplicaForNormalBalance(ctx context.Context, collectionID int64) []int64 {
	replicas := b.meta.ReplicaManager.GetByCollection(ctx, collectionID)
	return lo.Map(replicas, func(replica *meta.Replica, _ int) int64 {
		return replica.GetID()
	})
}

// generateBalanceTasksFromReplicas generates balance tasks for the given replicas.
// This method is the core of the balance operation that:
//  1. Uses the balancer to create segment and channel assignment plans
//  2. Converts these plans into executable tasks
//  3. Sets appropriate priorities and reasons for the tasks
//
// The process involves:
//   - Getting balance plans from the configured balancer for each replica
//   - Creating segment move tasks from segment assignment plans
//   - Creating channel move tasks from channel assignment plans
//   - Setting task metadata (priority, reason, timeout)
//
// Returns:
//   - segmentTasks: tasks for moving segments between nodes
//   - channelTasks: tasks for moving channels between nodes
func (b *BalanceChecker) generateBalanceTasksFromReplicas(ctx context.Context, replicas []int64, config balanceConfig) ([]task.Task, []task.Task) {
	if len(replicas) == 0 {
		return nil, nil
	}

	segmentPlans, channelPlans := make([]balance.SegmentAssignPlan, 0), make([]balance.ChannelAssignPlan, 0)
	for _, rid := range replicas {
		replica := b.meta.ReplicaManager.Get(ctx, rid)
		if replica == nil {
			continue
		}
		sPlans, cPlans := b.getBalancerFunc().BalanceReplica(ctx, replica)
		segmentPlans = append(segmentPlans, sPlans...)
		channelPlans = append(channelPlans, cPlans...)
		if len(segmentPlans) != 0 || len(channelPlans) != 0 {
			balance.PrintNewBalancePlans(replica.GetCollectionID(), replica.GetID(), sPlans, cPlans)
		}
	}

	segmentTasks := make([]task.Task, 0)
	channelTasks := make([]task.Task, 0)
	// Create segment tasks with error handling
	if len(segmentPlans) > 0 {
		tasks := balance.CreateSegmentTasksFromPlans(ctx, b.ID(), config.segmentTaskTimeout, segmentPlans)
		if len(tasks) > 0 {
			task.SetPriority(task.TaskPriorityLow, tasks...)
			task.SetReason("segment unbalanced", tasks...)
			segmentTasks = append(segmentTasks, tasks...)
		}
	}

	// Create channel tasks with error handling
	if len(channelPlans) > 0 {
		tasks := balance.CreateChannelTasksFromPlans(ctx, b.ID(), config.channelTaskTimeout, channelPlans)
		if len(tasks) > 0 {
			task.SetReason("channel unbalanced", tasks...)
			channelTasks = append(channelTasks, tasks...)
		}
	}

	return segmentTasks, channelTasks
}

// processBalanceQueue processes balance queue with common logic for both normal and stopping balance.
// This is a template method that implements the core queue processing algorithm while allowing
// different balance types to provide their own specific logic through function parameters.
//
// The method implements several safeguards:
//  1. Batch size limits to prevent generating too many tasks at once
//  2. Collection count limits to prevent long-running operations
//  3. Multi-collection balance control to avoid resource contention
//
// Processing flow:
//  1. Get or construct the priority queue for collections
//  2. Pop collections from queue in priority order
//  3. Get replicas that need balance for the collection
//  4. Generate balance tasks for those replicas
//  5. Accumulate tasks until batch limits are reached
//
// Parameters:
//   - ctx: context for the operation
//   - getReplicasFunc: function to get replicas for a collection (normal vs stopping)
//   - constructQueueFunc: function to construct a new priority queue if needed
//   - getQueueFunc: function to get the existing priority queue
//   - config: balance configuration with batch sizes and limits
//
// Returns:
//   - generatedSegmentTaskNum: number of generated segment balance tasks
//   - generatedChannelTaskNum: number of generated channel balance tasks
func (b *BalanceChecker) processBalanceQueue(
	ctx context.Context,
	getReplicasFunc func(context.Context, int64) []int64,
	constructQueueFunc func(context.Context) *balance.PriorityQueue,
	getQueueFunc func() *balance.PriorityQueue,
	config balanceConfig,
) (int, int) {
	checkCollectionCount := 0
	pq := getQueueFunc()
	if pq == nil || pq.Len() == 0 {
		pq = constructQueueFunc(ctx)
	}

	generatedSegmentTaskNum := 0
	generatedChannelTaskNum := 0

	for generatedSegmentTaskNum < config.segmentBatchSize &&
		generatedChannelTaskNum < config.channelBatchSize &&
		checkCollectionCount < config.maxCheckCollectionCount &&
		pq.Len() > 0 {
		// Break if balanceOnMultipleCollections is disabled and we already have tasks
		if !config.balanceOnMultipleCollections && (generatedSegmentTaskNum > 0 || generatedChannelTaskNum > 0) {
			log.Debug("Balance on multiple collections disabled, stopping after first collection")
			break
		}

		item := pq.Pop().(*collectionBalanceItem)
		checkCollectionCount++

		replicasToBalance := getReplicasFunc(ctx, item.collectionID)
		if len(replicasToBalance) == 0 {
			continue
		}

		newSegmentTasks, newChannelTasks := b.generateBalanceTasksFromReplicas(ctx, replicasToBalance, config)
		generatedSegmentTaskNum += len(newSegmentTasks)
		generatedChannelTaskNum += len(newChannelTasks)
		b.submitTasks(newSegmentTasks, newChannelTasks)
	}
	return generatedSegmentTaskNum, generatedChannelTaskNum
}

// submitTasks submits the generated balance tasks to the scheduler for execution.
// This method handles the final step of the balance process by adding all
// generated tasks to the task scheduler, which will execute them asynchronously.
func (b *BalanceChecker) submitTasks(segmentTasks, channelTasks []task.Task) {
	for _, task := range segmentTasks {
		b.scheduler.Add(task)
	}

	for _, task := range channelTasks {
		b.scheduler.Add(task)
	}
}

// Check is the main entry point for balance operations.
// This method implements a two-phase balance strategy with clear priorities:
//
// **Phase 1: Stopping Balance (Higher Priority)**
// - Handles nodes that are being gracefully stopped
// - Moves data off read-only nodes to active nodes
// - Critical for maintaining service availability during node shutdowns
// - Runs immediately when stopping nodes are detected
//
// **Phase 2: Normal Balance (Lower Priority)**
// - Performs regular load balancing to optimize cluster performance
// - Runs periodically based on autoBalanceInterval configuration
// - Considers all collections and distributes load evenly
// - Skipped if stopping balance tasks were generated
//
// **Key Design Decisions:**
//  1. Tasks are submitted directly to scheduler and nil is returned
//     (unlike other checkers that return tasks to caller)
//  2. Stopping balance always takes precedence over normal balance
//  3. Performance monitoring alerts for operations > 100ms
//  4. Configuration is loaded fresh each time to respect dynamic updates
//
// **Return Value:**
// Always returns nil because tasks are submitted directly to the scheduler.
// This design allows the balance checker to handle multiple collections
// and large numbers of tasks efficiently.
//
// **Performance Monitoring:**
// The method tracks execution time and logs warnings for slow operations
// to help identify performance bottlenecks in large clusters.
func (b *BalanceChecker) Check(ctx context.Context) []task.Task {
	// Skip balance operations if the checker is not active
	if !b.IsActive() {
		return nil
	}

	// Performance monitoring: track execution time
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		if duration > 100*time.Millisecond {
			log.Info("Balance check too slow", zap.Duration("duration", duration))
		}
	}()

	// Load current configuration to respect dynamic parameter changes
	config := b.loadBalanceConfig()

	// Phase 1: Process stopping balance first (higher priority)
	// This handles nodes that are being gracefully stopped and need immediate attention
	if paramtable.Get().QueryCoordCfg.EnableStoppingBalance.GetAsBool() {
		generatedSegmentTaskNum, generatedChannelTaskNum := b.processBalanceQueue(ctx,
			b.getReplicaForStoppingBalance,
			b.constructStoppingBalanceQueue,
			func() *balance.PriorityQueue { return b.stoppingBalanceQueue },
			config)

		if generatedSegmentTaskNum > 0 || generatedChannelTaskNum > 0 {
			// clean up the normal balance queue when stopping balance generated tasks
			// make sure that next time when trigger normal balance, a new normal balance round will be started
			b.normalBalanceQueue = nil

			return nil
		}
	}

	// Phase 2: Process normal balance if no stopping balance was needed
	// This handles regular load balancing operations for cluster optimization
	if paramtable.Get().QueryCoordCfg.AutoBalance.GetAsBool() {
		// Respect the auto balance interval to prevent too frequent operations
		if time.Since(b.autoBalanceTs) <= config.autoBalanceInterval {
			return nil
		}

		generatedSegmentTaskNum, generatedChannelTaskNum := b.processBalanceQueue(ctx,
			b.getReplicaForNormalBalance,
			b.constructNormalBalanceQueue,
			func() *balance.PriorityQueue { return b.normalBalanceQueue },
			config)

		// Submit normal balance tasks if any were generated
		// Update the auto balance timestamp to enforce the interval
		if generatedSegmentTaskNum > 0 || generatedChannelTaskNum > 0 {
			b.autoBalanceTs = time.Now()

			// clean up the stopping balance queue when normal balance generated tasks
			// make sure that next time when trigger stopping balance, a new stopping balance round will be started
			b.stoppingBalanceQueue = nil
		}
	}

	// Always return nil as tasks are submitted directly to scheduler
	return nil
}
