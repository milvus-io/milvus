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
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

// balanceEpochConfig is frozen once at the beginning of each checker tick.
// Task 7 intentionally keeps the production provider disabled; Task 8 loads
// these values from paramtable and freezes policy selection for the tick.
type balanceEpochConfig struct {
	enabled            bool
	shadow             bool
	budget             balance.BalanceWaveBudget
	deadline           time.Duration
	noProgressDeadline time.Duration
	segmentTaskTimeout time.Duration
	channelTaskTimeout time.Duration
	maxObjectRetries   int
	quarantineBackoff  time.Duration
	policyConfig       balance.EpochPolicyConfig
}

type balanceCheckerOption func(*BalanceChecker)

func withEpochControllerForTest(controller balance.BalanceEpochController) balanceCheckerOption {
	return func(checker *BalanceChecker) {
		checker.epochManager = controller
	}
}

func withEpochConfigForTest(config balanceEpochConfig) balanceCheckerOption {
	return func(checker *BalanceChecker) {
		checker.epochConfigProvider = func() balanceEpochConfig {
			return config
		}
	}
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
	*assign.BaseItem
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
		BaseItem:        &assign.BaseItem{},
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
	dist        *meta.DistributionManager
	nodeManager *session.NodeManager
	scheduler   task.Scheduler
	targetMgr   meta.TargetManagerInterface

	// normalBalanceQueue maintains collections pending normal balance operations,
	// ordered by priority (row count or collection ID)
	normalBalanceQueue *assign.PriorityQueue
	// stoppingBalanceQueue maintains collections pending stopping balance operations,
	// used when nodes are being gracefully stopped
	stoppingBalanceQueue *assign.PriorityQueue

	// autoBalanceTs records the timestamp of the last auto balance operation
	// to ensure balance operations don't happen too frequently
	autoBalanceTs time.Time

	epochManager        balance.BalanceEpochController
	epochConfigProvider func() balanceEpochConfig

	// normalEpochCollectionCursor is the last collection selected for an epoch
	// request set. It persists across checker ticks to avoid starving collections
	// outside the configured per-check window.
	normalEpochCollectionCursor    int64
	normalEpochCollectionCursorSet bool
}

func NewBalanceChecker(meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr meta.TargetManagerInterface,
	nodeMgr *session.NodeManager,
	scheduler task.Scheduler,
) *BalanceChecker {
	return newBalanceChecker(meta, dist, targetMgr, nodeMgr, scheduler)
}

func newBalanceChecker(
	meta *meta.Meta,
	dist *meta.DistributionManager,
	targetMgr meta.TargetManagerInterface,
	nodeMgr *session.NodeManager,
	scheduler task.Scheduler,
	opts ...balanceCheckerOption,
) *BalanceChecker {
	checker := &BalanceChecker{
		checkerActivation:    newCheckerActivation(),
		meta:                 meta,
		dist:                 dist,
		targetMgr:            targetMgr,
		nodeManager:          nodeMgr,
		normalBalanceQueue:   assign.NewPriorityQueuePtr(),
		stoppingBalanceQueue: assign.NewPriorityQueuePtr(),
		scheduler:            scheduler,
		epochConfigProvider: func() balanceEpochConfig {
			return balanceEpochConfig{}
		},
	}

	admitter, admitsGenerations := scheduler.(task.BalanceTaskGenerationAdmitter)
	inspector, inspectsPending := scheduler.(task.BalanceTaskInspector)
	if admitsGenerations && inspectsPending {
		checker.epochManager = balance.NewBalanceEpochManager(
			meta,
			dist,
			targetMgr,
			nodeMgr,
			scheduler,
			admitter,
			inspector,
			checker.ID(),
			balance.GetGlobalBalancerFactory().GetEpochPolicy,
		)
	}

	for _, opt := range opts {
		opt(checker)
	}
	return checker
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
func (b *BalanceChecker) constructStoppingBalanceQueue(ctx context.Context) *assign.PriorityQueue {
	sortOrder := strings.ToLower(Params.QueryCoordCfg.BalanceTriggerOrder.GetValue())
	if sortOrder == "" {
		sortOrder = "byrowcount" // Default to ByRowCount
	}

	ret := b.filterCollectionForBalance(ctx, b.readyToCheck)
	pq := assign.NewPriorityQueuePtr()
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
//  4. Be serviceable (loaded status, ensures consistency with segment_checker and channel_checker)
//
// Returns a new priority queue with all eligible collections for normal balance.
func (b *BalanceChecker) eligibleNormalBalanceCollections(ctx context.Context) []int64 {
	filterLoadedCollections := func(ctx context.Context, cid int64) bool {
		collection := b.meta.GetCollection(ctx, cid)
		return collection != nil && collection.GetStatus() == querypb.LoadStatus_Loaded
	}

	filterTargetReadyCollections := func(ctx context.Context, cid int64) bool {
		return b.targetMgr.IsCurrentTargetReady(ctx, cid)
	}

	// filter out collection which is not serviceable
	// cause segment_checker and channel checker use different assign policy
	filterServiceableCollections := func(ctx context.Context, cid int64) bool {
		// Get all channels for this collection from distribution
		channels := b.dist.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(cid))
		if len(channels) == 0 {
			// No channels in distribution means collection is not ready
			return false
		}
		// Check if ALL channels are serviceable
		for _, channel := range channels {
			if !channel.IsServiceable() {
				return false
			}
		}
		return true
	}

	return b.filterCollectionForBalance(ctx, b.readyToCheck, filterLoadedCollections, filterTargetReadyCollections, filterServiceableCollections)
}

func (b *BalanceChecker) constructNormalBalanceQueue(ctx context.Context) *assign.PriorityQueue {
	sortOrder := strings.ToLower(Params.QueryCoordCfg.BalanceTriggerOrder.GetValue())
	if sortOrder == "" {
		sortOrder = "byrowcount" // Default to ByRowCount
	}

	ret := b.eligibleNormalBalanceCollections(ctx)
	pq := assign.NewPriorityQueuePtr()
	for _, cid := range ret {
		rowCount := b.targetMgr.GetCollectionRowCount(ctx, cid, meta.CurrentTargetFirst)
		item := newCollectionBalanceItem(cid, int(rowCount), sortOrder)
		pq.Push(item)
	}
	b.normalBalanceQueue = pq
	return pq
}

func (b *BalanceChecker) selectNormalEpochCollections(collectionIDs []int64, config balanceConfig) []int64 {
	if len(collectionIDs) == 0 || config.maxCheckCollectionCount <= 0 {
		return nil
	}

	sortedCollectionIDs := append([]int64(nil), collectionIDs...)
	sort.Slice(sortedCollectionIDs, func(i, j int) bool {
		return sortedCollectionIDs[i] < sortedCollectionIDs[j]
	})
	limit := config.maxCheckCollectionCount
	if !config.balanceOnMultipleCollections {
		limit = 1
	}
	if limit > len(sortedCollectionIDs) {
		limit = len(sortedCollectionIDs)
	}

	start := 0
	if b.normalEpochCollectionCursorSet {
		start = sort.Search(len(sortedCollectionIDs), func(i int) bool {
			return sortedCollectionIDs[i] > b.normalEpochCollectionCursor
		})
		if start == len(sortedCollectionIDs) {
			start = 0
		}
	}

	selected := make([]int64, 0, limit)
	for i := 0; i < limit; i++ {
		selected = append(selected, sortedCollectionIDs[(start+i)%len(sortedCollectionIDs)])
	}
	return selected
}

func (b *BalanceChecker) collectNormalEpochRequests(
	ctx context.Context,
	config balanceConfig,
	epochConfig balanceEpochConfig,
) []balance.EpochRequest {
	collectionIDs := b.eligibleNormalBalanceCollections(ctx)
	selectedCollectionIDs := b.selectNormalEpochCollections(collectionIDs, config)
	if len(selectedCollectionIDs) == 0 {
		return nil
	}

	replicasByResourceGroup := make(map[string][]int64)
	for _, collectionID := range selectedCollectionIDs {
		// ReplicaManager owns the returned slice. Extract IDs into checker-owned
		// storage and sort only those copies below.
		for _, replica := range b.meta.GetByCollection(ctx, collectionID) {
			if replica == nil {
				continue
			}
			resourceGroup := replica.GetResourceGroup()
			replicasByResourceGroup[resourceGroup] = append(replicasByResourceGroup[resourceGroup], replica.GetID())
		}
	}
	b.normalEpochCollectionCursor = selectedCollectionIDs[len(selectedCollectionIDs)-1]
	b.normalEpochCollectionCursorSet = true
	if len(replicasByResourceGroup) == 0 {
		return nil
	}

	resourceGroups := make([]string, 0, len(replicasByResourceGroup))
	for resourceGroup := range replicasByResourceGroup {
		resourceGroups = append(resourceGroups, resourceGroup)
	}
	sort.Strings(resourceGroups)

	requests := make([]balance.EpochRequest, 0, len(resourceGroups))
	for _, resourceGroup := range resourceGroups {
		replicaIDs := append([]int64(nil), replicasByResourceGroup[resourceGroup]...)
		sort.Slice(replicaIDs, func(i, j int) bool {
			return replicaIDs[i] < replicaIDs[j]
		})
		requests = append(requests, balance.EpochRequest{
			ResourceGroup:      resourceGroup,
			EligibleReplicaIDs: replicaIDs,
			Budget:             epochConfig.budget,
			PolicyConfig:       epochConfig.policyConfig,
			AllowNew:           true,
			Shadow:             epochConfig.shadow,
			Deadline:           epochConfig.deadline,
			NoProgressDeadline: epochConfig.noProgressDeadline,
			SegmentTaskTimeout: epochConfig.segmentTaskTimeout,
			ChannelTaskTimeout: epochConfig.channelTaskTimeout,
			MaxObjectRetries:   epochConfig.maxObjectRetries,
			QuarantineBackoff:  epochConfig.quarantineBackoff,
		})
	}

	return requests
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
	replicas := b.meta.GetByCollection(ctx, collectionID)
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
	replicas := b.meta.GetByCollection(ctx, collectionID)
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
// Parameters:
//   - isStoppingBalance: if true, uses HIGH load priority for stopping balance (node draining);
//     otherwise uses LOW priority for normal balance operations
//
// Returns:
//   - segmentTasks: tasks for moving segments between nodes
//   - channelTasks: tasks for moving channels between nodes
func (b *BalanceChecker) generateBalanceTasksFromReplicas(ctx context.Context, balancer balance.Balance, replicas []int64, config balanceConfig, isStoppingBalance bool) ([]task.Task, []task.Task) {
	if len(replicas) == 0 {
		return nil, nil
	}

	segmentPlans, channelPlans := make([]assign.SegmentAssignPlan, 0), make([]assign.ChannelAssignPlan, 0)
	for _, rid := range replicas {
		replica := b.meta.Get(ctx, rid)
		if replica == nil {
			continue
		}
		sPlans, cPlans := balancer.BalanceReplica(ctx, replica)
		segmentPlans = append(segmentPlans, sPlans...)
		channelPlans = append(channelPlans, cPlans...)
		if len(segmentPlans) != 0 || len(channelPlans) != 0 {
			balance.PrintNewBalancePlans(replica.GetCollectionID(), replica.GetID(), sPlans, cPlans)
		}
	}

	// Set LoadPriority based on balance type:
	// - Stopping balance (node draining): HIGH priority to quickly move data off stopping nodes
	// - Normal balance: LOW priority to avoid interfering with user operations
	loadPriority := commonpb.LoadPriority_LOW
	if isStoppingBalance {
		loadPriority = commonpb.LoadPriority_HIGH
	}
	for i := range segmentPlans {
		segmentPlans[i].LoadPriority = loadPriority
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
//   - isStoppingBalance: if true, uses HIGH load priority for stopping balance
//
// Returns:
//   - acceptedSegmentTaskNum: number of segment balance tasks accepted by the scheduler
//   - acceptedChannelTaskNum: number of channel balance tasks accepted by the scheduler
func (b *BalanceChecker) processBalanceQueue(
	ctx context.Context,
	balancer balance.Balance,
	getReplicasFunc func(context.Context, int64) []int64,
	constructQueueFunc func(context.Context) *assign.PriorityQueue,
	getQueueFunc func() *assign.PriorityQueue,
	config balanceConfig,
	isStoppingBalance bool,
) (int, int) {
	checkCollectionCount := 0
	pq := getQueueFunc()
	if pq == nil || pq.Len() == 0 {
		pq = constructQueueFunc(ctx)
	}

	acceptedSegmentTaskNum := 0
	acceptedChannelTaskNum := 0
	for acceptedSegmentTaskNum < config.segmentBatchSize &&
		acceptedChannelTaskNum < config.channelBatchSize &&
		checkCollectionCount < config.maxCheckCollectionCount &&
		pq.Len() > 0 {
		// Break if balanceOnMultipleCollections is disabled and the scheduler
		// accepted work for a previous collection.
		if !config.balanceOnMultipleCollections && (acceptedSegmentTaskNum > 0 || acceptedChannelTaskNum > 0) {
			mlog.Debug(ctx, "Balance on multiple collections disabled, stopping after first collection")
			break
		}

		item := pq.Pop().(*collectionBalanceItem)
		checkCollectionCount++

		replicasToBalance := getReplicasFunc(ctx, item.collectionID)
		if len(replicasToBalance) == 0 {
			continue
		}

		newSegmentTasks, newChannelTasks := b.generateBalanceTasksFromReplicas(ctx, balancer, replicasToBalance, config, isStoppingBalance)
		acceptedSegments, acceptedChannels := b.submitTasks(newSegmentTasks, newChannelTasks)
		acceptedSegmentTaskNum += acceptedSegments
		acceptedChannelTaskNum += acceptedChannels
	}
	return acceptedSegmentTaskNum, acceptedChannelTaskNum
}

// submitTasks submits the generated balance tasks to the scheduler for execution.
// This method handles the final step of the balance process by adding all
// generated tasks to the task scheduler, which will execute them asynchronously.
func (b *BalanceChecker) submitTasks(segmentTasks, channelTasks []task.Task) (int, int) {
	acceptedSegments := 0
	for _, balanceTask := range segmentTasks {
		if err := b.scheduler.Add(balanceTask); err != nil {
			b.logLegacyBalanceAdmissionError(balanceTask, "segment", err)
			continue
		}
		acceptedSegments++
	}

	acceptedChannels := 0
	for _, balanceTask := range channelTasks {
		if err := b.scheduler.Add(balanceTask); err != nil {
			b.logLegacyBalanceAdmissionError(balanceTask, "channel", err)
			continue
		}
		acceptedChannels++
	}
	return acceptedSegments, acceptedChannels
}

func (b *BalanceChecker) logLegacyBalanceAdmissionError(balanceTask task.Task, kind string, err error) {
	object := balanceTask.Shard()
	switch typed := balanceTask.(type) {
	case *task.SegmentTask:
		object = fmt.Sprintf("segment:%d", typed.SegmentID())
	case *task.ChannelTask:
		object = fmt.Sprintf("channel:%s", typed.Channel())
	}
	mlog.Warn(balanceTask.Context(), "failed to submit legacy balance task",
		mlog.Int64("collectionID", balanceTask.CollectionID()),
		mlog.Int64("replicaID", balanceTask.ReplicaID()),
		mlog.String("taskKind", kind),
		mlog.String("object", object),
		mlog.String("taskIndex", balanceTask.Index()),
		mlog.Err(err))
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
	// Performance monitoring: track execution time
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		if duration > 100*time.Millisecond {
			mlog.Info(ctx, "Balance check too slow", mlog.Duration("duration", duration))
		}
	}()

	// Freeze configuration once for the checker tick. Task 8 replaces the
	// disabled epoch provider with paramtable-backed values.
	config := b.loadBalanceConfig()
	epochConfig := b.epochConfigProvider()

	// Phase 1: Process stopping balance first (higher priority).
	acceptedStoppingSegments, acceptedStoppingChannels := 0, 0
	if paramtable.Get().QueryCoordCfg.EnableStoppingBalance.GetAsBool() {
		acceptedStoppingSegments, acceptedStoppingChannels = b.processBalanceQueue(ctx,
			balance.GetGlobalBalancerFactory().GetStoppingBalancer(),
			b.getReplicaForStoppingBalance,
			b.constructStoppingBalanceQueue,
			func() *assign.PriorityQueue { return b.stoppingBalanceQueue },
			config,
			true, // isStoppingBalance: use HIGH priority for node draining
		)

		if acceptedStoppingSegments > 0 || acceptedStoppingChannels > 0 {
			// Accepted stopping work invalidates pending normal legacy work.
			b.normalBalanceQueue = nil
		}
	}

	// Existing epochs are observed independently of feature enablement, checker
	// activation, and auto-balance settings. Capture and retain this set for the
	// whole tick so a generation cleared by observation cannot restart here.
	activeAtTickStart := make(map[string]struct{})
	if b.epochManager != nil {
		for _, resourceGroup := range sortedUniqueResourceGroups(b.epochManager.ActiveResourceGroups()) {
			activeAtTickStart[resourceGroup] = struct{}{}
			b.normalBalanceQueue = nil
			b.epochManager.Advance(ctx, balance.EpochRequest{
				ResourceGroup: resourceGroup,
				AllowNew:      false,
			})
		}
	}

	if acceptedStoppingSegments > 0 || acceptedStoppingChannels > 0 {
		return nil
	}

	// Normal balancing still respects checker activation and AutoBalance.
	if !b.IsActive() {
		return nil
	}
	if !paramtable.Get().QueryCoordCfg.AutoBalance.GetAsBool() {
		return nil
	}
	if time.Since(b.autoBalanceTs) <= config.autoBalanceInterval {
		return nil
	}

	postObservationActive := []string(nil)
	if b.epochManager != nil {
		postObservationActive = sortedUniqueResourceGroups(b.epochManager.ActiveResourceGroups())
		if len(postObservationActive) > 0 {
			b.normalBalanceQueue = nil
		}
	}

	epochModeEnabled := epochConfig.enabled || epochConfig.shadow
	policySupported := false
	if epochModeEnabled && b.epochManager != nil {
		_, policySupported = balance.GetGlobalBalancerFactory().GetEpochPolicy()
	}

	// Disabled, unavailable, and unsupported epoch modes drain all active RGs
	// before returning to legacy normal balance.
	if !epochModeEnabled || b.epochManager == nil || !policySupported {
		if len(postObservationActive) > 0 {
			return nil
		}
		acceptedSegments, acceptedChannels := b.processBalanceQueue(ctx,
			balance.GetGlobalBalancerFactory().GetBalancer(),
			b.getReplicaForNormalBalance,
			b.constructNormalBalanceQueue,
			func() *assign.PriorityQueue { return b.normalBalanceQueue },
			config,
			false,
		)
		if acceptedSegments > 0 || acceptedChannels > 0 {
			b.stoppingBalanceQueue = nil
		}
		b.autoBalanceTs = time.Now()
		return nil
	}

	// One interval-eligible epoch/shadow planning attempt advances the throttle
	// exactly once, including empty, converged, rejected, and error results.
	requests := b.collectNormalEpochRequests(ctx, config, epochConfig)
	for _, request := range requests {
		if _, wasActive := activeAtTickStart[request.ResourceGroup]; wasActive {
			continue
		}
		activeAtTickStart[request.ResourceGroup] = struct{}{}
		if b.epochManager.HasActive(request.ResourceGroup) {
			b.normalBalanceQueue = nil
			continue
		}
		result := b.epochManager.Advance(ctx, request)
		if result.Started || result.Admitted > 0 || b.epochManager.HasActive(request.ResourceGroup) {
			b.normalBalanceQueue = nil
		}
		if result.Admitted > 0 {
			b.stoppingBalanceQueue = nil
		}
	}
	b.autoBalanceTs = time.Now()

	// Always return nil as tasks are submitted directly to scheduler
	return nil
}

func sortedUniqueResourceGroups(resourceGroups []string) []string {
	if len(resourceGroups) == 0 {
		return nil
	}
	sorted := append([]string(nil), resourceGroups...)
	sort.Strings(sorted)
	unique := sorted[:0]
	for _, resourceGroup := range sorted {
		if len(unique) == 0 || unique[len(unique)-1] != resourceGroup {
			unique = append(unique, resourceGroup)
		}
	}
	return unique
}
