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

package task

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	. "github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	TaskTypeGrow Type = iota + 1
	TaskTypeReduce
	TaskTypeMove
	TaskTypeUpdate
	TaskTypeStatsUpdate
)

var TaskTypeName = map[Type]string{
	TaskTypeGrow:        "Grow",
	TaskTypeReduce:      "Reduce",
	TaskTypeMove:        "Move",
	TaskTypeUpdate:      "Update",
	TaskTypeStatsUpdate: "StatsUpdate",
}

type Type int32

func (t Type) String() string {
	return TaskTypeName[t]
}

type replicaSegmentIndex struct {
	// Orphan cleanup has no replica; distinguish copies on different nodes.
	NodeID    int64
	ReplicaID int64
	SegmentID int64
	IsGrowing bool
}

func NewReplicaSegmentIndex(task *SegmentTask) replicaSegmentIndex {
	isGrowing := task.Actions()[0].(*SegmentAction).GetScope() == querypb.DataScope_Streaming
	index := replicaSegmentIndex{ReplicaID: task.ReplicaID(), SegmentID: task.SegmentID(), IsGrowing: isGrowing}
	if task.ReplicaID() == -1 {
		index.NodeID = task.Actions()[0].Node()
	}
	return index
}

func NewReplicaLeaderIndex(task *LeaderTask) replicaSegmentIndex {
	return replicaSegmentIndex{
		ReplicaID: task.ReplicaID(),
		SegmentID: task.SegmentID(),
		IsGrowing: false,
	}
}

type replicaChannelIndex struct {
	NodeID    int64
	ReplicaID int64
	Channel   string
}

func NewReplicaChannelIndex(task *ChannelTask) replicaChannelIndex {
	index := replicaChannelIndex{ReplicaID: task.ReplicaID(), Channel: task.Channel()}
	if task.ReplicaID() == -1 {
		index.NodeID = task.Actions()[0].Node()
	}
	return index
}

type taskQueue struct {
	mu sync.RWMutex
	// TaskPriority -> TaskID -> Task
	buckets []map[int64]Task
}

func newTaskQueue() *taskQueue {
	buckets := make([]map[int64]Task, len(TaskPriorities))
	for i := range buckets {
		buckets[i] = make(map[int64]Task)
	}
	return &taskQueue{
		buckets: buckets,
	}
}

func (queue *taskQueue) Len() int {
	queue.mu.RLock()
	defer queue.mu.RUnlock()
	taskNum := 0
	for _, tasks := range queue.buckets {
		taskNum += len(tasks)
	}
	return taskNum
}

func (queue *taskQueue) Add(task Task) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	bucket := queue.buckets[task.Priority()]
	bucket[task.ID()] = task
}

func (queue *taskQueue) Remove(task Task) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	bucket := queue.buckets[task.Priority()]
	delete(bucket, task.ID())
}

// Snapshot copies tasks in priority order without holding the queue lock while
// callers inspect or mutate scheduler state. A snapshot can contain retired tasks.
func (queue *taskQueue) Snapshot() []Task {
	queue.mu.RLock()
	defer queue.mu.RUnlock()
	tasks := make([]Task, 0)
	for priority := len(queue.buckets) - 1; priority >= 0; priority-- {
		for _, task := range queue.buckets[priority] {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

// Range iterates a snapshot ordered by priority from high to low.
func (queue *taskQueue) Range(fn func(task Task) bool) {
	for _, task := range queue.Snapshot() {
		if !fn(task) {
			return
		}
	}
}

type nodeTaskQueue struct {
	mu sync.RWMutex
	// NodeID -> TaskPriority -> TaskID -> Task
	buckets map[int64][]map[int64]Task
}

func newNodeTaskQueue() *nodeTaskQueue {
	return &nodeTaskQueue{
		buckets: make(map[int64][]map[int64]Task),
	}
}

// Len returns the total number of unique tasks across all nodes.
// A task appearing in multiple node buckets (e.g. Move task) is counted once.
func (queue *nodeTaskQueue) Len() int {
	queue.mu.RLock()
	defer queue.mu.RUnlock()
	seen := make(map[int64]struct{})
	for _, nodeBuckets := range queue.buckets {
		for _, bucket := range nodeBuckets {
			for taskID := range bucket {
				seen[taskID] = struct{}{}
			}
		}
	}
	return len(seen)
}

// LenByNode returns the number of tasks related to the specified node.
func (queue *nodeTaskQueue) LenByNode(nodeID int64) int {
	queue.mu.RLock()
	defer queue.mu.RUnlock()
	nodeBuckets, ok := queue.buckets[nodeID]
	if !ok {
		return 0
	}
	n := 0
	for _, bucket := range nodeBuckets {
		n += len(bucket)
	}
	return n
}

func (queue *nodeTaskQueue) Add(task Task) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	for _, action := range task.Actions() {
		queue.addToBucket(action.Node(), task)
		if la, ok := action.(*LeaderAction); ok {
			queue.addToBucket(la.GetLeaderID(), task)
		}
	}
}

func (queue *nodeTaskQueue) Remove(task Task) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	for _, action := range task.Actions() {
		queue.removeFromBucket(action.Node(), task)
		if la, ok := action.(*LeaderAction); ok {
			queue.removeFromBucket(la.GetLeaderID(), task)
		}
	}
}

func (queue *nodeTaskQueue) addToBucket(nodeID int64, task Task) {
	nodeBuckets, ok := queue.buckets[nodeID]
	if !ok {
		nodeBuckets = make([]map[int64]Task, len(TaskPriorities))
		for i := range nodeBuckets {
			nodeBuckets[i] = make(map[int64]Task)
		}
		queue.buckets[nodeID] = nodeBuckets
	}
	nodeBuckets[task.Priority()][task.ID()] = task
}

func (queue *nodeTaskQueue) removeFromBucket(nodeID int64, task Task) {
	if nodeBuckets, ok := queue.buckets[nodeID]; ok {
		delete(nodeBuckets[task.Priority()], task.ID())
	}
}

// SnapshotByNode copies tasks related to nodeID in priority order.
func (queue *nodeTaskQueue) SnapshotByNode(nodeID int64) []Task {
	queue.mu.RLock()
	defer queue.mu.RUnlock()
	nodeBuckets, ok := queue.buckets[nodeID]
	if !ok {
		return nil
	}
	tasks := make([]Task, 0)
	for priority := len(nodeBuckets) - 1; priority >= 0; priority-- {
		for _, task := range nodeBuckets[priority] {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

// RangeByNode iterates a snapshot ordered by priority from high to low.
func (queue *nodeTaskQueue) RangeByNode(nodeID int64, fn func(task Task) bool) {
	for _, task := range queue.SnapshotByNode(nodeID) {
		if !fn(task) {
			return
		}
	}
}

type SegmentTaskDelta struct {
	mu sync.RWMutex
	// taskID -> segment delta records generated by the task's segment actions.
	records map[int64][]segmentDeltaRecord
}

type segmentDeltaRecord struct {
	collectionID int64
	nodeID       int64
	segmentID    int64
	channelName  string
	scope        querypb.DataScope
	actionType   ActionType
	delta        int
}

type ChannelTaskDelta struct {
	data           map[int64]map[int64]int // nodeID -> collectionID -> taskDelta
	nodeTotalDelta map[int64]int           // nodeID -> totalTaskDelta
	mu             sync.RWMutex            // Mutex to protect the map

	taskIDRecords UniqueSet
}

type SegmentTaskDeltaSnapshot struct {
	nodeDeltas           map[int64]int
	nodeCollectionDeltas map[int64]int
}

func NewSegmentTaskDeltaSnapshot(nodeDeltas, nodeCollectionDeltas map[int64]int) *SegmentTaskDeltaSnapshot {
	if nodeDeltas == nil {
		nodeDeltas = make(map[int64]int)
	}
	if nodeCollectionDeltas == nil {
		nodeCollectionDeltas = make(map[int64]int)
	}
	return &SegmentTaskDeltaSnapshot{
		nodeDeltas:           nodeDeltas,
		nodeCollectionDeltas: nodeCollectionDeltas,
	}
}

func (snapshot *SegmentTaskDeltaSnapshot) GetByNode(nodeID int64) int {
	if snapshot == nil {
		return 0
	}
	return snapshot.nodeDeltas[nodeID]
}

// GetByNodeInCollection returns the pending segment-task delta on nodeID,
// scoped to the collection used when the snapshot was built.
func (snapshot *SegmentTaskDeltaSnapshot) GetByNodeInCollection(nodeID int64) int {
	if snapshot == nil {
		return 0
	}
	return snapshot.nodeCollectionDeltas[nodeID]
}

func NewSegmentTaskDelta() *SegmentTaskDelta {
	return &SegmentTaskDelta{
		records: make(map[int64][]segmentDeltaRecord),
	}
}

func NewChannelTaskDelta() *ChannelTaskDelta {
	return &ChannelTaskDelta{
		data:           make(map[int64]map[int64]int),
		nodeTotalDelta: make(map[int64]int),
		taskIDRecords:  NewUniqueSet(),
	}
}

// Add updates the segment task delta records.
func (delta *SegmentTaskDelta) Add(task *SegmentTask) {
	delta.mu.Lock()
	defer delta.mu.Unlock()

	if _, ok := delta.records[task.ID()]; ok {
		mlog.Warn(task.Context(), "segment task already exists in delta cache",
			mlog.Int64("collectionID", task.CollectionID()),
			mlog.Int64("replicaID", task.ReplicaID()),
			mlog.Int64("taskID", task.ID()))
		return
	}

	records := make([]segmentDeltaRecord, 0, len(task.Actions()))
	for _, action := range task.Actions() {
		segmentAction, ok := action.(*SegmentAction)
		if !ok {
			continue
		}
		records = append(records, segmentDeltaRecord{
			collectionID: task.CollectionID(),
			nodeID:       segmentAction.Node(),
			segmentID:    segmentAction.GetSegmentID(),
			channelName:  segmentAction.GetShard(),
			scope:        segmentAction.GetScope(),
			actionType:   segmentAction.Type(),
			delta:        segmentAction.WorkLoadEffect(),
		})
	}
	if delta.records == nil {
		delta.records = make(map[int64][]segmentDeltaRecord)
	}
	delta.records[task.ID()] = records
}

// Sub removes the segment task delta records and reclaims the cache when idle.
func (delta *SegmentTaskDelta) Sub(task *SegmentTask) {
	delta.mu.Lock()
	defer delta.mu.Unlock()

	if _, ok := delta.records[task.ID()]; !ok {
		mlog.Warn(task.Context(), "segment task does not exist in delta cache",
			mlog.Int64("collectionID", task.CollectionID()),
			mlog.Int64("replicaID", task.ReplicaID()),
			mlog.Int64("taskID", task.ID()))
		return
	}
	delete(delta.records, task.ID())
	if len(delta.records) == 0 {
		delta.records = nil
	}
}

// GetSegmentSnapshot builds a snapshot for nodeIDs. collectionID binds the scope
// used by SegmentTaskDeltaSnapshot.GetByNodeInCollection; -1 means all collections.
func (delta *SegmentTaskDelta) GetSegmentSnapshot(nodeIDs []int64, collectionID int64, distMgr *meta.DistributionManager) *SegmentTaskDeltaSnapshot {
	snapshot := NewSegmentTaskDeltaSnapshot(make(map[int64]int, len(nodeIDs)), make(map[int64]int, len(nodeIDs)))
	nodeSet := NewUniqueSet(nodeIDs...)

	records := make([]segmentDeltaRecord, 0)
	delta.mu.RLock()
	for _, taskRecords := range delta.records {
		for _, record := range taskRecords {
			if !nodeSet.Contain(record.nodeID) {
				continue
			}
			records = append(records, record)
		}
	}
	delta.mu.RUnlock()

	for _, record := range records {
		if distMgr != nil && record.isSegmentDistMatched(distMgr) {
			continue
		}
		snapshot.nodeDeltas[record.nodeID] += record.delta
		if collectionID == -1 || record.collectionID == collectionID {
			snapshot.nodeCollectionDeltas[record.nodeID] += record.delta
		}
	}
	return snapshot
}

func (record segmentDeltaRecord) isSegmentDistMatched(distMgr *meta.DistributionManager) bool {
	if record.segmentID == 0 {
		return false
	}

	inDist := segmentInDist(distMgr, record.nodeID, record.channelName, record.segmentID, record.scope)
	switch record.actionType {
	case ActionTypeGrow:
		return inDist
	case ActionTypeReduce:
		return !inDist
	default:
		// Only grow/reduce actions affect segment presence; other segment
		// actions have zero workload effect and are treated as reflected.
		return true
	}
}

func (delta *SegmentTaskDelta) printDetailInfos() {
	delta.mu.RLock()
	defer delta.mu.RUnlock()

	if len(delta.records) > 0 {
		mlog.Info(context.TODO(), "segment task delta cache info",
			mlog.String("records", fmt.Sprintf("%+v", delta.records)),
		)
	}
}

func (delta *SegmentTaskDelta) Clear() {
	delta.mu.Lock()
	defer delta.mu.Unlock()
	delta.records = make(map[int64][]segmentDeltaRecord)
}

// Add updates the channel task delta for the given nodeID and collectionID.
func (delta *ChannelTaskDelta) Add(task *ChannelTask) {
	delta.mu.Lock()
	defer delta.mu.Unlock()

	if delta.taskIDRecords.Contain(task.ID()) {
		mlog.Warn(task.Context(), "channel task already exists in delta cache",
			mlog.Int64("collectionID", task.CollectionID()),
			mlog.Int64("replicaID", task.ReplicaID()),
			mlog.Int64("taskID", task.ID()))
		return
	}
	delta.taskIDRecords.Insert(task.ID())

	collectionID := task.CollectionID()
	for _, action := range task.Actions() {
		nodeID := action.Node()
		effect := action.WorkLoadEffect()
		if _, exists := delta.data[nodeID]; !exists {
			delta.data[nodeID] = make(map[int64]int)
		}
		delta.data[nodeID][collectionID] += effect
		delta.nodeTotalDelta[nodeID] += effect
	}
}

// Sub removes a channel task reservation and reclaims aggregate entries when idle.
func (delta *ChannelTaskDelta) Sub(task *ChannelTask) {
	delta.mu.Lock()
	defer delta.mu.Unlock()

	if !delta.taskIDRecords.Contain(task.ID()) {
		mlog.Warn(task.Context(), "channel task does not exist in delta cache",
			mlog.Int64("collectionID", task.CollectionID()),
			mlog.Int64("replicaID", task.ReplicaID()),
			mlog.Int64("taskID", task.ID()))
		return
	}
	delta.taskIDRecords.Remove(task.ID())

	collectionID := task.CollectionID()
	for _, action := range task.Actions() {
		nodeID := action.Node()
		effect := action.WorkLoadEffect()
		if _, exists := delta.data[nodeID]; !exists {
			delta.data[nodeID] = make(map[int64]int)
		}

		delta.data[nodeID][collectionID] -= effect
		delta.nodeTotalDelta[nodeID] -= effect
	}
	if delta.taskIDRecords.Len() == 0 {
		delta.data = make(map[int64]map[int64]int)
		delta.nodeTotalDelta = make(map[int64]int)
	}
}

// Get retrieves the sum of channel task delta for the given nodeID and collectionID.
// If nodeID or collectionID is -1, it matches all.
func (delta *ChannelTaskDelta) Get(nodeID, collectionID int64) int {
	delta.mu.RLock()
	defer delta.mu.RUnlock()

	if nodeID != -1 && collectionID != -1 {
		nodeData, ok := delta.data[nodeID]
		if !ok {
			return 0
		}
		return nodeData[collectionID]
	}

	if nodeID != -1 {
		return delta.nodeTotalDelta[nodeID]
	}

	var sum int
	if collectionID != -1 {
		for _, collections := range delta.data {
			sum += collections[collectionID]
		}
		return sum
	}

	for _, effect := range delta.nodeTotalDelta {
		sum += effect
	}
	return sum
}

func (delta *ChannelTaskDelta) printDetailInfos() {
	delta.mu.RLock()
	defer delta.mu.RUnlock()

	if delta.taskIDRecords.Len() > 0 {
		mlog.Info(context.TODO(), "channel task delta cache info",
			mlog.Any("taskIDRecords", delta.taskIDRecords.Collect()),
			mlog.Any("data", delta.data),
			mlog.Any("nodeTotalDelta", delta.nodeTotalDelta),
		)
	}
}

func (delta *ChannelTaskDelta) Clear() {
	delta.mu.Lock()
	defer delta.mu.Unlock()
	delta.data = make(map[int64]map[int64]int)
	delta.nodeTotalDelta = make(map[int64]int)
	delta.taskIDRecords.Clear()
}

type Scheduler interface {
	Start()
	Stop()
	AddExecutor(nodeID int64)
	RemoveExecutor(nodeID int64)
	Add(task Task) error
	Dispatch(node int64)
	RemoveByNode(node int64)
	GetChannelTaskNum(filters ...TaskFilter) int
	GetSegmentTaskNum(filters ...TaskFilter) int
	GetTasksJSON() string

	// GetSegmentTaskDeltaSnapshot returns pending segment-task deltas for nodeIDs.
	// collectionID == -1 means the collection-scoped snapshot covers all collections.
	GetSegmentTaskDeltaSnapshot(nodeIDs []int64, collectionID int64) *SegmentTaskDeltaSnapshot
	GetChannelTaskDelta(nodeID int64, collectionID int64) int
}

type taskStage uint8

const (
	taskWaiting taskStage = iota
	taskProcessing
)

// scheduledTask records scheduler ownership independently of the task's
// execution status: both waiting and processing tasks can be Started.
// Entries are replaced by value under the collection lock or lifecycle writer.
type scheduledTask struct {
	task  Task
	stage taskStage
}

type taskScheduler struct {
	ctx         context.Context
	executors   *ConcurrentMap[int64, *Executor] // NodeID -> Executor
	idAllocator func() UniqueID

	distMgr   *meta.DistributionManager
	meta      *meta.Meta
	targetMgr meta.TargetManagerInterface
	broker    meta.Broker
	cluster   session.Cluster
	nodeMgr   *session.NodeManager

	// Lock order: lifecycleMu -> collKeyLock -> queue/map/delta locks.
	// Add and Dispatch hold lifecycleMu for reading; individual registration,
	// promotion, advancement/admission and retirement hold the collection lock.
	// Stop and RemoveByNode hold the lifecycle writer instead. Queue snapshots
	// release their internal lock before acquiring a collection lock.
	lifecycleMu  sync.RWMutex
	closed       bool
	collKeyLock  *lock.KeyLock[int64]
	tasks        *ConcurrentMap[UniqueID, scheduledTask]
	segmentTasks *ConcurrentMap[replicaSegmentIndex, Task]
	channelTasks *ConcurrentMap[replicaChannelIndex, Task]
	// Physical reservations also exclude conflicting work across replica IDs.
	segmentOwners *ConcurrentMap[nodeSegmentIndex, Task]
	channelOwners *ConcurrentMap[nodeChannelIndex, Task]
	processQueue  *nodeTaskQueue
	waitQueue     *taskQueue

	taskStats            *expirable.LRU[UniqueID, Task]
	lastUpdateMetricTime atomic.Time

	segmentTaskDelta *SegmentTaskDelta
	channelTaskDelta *ChannelTaskDelta
}

func NewScheduler(ctx context.Context,
	meta *meta.Meta,
	distMgr *meta.DistributionManager,
	targetMgr meta.TargetManagerInterface,
	broker meta.Broker,
	cluster session.Cluster,
	nodeMgr *session.NodeManager,
) *taskScheduler {
	id := atomic.NewInt64(time.Now().UnixMilli())
	return &taskScheduler{
		ctx:       ctx,
		executors: NewConcurrentMap[int64, *Executor](),
		idAllocator: func() UniqueID {
			return id.Inc()
		},

		distMgr:   distMgr,
		meta:      meta,
		targetMgr: targetMgr,
		broker:    broker,
		cluster:   cluster,
		nodeMgr:   nodeMgr,

		collKeyLock:      lock.NewKeyLock[int64](),
		tasks:            NewConcurrentMap[UniqueID, scheduledTask](),
		segmentTasks:     NewConcurrentMap[replicaSegmentIndex, Task](),
		channelTasks:     NewConcurrentMap[replicaChannelIndex, Task](),
		segmentOwners:    NewConcurrentMap[nodeSegmentIndex, Task](),
		channelOwners:    NewConcurrentMap[nodeChannelIndex, Task](),
		processQueue:     newNodeTaskQueue(),
		waitQueue:        newTaskQueue(),
		taskStats:        expirable.NewLRU[UniqueID, Task](256, nil, time.Minute*15),
		segmentTaskDelta: NewSegmentTaskDelta(),
		channelTaskDelta: NewChannelTaskDelta(),
	}
}

func (scheduler *taskScheduler) Start() {}

func (scheduler *taskScheduler) Stop() {
	scheduler.lifecycleMu.Lock()
	if scheduler.closed {
		scheduler.lifecycleMu.Unlock()
		return
	}
	scheduler.closed = true
	scheduler.tasks.Range(func(_ UniqueID, entry scheduledTask) bool {
		scheduler.remove(entry.task)
		return true
	})
	executors := scheduler.executors.Values()
	scheduler.executors.Range(func(nodeID int64, _ *Executor) bool {
		scheduler.executors.Remove(nodeID)
		return true
	})
	scheduler.lifecycleMu.Unlock()

	for _, executor := range executors {
		executor.Stop()
	}
}

func (scheduler *taskScheduler) AddExecutor(nodeID int64) {
	scheduler.lifecycleMu.RLock()
	defer scheduler.lifecycleMu.RUnlock()
	if scheduler.closed || scheduler.ctx.Err() != nil {
		return
	}
	executor := NewExecutor(nodeID,
		scheduler.meta,
		scheduler.distMgr,
		scheduler.broker,
		scheduler.targetMgr,
		scheduler.cluster,
		scheduler.nodeMgr)

	if _, exist := scheduler.executors.GetOrInsert(nodeID, executor); exist {
		return
	}
	executor.Start(scheduler.ctx)
	mlog.Info(scheduler.ctx, "add executor for new QueryNode", mlog.Int64("nodeID", nodeID))
}

func (scheduler *taskScheduler) RemoveExecutor(nodeID int64) {
	scheduler.lifecycleMu.RLock()
	executor, ok := scheduler.executors.GetAndRemove(nodeID)
	scheduler.lifecycleMu.RUnlock()
	if ok {
		executor.Stop()
		mlog.Info(scheduler.ctx, "remove executor of offline QueryNode", mlog.Int64("nodeID", nodeID))
	}
}

func (scheduler *taskScheduler) Add(task Task) error {
	scheduler.lifecycleMu.RLock()
	defer scheduler.lifecycleMu.RUnlock()
	if scheduler.closed || scheduler.ctx.Err() != nil {
		err := merr.WrapErrServiceUnavailable("task scheduler stopped")
		task.Cancel(err)
		return err
	}
	scheduler.collKeyLock.Lock(task.CollectionID())
	defer scheduler.collKeyLock.Unlock(task.CollectionID())
	err := scheduler.preAdd(task)
	if err != nil {
		task.Cancel(err)
		return err
	}

	task.SetID(scheduler.idAllocator())
	scheduler.tasks.Insert(task.ID(), scheduledTask{task: task, stage: taskWaiting})
	scheduler.incExecutingTaskDelta(task)
	switch task := task.(type) {
	case *SegmentTask:
		index := NewReplicaSegmentIndex(task)
		scheduler.segmentTasks.Insert(index, task)

	case *ChannelTask:
		index := NewReplicaChannelIndex(task)
		scheduler.channelTasks.Insert(index, task)

	case *LeaderTask:
		index := NewReplicaLeaderIndex(task)
		scheduler.segmentTasks.Insert(index, task)
	}

	scheduler.taskStats.Add(task.ID(), task)
	task.RecordStartTs()
	// Publish last: a queue-visible task has its identity, index and delta.
	scheduler.waitQueue.Add(task)
	scheduler.updateTaskMetrics()
	mlog.Info(task.Context(), "task added", mlog.String("task", task.String()))
	return nil
}

func (scheduler *taskScheduler) updateTaskMetrics() {
	if time.Since(scheduler.lastUpdateMetricTime.Load()) < 30*time.Second {
		return
	}
	segmentGrowNum, segmentReduceNum, segmentUpdateNum, segmentMoveNum := 0, 0, 0, 0
	leaderGrowNum, leaderReduceNum, leaderUpdateNum := 0, 0, 0
	channelGrowNum, channelReduceNum, channelMoveNum := 0, 0, 0
	scheduler.segmentTasks.Range(func(_ replicaSegmentIndex, task Task) bool {
		switch {
		case len(task.Actions()) > 1:
			segmentMoveNum++
		case task.Actions()[0].Type() == ActionTypeGrow:
			if _, ok := task.Actions()[0].(*SegmentAction); ok {
				segmentGrowNum++
			}
			if _, ok := task.Actions()[0].(*LeaderAction); ok {
				leaderGrowNum++
			}
		case task.Actions()[0].Type() == ActionTypeReduce:
			if _, ok := task.Actions()[0].(*SegmentAction); ok {
				segmentReduceNum++
			}
			if _, ok := task.Actions()[0].(*LeaderAction); ok {
				leaderReduceNum++
			}
		case task.Actions()[0].Type() == ActionTypeUpdate:
			if _, ok := task.Actions()[0].(*SegmentAction); ok {
				segmentUpdateNum++
			}
			if _, ok := task.Actions()[0].(*LeaderAction); ok {
				leaderUpdateNum++
			}
		}
		return true
	})

	scheduler.channelTasks.Range(func(_ replicaChannelIndex, task Task) bool {
		taskType := GetTaskType(task)
		switch taskType {
		case TaskTypeGrow:
			channelGrowNum++
		case TaskTypeReduce:
			channelReduceNum++
		case TaskTypeMove:
			channelMoveNum++
		}
		return true
	})

	metrics.QueryCoordTaskNum.WithLabelValues(metrics.SegmentGrowTaskLabel).Set(float64(segmentGrowNum))
	metrics.QueryCoordTaskNum.WithLabelValues(metrics.SegmentReduceTaskLabel).Set(float64(segmentReduceNum))
	metrics.QueryCoordTaskNum.WithLabelValues(metrics.SegmentMoveTaskLabel).Set(float64(segmentMoveNum))
	metrics.QueryCoordTaskNum.WithLabelValues(metrics.SegmentUpdateTaskLabel).Set(float64(segmentUpdateNum))

	metrics.QueryCoordTaskNum.WithLabelValues(metrics.LeaderGrowTaskLabel).Set(float64(leaderGrowNum))
	metrics.QueryCoordTaskNum.WithLabelValues(metrics.LeaderReduceTaskLabel).Set(float64(leaderReduceNum))
	metrics.QueryCoordTaskNum.WithLabelValues(metrics.LeaderUpdateTaskLabel).Set(float64(leaderUpdateNum))

	metrics.QueryCoordTaskNum.WithLabelValues(metrics.ChannelGrowTaskLabel).Set(float64(channelGrowNum))
	metrics.QueryCoordTaskNum.WithLabelValues(metrics.ChannelReduceTaskLabel).Set(float64(channelReduceNum))
	metrics.QueryCoordTaskNum.WithLabelValues(metrics.ChannelMoveTaskLabel).Set(float64(channelMoveNum))
	scheduler.lastUpdateMetricTime.Store(time.Now())
}

// preAdd checks admission while holding lifecycleMu for reading and the
// collection lock, including cleanup of a lower-priority replacement.
func (scheduler *taskScheduler) preAdd(task Task) error {
	var old Task
	var object string
	switch typed := task.(type) {
	case *SegmentTask:
		old, _ = scheduler.segmentTasks.Get(NewReplicaSegmentIndex(typed))
		object = "segment"
	case *ChannelTask:
		old, _ = scheduler.channelTasks.Get(NewReplicaChannelIndex(typed))
		object = "channel"
	case *LeaderTask:
		old, _ = scheduler.segmentTasks.Get(NewReplicaLeaderIndex(typed))
		object = "segment"
	default:
		panic(fmt.Sprintf("preAdd: forget to process task type: %+v", task))
	}
	if old != nil && (task.Priority() <= old.Priority() || !scheduler.isCurrent(old, taskWaiting)) {
		// A processing task may already have sent an RPC. Priority cannot revoke it.
		return merr.WrapErrServiceInternal("task with the same " + object + " exists")
	}
	if err := scheduler.checkTaskResources(task, old); err != nil {
		return err
	}
	if old == nil {
		switch typed := task.(type) {
		case *SegmentTask:
			if GetTaskType(task) == TaskTypeMove {
				if scheduler.getReplicaShardLeader(task.Shard(), task.ReplicaID()) == nil {
					return merr.WrapErrServiceInternal("segment's delegator leader not found, stop balancing")
				}
				segments := scheduler.distMgr.SegmentDistManager.GetByFilter(meta.WithNodeID(task.Actions()[1].Node()), meta.WithSegmentID(typed.SegmentID()))
				if len(segments) == 0 {
					return merr.WrapErrServiceInternal("source segment released, stop balancing")
				}
			}
		case *ChannelTask:
			switch GetTaskType(task) {
			case TaskTypeGrow:
				channels := scheduler.distMgr.ChannelDistManager.GetByFilter(meta.WithChannelName2Channel(typed.Channel()))
				nodes := lo.Map(channels, func(v *meta.DmChannel, _ int) UniqueID { return v.Node })
				replicas := utils.GroupNodesByReplica(task.Context(), scheduler.meta.ReplicaManager, task.CollectionID(), nodes)
				if _, ok := replicas[task.ReplicaID()]; ok {
					return merr.WrapErrServiceInternal("channel subscribed, it can be only balanced")
				}
			case TaskTypeMove:
				channels := scheduler.distMgr.ChannelDistManager.GetByFilter(meta.WithChannelName2Channel(typed.Channel()))
				_, ok := lo.Find(channels, func(v *meta.DmChannel) bool { return v.Node == task.Actions()[1].Node() })
				if !ok {
					return merr.WrapErrServiceInternal("source channel unsubscribed, stop balancing")
				}
			}
		}
	}
	if old != nil {
		mlog.Info(task.Context(), "replace waiting task with higher priority", mlog.Int64("oldID", old.ID()), mlog.Int64("newID", task.ID()))
		old.Cancel(merr.WrapErrServiceInternal("replaced with the other one with higher priority"))
		scheduler.remove(old)
	}
	return scheduler.reserveTaskResources(task)
}

func (scheduler *taskScheduler) getReplicaShardLeader(channelName string, replicaID int64) *meta.DmChannel {
	replica := scheduler.meta.Get(scheduler.ctx, replicaID)
	if replica == nil {
		return nil
	}
	return scheduler.distMgr.ChannelDistManager.GetShardLeader(channelName, replica)
}

func (scheduler *taskScheduler) tryPromoteAll() {
	for _, task := range scheduler.waitQueue.Snapshot() {
		func() {
			scheduler.collKeyLock.Lock(task.CollectionID())
			defer scheduler.collKeyLock.Unlock(task.CollectionID())
			// Another Dispatch can have promoted the same snapshot, or Add
			// can have replaced it. Started alone does not distinguish these.
			if !scheduler.isCurrent(task, taskWaiting) {
				return
			}
			if err := scheduler.promote(task); err != nil {
				task.Cancel(err)
				scheduler.remove(task)
				return
			}
			scheduler.tasks.Insert(task.ID(), scheduledTask{task: task, stage: taskProcessing})
			scheduler.waitQueue.Remove(task)
		}()
	}
}

// isCurrent requires the task's collection lock or the lifecycle writer.
func (scheduler *taskScheduler) isCurrent(task Task, stage taskStage) bool {
	entry, ok := scheduler.tasks.Get(task.ID())
	if !ok || entry.task != task || entry.stage != stage {
		return false
	}
	var indexed Task
	switch task := task.(type) {
	case *SegmentTask:
		indexed, _ = scheduler.segmentTasks.Get(NewReplicaSegmentIndex(task))
	case *ChannelTask:
		indexed, _ = scheduler.channelTasks.Get(NewReplicaChannelIndex(task))
	case *LeaderTask:
		indexed, _ = scheduler.segmentTasks.Get(NewReplicaLeaderIndex(task))
	}
	return indexed == task
}

func (scheduler *taskScheduler) promote(task Task) error {
	log := mlog.With(
		mlog.Int64("taskID", task.ID()),
		mlog.Int64("collectionID", task.CollectionID()),
		mlog.Int64("replicaID", task.ReplicaID()),
		mlog.String("source", task.Source().String()),
	)

	if err := scheduler.check(task, true); err != nil {
		log.Info(task.Context(), "failed to promote task", mlog.Err(err))
		return err
	}

	scheduler.processQueue.Add(task)
	return nil
}

func (scheduler *taskScheduler) Dispatch(node int64) {
	scheduler.lifecycleMu.RLock()
	defer scheduler.lifecycleMu.RUnlock()
	if scheduler.closed || scheduler.ctx.Err() != nil {
		return
	}
	scheduler.schedule(node)
}

func (scheduler *taskScheduler) GetSegmentTaskDeltaSnapshot(nodeIDs []int64, collectionID int64) *SegmentTaskDeltaSnapshot {
	return scheduler.segmentTaskDelta.GetSegmentSnapshot(nodeIDs, collectionID, scheduler.distMgr)
}

func (scheduler *taskScheduler) GetChannelTaskDelta(nodeID, collectionID int64) int {
	return scheduler.channelTaskDelta.Get(nodeID, collectionID)
}

func (scheduler *taskScheduler) incExecutingTaskDelta(task Task) {
	switch task := task.(type) {
	case *SegmentTask:
		scheduler.segmentTaskDelta.Add(task)
	case *ChannelTask:
		scheduler.channelTaskDelta.Add(task)
	}
}

func (scheduler *taskScheduler) decExecutingTaskDelta(task Task) {
	switch task := task.(type) {
	case *SegmentTask:
		scheduler.segmentTaskDelta.Sub(task)
	case *ChannelTask:
		scheduler.channelTaskDelta.Sub(task)
	}
}

type TaskFilter func(task Task) bool

func WithCollectionID2TaskFilter(collectionID int64) TaskFilter {
	return func(task Task) bool {
		return task.CollectionID() == collectionID
	}
}

func WithTaskTypeFilter(taskType Type) TaskFilter {
	return func(task Task) bool {
		return GetTaskType(task) == taskType
	}
}

func (scheduler *taskScheduler) GetChannelTaskNum(filters ...TaskFilter) int {
	if len(filters) == 0 {
		return scheduler.channelTasks.Len()
	}

	// rewrite this with for loop
	counter := 0
	scheduler.channelTasks.Range(func(_ replicaChannelIndex, task Task) bool {
		allMatch := true
		for _, filter := range filters {
			if !filter(task) {
				allMatch = false
				break
			}
		}
		if allMatch {
			counter++
		}
		return true
	})
	return counter
}

func (scheduler *taskScheduler) GetSegmentTaskNum(filters ...TaskFilter) int {
	if len(filters) == 0 {
		return scheduler.segmentTasks.Len()
	}

	// rewrite this with for loop
	counter := 0
	scheduler.segmentTasks.Range(func(_ replicaSegmentIndex, task Task) bool {
		allMatch := true
		for _, filter := range filters {
			if !filter(task) {
				allMatch = false
				break
			}
		}
		if allMatch {
			counter++
		}
		return true
	})
	return counter
}

// GetTasksJSON returns the JSON string of all tasks.
// the task stats object is thread safe and can be accessed without lock
func (scheduler *taskScheduler) GetTasksJSON() string {
	tasks := scheduler.taskStats.Values()
	ret, err := json.Marshal(tasks)
	if err != nil {
		mlog.Warn(scheduler.ctx, "marshal tasks fail", mlog.Err(err))
		return ""
	}
	return string(ret)
}

// schedule selects some tasks to execute, follow these steps for each started selected tasks:
// 1. check whether this task is stale, set status to canceled if stale
// 2. step up the task's actions, set status to succeeded if all actions finished
// 3. execute the current action of task
func (scheduler *taskScheduler) schedule(node int64) {
	if scheduler.tasks.Len() == 0 {
		scheduler.updateTaskMetrics()
		return
	}

	tr := timerecord.NewTimeRecorder("")
	log := mlog.With(
		mlog.Int64("nodeID", node),
	)

	scheduler.tryPromoteAll()
	promoteDur := tr.RecordSpan()

	log.Debug(scheduler.ctx, "process tasks related to node",
		mlog.Int("processingTaskNum", scheduler.processQueue.LenByNode(node)),
		mlog.Int("waitingTaskNum", scheduler.waitQueue.Len()),
		mlog.Int("segmentTaskNum", scheduler.segmentTasks.Len()),
		mlog.Int("channelTaskNum", scheduler.channelTasks.Len()),
	)

	// Snapshot without waiting on a collection while holding the queue lock.
	toProcess := scheduler.processQueue.SnapshotByNode(node)
	queueSnapshotDur := tr.RecordSpan()

	// The scheduler doesn't limit the number of tasks,
	// to commit tasks to executors as soon as possible, to reach higher merge possibility
	commmittedNum := atomic.NewInt32(0)
	removedNum := atomic.NewInt32(0)
	funcutil.ProcessFuncParallel(len(toProcess), hardware.GetCPUNum(), func(idx int) error {
		committed, removed := scheduler.processTask(toProcess[idx])
		if committed {
			commmittedNum.Inc()
		}
		if removed {
			removedNum.Inc()
		}
		return nil
	}, "process")
	processDur := tr.RecordSpan()

	scheduler.updateTaskMetrics()

	log.Info(scheduler.ctx, "processed tasks",
		mlog.Int("toProcessNum", len(toProcess)),
		mlog.Int32("committedNum", commmittedNum.Load()),
		mlog.Int32("toRemoveNum", removedNum.Load()),
		mlog.Duration("promoteDur", promoteDur),
		mlog.Duration("queueSnapshotDur", queueSnapshotDur),
		mlog.Duration("processDUr", processDur),
		mlog.Duration("totalDur", tr.ElapseSpan()),
	)

	log.Info(scheduler.ctx, "process tasks related to node done",
		mlog.Int("processingTaskNum", scheduler.processQueue.LenByNode(node)),
		mlog.Int("waitingTaskNum", scheduler.waitQueue.Len()),
		mlog.Int("segmentTaskNum", scheduler.segmentTasks.Len()),
		mlog.Int("channelTaskNum", scheduler.channelTasks.Len()),
	)
}

// processTask keeps identity/stage validation, advancement and synchronous
// executor admission in one collection critical section. Execute starts the
// action asynchronously rather than awaiting the remote action here. The
// existing retirement target refresh can still wait under the collection lock.
func (scheduler *taskScheduler) processTask(task Task) (committed, removed bool) {
	scheduler.collKeyLock.Lock(task.CollectionID())
	defer scheduler.collKeyLock.Unlock(task.CollectionID())
	if !scheduler.isCurrent(task, taskProcessing) {
		return false, false
	}
	if !scheduler.preProcess(task) {
		if task.Status() != TaskStatusStarted {
			scheduler.remove(task)
			return false, true
		}
		return false, false
	}
	return scheduler.process(task), false
}

func (scheduler *taskScheduler) isRelated(task Task, node int64) bool {
	for _, action := range task.Actions() {
		// For LeaderAction, the worker node and executor node are both related.
		if action.Node() == node || actionExecutorNode(action) == node {
			return true
		}
	}
	return false
}

func actionExecutorNode(action Action) int64 {
	if la, ok := action.(*LeaderAction); ok {
		return la.GetLeaderID()
	}
	return action.Node()
}

// preProcess checks the finished actions of task,
// and converts the task's status,
// return true if the task should be executed,
// false otherwise
func (scheduler *taskScheduler) preProcess(task Task) (shouldProcess bool) {
	if task.Status() != TaskStatusStarted {
		return false
	}

	actions, step := task.Actions(), task.Step()
	shouldProcess = true
	for step < len(actions) {
		finished, waitingDist := scheduler.checkActionFinish(actions[step])
		if !finished {
			if waitingDist {
				shouldProcess = false
			}
			break
		}
		if GetTaskType(task) == TaskTypeMove && actions[step].Type() == ActionTypeGrow {
			var newDelegatorReady bool
			switch action := actions[step].(type) {
			case *ChannelAction:
				// wait for new delegator becomes leader, then try to remove old leader
				task := task.(*ChannelTask)
				delegator := scheduler.getReplicaShardLeader(task.Shard(), task.ReplicaID())
				mlog.Debug(scheduler.ctx, "process channelAction", mlog.Bool("delegator is Nil", delegator == nil))
				if delegator != nil {
					mlog.Debug(scheduler.ctx, "process channelAction", mlog.Int64("delegator node", delegator.Node),
						mlog.Int64("action node", action.Node()))
				}
				newDelegatorReady = delegator != nil && delegator.Node == action.Node()
			default:
				newDelegatorReady = true
			}
			if !newDelegatorReady {
				mlog.RatedInfo(scheduler.ctx, rate.Limit(30), "Blocking reduce action in balance channel task",
					mlog.Int64("collectionID", task.CollectionID()),
					mlog.String("channelName", task.Shard()),
					mlog.Int64("taskID", task.ID()))

				break
			}
		}
		task.StepUp()
		step++
	}

	if task.IsFinished(scheduler.distMgr) {
		task.SetStatus(TaskStatusSucceeded)
	} else {
		if err := scheduler.check(task, false); err != nil {
			task.Cancel(err)
		}
	}

	return shouldProcess && task.Status() == TaskStatusStarted
}

func (scheduler *taskScheduler) checkActionFinish(action Action) (finished bool, waitingDist bool) {
	segmentAction, ok := action.(*SegmentAction)
	if !ok {
		return action.IsFinished(scheduler.distMgr), false
	}
	if !segmentAction.rpcReturned.Load() {
		return false, false
	}
	distMatched := segmentAction.isDistMatched(scheduler.distMgr)
	return distMatched, !distMatched
}

// process processes the given task,
// return true if the task is started and succeeds to commit the current action
func (scheduler *taskScheduler) process(task Task) bool {
	log := mlog.With(
		mlog.Int64("taskID", task.ID()),
		mlog.Int64("collectionID", task.CollectionID()),
		mlog.Int64("replicaID", task.ReplicaID()),
		mlog.String("type", GetTaskType(task).String()),
		mlog.String("source", task.Source().String()),
	)

	actions, step := task.Actions(), task.Step()
	nodeID := actionExecutorNode(actions[step])
	executor, ok := scheduler.executors.Get(nodeID)
	if !ok {
		log.Warn(task.Context(), "no executor for QueryNode",
			mlog.Int("step", step),
			mlog.Int64("nodeID", nodeID))
		return false
	}

	return executor.Execute(task, step)
}

func (scheduler *taskScheduler) check(task Task, checkDistExist bool) error {
	err := task.Context().Err()
	if err == nil {
		err = scheduler.checkStale(task, checkDistExist)
	}

	return err
}

func (scheduler *taskScheduler) RemoveByNode(node int64) {
	scheduler.lifecycleMu.Lock()
	defer scheduler.lifecycleMu.Unlock()

	scheduler.segmentTasks.Range(func(_ replicaSegmentIndex, task Task) bool {
		if scheduler.isRelated(task, node) {
			scheduler.remove(task)
		}
		return true
	})
	scheduler.channelTasks.Range(func(_ replicaChannelIndex, task Task) bool {
		if scheduler.isRelated(task, node) {
			scheduler.remove(task)
		}
		return true
	})
}

func (scheduler *taskScheduler) recordSegmentTaskError(task *SegmentTask) {
	mlog.Warn(scheduler.ctx, "task scheduler recordSegmentTaskError",
		mlog.Int64("taskID", task.ID()),
		mlog.Int64("collectionID", task.CollectionID()),
		mlog.Int64("replicaID", task.ReplicaID()),
		mlog.Int64("segmentID", task.SegmentID()),
		mlog.String("status", task.Status()),
		mlog.Err(task.err),
	)
	meta.GlobalFailedLoadCache.Put(task.collectionID, task.Err())
}

// remove retires bookkeeping at most once. The caller must hold the lifecycle
// writer or the lifecycle reader together with this task's collection lock.
func (scheduler *taskScheduler) remove(task Task) {
	_, ok := scheduler.tasks.GetAndRemove(task.ID())
	if !ok {
		return // A stale queue snapshot must not touch a replacement or its delta.
	}
	log := mlog.With(
		mlog.Int64("taskID", task.ID()),
		mlog.Int64("collectionID", task.CollectionID()),
		mlog.Int64("replicaID", task.ReplicaID()),
		mlog.String("status", task.Status()),
	)

	if errors.Is(task.Err(), merr.ErrSegmentNotFound) {
		log.Info(task.Context(), "segment in target has been cleaned, trigger force update next target")
		// Avoid using task.Ctx as it may be canceled before remove is called.
		scheduler.targetMgr.UpdateCollectionNextTarget(scheduler.ctx, task.CollectionID())
	}

	// If task failed due to resource exhaustion (OOM, disk full, GPU OOM, etc.),
	// mark the node as resource exhausted for a penalty period.
	// During this period, the balancer will skip this node when assigning new segments/channels.
	// This prevents continuous failures on the same node and allows it time to recover.
	if errors.Is(task.Err(), merr.ErrSegmentRequestResourceFailed) {
		for _, action := range task.Actions() {
			if action.Type() == ActionTypeGrow {
				nodeID := action.Node()
				duration := paramtable.Get().QueryCoordCfg.ResourceExhaustionPenaltyDuration.GetAsDuration(time.Second)
				scheduler.nodeMgr.MarkResourceExhaustion(nodeID, duration)
				log.Info(task.Context(), "mark resource exhaustion for node", mlog.Int64("nodeID", nodeID), mlog.Duration("duration", duration), mlog.Err(task.Err()))
			}
		}
	}

	// A task that is still Started when removed did not finish on its own: it
	// was dropped by RemoveByNode (a node went down) or Stop (shutdown). Cancel
	// it with a real error so a caller blocked in Wait can distinguish this
	// from a successful completion, whose err is nil. Terminal tasks keep
	// Cancel(nil): for already-failed/canceled tasks the CAS makes it a no-op,
	// and for a succeeded task nil preserves the success result.
	if task.Status() == TaskStatusStarted {
		task.Cancel(merr.WrapErrServiceUnavailable("task removed before finish"))
	} else {
		task.Cancel(nil)
	}
	scheduler.waitQueue.Remove(task)
	scheduler.processQueue.Remove(task)
	scheduler.decExecutingTaskDelta(task)
	scheduler.releaseTaskResources(task)

	switch task := task.(type) {
	case *SegmentTask:
		index := NewReplicaSegmentIndex(task)
		if indexed, ok := scheduler.segmentTasks.Get(index); ok && indexed == task {
			scheduler.segmentTasks.Remove(index)
		}
		log = mlog.With(mlog.Int64("segmentID", task.SegmentID()))
		if task.Status() == TaskStatusFailed &&
			task.Err() != nil &&
			!errors.IsAny(task.Err(), merr.ErrChannelNotFound, merr.ErrServiceTooManyRequests) {
			scheduler.recordSegmentTaskError(task)
		}

	case *ChannelTask:
		index := NewReplicaChannelIndex(task)
		if indexed, ok := scheduler.channelTasks.Get(index); ok && indexed == task {
			scheduler.channelTasks.Remove(index)
		}
		log = mlog.With(mlog.String("channel", task.Channel()))

	case *LeaderTask:
		index := NewReplicaLeaderIndex(task)
		if indexed, ok := scheduler.segmentTasks.Get(index); ok && indexed == task {
			scheduler.segmentTasks.Remove(index)
		}
		log = mlog.With(mlog.Int64("segmentID", task.SegmentID()))
	}

	log.Info(task.Context(), "task removed")

	if scheduler.meta.Exist(task.Context(), task.CollectionID()) {
		metrics.QueryCoordTaskLatency.WithLabelValues(fmt.Sprint(task.CollectionID()),
			scheduler.getTaskMetricsLabel(task), task.Shard()).Observe(float64(task.GetTaskLatency()))
	}
}

func (scheduler *taskScheduler) getTaskMetricsLabel(task Task) string {
	taskType := GetTaskType(task)
	switch task.(type) {
	case *SegmentTask:
		switch taskType {
		case TaskTypeGrow:
			return metrics.SegmentGrowTaskLabel
		case TaskTypeReduce:
			return metrics.SegmentReduceTaskLabel
		case TaskTypeMove:
			return metrics.SegmentMoveTaskLabel
		case TaskTypeUpdate:
			return metrics.SegmentUpdateTaskLabel
		}

	case *ChannelTask:
		switch taskType {
		case TaskTypeGrow:
			return metrics.ChannelGrowTaskLabel
		case TaskTypeReduce:
			return metrics.ChannelReduceTaskLabel
		case TaskTypeMove:
			return metrics.ChannelMoveTaskLabel
		}

	case *LeaderTask:
		switch taskType {
		case TaskTypeGrow:
			return metrics.LeaderGrowTaskLabel
		case TaskTypeReduce:
			return metrics.LeaderReduceTaskLabel
		}
	}

	return metrics.UnknownTaskLabel
}

func WrapTaskLog(task Task, fields ...mlog.Field) []mlog.Field {
	res := []mlog.Field{
		mlog.Int64("taskID", task.ID()),
		mlog.Int64("collectionID", task.CollectionID()),
		mlog.Int64("replicaID", task.ReplicaID()),
		mlog.String("source", task.Source().String()),
	}
	res = append(res, fields...)
	return res
}

func (scheduler *taskScheduler) checkStale(task Task, checkDistExist bool) error {
	// Resolve current membership rather than the task's replica snapshot.
	// NilReplica (ID=-1) is used for reduce-only tasks like unsubscribe channel
	var replica *meta.Replica
	if task.ReplicaID() != -1 {
		replica = scheduler.meta.Get(scheduler.ctx, task.ReplicaID())
		if replica == nil {
			mlog.Warn(task.Context(), "task stale due to replica not found", mlog.String("task", task.String()))
			return merr.WrapErrReplicaNotFound(task.ReplicaID())
		}
	}

	// For segment grow tasks, check if segment is already loaded in dist.
	// This prevents duplicate load RPCs when the checker creates tasks from a stale dist snapshot
	// but the segment has already been loaded before the task is dispatched.
	// Only checked during promote (waitQueue → processQueue), not during preProcess,
	// because during preProcess the segment may have been loaded by this task's own in-flight RPC,
	// and canceling the task would kill that RPC with a misleading "context canceled" error.
	if checkDistExist {
		if segmentTask, ok := task.(*SegmentTask); ok && GetTaskType(task) == TaskTypeGrow && replica != nil {
			existsInDist := scheduler.distMgr.SegmentDistManager.GetByFilter(
				meta.WithCollectionID(task.CollectionID()),
				meta.WithReplica(replica),
				meta.WithSegmentID(segmentTask.SegmentID()),
			)
			if len(existsInDist) > 0 {
				mlog.Info(task.Context(), "task stale due to segment already loaded in dist",
					mlog.String("task", task.String()),
					mlog.Int64("segmentID", segmentTask.SegmentID()))
				return merr.WrapErrServiceInternal("segment already loaded in dist")
			}
		}
	}

	for _, action := range task.Actions() {
		if orphanCleanupReassigned(scheduler.meta, task, action) {
			return merr.WrapErrServiceInternal("orphan cleanup node reassigned to a replica")
		}

		// Determine the target node for stale checking.
		// For LeaderAction, we need to check the leader node (delegator) instead of the worker node.
		// This is because LeaderAction.Node() returns the worker node where the segment resides,
		// but the task is executed on the leader node. If the worker node is an RO node while
		// the leader node is still RW, the task should NOT be marked as stale.
		// See issue #46737: Using action.Node() for LeaderAction incorrectly marks tasks as stale
		// when syncing segments from RO nodes to the delegator, blocking balance channel operations.
		var targetNode int64
		switch a := action.(type) {
		case *LeaderAction:
			targetNode = a.GetLeaderID()
		default:
			targetNode = a.Node()
		}

		nodeInfo := scheduler.nodeMgr.Get(targetNode)
		if nodeInfo == nil {
			mlog.Warn(task.Context(), "task stale due to node not found", mlog.String("task", task.String()), mlog.Int64("nodeID", targetNode))
			return merr.WrapErrNodeNotFound(targetNode)
		}
		if action.Type() == ActionTypeGrow {
			if nodeInfo.IsStoppingState() {
				mlog.Warn(task.Context(), "task stale due to node offline", mlog.String("task", task.String()), mlog.Int64("nodeID", targetNode))
				return merr.WrapErrNodeOffline(targetNode)
			}

			if replica != nil && (replica.ContainRONode(targetNode) || replica.ContainROSQNode(targetNode)) {
				mlog.Warn(task.Context(), "task stale due to node becomes ro node", mlog.String("task", task.String()), mlog.Int64("nodeID", targetNode))
				return merr.WrapErrNodeStateUnexpected(targetNode, "node becomes ro node")
			}

			// An RG update can remove a live destination from this replica
			// without leaving it in the RO set. It can no longer become this
			// replica's shard leader, so retaining a channel move would block
			// both its Reduce step and a replacement move until timeout.
			if _, ok := task.(*ChannelTask); ok && replica != nil && !replica.Contains(targetNode) {
				mlog.Warn(task.Context(), "task stale due to channel destination leaving replica", mlog.String("task", task.String()), mlog.Int64("nodeID", targetNode))
				return merr.WrapErrNodeStateUnexpected(targetNode, "channel destination no longer belongs to replica")
			}
		}
	}

	return nil
}
