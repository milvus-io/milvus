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
	"sort"
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
	TaskTypeDropIndex
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
	ReplicaID int64
	SegmentID int64
	IsGrowing bool
}

func NewReplicaSegmentIndex(task *SegmentTask) replicaSegmentIndex {
	isGrowing := task.Actions()[0].(*SegmentAction).GetScope() == querypb.DataScope_Streaming
	return replicaSegmentIndex{
		ReplicaID: task.ReplicaID(),
		SegmentID: task.SegmentID(),
		IsGrowing: isGrowing,
	}
}

func NewReplicaLeaderIndex(task *LeaderTask) replicaSegmentIndex {
	return replicaSegmentIndex{
		ReplicaID: task.ReplicaID(),
		SegmentID: task.SegmentID(),
		IsGrowing: false,
	}
}

func NewReplicaDropIndex(task *DropIndexTask) replicaSegmentIndex {
	return replicaSegmentIndex{
		ReplicaID: task.ReplicaID(),
		SegmentID: task.SegmentID(),
		IsGrowing: false,
	}
}

type replicaChannelIndex struct {
	ReplicaID int64
	Channel   string
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

// Range iterates all tasks in the queue ordered by priority from high to low
func (queue *taskQueue) Range(fn func(task Task) bool) {
	queue.mu.RLock()
	defer queue.mu.RUnlock()
	for priority := len(queue.buckets) - 1; priority >= 0; priority-- {
		for _, task := range queue.buckets[priority] {
			if !fn(task) {
				return
			}
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

// RangeByNode iterates tasks related to the specified node,
// ordered by priority from high to low.
func (queue *nodeTaskQueue) RangeByNode(nodeID int64, fn func(task Task) bool) {
	queue.mu.RLock()
	defer queue.mu.RUnlock()
	nodeBuckets, ok := queue.buckets[nodeID]
	if !ok {
		return
	}
	for priority := len(nodeBuckets) - 1; priority >= 0; priority-- {
		for _, task := range nodeBuckets[priority] {
			if !fn(task) {
				return
			}
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
	mu sync.RWMutex
	// taskID -> channel delta records generated by the task's channel actions.
	records map[int64][]channelDeltaRecord
}

type channelDeltaRecord struct {
	collectionID int64
	nodeID       int64
	channelName  string
	actionType   ActionType
	delta        int
}

type SegmentTaskDeltaSnapshot struct {
	nodeDeltas           map[int64]int
	nodeCollectionDeltas map[int64]int
}

type ChannelTaskDeltaSnapshot struct {
	nodeDeltas           map[int64]int
	nodeCollectionDeltas map[int64]int
}

type ChannelTaskDeltaProvider interface {
	GetChannelTaskDeltaSnapshot(nodeIDs []int64, collectionID int64) *ChannelTaskDeltaSnapshot
}

type PendingBalanceActionSnapshot struct {
	NodeID    int64
	Type      ActionType
	SegmentID int64
	Channel   string
	Shard     string
	Scope     querypb.DataScope
	Workload  int
}

type PendingBalanceTaskSnapshot struct {
	TaskID        int64
	CollectionID  int64
	ReplicaID     int64
	ResourceGroup string
	Epoch         BalanceEpochMeta
	Status        Status
	Actions       []PendingBalanceActionSnapshot
}

type PendingBalanceSnapshot struct {
	Revision uint64
	Tasks    []PendingBalanceTaskSnapshot
}

type BalanceTaskInspector interface {
	GetPendingBalanceTasks() PendingBalanceSnapshot
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

func NewChannelTaskDeltaSnapshot(nodeDeltas, nodeCollectionDeltas map[int64]int) *ChannelTaskDeltaSnapshot {
	if nodeDeltas == nil {
		nodeDeltas = make(map[int64]int)
	}
	if nodeCollectionDeltas == nil {
		nodeCollectionDeltas = make(map[int64]int)
	}
	return &ChannelTaskDeltaSnapshot{
		nodeDeltas:           nodeDeltas,
		nodeCollectionDeltas: nodeCollectionDeltas,
	}
}

func (snapshot *ChannelTaskDeltaSnapshot) GetByNode(nodeID int64) int {
	if snapshot == nil {
		return 0
	}
	return snapshot.nodeDeltas[nodeID]
}

// GetByNodeInCollection returns the pending channel-task delta on nodeID,
// scoped to the collection used when the snapshot was built.
func (snapshot *ChannelTaskDeltaSnapshot) GetByNodeInCollection(nodeID int64) int {
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
		records: make(map[int64][]channelDeltaRecord),
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
	delta.records[task.ID()] = records
}

// Sub removes the segment task delta records.
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

// Add updates the channel task delta records.
func (delta *ChannelTaskDelta) Add(task *ChannelTask) {
	delta.mu.Lock()
	defer delta.mu.Unlock()

	if _, ok := delta.records[task.ID()]; ok {
		mlog.Warn(task.Context(), "channel task already exists in delta cache",
			mlog.Int64("collectionID", task.CollectionID()),
			mlog.Int64("replicaID", task.ReplicaID()),
			mlog.Int64("taskID", task.ID()))
		return
	}

	records := make([]channelDeltaRecord, 0, len(task.Actions()))
	for _, action := range task.Actions() {
		channelAction, ok := action.(*ChannelAction)
		if !ok {
			continue
		}
		records = append(records, channelDeltaRecord{
			collectionID: task.CollectionID(),
			nodeID:       channelAction.Node(),
			channelName:  channelAction.ChannelName(),
			actionType:   channelAction.Type(),
			delta:        channelAction.WorkLoadEffect(),
		})
	}
	delta.records[task.ID()] = records
}

// Sub removes the channel task delta records.
func (delta *ChannelTaskDelta) Sub(task *ChannelTask) {
	delta.mu.Lock()
	defer delta.mu.Unlock()

	if _, ok := delta.records[task.ID()]; !ok {
		mlog.Warn(task.Context(), "channel task does not exist in delta cache",
			mlog.Int64("collectionID", task.CollectionID()),
			mlog.Int64("replicaID", task.ReplicaID()),
			mlog.Int64("taskID", task.ID()))
		return
	}
	delete(delta.records, task.ID())
}

// GetChannelSnapshot builds a snapshot for nodeIDs. collectionID binds the scope
// used by ChannelTaskDeltaSnapshot.GetByNodeInCollection; -1 means all collections.
func (delta *ChannelTaskDelta) GetChannelSnapshot(nodeIDs []int64, collectionID int64, distMgr *meta.DistributionManager) *ChannelTaskDeltaSnapshot {
	snapshot := NewChannelTaskDeltaSnapshot(make(map[int64]int, len(nodeIDs)), make(map[int64]int, len(nodeIDs)))
	nodeSet := NewUniqueSet(nodeIDs...)
	allNodes := nodeSet.Contain(-1)

	records := make([]channelDeltaRecord, 0)
	delta.mu.RLock()
	for _, taskRecords := range delta.records {
		for _, record := range taskRecords {
			if !allNodes && !nodeSet.Contain(record.nodeID) {
				continue
			}
			records = append(records, record)
		}
	}
	delta.mu.RUnlock()

	for _, record := range records {
		if distMgr != nil && record.isChannelDistMatched(distMgr) {
			continue
		}
		snapshot.nodeDeltas[record.nodeID] += record.delta
		if collectionID == -1 || record.collectionID == collectionID {
			snapshot.nodeCollectionDeltas[record.nodeID] += record.delta
		}
	}
	return snapshot
}

func (record channelDeltaRecord) isChannelDistMatched(distMgr *meta.DistributionManager) bool {
	channels := distMgr.ChannelDistManager.GetByFilter(
		meta.WithNodeID2Channel(record.nodeID),
		meta.WithChannelName2Channel(record.channelName),
	)
	present := len(channels) > 0
	switch record.actionType {
	case ActionTypeGrow:
		return present
	case ActionTypeReduce:
		return !present
	default:
		return true
	}
}

func getChannelSnapshotDelta(snapshot *ChannelTaskDeltaSnapshot, nodeID, collectionID int64) int {
	if nodeID != -1 {
		if collectionID == -1 {
			return snapshot.GetByNode(nodeID)
		}
		return snapshot.GetByNodeInCollection(nodeID)
	}

	var sum int
	if collectionID == -1 {
		for _, effect := range snapshot.nodeDeltas {
			sum += effect
		}
		return sum
	}
	for _, effect := range snapshot.nodeCollectionDeltas {
		sum += effect
	}
	return sum
}

func (delta *ChannelTaskDelta) printDetailInfos() {
	delta.mu.RLock()
	defer delta.mu.RUnlock()

	if len(delta.records) > 0 {
		mlog.Info(context.TODO(), "channel task delta cache info",
			mlog.String("records", fmt.Sprintf("%+v", delta.records)),
		)
	}
}

func (delta *ChannelTaskDelta) Clear() {
	delta.mu.Lock()
	defer delta.mu.Unlock()
	delta.records = make(map[int64][]channelDeltaRecord)
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

	scheduleMu      sync.Mutex           // guards schedule() and RemoveByNode()
	collKeyLock     *lock.KeyLock[int64] // guards Add() and AdmitBalanceTask()
	pendingMu       sync.RWMutex         // guards pending indexes, deltas, and pendingRevision
	pendingRevision uint64
	tasks           *ConcurrentMap[UniqueID, struct{}]
	segmentTasks    *ConcurrentMap[replicaSegmentIndex, Task]
	channelTasks    *ConcurrentMap[replicaChannelIndex, Task]
	processQueue    *nodeTaskQueue
	waitQueue       *taskQueue

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
		tasks:            NewConcurrentMap[UniqueID, struct{}](),
		segmentTasks:     NewConcurrentMap[replicaSegmentIndex, Task](),
		channelTasks:     NewConcurrentMap[replicaChannelIndex, Task](),
		processQueue:     newNodeTaskQueue(),
		waitQueue:        newTaskQueue(),
		taskStats:        expirable.NewLRU[UniqueID, Task](256, nil, time.Minute*15),
		segmentTaskDelta: NewSegmentTaskDelta(),
		channelTaskDelta: NewChannelTaskDelta(),
	}
}

func (scheduler *taskScheduler) Start() {}

func (scheduler *taskScheduler) Stop() {
	scheduler.executors.Range(func(nodeID int64, executor *Executor) bool {
		executor.Stop()
		return true
	})

	scheduler.segmentTasks.Range(func(_ replicaSegmentIndex, task Task) bool {
		scheduler.remove(task)
		return true
	})
	scheduler.channelTasks.Range(func(_ replicaChannelIndex, task Task) bool {
		scheduler.remove(task)
		return true
	})
}

func (scheduler *taskScheduler) AddExecutor(nodeID int64) {
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
	executor, ok := scheduler.executors.GetAndRemove(nodeID)
	if ok {
		executor.Stop()
		mlog.Info(scheduler.ctx, "remove executor of offline QueryNode", mlog.Int64("nodeID", nodeID))
	}
}

func (scheduler *taskScheduler) Add(task Task) error {
	scheduler.collKeyLock.Lock(task.CollectionID())
	defer scheduler.collKeyLock.Unlock(task.CollectionID())
	return scheduler.addLocked(task)
}

type schedulerAdmissionError struct {
	reason BalanceAdmissionReason
	err    error
}

func (scheduler *taskScheduler) AdmitBalanceTask(task Task, validate BalanceAdmissionValidator) BalanceAdmissionResult {
	scheduler.collKeyLock.Lock(task.CollectionID())
	defer scheduler.collKeyLock.Unlock(task.CollectionID())

	if reason := validate(); reason != BalanceAdmissionAccepted {
		err := merr.WrapErrServiceInternal(reason.String())
		task.Cancel(err)
		return BalanceAdmissionResult{Reason: reason, Err: err}
	}
	if admissionErr := scheduler.validateAddLocked(task); admissionErr != nil {
		task.Cancel(admissionErr.err)
		return BalanceAdmissionResult{Reason: admissionErr.reason, Err: admissionErr.err}
	}
	if reason := validate(); reason != BalanceAdmissionAccepted {
		err := merr.WrapErrServiceInternal(reason.String())
		task.Cancel(err)
		return BalanceAdmissionResult{Reason: reason, Err: err}
	}

	scheduler.commitAddLocked(task)
	return BalanceAdmissionResult{
		TaskID: task.ID(),
		Reason: BalanceAdmissionAccepted,
	}
}

func (scheduler *taskScheduler) addLocked(task Task) error {
	if admissionErr := scheduler.validateAddLocked(task); admissionErr != nil {
		task.Cancel(admissionErr.err)
		return admissionErr.err
	}
	scheduler.commitAddLocked(task)
	return nil
}

func (scheduler *taskScheduler) commitAddLocked(task Task) {
	scheduler.pendingMu.Lock()
	defer scheduler.pendingMu.Unlock()

	scheduler.replaceLowerPriorityTaskLocked(task)

	task.SetID(scheduler.idAllocator())
	scheduler.waitQueue.Add(task)
	scheduler.tasks.Insert(task.ID(), struct{}{})
	scheduler.incExecutingTaskDelta(task)
	switch task := task.(type) {
	case *SegmentTask:
		index := NewReplicaSegmentIndex(task)
		scheduler.segmentTasks.Insert(index, task)

	case *ChannelTask:
		index := replicaChannelIndex{task.ReplicaID(), task.Channel()}
		scheduler.channelTasks.Insert(index, task)

	case *LeaderTask:
		index := NewReplicaLeaderIndex(task)
		scheduler.segmentTasks.Insert(index, task)
	}

	scheduler.taskStats.Add(task.ID(), task)
	scheduler.updateTaskMetrics()
	mlog.Info(task.Context(), "task added", mlog.String("task", task.String()))
	task.RecordStartTs()
	scheduler.pendingRevision++
}

func (scheduler *taskScheduler) GetPendingBalanceTasks() PendingBalanceSnapshot {
	for {
		scheduler.pendingMu.RLock()
		before := scheduler.pendingRevision
		byID := make(map[int64]PendingBalanceTaskSnapshot)
		copyTask := func(task Task) bool {
			byID[task.ID()] = copyPendingBalanceTask(task)
			return true
		}
		scheduler.segmentTasks.Range(func(_ replicaSegmentIndex, task Task) bool { return copyTask(task) })
		scheduler.channelTasks.Range(func(_ replicaChannelIndex, task Task) bool { return copyTask(task) })
		after := scheduler.pendingRevision
		scheduler.pendingMu.RUnlock()
		if before != after {
			continue
		}

		tasks := make([]PendingBalanceTaskSnapshot, 0, len(byID))
		for _, pending := range byID {
			tasks = append(tasks, pending)
		}
		sort.Slice(tasks, func(i, j int) bool { return tasks[i].TaskID < tasks[j].TaskID })
		return PendingBalanceSnapshot{Revision: after, Tasks: tasks}
	}
}

func copyPendingBalanceTask(task Task) PendingBalanceTaskSnapshot {
	pending := PendingBalanceTaskSnapshot{
		TaskID:        task.ID(),
		CollectionID:  task.CollectionID(),
		ReplicaID:     task.ReplicaID(),
		ResourceGroup: task.ResourceGroup(),
		Epoch:         task.BalanceEpoch(),
		Status:        task.Status(),
		Actions:       make([]PendingBalanceActionSnapshot, 0, len(task.Actions())),
	}
	for _, action := range task.Actions() {
		primitive := PendingBalanceActionSnapshot{
			NodeID:   action.Node(),
			Type:     action.Type(),
			Workload: action.WorkLoadEffect(),
		}
		switch action := action.(type) {
		case *SegmentAction:
			primitive.SegmentID = action.GetSegmentID()
			primitive.Shard = action.GetShard()
			primitive.Scope = action.GetScope()
		case *ChannelAction:
			primitive.Channel = action.ChannelName()
			primitive.Shard = action.GetShard()
		case *LeaderAction:
			primitive.SegmentID = action.SegmentID()
			primitive.Shard = action.GetShard()
		case *DropIndexAction:
			primitive.Shard = action.GetShard()
		}
		pending.Actions = append(pending.Actions, primitive)
	}
	return pending
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

// validateAddLocked checks whether the task is valid to add without making it
// visible to scheduler queues, indexes, deltas, or metrics. The collection lock
// must be held by the caller.
func (scheduler *taskScheduler) validateAddLocked(task Task) *schedulerAdmissionError {
	switch task := task.(type) {
	case *SegmentTask:
		index := NewReplicaSegmentIndex(task)
		if old, ok := scheduler.segmentTasks.Get(index); ok {
			if task.Priority() > old.Priority() {
				return nil
			}
			return &schedulerAdmissionError{
				reason: BalanceAdmissionDuplicate,
				err:    merr.WrapErrServiceInternal("task with the same segment exists"),
			}
		}

		taskType := GetTaskType(task)

		if taskType == TaskTypeMove {
			leader := scheduler.getReplicaShardLeader(task.Shard(), task.ReplicaID())
			if leader == nil {
				return &schedulerAdmissionError{
					reason: BalanceAdmissionLeaderMissing,
					err:    merr.WrapErrServiceInternal("segment's delegator leader not found, stop balancing"),
				}
			}
			segmentInTargetNode := scheduler.distMgr.SegmentDistManager.GetByFilter(meta.WithNodeID(task.Actions()[1].Node()), meta.WithSegmentID(task.SegmentID()))
			if len(segmentInTargetNode) == 0 {
				return &schedulerAdmissionError{
					reason: BalanceAdmissionSourceGone,
					err:    merr.WrapErrServiceInternal("source segment released, stop balancing"),
				}
			}
		}

	case *ChannelTask:
		index := replicaChannelIndex{task.ReplicaID(), task.Channel()}
		if old, ok := scheduler.channelTasks.Get(index); ok {
			if task.Priority() > old.Priority() {
				return nil
			}
			return &schedulerAdmissionError{
				reason: BalanceAdmissionDuplicate,
				err:    merr.WrapErrServiceInternal("task with the same channel exists"),
			}
		}

		taskType := GetTaskType(task)
		switch taskType {
		case TaskTypeGrow:
			delegatorList := scheduler.distMgr.ChannelDistManager.GetByFilter(meta.WithChannelName2Channel(task.Channel()))
			nodesWithChannel := lo.Map(delegatorList, func(v *meta.DmChannel, _ int) UniqueID { return v.Node })
			replicaNodeMap := utils.GroupNodesByReplica(task.ctx, scheduler.meta.ReplicaManager, task.CollectionID(), nodesWithChannel)
			if _, ok := replicaNodeMap[task.ReplicaID()]; ok {
				return &schedulerAdmissionError{
					reason: BalanceAdmissionDuplicate,
					err:    merr.WrapErrServiceInternal("channel subscribed, it can be only balanced"),
				}
			}
		case TaskTypeMove:
			delegatorList := scheduler.distMgr.ChannelDistManager.GetByFilter(meta.WithChannelName2Channel(task.Channel()))
			_, ok := lo.Find(delegatorList, func(v *meta.DmChannel) bool { return v.Node == task.Actions()[1].Node() })
			if !ok {
				return &schedulerAdmissionError{
					reason: BalanceAdmissionSourceGone,
					err:    merr.WrapErrServiceInternal("source channel unsubscribed, stop balancing"),
				}
			}
		}
	case *LeaderTask:
		index := NewReplicaLeaderIndex(task)
		if old, ok := scheduler.segmentTasks.Get(index); ok {
			if task.Priority() <= old.Priority() {
				return &schedulerAdmissionError{
					reason: BalanceAdmissionDuplicate,
					err:    merr.WrapErrServiceInternal("task with the same segment exists"),
				}
			}
		}
	case *DropIndexTask:
		index := NewReplicaDropIndex(task)
		if old, ok := scheduler.segmentTasks.Get(index); ok {
			if task.Priority() <= old.Priority() {
				return &schedulerAdmissionError{
					reason: BalanceAdmissionDuplicate,
					err:    merr.WrapErrServiceInternal("task with the same segment exists"),
				}
			}
		}
	default:
		panic(fmt.Sprintf("validateAddLocked: forget to process task type: %+v", task))
	}
	return nil
}

// replaceLowerPriorityTaskLocked preserves Add's existing replacement behavior,
// but delays the mutation until admission has passed its final validation.
func (scheduler *taskScheduler) replaceLowerPriorityTaskLocked(task Task) {
	var old Task
	var ok bool
	switch task := task.(type) {
	case *SegmentTask:
		old, ok = scheduler.segmentTasks.Get(NewReplicaSegmentIndex(task))
	case *ChannelTask:
		old, ok = scheduler.channelTasks.Get(replicaChannelIndex{task.ReplicaID(), task.Channel()})
	case *LeaderTask:
		old, ok = scheduler.segmentTasks.Get(NewReplicaLeaderIndex(task))
	case *DropIndexTask:
		old, ok = scheduler.segmentTasks.Get(NewReplicaDropIndex(task))
	default:
		panic(fmt.Sprintf("replaceLowerPriorityTaskLocked: forget to process task type: %+v", task))
	}
	if !ok || task.Priority() <= old.Priority() {
		return
	}

	mlog.Info(scheduler.ctx, "replace old task, the new one with higher priority",
		mlog.Int64("oldID", old.ID()),
		mlog.String("oldPriority", old.Priority().String()),
		mlog.Int64("newID", task.ID()),
		mlog.String("newPriority", task.Priority().String()),
	)
	old.Cancel(merr.WrapErrServiceInternal("replaced with the other one with higher priority"))
	scheduler.removePendingLocked(old)
}

func (scheduler *taskScheduler) getReplicaShardLeader(channelName string, replicaID int64) *meta.DmChannel {
	replica := scheduler.meta.Get(scheduler.ctx, replicaID)
	if replica == nil {
		return nil
	}
	return scheduler.distMgr.ChannelDistManager.GetShardLeader(channelName, replica)
}

func (scheduler *taskScheduler) tryPromoteAll() {
	// Promote waiting tasks
	toPromote := make([]Task, 0, scheduler.waitQueue.Len())
	toRemove := make([]Task, 0)
	scheduler.waitQueue.Range(func(task Task) bool {
		err := scheduler.promote(task)
		if err != nil {
			task.Cancel(err)
			toRemove = append(toRemove, task)
			mlog.Warn(scheduler.ctx, "failed to promote task",
				mlog.Int64("taskID", task.ID()),
				mlog.Err(err),
			)
		} else {
			toPromote = append(toPromote, task)
		}

		return true
	})

	for _, task := range toPromote {
		scheduler.waitQueue.Remove(task)
	}
	for _, task := range toRemove {
		scheduler.remove(task)
	}

	if len(toPromote) > 0 || len(toRemove) > 0 {
		mlog.Debug(scheduler.ctx, "promoted tasks",
			mlog.Int("promotedNum", len(toPromote)),
			mlog.Int("toRemoveNum", len(toRemove)))
	}
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
	task.SetStatus(TaskStatusStarted)
	return nil
}

func (scheduler *taskScheduler) Dispatch(node int64) {
	select {
	case <-scheduler.ctx.Done():
		mlog.Info(scheduler.ctx, "scheduler stopped")

	default:
		scheduler.scheduleMu.Lock()
		defer scheduler.scheduleMu.Unlock()
		scheduler.schedule(node)
	}
}

func (scheduler *taskScheduler) GetSegmentTaskDeltaSnapshot(nodeIDs []int64, collectionID int64) *SegmentTaskDeltaSnapshot {
	return scheduler.segmentTaskDelta.GetSegmentSnapshot(nodeIDs, collectionID, scheduler.distMgr)
}

func (scheduler *taskScheduler) GetChannelTaskDeltaSnapshot(nodeIDs []int64, collectionID int64) *ChannelTaskDeltaSnapshot {
	return scheduler.channelTaskDelta.GetChannelSnapshot(nodeIDs, collectionID, scheduler.distMgr)
}

func (scheduler *taskScheduler) GetChannelTaskDelta(nodeID, collectionID int64) int {
	snapshot := scheduler.GetChannelTaskDeltaSnapshot([]int64{nodeID}, collectionID)
	return getChannelSnapshotDelta(snapshot, nodeID, collectionID)
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

	// Process tasks
	toProcess := make([]Task, 0)
	toRemove := make([]Task, 0)
	scheduler.processQueue.RangeByNode(node, func(task Task) bool {
		shouldProcess := scheduler.preProcess(task)
		if shouldProcess {
			toProcess = append(toProcess, task)
		} else if task.Status() != TaskStatusStarted {
			toRemove = append(toRemove, task)
		}

		return true
	})
	preprocessDur := tr.RecordSpan()

	// The scheduler doesn't limit the number of tasks,
	// to commit tasks to executors as soon as possible, to reach higher merge possibility
	commmittedNum := atomic.NewInt32(0)
	funcutil.ProcessFuncParallel(len(toProcess), hardware.GetCPUNum(), func(idx int) error {
		if scheduler.process(toProcess[idx]) {
			commmittedNum.Inc()
		}
		return nil
	}, "process")
	processDur := tr.RecordSpan()

	for _, task := range toRemove {
		scheduler.remove(task)
	}

	scheduler.updateTaskMetrics()

	log.Info(scheduler.ctx, "processed tasks",
		mlog.Int("toProcessNum", len(toProcess)),
		mlog.Int32("committedNum", commmittedNum.Load()),
		mlog.Int("toRemoveNum", len(toRemove)),
		mlog.Duration("promoteDur", promoteDur),
		mlog.Duration("preprocessDUr", preprocessDur),
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
	scheduler.scheduleMu.Lock()
	defer scheduler.scheduleMu.Unlock()

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

func (scheduler *taskScheduler) remove(task Task) {
	scheduler.pendingMu.Lock()
	defer scheduler.pendingMu.Unlock()
	scheduler.removePendingLocked(task)
}

func (scheduler *taskScheduler) removePendingLocked(task Task) {
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

	task.Cancel(nil)
	_, ok := scheduler.tasks.GetAndRemove(task.ID())
	scheduler.waitQueue.Remove(task)
	scheduler.processQueue.Remove(task)
	if ok {
		scheduler.decExecutingTaskDelta(task)

		if scheduler.tasks.Len() == 0 {
			// in case of task delta leak, try to print detail info before clear
			scheduler.segmentTaskDelta.printDetailInfos()
			scheduler.segmentTaskDelta.Clear()
			scheduler.channelTaskDelta.printDetailInfos()
			scheduler.channelTaskDelta.Clear()
		}
	}

	switch task := task.(type) {
	case *SegmentTask:
		index := NewReplicaSegmentIndex(task)
		scheduler.segmentTasks.Remove(index)
		log = mlog.With(mlog.Int64("segmentID", task.SegmentID()))
		if task.Status() == TaskStatusFailed &&
			task.Err() != nil &&
			!errors.IsAny(task.Err(), merr.ErrChannelNotFound, merr.ErrServiceTooManyRequests) {
			scheduler.recordSegmentTaskError(task)
		}

	case *ChannelTask:
		index := replicaChannelIndex{task.ReplicaID(), task.Channel()}
		scheduler.channelTasks.Remove(index)
		log = mlog.With(mlog.String("channel", task.Channel()))

	case *LeaderTask:
		index := NewReplicaLeaderIndex(task)
		scheduler.segmentTasks.Remove(index)
		log = mlog.With(mlog.Int64("segmentID", task.SegmentID()))
	}

	log.Info(task.Context(), "task removed")

	if scheduler.meta.Exist(task.Context(), task.CollectionID()) {
		metrics.QueryCoordTaskLatency.WithLabelValues(fmt.Sprint(task.CollectionID()),
			scheduler.getTaskMetricsLabel(task), task.Shard()).Observe(float64(task.GetTaskLatency()))
	}
	if ok {
		scheduler.pendingRevision++
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
	// Get replica, but only fail if we need it for RO node check
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
		}
	}

	return nil
}
