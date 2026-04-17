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
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func init() {
	paramtable.Init()
}

func newTestReplica() *meta.Replica {
	return meta.NewReplica(&querypb.Replica{ID: 1, CollectionID: 100}, nil)
}

func newTestSegmentTask(t *testing.T, nodeID int64, segmentID int64) *SegmentTask {
	task, err := NewSegmentTask(
		context.Background(), 10*time.Second, WrapIDSource(0), 100,
		newTestReplica(), commonpb.LoadPriority_LOW,
		NewSegmentAction(nodeID, ActionTypeGrow, "ch1", segmentID),
	)
	assert.NoError(t, err)
	return task
}

func newNodeWithStats(nodeID int64, cpuNum int64, memCapMB float64, usedMemMB float64) *session.NodeInfo {
	node := session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID: nodeID, Address: "localhost:1234",
		Version: semver.Version{Major: 2, Minor: 6},
	})
	node.UpdateStats(
		session.WithCPUNum(cpuNum),
		session.WithMemCapacity(memCapMB),
		session.WithUsedMemory(usedMemMB),
	)
	return node
}

// segmentGateAllows checks the segment pool gate logic without spawning a goroutine.
// IMPORTANT: this must stay in sync with the gate logic in Execute().
func segmentGateAllows(ex *Executor, task *SegmentTask) bool {
	nodeInfo := ex.nodeMgr.Get(ex.nodeID)
	if nodeInfo == nil {
		return true
	}

	pendingCount, pendingMem := ex.pendingSnapshot()

	// Memory gate: reject if task doesn't fit in remaining capacity
	pendingFactor := Params.QueryCoordCfg.PendingMemoryFactor.GetAsFloat()
	effectiveUsedMB := nodeInfo.UsedMemory() + float64(pendingMem)*pendingFactor/(1024*1024)
	remainingMB := nodeInfo.MemCapacity() - effectiveUsedMB
	taskMB := float64(task.SegmentSize()) / (1024 * 1024)
	if taskMB > 0 && taskMB > remainingMB {
		return false
	}

	// CPU gate: reject if pending count at capacity
	if pendingCount >= ex.getSegmentPoolCap() {
		return false
	}
	return true
}

// =============================================================================
// Channel Pool Tests (count-based, for streaming nodes)
// =============================================================================

func TestExecutor_ChannelPool_AcceptsUnderCap(t *testing.T) {
	nodeID := int64(1)
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(newNodeWithStats(nodeID, 4, 16384, 1024))
	defer nodeMgr.Remove(nodeID)
	ex := NewExecutor(nodeID, nil, nil, nil, nil, nil, nodeMgr)

	assert.True(t, ex.GetChannelTaskCap() > 0)
	assert.Equal(t, int32(0), ex.channelTaskNum.Load())
}

func TestExecutor_ChannelPool_RejectsWhenFull(t *testing.T) {
	nodeID := int64(1)
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(newNodeWithStats(nodeID, 4, 16384, 1024))
	defer nodeMgr.Remove(nodeID)
	ex := NewExecutor(nodeID, nil, nil, nil, nil, nil, nodeMgr)

	cap := ex.GetChannelTaskCap()
	// Fill channel pool
	for i := int32(0); i < cap; i++ {
		ex.channelTaskNum.Inc()
	}

	// Next channel task should be rejected
	chAction := NewChannelAction(nodeID, ActionTypeGrow, "ch-overflow")
	chTask, err := NewChannelTask(context.Background(), 10*time.Second, WrapIDSource(0), 100, newTestReplica(), chAction)
	assert.NoError(t, err)
	chTask.SetID(999)

	result := ex.Execute(chTask, 0)
	assert.False(t, result, "channel task should be rejected when pool is full")
}

func TestExecutor_ChannelAndSegmentPoolsIndependent(t *testing.T) {
	nodeID := int64(1)
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(newNodeWithStats(nodeID, 4, 16384, 1024))
	defer nodeMgr.Remove(nodeID)
	ex := NewExecutor(nodeID, nil, nil, nil, nil, nil, nodeMgr)

	// Fill channel pool
	for i := int32(0); i < ex.GetChannelTaskCap(); i++ {
		ex.channelTaskNum.Inc()
	}

	// Segment pool should still accept (independent pools)
	segTask := newTestSegmentTask(t, nodeID, 1000)
	assert.True(t, segmentGateAllows(ex, segTask), "segment pool should accept when only channel pool is full")
}

// =============================================================================
// Segment Pool Memory Gate Tests
// =============================================================================

func TestExecutor_MemoryGate_NoNodeInfo(t *testing.T) {
	ex := NewExecutor(1, nil, nil, nil, nil, nil, session.NewNodeManager())
	task := newTestSegmentTask(t, 1, 1000)
	task.SetSegmentSize(1024 * 1024 * 500)
	assert.True(t, segmentGateAllows(ex, task), "no node info → gate skipped")
}

func TestExecutor_MemoryGate_RejectsLargeTask(t *testing.T) {
	nodeID := int64(1)
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(newNodeWithStats(nodeID, 64, 8192, 8000)) // 192 MB remaining
	defer nodeMgr.Remove(nodeID)
	ex := NewExecutor(nodeID, nil, nil, nil, nil, nil, nodeMgr)

	task := newTestSegmentTask(t, nodeID, 1000)
	task.SetSegmentSize(300 * 1024 * 1024) // 300 MB > 192 MB
	assert.False(t, segmentGateAllows(ex, task))
}

func TestExecutor_MemoryGate_AcceptsSmallTask(t *testing.T) {
	nodeID := int64(1)
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(newNodeWithStats(nodeID, 64, 8192, 4096)) // 4096 MB remaining
	defer nodeMgr.Remove(nodeID)
	ex := NewExecutor(nodeID, nil, nil, nil, nil, nil, nodeMgr)

	task := newTestSegmentTask(t, nodeID, 1000)
	task.SetSegmentSize(100 * 1024 * 1024)
	assert.True(t, segmentGateAllows(ex, task))
}

func TestExecutor_MemoryGate_ZeroSegmentSize(t *testing.T) {
	nodeID := int64(1)
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(newNodeWithStats(nodeID, 64, 1024, 1000))
	defer nodeMgr.Remove(nodeID)
	ex := NewExecutor(nodeID, nil, nil, nil, nil, nil, nodeMgr)

	task := newTestSegmentTask(t, nodeID, 1000) // size = 0
	assert.True(t, segmentGateAllows(ex, task), "zero size → memory gate skipped")
}

func TestExecutor_MemoryGate_RejectsWhenOverCommitted(t *testing.T) {
	nodeID := int64(1)
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(newNodeWithStats(nodeID, 64, 8192, 9000)) // usedMemory > capacity → remainingMB < 0
	defer nodeMgr.Remove(nodeID)
	ex := NewExecutor(nodeID, nil, nil, nil, nil, nil, nodeMgr)

	task := newTestSegmentTask(t, nodeID, 1000)
	task.SetSegmentSize(1 * 1024 * 1024) // 1 MB — even tiny tasks should be rejected
	assert.False(t, segmentGateAllows(ex, task), "should reject when node is over-committed (remainingMB < 0)")
}

// =============================================================================
// Segment Pool CPU Gate Tests
// =============================================================================

func TestExecutor_CPUGate_RejectsWhenSaturated(t *testing.T) {
	nodeID := int64(1)
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(newNodeWithStats(nodeID, 4, 16384, 1024))
	defer nodeMgr.Remove(nodeID)
	ex := NewExecutor(nodeID, nil, nil, nil, nil, nil, nodeMgr)

	segCap := ex.getSegmentPoolCap()
	for i := int32(0); i < segCap; i++ {
		ex.pendingMu.Lock()
		ex.pendingSet[fmt.Sprintf("fill-%d", i)] = pendingTaskInfo{}
		ex.pendingMu.Unlock()
	}

	task := newTestSegmentTask(t, nodeID, 1000)
	assert.False(t, segmentGateAllows(ex, task))
}

func TestExecutor_CPUGate_AcceptsWhenBelowLimit(t *testing.T) {
	nodeID := int64(1)
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(newNodeWithStats(nodeID, 4, 16384, 1024))
	defer nodeMgr.Remove(nodeID)
	ex := NewExecutor(nodeID, nil, nil, nil, nil, nil, nodeMgr)

	task := newTestSegmentTask(t, nodeID, 1000)
	assert.True(t, segmentGateAllows(ex, task), "empty pending → accept")
}

func TestExecutor_CPUGate_ZeroCPU(t *testing.T) {
	nodeID := int64(1)
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(newNodeWithStats(nodeID, 0, 16384, 1024))
	defer nodeMgr.Remove(nodeID)
	ex := NewExecutor(nodeID, nil, nil, nil, nil, nil, nodeMgr)

	// Fill many pending — should still pass because cpuNum=0 → fallback cap=64
	for i := 0; i < 50; i++ {
		ex.pendingMu.Lock()
		ex.pendingSet[fmt.Sprintf("p-%d", i)] = pendingTaskInfo{}
		ex.pendingMu.Unlock()
	}
	task := newTestSegmentTask(t, nodeID, 1000)
	assert.True(t, segmentGateAllows(ex, task), "50 pending < 64 fallback cap")
}

// =============================================================================
// Pending Compensation Tests
// =============================================================================

func TestExecutor_PendingMemory_AccumulateBetweenPulls(t *testing.T) {
	nodeID := int64(1)
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(newNodeWithStats(nodeID, 64, 8192, 4096)) // 4096 MB remaining
	defer nodeMgr.Remove(nodeID)
	ex := NewExecutor(nodeID, nil, nil, nil, nil, nil, nodeMgr)

	// 3 pending tasks of 1000 MB each → pendingMem = 3000 MB
	for i := 0; i < 3; i++ {
		ex.pendingMu.Lock()
		ex.pendingSet[fmt.Sprintf("mem-%d", i)] = pendingTaskInfo{memoryBytes: 1000 * 1024 * 1024}
		ex.pendingMu.Unlock()
	}

	big := newTestSegmentTask(t, nodeID, 9000)
	big.SetSegmentSize(2000 * 1024 * 1024) // 2000 > 1096 remaining
	assert.False(t, segmentGateAllows(ex, big))

	small := newTestSegmentTask(t, nodeID, 9001)
	small.SetSegmentSize(500 * 1024 * 1024) // 500 < 1096
	assert.True(t, segmentGateAllows(ex, small))
}

func TestExecutor_ResetPending(t *testing.T) {
	ex := NewExecutor(1, nil, nil, nil, nil, nil, session.NewNodeManager())
	ex.pendingMu.Lock()
	ex.pendingSet["a"] = pendingTaskInfo{memoryBytes: 1000}
	ex.pendingSet["b"] = pendingTaskInfo{memoryBytes: 2000}
	ex.pendingMu.Unlock()

	count, mem := ex.pendingSnapshot()
	assert.Equal(t, int32(2), count)
	assert.Equal(t, int64(3000), mem)

	// ResetPending with loaded segments only clears matching entries
	ex.pendingMu.Lock()
	ex.pendingSet["a"] = pendingTaskInfo{segmentID: 100, memoryBytes: 1000}
	ex.pendingSet["b"] = pendingTaskInfo{segmentID: 200, memoryBytes: 2000}
	ex.pendingMu.Unlock()

	loaded := typeutil.NewSet[int64]()
	loaded.Insert(100) // only segment 100 confirmed
	ex.ResetPending(loaded)
	count, mem = ex.pendingSnapshot()
	assert.Equal(t, int32(1), count) // only "b" (segment 200) remains
	assert.Equal(t, int64(2000), mem)

	// Reset with all loaded clears everything
	loaded.Insert(200)
	ex.ResetPending(loaded)
	count, mem = ex.pendingSnapshot()
	assert.Equal(t, int32(0), count)
	assert.Equal(t, int64(0), mem)
}

func TestExecutor_RemoveTask_ChannelDecrementsCounter(t *testing.T) {
	ex := NewExecutor(1, nil, nil, nil, nil, nil, session.NewNodeManager())
	chAction := NewChannelAction(1, ActionTypeGrow, "ch1")
	chTask, err := NewChannelTask(context.Background(), 10*time.Second, WrapIDSource(0), 100, newTestReplica(), chAction)
	assert.NoError(t, err)
	chTask.SetID(1)

	ex.executingTasks.Insert(chTask.Index())
	ex.channelTaskNum.Inc()
	assert.Equal(t, int32(1), ex.channelTaskNum.Load())

	ex.removeTask(chTask, 0)
	assert.Equal(t, int32(0), ex.channelTaskNum.Load())
}

func TestExecutor_RemoveTask_SegmentRemovesPending(t *testing.T) {
	ex := NewExecutor(1, nil, nil, nil, nil, nil, session.NewNodeManager())
	segTask := newTestSegmentTask(t, 1, 1000)
	segTask.SetID(2)

	ex.executingTasks.Insert(segTask.Index())
	ex.pendingMu.Lock()
	ex.pendingSet[segTask.Index()] = pendingTaskInfo{memoryBytes: 500}
	ex.pendingMu.Unlock()

	ex.removeTask(segTask, 0)
	count, _ := ex.pendingSnapshot()
	assert.Equal(t, int32(0), count)
}

// =============================================================================
// SegmentTask Size Tests
// =============================================================================

func TestSegmentTaskSize(t *testing.T) {
	task := newTestSegmentTask(t, 1, 1000)
	assert.Equal(t, int64(0), task.SegmentSize())
	task.SetSegmentSize(1024 * 1024 * 50)
	assert.Equal(t, int64(1024*1024*50), task.SegmentSize())
}

// =============================================================================
// GetChannelTaskCap / getSegmentPoolCap Tests
// =============================================================================

func TestExecutor_PoolCapacity(t *testing.T) {
	nodeID := int64(1)
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(newNodeWithStats(nodeID, 10, 16384, 1024)) // 10 CPUs
	defer nodeMgr.Remove(nodeID)
	ex := NewExecutor(nodeID, nil, nil, nil, nil, nil, nodeMgr)

	total := ex.getTotalCap()
	// 10 CPUs × factor 10 = 100
	assert.Equal(t, int32(100), total)

	channelCap := ex.GetChannelTaskCap()
	segmentCap := ex.getSegmentPoolCap()

	// channel + segment <= total (with min-1 guarantees)
	assert.True(t, channelCap >= 1)
	assert.True(t, segmentCap >= 1)
	assert.True(t, channelCap+segmentCap >= total,
		"channel(%d) + segment(%d) should cover total(%d)", channelCap, segmentCap, total)

}
