// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package datanode

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datanode/index"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// This task exercises node wiring without invoking a native index build.
type nodeMemoryTestTask struct {
	index.Task
	started chan struct{}
	memory  int64
	execute func(context.Context) error
}

func (*nodeMemoryTestTask) Ctx() context.Context { return context.Background() }
func (*nodeMemoryTestTask) Name() string         { return "node-memory-wiring" }
func (*nodeMemoryTestTask) GetSlot() int64       { return 128 }
func (t *nodeMemoryTestTask) GetResource() taskcommon.Resource {
	memory := t.memory
	if memory == 0 {
		memory = 4 << 30
	}
	return taskcommon.Resource{CPU: 8, Memory: memory}
}
func (*nodeMemoryTestTask) OnEnqueue(context.Context) error    { return nil }
func (*nodeMemoryTestTask) IsVectorIndex() bool                { return false }
func (*nodeMemoryTestTask) SetState(indexpb.JobState, string)  {}
func (*nodeMemoryTestTask) GetState() indexpb.JobState         { return indexpb.JobState_JobStateInProgress }
func (t *nodeMemoryTestTask) PreExecute(context.Context) error { close(t.started); return nil }
func (t *nodeMemoryTestTask) Execute(ctx context.Context) error {
	if t.execute != nil {
		return t.execute(ctx)
	}
	return nil
}
func (*nodeMemoryTestTask) PostExecute(context.Context) error { return nil }
func (*nodeMemoryTestTask) Reset()                            {}

func TestNodeMemoryAdmissionWiringSharesBudgetWithOtherExecutors(t *testing.T) {
	paramtable.Init()
	cpu := mockey.Mock(hardware.GetCPUNum).Return(8).Build()
	defer cpu.UnPatch()
	memory := mockey.Mock(hardware.GetMemoryCount).Return(uint64(8) << 30).Build()
	defer memory.UnPatch()
	resident := mockey.Mock(hardware.GetUsedMemoryCount).Return(uint64(0)).Build()
	defer resident.UnPatch()
	role := mockey.Mock(paramtable.GetRole).Return(typeutil.DataNodeRole).Build()
	defer role.UnPatch()
	node := newResourceTestNode(t)
	// The production Init path invokes this after creating all executors.
	node.initTaskMemoryAdmission()
	require.NoError(t, node.taskScheduler.Start())
	external := make(chan struct{})
	// Always release the external task before the node cleanup waits on it.
	t.Cleanup(func() {
		select {
		case <-external:
		default:
			close(external)
		}
	})
	require.NoError(t, node.externalCollectionManager.SubmitTask("memory-test",
		&datapb.RefreshExternalCollectionTaskRequest{TaskID: 999, CollectionID: 100},
		taskcommon.Resource{CPU: 1, Memory: 6 << 30},
		func(context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
			<-external
			return &datapb.RefreshExternalCollectionTaskResponse{}, nil
		}))
	task := &nodeMemoryTestTask{started: make(chan struct{})}
	require.NoError(t, node.taskScheduler.TaskQueue.Enqueue(task))
	select {
	case <-task.started:
		t.Fatal("index task ran despite other executor's memory booking")
	case <-time.After(150 * time.Millisecond):
	}
	report, err := node.QuerySlot(context.Background(), nil)
	require.NoError(t, err)
	require.Zero(t, report.GetAvailableSlots(), "queued scalar demand still reaches the compatibility report")
	require.Zero(t, report.GetAvailableMemory(), "accepted work includes the pending task")
	close(external)
	select {
	case <-task.started:
	case <-time.After(5 * time.Second):
		t.Fatal("index task did not start after external memory was released")
	}
	require.Eventually(t, func() bool { return node.taskScheduler.TaskQueue.GetUsingResource().Memory == 0 }, 5*time.Second, time.Millisecond)
}

type nodeMemoryV3Task struct {
	id      int64
	execute func(context.Context) error
}

func (t *nodeMemoryV3Task) TaskID() int64 { return t.id }
func (*nodeMemoryV3Task) RunID() int64    { return 1 }
func (*nodeMemoryV3Task) Kind() string    { return "import" }
func (*nodeMemoryV3Task) Slot() int64     { return 128 }
func (*nodeMemoryV3Task) Resource() taskcommon.Resource {
	return taskcommon.Resource{CPU: 8, Memory: 2 << 30}
}
func (t *nodeMemoryV3Task) Execute(ctx context.Context) (any, error) { return nil, t.execute(ctx) }

// Run both real schedulers concurrently with more accepted work than execution
// capacity. Independent budgets would start eight tasks; counting queued work
// in the execution budget would start none. Exactly four must run together.
func TestNodeMemoryAdmissionIndexAndV3ShareAtomicExecutionBudget(t *testing.T) {
	paramtable.Init()
	cpu := mockey.Mock(hardware.GetCPUNum).Return(8).Build()
	defer cpu.UnPatch()
	memory := mockey.Mock(hardware.GetMemoryCount).Return(uint64(8) << 30).Build()
	defer memory.UnPatch()
	resident := mockey.Mock(hardware.GetUsedMemoryCount).Return(uint64(0)).Build()
	defer resident.UnPatch()
	role := mockey.Mock(paramtable.GetRole).Return(typeutil.DataNodeRole).Build()
	defer role.UnPatch()
	node := newResourceTestNode(t)
	node.initTaskMemoryAdmission()
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	entered := make(chan struct{}, 8)
	var active, peak atomic.Int32
	execute := func(context.Context) error {
		now := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); now > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, now) {
				break
			}
		}
		entered <- struct{}{}
		<-gate
		return nil
	}
	for i := int64(1); i <= 4; i++ {
		require.NoError(t, node.taskScheduler.TaskQueue.Enqueue(&nodeMemoryTestTask{
			started: make(chan struct{}), memory: 2 << 30, execute: execute,
		}))
		require.NoError(t, node.importV3Scheduler.Submit(&nodeMemoryV3Task{id: i, execute: execute}))
	}
	require.EqualValues(t, 16<<30, node.taskScheduler.TaskQueue.GetUsingResource().Memory+node.importV3Scheduler.Resource().Memory)
	require.NoError(t, node.taskScheduler.Start())
	go node.importV3Scheduler.Start()
	for i := 0; i < 4; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("queued acceptance blocked execution")
		}
	}
	select {
	case <-entered:
		t.Fatal("schedulers double-booked the same node memory")
	case <-time.After(150 * time.Millisecond):
	}
	require.EqualValues(t, 4, active.Load())
	release()
	for i := 0; i < 4; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("shared memory was not returned")
		}
	}
	require.Eventually(t, func() bool {
		return node.taskScheduler.TaskQueue.GetUsingResource().Memory+node.importV3Scheduler.Resource().Memory == 0
	}, 5*time.Second, time.Millisecond)
	require.EqualValues(t, 4, peak.Load())
}
