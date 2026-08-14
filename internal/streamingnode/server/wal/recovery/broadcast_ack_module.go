package recovery

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

const broadcastAckRetryInterval = 200 * time.Millisecond

type broadcastAckModule struct {
	runtime    moduleapi.Runtime
	ctx        context.Context
	cancel     context.CancelFunc
	retryDelay time.Duration
	closeOnce  sync.Once
	workerMu   sync.Mutex
	closed     bool
	workerWG   sync.WaitGroup
	ackTaskMu  sync.Mutex
	ackTasks   []*broadcastAckTask
	wakeup     chan struct{}
	ack        func(context.Context, message.ImmutableMessage) error
}

func newBroadcastAckModule(runtime moduleapi.Runtime) *broadcastAckModule {
	ctx, cancel := context.WithCancel(context.Background())
	m := &broadcastAckModule{
		runtime:    runtime,
		ctx:        ctx,
		cancel:     cancel,
		retryDelay: broadcastAckRetryInterval,
		wakeup:     make(chan struct{}, 1),
		ack: func(ctx context.Context, msg message.ImmutableMessage) error {
			return streaming.WAL().Broadcast().Ack(ctx, msg)
		},
	}
	m.workerWG.Add(1)
	go m.background()
	return m
}

func (m *broadcastAckModule) Accept(
	owner message.OwnedImmutableMessage,
) {
	if owner.Message().BroadcastHeader() == nil {
		owner.Release()
		return
	}
	task := &broadcastAckTask{
		module:       m,
		owner:        owner,
		resourceKeys: normalizeBroadcastAckResourceKeys(owner.Message().BroadcastHeader().ResourceKeys.Collect()),
	}
	m.ackTaskMu.Lock()
	m.ackTasks = append(m.ackTasks, task)
	m.ackTaskMu.Unlock()
	owner.RegisterExclusiveCallback(func() {
		task.exclusive.Store(true)
		m.wakeDispatcher()
	})
}

func (m *broadcastAckModule) background() {
	defer m.workerWG.Done()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.wakeup:
			m.dispatchReadyTasks()
		}
	}
}

func (m *broadcastAckModule) wakeDispatcher() {
	select {
	case m.wakeup <- struct{}{}:
	default:
	}
}

func (m *broadcastAckModule) dispatchReadyTasks() {
	if m.runtime.Scheduler == nil || m.ctx.Err() != nil {
		return
	}

	m.ackTaskMu.Lock()
	m.compactCompletedTasksLocked()
	readyTasks := make([]*broadcastAckTask, 0)
	for idx, task := range m.ackTasks {
		if !task.exclusive.Load() || task.inFlight {
			continue
		}
		if hasEarlierBroadcastAckConflict(m.ackTasks[:idx], task) {
			continue
		}
		task.inFlight = true
		readyTasks = append(readyTasks, task)
	}
	m.ackTaskMu.Unlock()

	for _, task := range readyTasks {
		m.runtime.Scheduler.Submit(task)
	}
}

func (m *broadcastAckModule) retry(task *broadcastAckTask) {
	if !m.beginWorker() {
		return
	}
	go func() {
		defer m.workerWG.Done()
		timer := time.NewTimer(m.retryDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
			m.ackTaskMu.Lock()
			if !task.completed {
				task.inFlight = false
			}
			m.ackTaskMu.Unlock()
			m.wakeDispatcher()
		case <-m.ctx.Done():
		}
	}()
}

func (m *broadcastAckModule) Close() {
	m.closeOnce.Do(func() {
		m.workerMu.Lock()
		m.closed = true
		m.cancel()
		m.workerMu.Unlock()
		m.workerWG.Wait()
	})
}

func (m *broadcastAckModule) beginWorker() bool {
	m.workerMu.Lock()
	defer m.workerMu.Unlock()
	if m.closed {
		return false
	}
	m.workerWG.Add(1)
	return true
}

func (m *broadcastAckModule) finishTask(task *broadcastAckTask) {
	m.ackTaskMu.Lock()
	task.completed = true
	task.inFlight = false
	m.ackTaskMu.Unlock()
	m.wakeDispatcher()
}

func (m *broadcastAckModule) compactCompletedTasksLocked() {
	pending := m.ackTasks[:0]
	for _, task := range m.ackTasks {
		if !task.completed {
			pending = append(pending, task)
		}
	}
	clear(m.ackTasks[len(pending):])
	m.ackTasks = pending
}

type broadcastAckTask struct {
	module       *broadcastAckModule
	owner        message.OwnedImmutableMessage
	resourceKeys []message.ResourceKey
	exclusive    atomic.Bool
	inFlight     bool
	completed    bool
}

func (t *broadcastAckTask) Execute(ctx context.Context) error {
	if err := t.module.ack(ctx, t.owner.Message()); err != nil {
		t.module.retry(t)
		return nil
	}
	t.owner.Release()
	t.module.finishTask(t)
	return nil
}

func normalizeBroadcastAckResourceKeys(keys []message.ResourceKey) []message.ResourceKey {
	if len(keys) == 0 {
		return []message.ResourceKey{message.NewExclusiveClusterResourceKey()}
	}
	for _, key := range keys {
		if key.Domain == message.NewSharedClusterResourceKey().Domain {
			return keys
		}
	}
	return append(keys, message.NewSharedClusterResourceKey())
}

func hasEarlierBroadcastAckConflict(earlier []*broadcastAckTask, task *broadcastAckTask) bool {
	for _, candidate := range earlier {
		if broadcastAckResourceKeysConflict(candidate.resourceKeys, task.resourceKeys) {
			return true
		}
	}
	return false
}

func broadcastAckResourceKeysConflict(left, right []message.ResourceKey) bool {
	for _, leftKey := range left {
		for _, rightKey := range right {
			if leftKey.Domain == rightKey.Domain &&
				leftKey.Key == rightKey.Key &&
				(!leftKey.Shared || !rightKey.Shared) {
				return true
			}
		}
	}
	return false
}

var _ nodescheduler.Task = (*broadcastAckTask)(nil)
