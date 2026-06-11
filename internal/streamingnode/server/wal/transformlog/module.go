package transformlog

import (
	"context"
	"math"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	transformlogapi "github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

type moduleMode int

const (
	moduleModeMetaOnly moduleMode = iota
	moduleModeMetaAndData
)

type Module struct {
	mu          sync.Mutex
	pchannel    string
	logs        map[string]*moduleLog
	store       Store
	maxRows     uint64
	runtime     moduleapi.Runtime
	mode        moduleMode
	initialized bool
}

type ModuleOption func(*Module)

func WithModuleRuntime(runtime moduleapi.Runtime) ModuleOption {
	return func(m *Module) {
		m.runtime = runtime
	}
}

func WithModuleMaxRows(maxRows uint64) ModuleOption {
	return func(m *Module) {
		m.maxRows = maxRows
	}
}

func NewModule(
	pchannel string,
	metas map[string]*streamingpb.VChannelTransformLogMeta,
	store Store,
	opts ...ModuleOption,
) *Module {
	module := &Module{
		pchannel: pchannel,
		logs:     make(map[string]*moduleLog, len(metas)),
		store:    store,
	}
	for _, opt := range opts {
		opt(module)
	}
	for vchannel, meta := range metas {
		module.logs[vchannel] = newModuleLog(module.newTransformLog(vchannel, meta))
	}
	return module
}

func (m *Module) Name() moduleapi.ModuleName {
	return moduleapi.ModuleNameTransformLog
}

func (m *Module) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	switch msg.MessageType() {
	case message.MessageTypeDelete:
		return m.observeTransformLogMessage(msg)
	case message.MessageTypeTxn:
		return m.observeTransformLogMessage(msg)
	case message.MessageTypeDropCollection,
		message.MessageTypeTruncateCollection,
		message.MessageTypeDropPartition,
		message.MessageTypeFlushAll,
		message.MessageTypeAlterWAL:
		return m.flushByMessageTimeTick(msg.TimeTick(), msg.VChannel(), msg.MessageType())
	case message.MessageTypeAlterCollection:
		alter := message.MustAsImmutableAlterCollectionMessageV2(msg)
		if messageutil.IsSchemaChange(alter.Header()) {
			return m.flushByMessageTimeTick(msg.TimeTick(), msg.VChannel(), msg.MessageType())
		}
	}
	return moduleapi.ObserveResult{}
}

func (m *Module) SwitchIntoMetaAndData() moduleapi.ModuleSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mode = moduleModeMetaAndData
	snapshot := make(map[string]*streamingpb.VChannelTransformLogMeta, len(m.logs))
	for vchannel, log := range m.logs {
		snapshot[vchannel] = log.log.SnapshotMeta()
	}
	return &moduleapi.TransformLogModuleSnapshot{TransformLogs: snapshot}
}

func (m *Module) ConsumeDirtySnapshots() []moduleapi.DirtySnapshot {
	logs := m.snapshotLogs()
	dirtySnapshots := make([]moduleapi.DirtySnapshot, 0, len(logs))
	for vchannel, log := range logs {
		meta := log.log.ConsumeDirtyAndGetSnapshot()
		if meta == nil {
			continue
		}
		owner := log
		dirtySnapshots = append(dirtySnapshots, newModuleDirtySnapshot(
			moduleapi.SnapshotKey{PChannel: m.pchannel, VChannel: vchannel},
			moduleapi.SnapshotOpUpsert,
			meta,
			meta.GetCheckpointTimeTick(),
			func() {
				owner.log.MarkSnapshotPersisted(meta)
			},
		))
	}
	return dirtySnapshots
}

func (m *Module) Recover(ctx context.Context) error {
	for _, log := range m.snapshotLogs() {
		if _, err := log.log.Recover(ctx, nil); err != nil {
			return err
		}
	}
	return nil
}

func (m *Module) Read(ctx context.Context, opt transformlogapi.ReadOption) transformlogapi.Scanner {
	if opt.VChannel == "" {
		return transformlogapi.NewErrorScanner(opt.Name, errors.Wrap(transformlogapi.ErrInvalidReadOption, "vchannel is empty"))
	}
	log := m.getLog(opt.VChannel)
	if log == nil {
		return transformlogapi.NewErrorScanner(opt.Name, errors.Wrap(transformlogapi.ErrVChannelUnavailable, "transform log is not found"))
	}
	return log.log.Read(ctx, opt)
}

func (m *Module) DataFrontier(scope moduleapi.Scope) walcheckpoint.Barrier {
	logs := make(transformLogFrontierOwners, 0)
	switch scope.Type {
	case moduleapi.ScopeAll:
		for _, log := range m.snapshotLogs() {
			logs = append(logs, log)
		}
	case moduleapi.ScopeVChannel, moduleapi.ScopePartition:
		if log := m.getLog(scope.VChannel); log != nil {
			logs = append(logs, log)
		}
	default:
		return nil
	}
	return logs
}

func (m *Module) observeTransformLogMessage(msg message.ImmutableMessage) moduleapi.ObserveResult {
	if msg.VChannel() == "" || m.currentMode() != moduleModeMetaAndData {
		return moduleapi.ObserveResult{}
	}
	log := m.getOrCreateLog(msg.VChannel())
	if msg.TimeTick() <= log.log.DataCheckpointTimeTick() {
		return moduleapi.ObserveResult{}
	}
	appendResult := log.log.Append(msg, AppendOption{})
	if !appendResult.Appended {
		return moduleapi.ObserveResult{}
	}
	if appendResult.ShouldFlush {
		m.submitFlushTask(log, msg.VChannel(), appendResult.DataTimeTick)
	}
	return moduleapi.ObserveResult{Data: log.dataBarrier()}
}

func (m *Module) flushByMessageTimeTick(timetick uint64, vchannel string, msgType message.MessageType) moduleapi.ObserveResult {
	if m.currentMode() != moduleModeMetaAndData {
		return moduleapi.ObserveResult{}
	}
	if msgType == message.MessageTypeFlushAll || msgType == message.MessageTypeAlterWAL {
		result := moduleapi.ObserveResult{}
		for name, log := range m.snapshotLogs() {
			result.Data = composeBarrier(result.Data, m.flushLog(name, log, timetick))
		}
		return result
	}
	if vchannel == "" {
		return moduleapi.ObserveResult{}
	}
	return moduleapi.ObserveResult{Data: m.flushLog(vchannel, m.getLog(vchannel), timetick)}
}

func (m *Module) flushLog(vchannel string, log *moduleLog, timetick uint64) walcheckpoint.Barrier {
	if log == nil {
		return nil
	}
	if !log.log.HasPendingWork() && timetick <= log.log.DataCheckpointTimeTick() {
		return nil
	}
	m.submitFlushTask(log, vchannel, timetick)
	return log.dataBarrier()
}

func (m *Module) submitFlushTask(log *moduleLog, vchannel string, timetick uint64) {
	if m.runtime.Scheduler == nil {
		return
	}
	task := log.startFlushTask(m, vchannel, timetick)
	m.runtime.Scheduler.Submit(task)
}

func (m *Module) currentMode() moduleMode {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mode
}

func (m *Module) getLog(vchannel string) *moduleLog {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.logs[vchannel]
}

func (m *Module) getOrCreateLog(vchannel string) *moduleLog {
	m.mu.Lock()
	defer m.mu.Unlock()
	if log := m.logs[vchannel]; log != nil {
		return log
	}
	log := newModuleLog(m.newTransformLog(vchannel, nil))
	m.logs[vchannel] = log
	return log
}

func (m *Module) snapshotLogs() map[string]*moduleLog {
	m.mu.Lock()
	defer m.mu.Unlock()
	logs := make(map[string]*moduleLog, len(m.logs))
	for vchannel, log := range m.logs {
		logs[vchannel] = log
	}
	return logs
}

func (m *Module) newTransformLog(vchannel string, meta *streamingpb.VChannelTransformLogMeta) TransformLog {
	return New(Config{
		VChannel: vchannel,
		MaxRows:  m.maxRows,
		Meta:     meta,
		Store:    m.store,
	})
}

type moduleLog struct {
	mu    sync.Mutex
	log   TransformLog
	tasks []scheduler.TaskHandle
}

func newModuleLog(log TransformLog) *moduleLog {
	return &moduleLog{log: log}
}

func (l *moduleLog) hasDataCheckpoint() bool {
	return l.log.DataBarrierTimeTick() > 0 || l.log.HasPendingWork() || l.hasPendingTask()
}

func (l *moduleLog) dataBarrier() walcheckpoint.Barrier {
	return walcheckpoint.BarrierFunc(l.log.DataBarrierTimeTick)
}

func (l *moduleLog) frontierTimeTick() uint64 {
	if l.log.HasDirty() || l.log.HasPendingWork() || l.hasPendingTask() {
		return l.log.DataBarrierTimeTick()
	}
	return math.MaxUint64
}

func (l *moduleLog) hasPendingTask() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, task := range l.tasks {
		if task != nil && !task.Done() {
			return true
		}
	}
	return false
}

func (l *moduleLog) startFlushTask(module *Module, vchannel string, timetick uint64) scheduler.Task {
	l.mu.Lock()
	defer l.mu.Unlock()
	task := &flushTask{
		module:       module,
		vchannel:     vchannel,
		log:          l,
		timetick:     timetick,
		precondition: l.taskPreconditionLocked(),
	}
	l.tasks = append(l.tasks, task)
	return task
}

func (l *moduleLog) taskPreconditionLocked() scheduler.Precondition {
	pending := l.tasks[:0]
	preconditions := make([]scheduler.Precondition, 0, len(l.tasks))
	for _, task := range l.tasks {
		if task == nil || task.Done() {
			continue
		}
		pending = append(pending, task)
		preconditions = append(preconditions, scheduler.After(task))
	}
	l.tasks = pending
	return scheduler.All(preconditions...)
}

type flushTask struct {
	module       *Module
	vchannel     string
	log          *moduleLog
	timetick     uint64
	precondition scheduler.Precondition
	done         atomic.Bool
}

func (t *flushTask) Name() string {
	return "transformlog-flush"
}

func (t *flushTask) Precondition() scheduler.Precondition {
	return t.precondition
}

func (t *flushTask) Done() bool {
	return t.done.Load()
}

func (t *flushTask) Run(ctx context.Context) error {
	result, err := t.log.log.Flush(ctx, FlushOption{TargetTimeTick: t.timetick})
	if err != nil {
		return err
	}
	t.done.Store(true)
	if result.NextTargetTimeTick > 0 {
		t.module.submitFlushTask(t.log, t.vchannel, result.NextTargetTimeTick)
	}
	if t.module.runtime.Notifier != nil {
		t.module.runtime.Notifier.NotifyModuleUpdated(moduleapi.ModuleNameTransformLog)
	}
	return nil
}

type transformLogFrontierOwner interface {
	frontierTimeTick() uint64
}

type transformLogFrontierOwners []transformLogFrontierOwner

func (owners transformLogFrontierOwners) TimeTick() uint64 {
	if len(owners) == 0 {
		return math.MaxUint64
	}
	frontier := uint64(math.MaxUint64)
	for _, owner := range owners {
		if timetick := owner.frontierTimeTick(); timetick < frontier {
			frontier = timetick
		}
	}
	return frontier
}

func composeBarrier(left walcheckpoint.Barrier, right walcheckpoint.Barrier) walcheckpoint.Barrier {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return walcheckpoint.NewCompositeBarrier(left, right)
}

var (
	_ moduleapi.Module           = (*Module)(nil)
	_ moduleapi.DataFrontierView = (*Module)(nil)
	_ transformlogapi.Accesser   = (*Module)(nil)
	_ walcheckpoint.Barrier      = (transformLogFrontierOwners)(nil)
)
