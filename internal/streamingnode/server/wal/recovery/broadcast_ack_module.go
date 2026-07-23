package recovery

import (
	"context"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type dataFrontierProvider struct {
	views []moduleapi.DataFrontierProvider
}

func newDataFrontierProvider(views ...moduleapi.DataFrontierProvider) moduleapi.DataFrontierProvider {
	filtered := make([]moduleapi.DataFrontierProvider, 0, len(views))
	for _, view := range views {
		if view != nil {
			filtered = append(filtered, view)
		}
	}
	return dataFrontierProvider{views: filtered}
}

func (p dataFrontierProvider) DataFrontier(scope moduleapi.Scope) walcheckpoint.Barrier {
	barriers := make([]walcheckpoint.Barrier, 0, len(p.views))
	for _, view := range p.views {
		if barrier := view.DataFrontier(scope); barrier != nil {
			barriers = append(barriers, barrier)
		}
	}
	return walcheckpoint.NewCompositeBarrier(barriers...)
}

type moduleMode int

const (
	moduleModeMetaOnly moduleMode = iota
	moduleModeMetaAndData
)

type broadcastAckModule struct {
	channelName  string
	frontierView moduleapi.DataFrontierProvider
	runtime      moduleapi.Runtime
	acked        *atomic.Uint64
	mode         moduleMode
	lastAckTask  *broadcastAckTask
	ack          func(context.Context, message.ImmutableMessage) error
}

func newBroadcastAckModule(
	channelName string,
	frontierView moduleapi.DataFrontierProvider,
	runtime moduleapi.Runtime,
) *broadcastAckModule {
	return &broadcastAckModule{
		channelName:  channelName,
		frontierView: frontierView,
		runtime:      runtime,
		acked:        atomic.NewUint64(0),
		mode:         moduleModeMetaOnly,
		ack: func(ctx context.Context, msg message.ImmutableMessage) error {
			return streaming.WAL().Broadcast().Ack(ctx, msg)
		},
	}
}

func (m *broadcastAckModule) Name() moduleapi.ModuleName {
	return moduleapi.ModuleNameAck
}

func (m *broadcastAckModule) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	header := msg.BroadcastHeader()
	if header == nil || m.mode != moduleModeMetaAndData {
		return moduleapi.ObserveResult{}
	}

	barrier := &broadcastAckBarrier{
		timetick: msg.TimeTick(),
		acked:    m.acked,
	}
	task := m.newTask(msg)
	if m.runtime.Scheduler != nil {
		m.lastAckTask = task
		m.runtime.Scheduler.Submit(task)
	}
	return moduleapi.ObserveResult{Data: barrier}
}

func (m *broadcastAckModule) SwitchIntoMetaAndData() moduleapi.ModuleSnapshot {
	m.mode = moduleModeMetaAndData
	return nil
}

func (m *broadcastAckModule) ConsumeDirtySnapshots() []moduleapi.DirtySnapshot {
	return nil
}

func (m *broadcastAckModule) newTask(msg message.ImmutableMessage) *broadcastAckTask {
	return &broadcastAckTask{
		module:   m,
		msg:      msg,
		previous: m.lastAckTask,
		frontier: m.buildFrontier(msg),
	}
}

func (m *broadcastAckModule) buildFrontier(msg message.ImmutableMessage) walcheckpoint.Barrier {
	switch msg.MessageType() {
	case message.MessageTypeDropCollection:
		return m.vchannelFrontier(msg.VChannel(), moduleapi.DataProgressMaterialized)
	case message.MessageTypeTruncateCollection:
		return m.vchannelFrontier(msg.VChannel(), moduleapi.DataProgressDurable)
	case message.MessageTypeDropPartition:
		drop := message.MustAsImmutableDropPartitionMessageV1(msg)
		header := drop.Header()
		return m.partitionFrontier(drop.VChannel(), header.GetCollectionId(), header.GetPartitionId(), moduleapi.DataProgressDurable)
	case message.MessageTypeManualFlush:
		return m.vchannelFrontier(msg.VChannel(), moduleapi.DataProgressMaterialized)
	case message.MessageTypeFlushAll:
		return m.allFrontier(moduleapi.DataProgressMaterialized)
	case message.MessageTypeAlterCollection:
		alter := message.MustAsImmutableAlterCollectionMessageV2(msg)
		if messageutil.IsSchemaChange(alter.Header()) {
			return m.vchannelFrontier(alter.VChannel(), moduleapi.DataProgressDurable)
		}
	case message.MessageTypeAlterWAL:
		return m.allFrontier(moduleapi.DataProgressDurable)
	default:
	}
	return nil
}

func (m *broadcastAckModule) partitionFrontier(
	vchannel string,
	collectionID int64,
	partitionID int64,
	kind moduleapi.DataProgressKind,
) walcheckpoint.Barrier {
	return m.frontier(moduleapi.Scope{
		Type:         moduleapi.ScopePartition,
		Kind:         kind,
		VChannel:     vchannel,
		CollectionID: collectionID,
		PartitionID:  partitionID,
	})
}

func (m *broadcastAckModule) allFrontier(kind moduleapi.DataProgressKind) walcheckpoint.Barrier {
	return m.frontier(moduleapi.Scope{
		Type: moduleapi.ScopeAll,
		Kind: kind,
	})
}

func (m *broadcastAckModule) vchannelFrontier(vchannel string, kind moduleapi.DataProgressKind) walcheckpoint.Barrier {
	return m.frontier(moduleapi.Scope{
		Type:     moduleapi.ScopeVChannel,
		Kind:     kind,
		VChannel: vchannel,
	})
}

func (m *broadcastAckModule) markAcked(timetick uint64) {
	for {
		current := m.acked.Load()
		if current >= timetick {
			return
		}
		if m.acked.CompareAndSwap(current, timetick) {
			return
		}
	}
}

func (m *broadcastAckModule) frontier(scope moduleapi.Scope) walcheckpoint.Barrier {
	if m.frontierView == nil {
		return nil
	}
	return m.frontierView.DataFrontier(scope)
}

type broadcastAckBarrier struct {
	timetick uint64
	acked    *atomic.Uint64
}

func (b *broadcastAckBarrier) TimeTick() uint64 {
	if b.acked.Load() < b.timetick {
		return 0
	}
	return b.timetick
}

type broadcastAckTask struct {
	module   *broadcastAckModule
	msg      message.ImmutableMessage
	previous *broadcastAckTask
	frontier walcheckpoint.Barrier
	done     atomic.Bool
}

func (t *broadcastAckTask) Done() bool {
	return t.done.Load()
}

func (t *broadcastAckTask) Execute(ctx context.Context) error {
	if t.previous != nil && !t.previous.Done() {
		return nodescheduler.ErrDelay
	}
	if t.frontier != nil && t.frontier.TimeTick() < t.msg.TimeTick() {
		return nodescheduler.ErrDelay
	}
	defer t.done.Store(true)
	if err := t.module.ack(ctx, t.msg); err != nil {
		return err
	}
	t.module.markAcked(t.msg.TimeTick())
	if t.module.runtime.Notifier != nil {
		t.module.runtime.Notifier.NotifyBarrierUpdated()
	}
	return nil
}

var (
	_ moduleapi.Module      = (*broadcastAckModule)(nil)
	_ walcheckpoint.Barrier = (*broadcastAckBarrier)(nil)
	_ nodescheduler.Task    = (*broadcastAckTask)(nil)
)
