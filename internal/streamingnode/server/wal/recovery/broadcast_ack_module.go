package recovery

import (
	"context"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

type dataFrontierProvider struct {
	views []moduleapi.DataFrontierView
}

func newDataFrontierProvider(views ...moduleapi.DataFrontierView) moduleapi.DataFrontierProvider {
	filtered := make([]moduleapi.DataFrontierView, 0, len(views))
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
	lastAckTask  scheduler.TaskHandle
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
	task := &broadcastAckTask{
		module:       m,
		msg:          msg,
		precondition: scheduler.All(scheduler.After(m.lastAckTask), m.buildPrecondition(msg)),
	}
	if m.runtime.Scheduler != nil {
		m.lastAckTask = m.runtime.Scheduler.Submit(task)
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

func (m *broadcastAckModule) buildPrecondition(msg message.ImmutableMessage) scheduler.Precondition {
	switch msg.MessageType() {
	case message.MessageTypeDropCollection:
		return m.vchannelPrecondition(msg.TimeTick(), msg.VChannel(), moduleapi.DataProgressMaterialized)
	case message.MessageTypeTruncateCollection:
		return m.vchannelPrecondition(msg.TimeTick(), msg.VChannel(), moduleapi.DataProgressDurable)
	case message.MessageTypeDropPartition:
		drop := message.MustAsImmutableDropPartitionMessageV1(msg)
		header := drop.Header()
		return m.partitionPrecondition(drop.TimeTick(), drop.VChannel(), header.GetCollectionId(), header.GetPartitionId(), moduleapi.DataProgressDurable)
	case message.MessageTypeManualFlush:
		return m.vchannelPrecondition(msg.TimeTick(), msg.VChannel(), moduleapi.DataProgressMaterialized)
	case message.MessageTypeFlushAll:
		return m.allPrecondition(msg.TimeTick(), moduleapi.DataProgressMaterialized)
	case message.MessageTypeAlterCollection:
		alter := message.MustAsImmutableAlterCollectionMessageV2(msg)
		if messageutil.IsSchemaChange(alter.Header()) {
			return m.vchannelPrecondition(alter.TimeTick(), alter.VChannel(), moduleapi.DataProgressDurable)
		}
	case message.MessageTypeAlterWAL:
		return m.allPrecondition(msg.TimeTick(), moduleapi.DataProgressDurable)
	default:
	}
	return scheduler.AlwaysReady{}
}

func (m *broadcastAckModule) partitionPrecondition(
	timetick uint64,
	vchannel string,
	collectionID int64,
	partitionID int64,
	kind moduleapi.DataProgressKind,
) scheduler.Precondition {
	return m.frontierPrecondition(timetick, moduleapi.Scope{
		Type:         moduleapi.ScopePartition,
		Kind:         kind,
		VChannel:     vchannel,
		CollectionID: collectionID,
		PartitionID:  partitionID,
	})
}

func (m *broadcastAckModule) allPrecondition(timetick uint64, kind moduleapi.DataProgressKind) scheduler.Precondition {
	return m.frontierPrecondition(timetick, moduleapi.Scope{
		Type: moduleapi.ScopeAll,
		Kind: kind,
	})
}

func (m *broadcastAckModule) vchannelPrecondition(timetick uint64, vchannel string, kind moduleapi.DataProgressKind) scheduler.Precondition {
	return m.frontierPrecondition(timetick, moduleapi.Scope{
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

func (m *broadcastAckModule) frontierPrecondition(
	timetick uint64,
	scope moduleapi.Scope,
) scheduler.Precondition {
	if m.frontierView == nil {
		return scheduler.AlwaysReady{}
	}
	viewFrontier := m.frontierView.DataFrontier(scope)
	return scheduler.PreconditionFunc(func() bool {
		return viewFrontier == nil || viewFrontier.TimeTick() >= timetick
	})
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
	module       *broadcastAckModule
	msg          message.ImmutableMessage
	precondition scheduler.Precondition
}

func (t *broadcastAckTask) Name() string {
	return "broadcast-ack"
}

func (t *broadcastAckTask) Precondition() scheduler.Precondition {
	return t.precondition
}

func (t *broadcastAckTask) Run(ctx context.Context) error {
	if err := streaming.WAL().Broadcast().Ack(ctx, t.msg); err != nil {
		return err
	}
	t.module.markAcked(t.msg.TimeTick())
	if t.module.runtime.Notifier != nil {
		t.module.runtime.Notifier.NotifyBarrierUpdated()
	}
	return nil
}

var _ moduleapi.Module = (*broadcastAckModule)(nil)
var _ walcheckpoint.Barrier = (*broadcastAckBarrier)(nil)
