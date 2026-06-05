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

type moduleMode int

const (
	moduleModeMetaOnly moduleMode = iota
	moduleModeMetaAndData
)

type broadcastAckModule struct {
	channelName  string
	frontierView moduleapi.DurableFrontierView
	runtime      moduleapi.Runtime
	acked        *atomic.Uint64
	mode         moduleMode
	lastAckTask  scheduler.TaskHandle
}

func newBroadcastAckModule(
	channelName string,
	frontierView moduleapi.DurableFrontierView,
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

func (m *broadcastAckModule) Name() string {
	return "broadcast-ack"
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

func (m *broadcastAckModule) SwitchIntoMetaAndData() moduleapi.Snapshot {
	m.mode = moduleModeMetaAndData
	return nil
}

func (m *broadcastAckModule) RequirePersist() {
}

func (m *broadcastAckModule) buildPrecondition(msg message.ImmutableMessage) scheduler.Precondition {
	switch msg.MessageType() {
	case message.MessageTypeDropCollection:
		return m.vchannelDurablePrecondition(msg.TimeTick(), msg.VChannel())
	case message.MessageTypeTruncateCollection:
		return m.vchannelDurablePrecondition(msg.TimeTick(), msg.VChannel())
	case message.MessageTypeDropPartition:
		drop := message.MustAsImmutableDropPartitionMessageV1(msg)
		header := drop.Header()
		return m.partitionDurablePrecondition(drop.TimeTick(), header.GetCollectionId(), header.GetPartitionId())
	case message.MessageTypeManualFlush:
		return m.vchannelDurablePrecondition(msg.TimeTick(), msg.VChannel())
	case message.MessageTypeFlushAll:
		return m.allDurablePrecondition(msg.TimeTick())
	case message.MessageTypeAlterCollection:
		alter := message.MustAsImmutableAlterCollectionMessageV2(msg)
		if messageutil.IsSchemaChange(alter.Header()) {
			return m.vchannelDurablePrecondition(alter.TimeTick(), alter.VChannel())
		}
	case message.MessageTypeAlterWAL:
		return m.allDurablePrecondition(msg.TimeTick())
	default:
	}
	return scheduler.AlwaysReady{}
}

func (m *broadcastAckModule) partitionDurablePrecondition(timetick uint64, collectionID int64, partitionID int64) scheduler.Precondition {
	return m.frontierPrecondition(timetick, func(view moduleapi.DurableFrontierView) walcheckpoint.Barrier {
		return view.PartitionDurableFrontier(collectionID, partitionID)
	})
}

func (m *broadcastAckModule) allDurablePrecondition(timetick uint64) scheduler.Precondition {
	return m.frontierPrecondition(timetick, func(view moduleapi.DurableFrontierView) walcheckpoint.Barrier {
		return view.AllDurableFrontier()
	})
}

func (m *broadcastAckModule) vchannelDurablePrecondition(timetick uint64, vchannel string) scheduler.Precondition {
	return m.frontierPrecondition(timetick, func(view moduleapi.DurableFrontierView) walcheckpoint.Barrier {
		return view.VChannelDurableFrontier(vchannel)
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
	frontier func(moduleapi.DurableFrontierView) walcheckpoint.Barrier,
) scheduler.Precondition {
	if m.frontierView == nil {
		return scheduler.AlwaysReady{}
	}
	viewFrontier := frontier(m.frontierView)
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
