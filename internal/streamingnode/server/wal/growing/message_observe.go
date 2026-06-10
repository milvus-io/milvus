package growing

import (
	"context"

	walcheckpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/checkpoint"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	waltransformlog "github.com/milvus-io/milvus/internal/streamingnode/server/wal/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	scheduler "github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

func (m *Manager) observeMessage(ctx context.Context, msg message.ImmutableMessage) moduleapi.ObserveResult {
	switch msg.MessageType() {
	case message.MessageTypeTimeTick:
		return emptyObserveResult()
	case message.MessageTypeCreateCollection:
		return m.observeCreateCollectionMessage(message.MustAsImmutableCreateCollectionMessageV1(msg))
	case message.MessageTypeCreatePartition:
		return m.observeCreatePartitionMessage(message.MustAsImmutableCreatePartitionMessageV1(msg))
	case message.MessageTypeSchemaChange:
		return m.observeSchemaChangeMessage(ctx, message.MustAsImmutableSchemaChangeMessageV2(msg))
	case message.MessageTypeAlterCollection:
		return m.observeAlterCollectionMessage(ctx, message.MustAsImmutableAlterCollectionMessageV2(msg))
	case message.MessageTypeCreateSegment:
		return m.observeCreateSegmentMessage(ctx, message.MustAsImmutableCreateSegmentMessageV2(msg))
	case message.MessageTypeInsert:
		return m.observeInsertMessage(ctx, message.MustAsImmutableInsertMessageV1(msg))
	case message.MessageTypeDelete:
		return m.observeDeleteMessage(ctx, message.MustAsImmutableDeleteMessageV1(msg))
	case message.MessageTypeFlush:
		return m.observeFlushMessage(ctx, message.MustAsImmutableFlushMessageV2(msg))
	case message.MessageTypeManualFlush:
		return m.observeManualFlushMessage(ctx, message.MustAsImmutableManualFlushMessageV2(msg))
	case message.MessageTypeFlushAll:
		return m.observeFlushAllMessage(ctx, message.MustAsImmutableFlushAllMessageV2(msg))
	case message.MessageTypeDropCollection:
		return m.observeDropCollectionMessage(ctx, message.MustAsImmutableDropCollectionMessageV1(msg))
	case message.MessageTypeDropPartition:
		return m.observeDropPartitionMessage(ctx, message.MustAsImmutableDropPartitionMessageV1(msg))
	case message.MessageTypeTruncateCollection:
		return m.observeTruncateCollectionMessage(ctx, message.MustAsImmutableTruncateCollectionMessageV2(msg))
	case message.MessageTypeAlterWAL:
		return m.observeAlterWALMessage(ctx, message.MustAsImmutableAlterWALMessageV2(msg))
	case message.MessageTypeTxn:
		return m.observeTxnMessage(ctx, message.AsImmutableTxnMessage(msg))
	default:
		return emptyObserveResult()
	}
}

func (m *Manager) observeCreateCollectionMessage(msg message.ImmutableCreateCollectionMessageV1) moduleapi.ObserveResult {
	if vchannel := m.vchannelViews[msg.VChannel()]; vchannel != nil {
		decision, result := vchannel.ObserveExistingCreateCollectionMessageV1(msg)
		switch decision {
		case existingCreateCollectionStartNew:
			vchannel := m.addVChannel(newVChannelMetaFromCreateCollectionMessage(msg))
			return moduleapi.ObserveResult{Meta: vchannel.MetaBarrier()}
		case existingCreateCollectionInconsistent:
			m.logInconsistency(msg, "create collection vchannel already exists", mlog.String("vchannel", msg.VChannel()))
		}
		return result
	}
	vchannel := m.addVChannel(newVChannelMetaFromCreateCollectionMessage(msg))
	return moduleapi.ObserveResult{Meta: vchannel.MetaBarrier()}
}

func (m *Manager) observeCreatePartitionMessage(msg message.ImmutableCreatePartitionMessageV1) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil {
		return emptyObserveResult()
	}
	return vchannel.ObserveCreatePartitionMessageV1(msg)
}

func (m *Manager) observeSchemaChangeMessage(ctx context.Context, msg message.ImmutableSchemaChangeMessageV2) moduleapi.ObserveResult {
	vchannel, result, canMutate := m.prepareActiveVChannelObserve(ctx, msg.TimeTick(), msg.VChannel(), true)
	if !canMutate {
		return result
	}
	result = composeObserveResults(result, m.flushVChannelTransformLogBuffer(msg.TimeTick(), msg.VChannel()))
	observeResult := vchannel.ObserveSchemaChangeMessageV2(msg)
	m.refreshRetainedSegmentSchemas(vchannel)
	return composeObserveResults(result, observeResult)
}

func (m *Manager) observeAlterCollectionMessage(ctx context.Context, msg message.ImmutableAlterCollectionMessageV2) moduleapi.ObserveResult {
	schemaChange := messageutil.IsSchemaChange(msg.Header())
	vchannel, result, canMutate := m.prepareActiveVChannelObserve(ctx, msg.TimeTick(), msg.VChannel(), schemaChange)
	if !canMutate {
		return result
	}
	observeResult := vchannel.ObserveAlterCollectionMessageV2(msg)
	if schemaChange {
		result = composeObserveResults(result, m.flushVChannelTransformLogBuffer(msg.TimeTick(), msg.VChannel()))
		m.refreshRetainedSegmentSchemas(vchannel)
	}
	return composeObserveResults(result, observeResult)
}

func (m *Manager) prepareActiveVChannelObserve(
	ctx context.Context,
	timetick uint64,
	vchannelName string,
	flushSegments bool,
) (*vChannelView, moduleapi.ObserveResult, bool) {
	vchannel := m.retainedVChannel(vchannelName)
	if vchannel == nil {
		return nil, emptyObserveResult(), false
	}
	result := emptyObserveResult()
	if flushSegments {
		result = m.flushRetainedVChannelSegmentsCreatedBefore(ctx, timetick, vchannelName)
	}
	if !vchannel.CanObserveActiveAt(timetick) {
		return vchannel, result, false
	}
	return vchannel, result, true
}

func (m *Manager) observeCreateSegmentMessage(ctx context.Context, msg message.ImmutableCreateSegmentMessageV2) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil {
		m.logInconsistency(msg, "create segment vchannel not found", mlog.String("vchannel", msg.VChannel()), mlog.Int64("segmentID", msg.Header().GetSegmentId()))
		return emptyObserveResult()
	}
	segment := m.segmentViews[msg.Header().GetSegmentId()]
	result := emptyObserveResult()
	if segment == nil {
		schema := vchannel.CreateSegmentSchema(msg.Header().GetPartitionId(), msg.TimeTick())
		if schema == nil {
			m.logInconsistency(msg, "create segment partition not found", mlog.String("vchannel", msg.VChannel()), mlog.Int64("partitionID", msg.Header().GetPartitionId()), mlog.Int64("segmentID", msg.Header().GetSegmentId()))
			return emptyObserveResult()
		}
		segment = newSegmentViewFromCreateSegmentMessage(msg, schema, m.runtimeConfig())
		m.addSegmentView(segment)
		result.Meta = segment.metaBarrier()
	}
	return composeObserveResults(result, segment.ObserveCreateSegmentMessageV2(ctx, msg))
}

func (m *Manager) observeFlushMessage(ctx context.Context, msg message.ImmutableFlushMessageV2) moduleapi.ObserveResult {
	segment := m.segmentViews[msg.Header().GetSegmentId()]
	if segment == nil {
		m.logInconsistency(msg, "flush segment not found", mlog.Int64("segmentID", msg.Header().GetSegmentId()))
		return emptyObserveResult()
	}
	return segment.Flush(ctx, msg.TimeTick())
}

func (m *Manager) observeManualFlushMessage(ctx context.Context, msg message.ImmutableManualFlushMessageV2) moduleapi.ObserveResult {
	return m.flushVChannelSegmentsCreatedBefore(ctx, msg.TimeTick(), msg.VChannel())
}

func (m *Manager) observeFlushAllMessage(ctx context.Context, msg message.ImmutableFlushAllMessageV2) moduleapi.ObserveResult {
	result := m.observeSegments(func(segment *segmentView) bool {
		return segment.CreateTimeTick() < msg.TimeTick()
	}, func(segment *segmentView) moduleapi.ObserveResult {
		return segment.Flush(ctx, msg.TimeTick())
	})
	return composeObserveResults(result, m.flushAllTransformLogBuffers(msg.TimeTick()))
}

func (m *Manager) observeDropCollectionMessage(ctx context.Context, msg message.ImmutableDropCollectionMessageV1) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil {
		return emptyObserveResult()
	}
	result := m.flushVChannelSegmentsCreatedBefore(ctx, msg.TimeTick(), msg.VChannel())
	return composeObserveResults(result, vchannel.ObserveDropCollectionMessageV1(msg))
}

func (m *Manager) observeDropPartitionMessage(ctx context.Context, msg message.ImmutableDropPartitionMessageV1) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil {
		return emptyObserveResult()
	}
	result := m.flushPartitionSegmentsCreatedBefore(ctx, msg.TimeTick(), msg.VChannel(), msg.Header().GetPartitionId())
	return composeObserveResults(result, vchannel.ObserveDropPartitionMessageV1(msg))
}

func (m *Manager) observeTruncateCollectionMessage(ctx context.Context, msg message.ImmutableTruncateCollectionMessageV2) moduleapi.ObserveResult {
	vchannel := m.retainedVChannel(msg.VChannel())
	if vchannel == nil {
		return emptyObserveResult()
	}
	result := m.flushVChannelSegmentsCreatedBefore(ctx, msg.TimeTick(), msg.VChannel())
	return composeObserveResults(result, vchannel.ObserveTruncateCollectionMessageV2(msg))
}

func (m *Manager) observeAlterWALMessage(ctx context.Context, msg message.ImmutableAlterWALMessageV2) moduleapi.ObserveResult {
	result := m.observeSegments(func(segment *segmentView) bool {
		return segment.CreateTimeTick() < msg.TimeTick()
	}, func(segment *segmentView) moduleapi.ObserveResult {
		return segment.Flush(ctx, msg.TimeTick())
	})
	return composeObserveResults(result, m.flushAllTransformLogBuffers(msg.TimeTick()))
}

func (m *Manager) flushVChannelSegmentsCreatedBefore(
	ctx context.Context,
	timetick uint64,
	vchannel string,
) moduleapi.ObserveResult {
	info := m.retainedVChannel(vchannel)
	if info == nil {
		return emptyObserveResult()
	}
	result := m.flushRetainedVChannelSegmentsCreatedBefore(ctx, timetick, vchannel)
	return composeObserveResults(result, m.flushVChannelTransformLogBuffer(timetick, vchannel))
}

func (m *Manager) flushRetainedVChannelSegmentsCreatedBefore(
	ctx context.Context,
	timetick uint64,
	vchannel string,
) moduleapi.ObserveResult {
	info := m.retainedVChannel(vchannel)
	if info == nil {
		return emptyObserveResult()
	}
	return m.observeSegmentViews(info.SegmentsForFlush(common.AllPartitionsID, timetick), func(segment *segmentView) moduleapi.ObserveResult {
		return segment.Flush(ctx, timetick)
	})
}

func (m *Manager) flushPartitionSegmentsCreatedBefore(
	ctx context.Context,
	timetick uint64,
	vchannel string,
	partitionID int64,
) moduleapi.ObserveResult {
	info := m.retainedVChannel(vchannel)
	if info == nil {
		return emptyObserveResult()
	}
	result := m.flushRetainedPartitionSegmentsCreatedBefore(ctx, timetick, vchannel, partitionID)
	return composeObserveResults(result, m.flushVChannelTransformLogBuffer(timetick, vchannel))
}

func (m *Manager) flushRetainedPartitionSegmentsCreatedBefore(
	ctx context.Context,
	timetick uint64,
	vchannel string,
	partitionID int64,
) moduleapi.ObserveResult {
	info := m.retainedVChannel(vchannel)
	if info == nil {
		return emptyObserveResult()
	}
	return m.observeSegmentViews(info.SegmentsForFlush(partitionID, timetick), func(segment *segmentView) moduleapi.ObserveResult {
		return segment.Flush(ctx, timetick)
	})
}

func (m *Manager) flushVChannelTransformLogBuffer(timetick uint64, vchannel string) moduleapi.ObserveResult {
	info := m.retainedVChannel(vchannel)
	transformLog := m.transformLog(vchannel)
	if info == nil || transformLog == nil {
		return emptyObserveResult()
	}
	info.mu.Lock()
	if !info.canReplayAtLocked(timetick) ||
		!info.metaAndData ||
		info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED ||
		(!transformLog.log.HasPendingWork() && timetick <= transformLog.log.DataCheckpointTimeTick()) {
		info.mu.Unlock()
		return emptyObserveResult()
	}
	runtime := info.runtime
	info.mu.Unlock()
	if task := m.startFlushTransformLogBufferTask(vchannel, timetick); task != nil {
		runtime.Scheduler.Submit(task)
	}
	return dataBarrierResult(transformLog.dataBarrier())
}

func (m *Manager) flushAllTransformLogBuffers(timetick uint64) moduleapi.ObserveResult {
	result := emptyObserveResult()
	for vchannel := range m.vchannelViews {
		result = composeObserveResults(result, m.flushVChannelTransformLogBuffer(timetick, vchannel))
	}
	return result
}

func (m *Manager) startFlushTransformLogBufferTask(vchannel string, timetick uint64) scheduler.Task {
	transformLog := m.transformLog(vchannel)
	if transformLog == nil {
		return nil
	}
	return transformLog.startFlushTask(m, vchannel, timetick)
}

func (m *Manager) observeSegments(matches func(*segmentView) bool, observe func(*segmentView) moduleapi.ObserveResult) moduleapi.ObserveResult {
	segments := make([]*segmentView, 0, len(m.segmentViews))
	for _, segment := range m.segmentViews {
		if matches(segment) {
			segments = append(segments, segment)
		}
	}
	return m.observeSegmentViews(segments, observe)
}

func (m *Manager) observeSegmentViews(segments []*segmentView, observe func(*segmentView) moduleapi.ObserveResult) moduleapi.ObserveResult {
	result := emptyObserveResult()
	for _, segment := range segments {
		segmentResult := observe(segment)
		result = composeObserveResults(result, segmentResult)
	}
	return result
}

func (m *Manager) observeInsertMessage(ctx context.Context, msg message.ImmutableInsertMessageV1) moduleapi.ObserveResult {
	vchannelManager := m.retainedVChannel(msg.VChannel())
	if vchannelManager == nil {
		m.logInconsistency(msg, "insert vchannel not found", mlog.String("vchannel", msg.VChannel()))
		return emptyObserveResult()
	}
	result := emptyObserveResult()
	for _, partition := range msg.Header().GetPartitions() {
		segmentID := partition.GetSegmentAssignment().GetSegmentId()
		segment := vchannelManager.SegmentForInsert(partition.GetPartitionId(), segmentID, msg.TimeTick())
		if segment == nil {
			continue
		}
		segmentResult := segment.ObserveInsertMessageV1(ctx, msg, partition)
		result = composeObserveResults(result, segmentResult)
	}
	return result
}

func (m *Manager) observeDeleteMessage(ctx context.Context, msg message.ImmutableDeleteMessageV1) moduleapi.ObserveResult {
	if !m.metaAndData {
		return emptyObserveResult()
	}
	vchannelManager := m.retainedVChannel(msg.VChannel())
	if vchannelManager == nil {
		m.logInconsistency(msg, "delete vchannel not found", mlog.String("vchannel", msg.VChannel()))
		return emptyObserveResult()
	}
	return m.observeTransformLogMessage(vchannelManager, msg)
}

func (m *Manager) observeTxnMessage(ctx context.Context, msg message.ImmutableTxnMessage) moduleapi.ObserveResult {
	result := emptyObserveResult()
	observedSegments := make(map[int64]struct{})
	hasDelete := false
	timetick := msg.TimeTick()
	msg.RangeOver(func(im message.ImmutableMessage) error {
		var subResult moduleapi.ObserveResult
		switch im.MessageType() {
		case message.MessageTypeInsert:
			insert := message.MustAsImmutableInsertMessageV1(im)
			vchannelView := m.retainedVChannel(insert.VChannel())
			if vchannelView == nil {
				m.logInconsistency(insert, "txn insert vchannel not found", mlog.String("vchannel", insert.VChannel()))
				return nil
			}
			for _, partition := range insert.Header().GetPartitions() {
				segmentID := partition.GetSegmentAssignment().GetSegmentId()
				if _, observed := observedSegments[segmentID]; observed {
					continue
				}
				segment := vchannelView.SegmentForInsert(partition.GetPartitionId(), segmentID, timetick)
				if segment == nil {
					continue
				}
				observedSegments[segmentID] = struct{}{}
				subResult = composeObserveResults(subResult, segment.ObserveTxnMessage(ctx, msg))
			}
		case message.MessageTypeDelete:
			hasDelete = true
		default:
			m.logInconsistency(im, "unexpected message type in txn message", mlog.String("messageType", im.MessageType().String()))
			return nil
		}
		result = composeObserveResults(result, subResult)
		return nil
	})
	if hasDelete {
		vchannelManager := m.retainedVChannel(msg.VChannel())
		if vchannelManager == nil {
			m.logInconsistency(msg, "txn delete vchannel not found", mlog.String("vchannel", msg.VChannel()))
			return result
		}
		result = composeObserveResults(result, m.observeTransformLogMessage(vchannelManager, msg))
	}
	return result
}

func (m *Manager) observeTransformLogMessage(vchannel *vChannelView, msg message.ImmutableMessage) moduleapi.ObserveResult {
	transformLog := m.transformLog(vchannel.Name())
	if transformLog == nil {
		return emptyObserveResult()
	}
	var task scheduler.Task
	vchannel.mu.Lock()
	if !vchannel.metaAndData {
		vchannel.mu.Unlock()
		return emptyObserveResult()
	}
	if !vchannel.canReplayAtLocked(msg.TimeTick()) ||
		msg.TimeTick() <= transformLog.log.DataCheckpointTimeTick() {
		vchannel.mu.Unlock()
		return emptyObserveResult()
	}
	appendOpt := waltransformlog.AppendOption{
		DeleteFilter: func(partitionID int64, timetick uint64) bool {
			return vchannel.canReplayExistingPartitionAtLocked(partitionID, timetick)
		},
	}
	appendResult := transformLog.log.Append(msg, appendOpt)
	runtime := vchannel.runtime
	vchannelName := vchannel.meta.GetVchannel()
	vchannel.mu.Unlock()
	if !appendResult.Appended {
		return emptyObserveResult()
	}
	if appendResult.ShouldFlush {
		task = m.startFlushTransformLogBufferTask(vchannelName, appendResult.DataTimeTick)
	}
	if task != nil {
		runtime.Scheduler.Submit(task)
	}
	return dataBarrierResult(transformLog.dataBarrier())
}

func emptyObserveResult() moduleapi.ObserveResult {
	return moduleapi.ObserveResult{}
}

func dataBarrierResult(barrier walcheckpoint.Barrier) moduleapi.ObserveResult {
	return moduleapi.ObserveResult{Data: barrier}
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

func composeObserveResults(results ...moduleapi.ObserveResult) moduleapi.ObserveResult {
	composed := moduleapi.ObserveResult{}
	for _, result := range results {
		composed.Meta = composeBarrier(composed.Meta, result.Meta)
		composed.Data = composeBarrier(composed.Data, result.Data)
	}
	return composed
}

func (m *Manager) logInconsistency(msg message.ImmutableMessage, reason string, fields ...mlog.Field) {
	if m.logger == nil {
		return
	}
	fields = append(fields, mlog.FieldMessage(msg))
	m.logger.Warn(context.TODO(), "inconsistent growing observe state", append([]mlog.Field{mlog.String("reason", reason)}, fields...)...)
}
