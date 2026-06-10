package growing

import (
	"context"
	"sort"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/syncutil/preconditioned"
)

type cleanupSnapshot struct {
	vchannelsToDrop    map[string]*streamingpb.VChannelMeta
	vchannelOwners     map[string]*vChannelView
	vchannelPartitions map[string]map[int64]uint64
	segmentIDsToDrop   []int64
	segmentOwners      map[int64]*segmentView
}

func (s *cleanupSnapshot) empty() bool {
	if s == nil {
		return true
	}
	return len(s.vchannelsToDrop) == 0 &&
		len(s.vchannelPartitions) == 0 &&
		len(s.segmentIDsToDrop) == 0
}

func (m *Manager) newCleanupTask(
	metaPhysicalTimeTick uint64,
	dataPhysicalTimeTick uint64,
	precondition preconditioned.Precondition,
) preconditioned.Task {
	snapshot := m.collectCleanupSnapshot(metaPhysicalTimeTick, dataPhysicalTimeTick)
	if snapshot.empty() {
		return nil
	}
	return &cleanupTask{
		channelName:  m.channelName,
		catalog:      m.catalog,
		logger:       m.logger,
		manager:      m,
		snapshot:     snapshot,
		precondition: precondition,
	}
}

func (m *Manager) collectCleanupSnapshot(metaPhysicalTimeTick uint64, dataPhysicalTimeTick uint64) *cleanupSnapshot {
	snapshot := &cleanupSnapshot{
		vchannelsToDrop:    make(map[string]*streamingpb.VChannelMeta),
		vchannelOwners:     make(map[string]*vChannelView),
		vchannelPartitions: make(map[string]map[int64]uint64),
		segmentOwners:      make(map[int64]*segmentView),
	}
	for segmentID, segment := range m.segmentViews {
		if !segment.TombstonedCleanupReady(metaPhysicalTimeTick, dataPhysicalTimeTick) {
			continue
		}
		snapshot.segmentIDsToDrop = append(snapshot.segmentIDsToDrop, segmentID)
		snapshot.segmentOwners[segmentID] = segment
	}
	sort.Slice(snapshot.segmentIDsToDrop, func(i, j int) bool {
		return snapshot.segmentIDsToDrop[i] < snapshot.segmentIDsToDrop[j]
	})
	for vchannelName, vchannel := range m.vchannelViews {
		transformLog := m.transformLog(vchannelName)
		if transformLog == nil {
			continue
		}
		persistedDataTimeTick := transformLog.log.DataBarrierTimeTick()
		dropSnapshot, cleanupPartitions := vchannel.TombstonedCleanupPlan(metaPhysicalTimeTick, dataPhysicalTimeTick, persistedDataTimeTick)
		if dropSnapshot != nil {
			snapshot.vchannelsToDrop[vchannelName] = dropSnapshot
			snapshot.vchannelOwners[vchannelName] = vchannel
			continue
		}
		if len(cleanupPartitions) > 0 {
			snapshot.vchannelPartitions[vchannelName] = cleanupPartitions
			snapshot.vchannelOwners[vchannelName] = vchannel
		}
	}
	return snapshot
}

type cleanupTask struct {
	channelName  string
	catalog      recoveryCatalog
	logger       *mlog.Logger
	manager      *Manager
	snapshot     *cleanupSnapshot
	precondition preconditioned.Precondition
}

func (t *cleanupTask) Name() string {
	return "growing-cleanup-tombstoned-meta"
}

func (t *cleanupTask) Precondition() preconditioned.Precondition {
	return t.precondition
}

func (t *cleanupTask) Run(ctx context.Context) error {
	logger := t.logger
	if logger == nil {
		logger = mlog.With()
	}
	logger = logger.With(
		mlog.String("op", "cleanupGrowingTombstonedMeta"),
		mlog.String("channel", t.channelName),
		mlog.Strings("dropVChannels", lo.Keys(t.snapshot.vchannelsToDrop)),
		mlog.Int64s("dropSegmentIDs", t.snapshot.segmentIDsToDrop),
		mlog.Strings("cleanupPartitionVChannels", lo.Keys(t.snapshot.vchannelPartitions)),
	)

	t.filterPartitionCleanupPlan()
	vchannelsToDrop := t.vchannelDropSnapshots()
	if len(t.snapshot.segmentIDsToDrop) > 0 {
		if err := retryOperationWithBackoff(ctx, logger.With(mlog.String("op", "dropSegmentAssignments")), func(ctx context.Context) error {
			return t.catalog.DropSegmentAssignments(ctx, t.channelName, t.snapshot.segmentIDsToDrop)
		}); err != nil {
			return err
		}
	}
	if len(vchannelsToDrop) > 0 {
		if err := retryOperationWithBackoff(ctx, logger.With(mlog.String("op", "dropTransformLogs")), func(ctx context.Context) error {
			return t.catalog.DropTransformLogMeta(ctx, t.channelName, lo.Keys(vchannelsToDrop))
		}); err != nil {
			return err
		}
		if err := retryOperationWithBackoff(ctx, logger.With(mlog.String("op", "dropVChannels")), func(ctx context.Context) error {
			return t.catalog.DropVChannels(ctx, t.channelName, vchannelsToDrop)
		}); err != nil {
			return err
		}
	}
	if t.apply() {
		t.manager.RequirePersist()
	}
	return nil
}

func (t *cleanupTask) vchannelDropSnapshots() map[string]*streamingpb.VChannelMeta {
	snapshots := make(map[string]*streamingpb.VChannelMeta)
	for vchannel, snapshot := range t.snapshot.vchannelsToDrop {
		owner := t.manager.retainedVChannel(vchannel)
		if owner == nil || owner != t.snapshot.vchannelOwners[vchannel] {
			continue
		}
		transformLog := t.manager.transformLog(vchannel)
		if transformLog == nil {
			continue
		}
		dropSnapshot := owner.VChannelDropCleanupSnapshot(snapshot.GetTombstoneTimeTick(), transformLog.log.DataBarrierTimeTick())
		if dropSnapshot == nil {
			continue
		}
		snapshots[vchannel] = dropSnapshot
	}
	t.snapshot.vchannelsToDrop = snapshots
	return snapshots
}

func (t *cleanupTask) filterPartitionCleanupPlan() {
	actualCleanup := make(map[string]map[int64]uint64)
	for vchannel, partitions := range t.snapshot.vchannelPartitions {
		owner := t.snapshot.vchannelOwners[vchannel]
		if owner == nil || owner != t.manager.retainedVChannel(vchannel) {
			continue
		}
		cleanupPartitions := owner.PartitionCleanupPlan(partitions)
		if len(cleanupPartitions) == 0 {
			continue
		}
		actualCleanup[vchannel] = cleanupPartitions
	}
	t.snapshot.vchannelPartitions = actualCleanup
}

func (t *cleanupTask) apply() bool {
	partitionMetaChanged := false
	for vchannel, partitions := range t.snapshot.vchannelPartitions {
		if t.snapshot.vchannelOwners[vchannel].ApplyPartitionCleanup(partitions) {
			partitionMetaChanged = true
		}
	}
	for _, segmentID := range t.snapshot.segmentIDsToDrop {
		segment := t.snapshot.segmentOwners[segmentID]
		if vchannel := t.manager.retainedVChannel(segment.AssignmentMeta().GetVchannel()); vchannel != nil {
			vchannel.RemoveSegment(segmentID)
		}
		delete(t.manager.segmentViews, segmentID)
	}
	for vchannel := range t.snapshot.vchannelsToDrop {
		delete(t.manager.vchannelViews, vchannel)
		delete(t.manager.transformLogs, vchannel)
	}
	return partitionMetaChanged
}
