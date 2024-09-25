package manager

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/inspector"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// RecoverPChannelSegmentAllocManager recovers the segment assignment manager at the specified pchannel.
func RecoverPChannelSegmentAllocManager(
	ctx context.Context,
	pchannel types.PChannelInfo,
	wal *syncutil.Future[wal.WAL],
) (*PChannelSegmentAllocManager, error) {
	// recover streaming node growing segment metas.
	rawMetas, err := resource.Resource().StreamingNodeCatalog().ListSegmentAssignment(ctx, pchannel.Name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list segment assignment from catalog")
	}
	// get collection and parition info from rootcoord.
	resp, err := resource.Resource().RootCoordClient().GetPChannelInfo(ctx, &rootcoordpb.GetPChannelInfoRequest{
		Pchannel: pchannel.Name,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return nil, errors.Wrap(err, "failed to get pchannel info from rootcoord")
	}
	managers, waitForSealed := buildNewPartitionManagers(pchannel, rawMetas, resp.GetCollections())

	// PChannelSegmentAllocManager is the segment assign manager of determined pchannel.
	logger := log.With(zap.Any("pchannel", pchannel))

	return &PChannelSegmentAllocManager{
		lifetime: lifetime.NewLifetime(lifetime.Working),
		logger:   logger,
		pchannel: pchannel,
		managers: managers,
		helper:   newSealQueue(logger, wal, waitForSealed),
	}, nil
}

// PChannelSegmentAllocManager is a segment assign manager of determined pchannel.
type PChannelSegmentAllocManager struct {
	lifetime lifetime.Lifetime[lifetime.State]

	logger   *log.MLogger
	pchannel types.PChannelInfo
	managers *partitionSegmentManagers
	// There should always
	helper *sealQueue
}

// Channel returns the pchannel info.
func (m *PChannelSegmentAllocManager) Channel() types.PChannelInfo {
	return m.pchannel
}

// NewPartitions creates a new partition with the specified partitionIDs.
func (m *PChannelSegmentAllocManager) NewCollection(collectionID int64, vchannel string, partitionIDs []int64) error {
	if err := m.checkLifetime(); err != nil {
		return err
	}
	defer m.lifetime.Done()

	m.managers.NewCollection(collectionID, vchannel, partitionIDs)
	return nil
}

// NewPartition creates a new partition with the specified partitionID.
func (m *PChannelSegmentAllocManager) NewPartition(collectionID int64, partitionID int64) error {
	if err := m.checkLifetime(); err != nil {
		return err
	}
	defer m.lifetime.Done()

	m.managers.NewPartition(collectionID, partitionID)
	return nil
}

// AssignSegment assigns a segment for a assign segment request.
func (m *PChannelSegmentAllocManager) AssignSegment(ctx context.Context, req *AssignSegmentRequest) (*AssignSegmentResult, error) {
	if err := m.checkLifetime(); err != nil {
		return nil, err
	}
	defer m.lifetime.Done()

	manager, err := m.managers.Get(req.CollectionID, req.PartitionID)
	if err != nil {
		return nil, err
	}
	return manager.AssignSegment(ctx, req)
}

// RemoveCollection removes the specified collection.
func (m *PChannelSegmentAllocManager) RemoveCollection(ctx context.Context, collectionID int64) error {
	if err := m.checkLifetime(); err != nil {
		return err
	}
	defer m.lifetime.Done()

	waitForSealed := m.managers.RemoveCollection(collectionID)
	m.helper.AsyncSeal(waitForSealed...)

	// trigger a seal operation in background rightnow.
	inspector.GetSegmentSealedInspector().TriggerSealWaited(ctx, m.pchannel.Name)

	// wait for all segment has been flushed.
	return m.helper.WaitUntilNoWaitSeal(ctx)
}

// RemovePartition removes the specified partitions.
func (m *PChannelSegmentAllocManager) RemovePartition(ctx context.Context, collectionID int64, partitionID int64) error {
	if err := m.checkLifetime(); err != nil {
		return err
	}
	defer m.lifetime.Done()

	// Remove the given partition from the partition managers.
	// And seal all segments that should be sealed.
	waitForSealed := m.managers.RemovePartition(collectionID, partitionID)
	m.helper.AsyncSeal(waitForSealed...)

	// trigger a seal operation in background rightnow.
	inspector.GetSegmentSealedInspector().TriggerSealWaited(ctx, m.pchannel.Name)

	// wait for all segment has been flushed.
	return m.helper.WaitUntilNoWaitSeal(ctx)
}

// SealAllSegmentsAndFenceUntil seals all segments and fence assign until timetick and return the segmentIDs.
func (m *PChannelSegmentAllocManager) SealAllSegmentsAndFenceUntil(ctx context.Context, collectionID int64, timetick uint64) ([]int64, error) {
	if err := m.checkLifetime(); err != nil {
		return nil, err
	}
	defer m.lifetime.Done()

	// All message's timetick less than incoming timetick is all belong to the output sealed segment.
	// So the output sealed segment transfer into flush == all message's timetick less than incoming timetick are flushed.
	sealedSegments, err := m.managers.SealAllSegmentsAndFenceUntil(collectionID, timetick)
	if err != nil {
		return nil, err
	}

	segmentIDs := make([]int64, 0, len(sealedSegments))
	for _, segment := range sealedSegments {
		segmentIDs = append(segmentIDs, segment.GetSegmentID())
	}

	// trigger a seal operation in background rightnow.
	m.helper.AsyncSeal(sealedSegments...)

	// wait for all segment has been flushed.
	if err := m.helper.WaitUntilNoWaitSeal(ctx); err != nil {
		return nil, err
	}

	return segmentIDs, nil
}

// TryToSealSegments tries to seal the specified segments.
func (m *PChannelSegmentAllocManager) TryToSealSegments(ctx context.Context, infos ...stats.SegmentBelongs) {
	if err := m.lifetime.Add(lifetime.IsWorking); err != nil {
		return
	}
	defer m.lifetime.Done()

	if len(infos) == 0 {
		// if no segment info specified, try to seal all segments.
		m.managers.Range(func(pm *partitionSegmentManager) {
			m.helper.AsyncSeal(pm.CollectShouldBeSealed()...)
		})
	} else {
		// if some segment info specified, try to seal the specified partition.
		for _, info := range infos {
			if pm, err := m.managers.Get(info.CollectionID, info.PartitionID); err == nil {
				m.helper.AsyncSeal(pm.CollectShouldBeSealed()...)
			}
		}
	}
	m.helper.SealAllWait(ctx)
}

func (m *PChannelSegmentAllocManager) MustSealSegments(ctx context.Context, infos ...stats.SegmentBelongs) {
	if err := m.lifetime.Add(lifetime.IsWorking); err != nil {
		return
	}
	defer m.lifetime.Done()

	for _, info := range infos {
		if pm, err := m.managers.Get(info.CollectionID, info.PartitionID); err == nil {
			m.helper.AsyncSeal(pm.CollectionMustSealed(info.SegmentID))
		}
	}
	m.helper.SealAllWait(ctx)
}

// TryToSealWaitedSegment tries to seal the wait for sealing segment.
func (m *PChannelSegmentAllocManager) TryToSealWaitedSegment(ctx context.Context) {
	if err := m.lifetime.Add(lifetime.IsWorking); err != nil {
		return
	}
	defer m.lifetime.Done()

	m.helper.SealAllWait(ctx)
}

// IsNoWaitSeal returns whether the segment manager is no segment wait for seal.
func (m *PChannelSegmentAllocManager) IsNoWaitSeal() bool {
	return m.helper.IsEmpty()
}

// WaitUntilNoWaitSeal waits until no segment wait for seal.
func (m *PChannelSegmentAllocManager) WaitUntilNoWaitSeal(ctx context.Context) error {
	if err := m.lifetime.Add(lifetime.IsWorking); err != nil {
		return err
	}
	defer m.lifetime.Done()

	return m.helper.WaitUntilNoWaitSeal(ctx)
}

// checkLifetime checks the lifetime of the segment manager.
func (m *PChannelSegmentAllocManager) checkLifetime() error {
	if err := m.lifetime.Add(lifetime.IsWorking); err != nil {
		m.logger.Warn("unreachable: segment assignment manager is not working, so the wal is on closing", zap.Error(err))
		return errors.New("segment assignment manager is not working")
	}
	return nil
}

// Close try to persist all stats and invalid the manager.
func (m *PChannelSegmentAllocManager) Close(ctx context.Context) {
	m.logger.Info("segment assignment manager start to close")
	m.lifetime.SetState(lifetime.Stopped)
	m.lifetime.Wait()

	// Try to seal all wait
	m.helper.SealAllWait(ctx)
	m.logger.Info("seal all waited segments done", zap.Int("waitCounter", m.helper.WaitCounter()))

	segments := make([]*segmentAllocManager, 0)
	m.managers.Range(func(pm *partitionSegmentManager) {
		segments = append(segments, pm.CollectDirtySegmentsAndClear()...)
	})

	// commitAllSegmentsOnSamePChannel commits all segments on the same pchannel.
	protoSegments := make([]*streamingpb.SegmentAssignmentMeta, 0, len(segments))
	for _, segment := range segments {
		protoSegments = append(protoSegments, segment.Snapshot())
	}

	m.logger.Info("segment assignment manager save all dirty segment assignments info", zap.Int("segmentCount", len(protoSegments)))
	if err := resource.Resource().StreamingNodeCatalog().SaveSegmentAssignments(ctx, m.pchannel.Name, protoSegments); err != nil {
		m.logger.Warn("commit segment assignment at pchannel failed", zap.Error(err))
	}

	// remove the stats from stats manager.
	m.logger.Info("segment assignment manager remove all segment stats from stats manager")
	for _, segment := range segments {
		if segment.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING {
			resource.Resource().SegmentAssignStatsManager().UnregisterSealedSegment(segment.GetSegmentID())
		}
	}
}
