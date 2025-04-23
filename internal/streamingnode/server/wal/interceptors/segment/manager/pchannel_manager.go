package manager

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/utils"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const gracefulCloseTimeout = 3 * time.Second

// RecoverPChannelSegmentAllocManager recovers the segment assignment manager at the specified pchannel.
// TODO: current recovery process rely on the rootcoord and catalog.
// and the alloc operation rely on datacoord.
// we should refactor the code to remove the dependency on datacoord and rootcoord.
// only use the wal and meta of streamingnode to recover the segment assignment manager.
func RecoverPChannelSegmentAllocManager(
	ctx context.Context,
	param *interceptors.InterceptorBuildParam,
) (*PChannelSegmentAllocManager, error) {
	metrics := metricsutil.NewSegmentAssignMetrics(param.ChannelInfo.Name)
	managers, growingBelongs, growingStats, waitForSealed := buildNewPartitionManagers(param.WAL, param.ChannelInfo, param.InitialRecoverSnapshot, metrics)

	// PChannelSegmentAllocManager is the segment assign manager of determined pchannel.
	logger := log.With(zap.Stringer("pchannel", param.ChannelInfo))
	m := &PChannelSegmentAllocManager{
		lifetime:   typeutil.NewLifetime(),
		logger:     logger,
		pchannel:   param.ChannelInfo,
		managers:   managers,
		sealWorker: newSealQueue(logger, param.WAL, param.TxnManager, metrics),
		metrics:    metrics,
	}
	// seal the sealed segments from recovery asynchronously.
	_ = m.sealWorker.AsyncSeal(ctx, waitForSealed)
	// register the segment assignment manager to the global manager.
	resource.Resource().SegmentAssignStatsManager().RegisterSealOperator(m, growingBelongs, growingStats)
	return m, nil
}

// PChannelSegmentAllocManager is a segment assign manager of determined pchannel.
type PChannelSegmentAllocManager struct {
	lifetime *typeutil.Lifetime

	logger   *log.MLogger
	pchannel types.PChannelInfo
	managers *partitionSegmentManagers
	// There should always
	sealWorker *sealQueue
	metrics    *metricsutil.SegmentAssignMetrics
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

	_ = m.managers.RemoveCollection(collectionID)
	return nil
}

// RemovePartition removes the specified partitions.
func (m *PChannelSegmentAllocManager) RemovePartition(ctx context.Context, collectionID int64, partitionID int64) error {
	if err := m.checkLifetime(); err != nil {
		return err
	}
	defer m.lifetime.Done()

	// Remove the given partition from the partition managers.
	// And seal all segments that should be sealed.
	_ = m.managers.RemovePartition(collectionID, partitionID)
	return nil
}

// SealAndFenceSegmentUntil seal all segment that contains the message less than the incoming timetick.
func (m *PChannelSegmentAllocManager) SealAndFenceSegmentUntil(ctx context.Context, collectionID int64, timetick uint64) ([]int64, error) {
	if err := m.checkLifetime(); err != nil {
		return nil, err
	}
	defer m.lifetime.Done()

	// All message's timetick less than incoming timetick is all belong to the output sealed segment.
	// So the output sealed segment transfer into flush == all message's timetick less than incoming timetick are flushed.
	sealedSegments, err := m.managers.SealAndFenceSegmentUntil(collectionID, timetick)
	if err != nil {
		return nil, err
	}
	segmentIDs := make([]int64, 0, len(sealedSegments))
	for _, segment := range sealedSegments {
		segmentIDs = append(segmentIDs, segment.GetSegmentID())
	}
	return segmentIDs, nil
}

// AsyncMustSealSegments seals the specified segments asynchronously.
func (m *PChannelSegmentAllocManager) AsyncMustSealSegments(signal utils.SealSegmentSignal) {
	if !m.lifetime.Add(typeutil.LifetimeStateWorking) {
		return
	}
	defer m.lifetime.Done()

	if pm, err := m.managers.Get(signal.SegmentBelongs.CollectionID, signal.SegmentBelongs.PartitionID); err == nil {
		if segment := pm.GetAndRemoveSegment(signal.SegmentBelongs.SegmentID); segment != nil {
			segment.WithSealPolicy(signal.SealPolicy)
		} else {
			m.logger.Info(
				"segment not found when trigger must seal, may be already sealed",
				zap.Int64("collectionID", signal.SegmentBelongs.CollectionID),
				zap.Int64("partitionID", signal.SegmentBelongs.PartitionID),
				zap.Int64("segmentID", signal.SegmentBelongs.SegmentID),
			)
		}
	}
}

// checkLifetime checks the lifetime of the segment manager.
func (m *PChannelSegmentAllocManager) checkLifetime() error {
	if !m.lifetime.Add(typeutil.LifetimeStateWorking) {
		m.logger.Warn("unreachable: segment assignment manager is not working, so the wal is on closing")
		return errors.New("segment assignment manager is not working")
	}
	return nil
}

// Close try to persist all stats and invalid the manager.
func (m *PChannelSegmentAllocManager) Close() {
	m.logger.Info("segment assignment manager start to close")
	m.lifetime.SetState(typeutil.LifetimeStateStopped)
	m.lifetime.Wait()

	m.sealWorker.Close()
	m.logger.Info("seal worker closed")
	// Remove the segment assignment manager from the global manager.
	resource.Resource().SegmentAssignStatsManager().UnregisterSealOperator(m)
	m.metrics.Close()
}
