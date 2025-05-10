package stats

import (
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/policy"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/utils"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var (
	defaultSealWorkerTimerInterval = 1 * time.Minute
	growingBytesNotifyCooldown     = 5 * time.Second
)

// newSealWorker creates a new seal worker.
func newSealWorker(statsManager *StatsManager) *sealWorker {
	w := &sealWorker{
		statsManager:         statsManager,
		sealNotifier:         make(chan sealSegmentIDWithPolicy, 1),
		growingBytesNotifier: syncutil.NewCooldownNotifier[uint64](growingBytesNotifyCooldown, 1),
	}
	return w
}

// sealWorker is the background task that handles the seal signal from stats manager or timer.
type sealWorker struct {
	log.Binder
	statsManager         *StatsManager // reference to the stats manager.
	sealNotifier         chan sealSegmentIDWithPolicy
	growingBytesNotifier *syncutil.CooldownNotifier[uint64]
}

// NotifySealSegment is used to notify the seal worker to seal the segment.
func (m *sealWorker) NotifySealSegment(segmentID int64, sealPolicy policy.SealPolicy) {
	m.sealNotifier <- sealSegmentIDWithPolicy{segmentID: segmentID, sealPolicy: sealPolicy}
}

// NotifyGrowingBytes is used to notify the seal worker to seal the segment when the total size exceeds the threshold.
func (m *sealWorker) NotifyGrowingBytes(totalBytes uint64) {
	m.growingBytesNotifier.Notify(totalBytes)
}

// loop is the main loop of stats manager.
func (m *sealWorker) loop() {
	memoryNotifier := make(chan policy.SealPolicy, 1)
	timer := time.NewTicker(defaultSealWorkerTimerInterval)
	listener := &hardware.SystemMetricsListener{
		Cooldown: 30 * time.Second,
		Condition: func(sm hardware.SystemMetrics) bool {
			memoryThreshold := m.statsManager.getConfig().memoryThreshold
			return sm.UsedRatio() > memoryThreshold
		},
		Callback: func(sm hardware.SystemMetrics) {
			select {
			case memoryNotifier <- policy.PolicyNodeMemory(sm.UsedRatio()):
				// the repeated notify can be ignored.
			default:
			}
		},
	}
	defer func() {
		timer.Stop()
		hardware.UnregisterSystemMetricsListener(listener)
	}()
	hardware.RegisterSystemMetricsListener(listener)

	for {
		select {
		case targetSegment := <-m.sealNotifier:
			// Notify to seal the segment.
			m.asyncMustSealSegment(targetSegment.segmentID, targetSegment.sealPolicy)
		case <-timer.C:
			m.statsManager.updateConfig()
			m.notifyToSealSegmentWithTimePolicy()
		case policy := <-memoryNotifier:
			m.statsManager.updateConfig()
			m.notifyToSealSegmentUntilLessThanLWM(policy)
		case totalBytes := <-m.growingBytesNotifier.Chan():
			m.statsManager.updateConfig()
			m.notifyToSealSegmentUntilLessThanLWM(policy.PolicyGrowingSegmentBytesHWM(totalBytes))
		}
	}
}

// notifyToSealSegmentWithTimePolicy notifies to seal segments with time policy.
func (m *sealWorker) notifyToSealSegmentWithTimePolicy() {
	sealSegmentIDs := m.statsManager.selectSegmentsWithTimePolicy()
	if len(sealSegmentIDs) != 0 {
		m.Logger().Info("notify to seal segments with time policy", zap.Int("segmentNum", len(sealSegmentIDs)))
		for segmentID, sealPolicy := range sealSegmentIDs {
			m.asyncMustSealSegment(segmentID, sealPolicy)
		}
	}
}

// notifyToSealSegmentUntilLessThanLWM notifies to seal segments until the total size is less than the threshold.
func (m *sealWorker) notifyToSealSegmentUntilLessThanLWM(sealPolicy policy.SealPolicy) {
	segmentIDs := m.statsManager.selectSegmentsUntilLessThanLWM()
	if len(segmentIDs) != 0 {
		m.Logger().Info("notify to seal segments until less than LWM", zap.Int("segmentNum", len(segmentIDs)), zap.String("policy", string(sealPolicy.Policy)))
		for _, segmentID := range segmentIDs {
			m.asyncMustSealSegment(segmentID, sealPolicy)
		}
	}
}

// asyncMustSealSegment seals the segment asynchronously.
func (m *sealWorker) asyncMustSealSegment(segmentID int64, policy policy.SealPolicy) {
	belongs, stats, operator, ok := m.statsManager.getSealOperator(segmentID)
	if !ok {
		// The segment seal operation is performed asynchronously,
		// so the segment may be unregistered, should be ignored.
		return
	}
	// Notify the seal operator to do the seal.
	operator.AsyncFlushSegment(utils.SealSegmentSignal{
		SegmentBelongs: belongs,
		Stats:          *stats,
		SealPolicy:     policy,
	})
}
