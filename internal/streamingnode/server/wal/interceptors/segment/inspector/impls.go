package inspector

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	defaultSealAllInterval  = 10 * time.Second
	defaultMustSealInterval = 200 * time.Millisecond
)

// NewSealedInspector creates a new seal inspector.
func NewSealedInspector(n *stats.SealSignalNotifier) SealOperationInspector {
	s := &sealOperationInspectorImpl{
		taskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		managers:     typeutil.NewConcurrentMap[string, SealOperator](),
		notifier:     n,
		backOffTimer: typeutil.NewBackoffTimer(typeutil.BackoffTimerConfig{
			Default: 1 * time.Second,
			Backoff: typeutil.BackoffConfig{
				InitialInterval: 20 * time.Millisecond,
				Multiplier:      2.0,
				MaxInterval:     1 * time.Second,
			},
		}),
		triggerCh: make(chan string),
	}
	go s.background()
	return s
}

// sealOperationInspectorImpl is the implementation of SealInspector.
type sealOperationInspectorImpl struct {
	taskNotifier *syncutil.AsyncTaskNotifier[struct{}]

	managers     *typeutil.ConcurrentMap[string, SealOperator]
	notifier     *stats.SealSignalNotifier
	backOffTimer *typeutil.BackoffTimer
	triggerCh    chan string
}

// TriggerSealWaited implements SealInspector.TriggerSealWaited.
func (s *sealOperationInspectorImpl) TriggerSealWaited(ctx context.Context, pchannel string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.triggerCh <- pchannel:
		return nil
	}
}

// RegsiterPChannelManager implements SealInspector.RegsiterPChannelManager.
func (s *sealOperationInspectorImpl) RegsiterPChannelManager(m SealOperator) {
	_, loaded := s.managers.GetOrInsert(m.Channel().Name, m)
	if loaded {
		panic("pchannel manager already exists, critical bug in code")
	}
}

// UnregisterPChannelManager implements SealInspector.UnregisterPChannelManager.
func (s *sealOperationInspectorImpl) UnregisterPChannelManager(m SealOperator) {
	_, loaded := s.managers.GetAndRemove(m.Channel().Name)
	if !loaded {
		panic("pchannel manager not found, critical bug in code")
	}
}

// Close implements SealInspector.Close.
func (s *sealOperationInspectorImpl) Close() {
	s.taskNotifier.Cancel()
	s.taskNotifier.BlockUntilFinish()
}

// background is the background task to inspect if a segment should be sealed or not.
func (s *sealOperationInspectorImpl) background() {
	defer s.taskNotifier.Finish(struct{}{})

	sealAllTicker := time.NewTicker(defaultSealAllInterval)
	defer sealAllTicker.Stop()

	mustSealTicker := time.NewTicker(defaultMustSealInterval)
	defer mustSealTicker.Stop()

	var backoffCh <-chan time.Time
	for {
		if s.shouldEnableBackoff() {
			// start a backoff if there's some pchannel wait for seal.
			s.backOffTimer.EnableBackoff()
			backoffCh, _ = s.backOffTimer.NextTimer()
		} else {
			s.backOffTimer.DisableBackoff()
		}

		select {
		case <-s.taskNotifier.Context().Done():
			return
		case pchannel := <-s.triggerCh:
			if manager, ok := s.managers.Get(pchannel); ok {
				manager.TryToSealWaitedSegment(s.taskNotifier.Context())
			}
		case <-s.notifier.WaitChan():
			s.tryToSealPartition(s.notifier.Get())
		case <-backoffCh:
			// only seal waited segment for backoff.
			s.managers.Range(func(_ string, pm SealOperator) bool {
				pm.TryToSealWaitedSegment(s.taskNotifier.Context())
				return true
			})
		case <-sealAllTicker.C:
			s.managers.Range(func(_ string, pm SealOperator) bool {
				pm.TryToSealSegments(s.taskNotifier.Context())
				return true
			})
		case <-mustSealTicker.C:
			segmentBelongs := resource.Resource().SegmentAssignStatsManager().SealByTotalGrowingSegmentsSize()
			if pm, ok := s.managers.Get(segmentBelongs.PChannel); ok {
				pm.MustSealSegments(s.taskNotifier.Context(), segmentBelongs)
			}
		}
	}
}

// shouldEnableBackoff checks if the backoff should be enabled.
// if there's any pchannel has a segment wait for seal, enable backoff.
func (s *sealOperationInspectorImpl) shouldEnableBackoff() bool {
	enableBackoff := false
	s.managers.Range(func(_ string, pm SealOperator) bool {
		if !pm.IsNoWaitSeal() {
			enableBackoff = true
			return false
		}
		return true
	})
	return enableBackoff
}

// tryToSealPartition tries to seal the segment with the specified policies.
func (s *sealOperationInspectorImpl) tryToSealPartition(infos typeutil.Set[stats.SegmentBelongs]) {
	for info := range infos {
		pm, ok := s.managers.Get(info.PChannel)
		if !ok {
			continue
		}
		pm.TryToSealSegments(s.taskNotifier.Context(), info)
	}
}
