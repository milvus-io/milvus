package qnview

import (
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const resourceRetryMaxDelay = 5 * time.Second

// A quiet admission window tolerates brief pressure. Competing loads extend it
// only up to a bounded admission retry window, never indefinitely. Neither
// window is a deadline on metadata, I/O or physical loading.
const (
	resourceCapacityRetryWindow   = 5 * time.Second
	resourceContentionRetryWindow = 30 * time.Second
)

func (b *segmentBinding) reservationNeedsReplanLocked(now time.Time, competing bool, err error) bool {
	capacity := reservationFailureReason(err) != viewpb.SegmentPerparingFailure_Unknown
	if !capacity {
		b.capacityRejectedSince, b.capacityFirstRejected = time.Time{}, time.Time{}
		return false
	}
	if b.capacityFirstRejected.IsZero() {
		b.capacityFirstRejected = now
	}
	if now.Sub(b.capacityFirstRejected) >= resourceContentionRetryWindow {
		return true
	}
	if competing {
		b.capacityRejectedSince = time.Time{}
		return false
	}

	if b.capacityRejectedSince.IsZero() {
		b.capacityRejectedSince = now
		return false
	}
	return now.Sub(b.capacityRejectedSince) >= resourceCapacityRetryWindow
}

func reservationFailureReason(err error) viewpb.SegmentPerparingFailure_Reason {
	var admission interface {
		ResourceFailureReason() viewpb.SegmentPerparingFailure_Reason
	}
	if errors.As(err, &admission) {
		return admission.ResourceFailureReason()
	}
	switch {
	case errors.Is(err, merr.ErrServiceMemoryLimitExceeded):
		return viewpb.SegmentPerparingFailure_MemoryLimitExceeded
	case errors.Is(err, merr.ErrServiceDiskLimitExceeded):
		return viewpb.SegmentPerparingFailure_DiskLimitExceeded
	case errors.Is(err, merr.ErrSegmentRequestResourceFailed):
		return viewpb.SegmentPerparingFailure_ResourceReservationFailed
	default:
		return viewpb.SegmentPerparingFailure_Unknown
	}
}

func newResourceRetryBackoff(initial time.Duration) *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = initial
	b.Multiplier = 2
	b.RandomizationFactor = 0.2
	b.MaxInterval = resourceRetryMaxDelay
	// Live requirements keep retrying until fulfilled or released.
	b.MaxElapsedTime = 0
	b.Reset()
	return b
}

func nextResourceRetryDelay(b *backoff.ExponentialBackOff) time.Duration {
	// MaxInterval limits the base interval, not the randomized delay.
	return min(b.NextBackOff(), resourceRetryMaxDelay)
}

func (b *segmentBinding) backoffLocked() {
	if b.retryBackoff == nil {
		b.retryBackoff = newResourceRetryBackoff(200 * time.Millisecond)
	}
	b.retryAfter = time.Now().Add(nextResourceRetryDelay(b.retryBackoff))
}

func (b *segmentBinding) resetRetryLocked() {
	b.retryBackoff = nil
	b.retryAfter = time.Time{}
	b.capacityRejectedSince = time.Time{}
	b.capacityFirstRejected = time.Time{}
}
