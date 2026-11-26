package qnview

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestReservationCapacityWindow(t *testing.T) {
	for _, err := range []error{merr.ErrServiceMemoryLimitExceeded, merr.ErrServiceDiskLimitExceeded, merr.ErrSegmentRequestResourceFailed} {
		t.Run(err.Error(), func(t *testing.T) {
			b := &segmentBinding{}
			now := time.Now()
			err = merr.Wrap(err, "admission")
			require.False(t, b.reservationNeedsReplanLocked(now, false, err))
			require.False(t, b.reservationNeedsReplanLocked(now.Add(resourceCapacityRetryWindow-time.Nanosecond), false, err))
			require.True(t, b.reservationNeedsReplanLocked(now.Add(resourceCapacityRetryWindow), false, err))
			// Competing reservations extend, but cannot permanently reset, retries.
			b.resetRetryLocked()
			require.False(t, b.reservationNeedsReplanLocked(now, true, err))
			require.True(t, b.reservationNeedsReplanLocked(now.Add(resourceContentionRetryWindow), true, err))
			b.resetRetryLocked()
			require.True(t, b.capacityRejectedSince.IsZero())
			require.False(t, b.reservationNeedsReplanLocked(now.Add(2*time.Hour), false, err))
			// Success and unrelated failures start a fresh admission window.
			for _, next := range []error{nil, merr.ErrServiceNotReady, context.Canceled} {
				require.False(t, b.reservationNeedsReplanLocked(now, false, next))
				require.True(t, b.capacityRejectedSince.IsZero())
				require.False(t, b.reservationNeedsReplanLocked(now, false, err))
			}
			b.resetRetryLocked()
			require.True(t, b.capacityRejectedSince.IsZero())
		})
	}
}

func TestSegmentResourceCapacityRecoversLocally(t *testing.T) {
	mockey.PatchConvey("brief contention recovers without another Acquire or failure report", t, func() {
		f := setupResourceTest(t, nil)
		f.manager.estimator = &resourceTestEstimator{}
		var attempts, releases atomic.Int32
		mockey.Mock((*resourceTestEstimator).Reserve).To(func(*resourceTestEstimator, context.Context, *querypb.SegmentLoadInfo, CollectionRuntime) (ResourceReservation, bool, error) {
			if attempts.Add(1) <= 2 {
				return nil, true, merr.ErrServiceMemoryLimitExceeded
			}
			return &resourceTestReservation{}, false, nil
		}).Build()
		mockey.Mock((*resourceTestReservation).Release).To(func(*resourceTestReservation) { releases.Add(1) }).Build()
		var failures atomic.Int32
		req := f.request(1, 1, 1)
		req.OnUnrecoverable = func(...*viewpb.SegmentPerparingFailure) { failures.Add(1) }
		f.manager.Acquire(req)
		f.await(t, 1)
		require.EqualValues(t, 3, attempts.Load())
		require.EqualValues(t, 1, releases.Load())
		require.Zero(t, failures.Load())
		f.drop(t, req)
	})
}

func TestSegmentResourcePersistentCapacityPreservesServingView(t *testing.T) {
	for _, tc := range []struct {
		err    error
		reason viewpb.SegmentPerparingFailure_Reason
	}{
		{merr.ErrServiceMemoryLimitExceeded, viewpb.SegmentPerparingFailure_MemoryLimitExceeded},
		{merr.ErrServiceDiskLimitExceeded, viewpb.SegmentPerparingFailure_DiskLimitExceeded},
		{merr.ErrSegmentRequestResourceFailed, viewpb.SegmentPerparingFailure_ResourceReservationFailed},
	} {
		capacityErr := tc.err
		t.Run(capacityErr.Error(), func(t *testing.T) {
			mockey.PatchConvey("only unmet preparation fails; the existing view remains queryable", t, func() {
				f := setupResourceTest(t, nil)
				old := f.acquire(1, 1, 1)
				f.await(t, 1)
				f.manager.estimator = &resourceTestEstimator{}
				var attempts atomic.Int32
				mockey.Mock((*resourceTestEstimator).Reserve).To(func(*resourceTestEstimator, context.Context, *querypb.SegmentLoadInfo, CollectionRuntime) (ResourceReservation, bool, error) {
					attempts.Add(1)
					return nil, false, capacityErr
				}).Build()
				type failureFact struct {
					segment int64
					reason  viewpb.SegmentPerparingFailure_Reason
				}
				failed := make(chan failureFact, 2)
				next := f.request(2, 1, 2)
				next.OnUnrecoverable = func(failures ...*viewpb.SegmentPerparingFailure) {
					for _, failure := range failures {
						failed <- failureFact{segment: failure.GetSegmentId(), reason: failure.GetReason()}
					}
				}
				f.manager.Acquire(next)
				select {
				case failure := <-failed:
					require.Equal(t, failureFact{segment: 1000, reason: tc.reason}, failure)
				case <-time.After(15 * time.Second):
					t.Fatal("persistent capacity rejection never reached the coordinator callback")
				}
				require.Greater(t, attempts.Load(), int32(1))
				require.Empty(t, failed)
				require.Empty(t, f.ready)
				handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), old.Key, old.View, 100)
				require.NoError(t, err)
				handles[0].Release()
				f.drop(t, next)
				f.drop(t, old)
			})
		})
	}
}

type dimensionedCapacityError struct{ error }

func (dimensionedCapacityError) ResourceFailureReason() viewpb.SegmentPerparingFailure_Reason {
	return viewpb.SegmentPerparingFailure_DiskLimitExceeded
}
func (e dimensionedCapacityError) Unwrap() error { return e.error }

func TestReservationCapacityDimensionSurvivesWrapping(t *testing.T) {
	err := merr.Wrap(dimensionedCapacityError{merr.ErrSegmentRequestResourceFailed}, "loader admission")
	require.Equal(t, viewpb.SegmentPerparingFailure_DiskLimitExceeded, reservationFailureReason(err))
	require.ErrorIs(t, err, merr.ErrSegmentRequestResourceFailed)
}
