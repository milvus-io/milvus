package qnview

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestSegmentUpdateTaskResourceLifecycle(t *testing.T) {
	for _, scenario := range []string{"success", "reserve_failure", "update_failure", "cancel", "unchanged"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			estimator := &fakeSegmentResourceEstimator{}
			reservation := &fakeResourceReservation{}
			loader := &fakePhysicalLoader{}
			var order []string
			var failure error
			finished := 0
			reservePatch := mockey.Mock((*fakeSegmentResourceEstimator).Reserve).To(func(_ *fakeSegmentResourceEstimator, _ context.Context, info *querypb.SegmentLoadInfo, _ CollectionRuntime) (ResourceReservation, error) {
				order = append(order, "reserve")
				require.EqualValues(t, 1000, info.GetSegmentID())
				if scenario == "reserve_failure" {
					return nil, assert.AnError
				}
				return reservation, nil
			}).Build()
			t.Cleanup(func() { reservePatch.UnPatch() })
			releasePatch := mockey.Mock((*fakeResourceReservation).Release).To(func(_ *fakeResourceReservation) {
				order = append(order, "release")
			}).Build()
			t.Cleanup(func() { releasePatch.UnPatch() })
			updatePatch := mockey.Mock((*fakePhysicalLoader).Update).To(func(_ *fakePhysicalLoader, _ context.Context, _ TransformSegment, _ CollectionRuntime, _ SegmentLoadInfoSnapshot, _ SegmentUpdateAction) error {
				order = append(order, "update")
				if scenario == "cancel" {
					cancel()
					return context.Canceled
				}
				if scenario == "update_failure" {
					return assert.AnError
				}
				return nil
			}).Build()
			t.Cleanup(func() { updatePatch.UnPatch() })
			current := SegmentLoadInfoRevision{Revision: 1}
			if scenario == "unchanged" {
				current.Revision = 2
			}
			task := newSegmentUpdateTask(loader, SegmentUpdateTask{
				Context: ctx, Segment: &fakeTransformSegment{id: 1000}, Collection: &fakeCollectionRuntimeGuard{},
				Current:    current,
				Snapshot:   SegmentLoadInfoSnapshot{Revision: SegmentLoadInfoRevision{Revision: 2}, LoadInfo: &querypb.SegmentLoadInfo{SegmentID: 1000}},
				OnUpdated:  func(SegmentLoadInfoRevision) { order = append(order, "updated") },
				OnFailed:   func(err error) { failure = err; order = append(order, "failed") },
				OnFinished: func() { finished++ },
			}, estimator)
			err := task.Execute(context.Background())
			switch scenario {
			case "success":
				require.NoError(t, err)
				require.Equal(t, []string{"reserve", "update"}, order[:2])
				require.ElementsMatch(t, []string{"release", "updated"}, order[2:])
			case "reserve_failure":
				require.ErrorIs(t, err, nodescheduler.ErrDelay)
				require.Equal(t, []string{"reserve"}, order)
			case "update_failure":
				require.ErrorIs(t, err, assert.AnError)
				require.ErrorIs(t, failure, assert.AnError)
				require.Equal(t, []string{"reserve", "update", "release", "failed"}, order)
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
				require.ErrorIs(t, failure, context.Canceled)
				require.Equal(t, []string{"reserve", "update", "release", "failed"}, order)
			case "unchanged":
				require.NoError(t, err)
				require.Equal(t, []string{"updated"}, order)
			}
			if scenario == "reserve_failure" {
				require.Zero(t, finished, "admission retries keep the attempt alive")
			} else {
				require.Equal(t, 1, finished)
			}
		})
	}
}
