//go:build test && dynamic

package qvresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewSegmentResourceEstimator_DelegatesToLoader(t *testing.T) {
	mockey.PatchConvey("admission preserves reservation, error and competing usage", t, func() {
		reservation := &fakeQVResourceReservation{}
		loader := &fakeQVResourceLoader{reservation: reservation}
		estimator := newQueryViewSegmentResourceEstimator(loader)
		info := &querypb.SegmentLoadInfo{SegmentID: 10}

		var usage segments.LoadResource
		var admissionErr error
		mockey.Mock((*fakeQVResourceLoader).ReserveLoadResourceWithStatus).To(func(_ *fakeQVResourceLoader, _ context.Context, _ *schemapb.CollectionSchema, infos ...*querypb.SegmentLoadInfo) (segments.LoadResourceReservation, segments.LoadResource, error) {
			assert.Same(t, info, infos[0])
			return reservation, usage, admissionErr
		}).Build()
		got, competing, err := estimator.Reserve(context.Background(), info, fakeQVCollectionRuntime{collectionID: 1})
		require.NoError(t, err)
		assert.False(t, competing)
		assert.Same(t, reservation, got)
		for _, u := range []segments.LoadResource{{MemorySize: 1}, {DiskSize: 1}, {}} {
			usage = u
			admissionErr = merr.WrapErrServiceMemoryLimitExceeded(2, 1)
			_, competing, err = estimator.Reserve(t.Context(), info, fakeQVCollectionRuntime{})
			require.ErrorIs(t, err, merr.ErrServiceMemoryLimitExceeded)
			require.Equal(t, u != (segments.LoadResource{}), competing)
		}
	})
}
