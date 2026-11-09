package qvresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
)

func TestQueryViewPhysicalSegmentLoaderPrewarmsTheNativeSegment(t *testing.T) {
	mockey.PatchConvey("prewarm warms every field of the wrapped native segment", t, func() {
		native := &segments.LocalSegment{}
		physical := newQueryViewPhysicalSegmentLoader(nil, &fakeQVLoader{})
		base := newQueryViewTransformSegment(&qvLocalSegment{segment: native}, "v1", 50)
		var warmed []*segments.LocalSegment
		mockey.Mock((*segments.LocalSegment).Prewarm).To(func(s *segments.LocalSegment, _ context.Context, fieldIDs []int64) error {
			require.Empty(t, fieldIDs)
			warmed = append(warmed, s)
			return nil
		}).Build()
		require.NoError(t, physical.Prewarm(t.Context(), &testTransformSegmentWrapper{TransformSegment: base}))
		require.Equal(t, []*segments.LocalSegment{native}, warmed)
	})
}
