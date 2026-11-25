package segments

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestReservationStatusPreservesAdmissionSnapshot(t *testing.T) {
	mockey.PatchConvey("status is the competing usage captured during admission", t, func() {
		loader := &segmentLoader{}
		result := requestResourceResult{CommittedResource: LoadResource{MemorySize: 10, DiskSize: 20}}
		var admissionErr error
		mockey.Mock((*segmentLoader).requestResourceWithSchema).To(func(*segmentLoader, context.Context, *schemapb.CollectionSchema, ...*querypb.SegmentLoadInfo) (requestResourceResult, error) {
			return result, admissionErr
		}).Build()
		for _, e := range []error{nil, merr.ErrServiceMemoryLimitExceeded, merr.ErrServiceDiskLimitExceeded, merr.ErrSegmentRequestResourceFailed} {
			admissionErr = e
			reservation, competing, err := loader.ReserveLoadResourceWithStatus(t.Context(), nil)
			require.Equal(t, result.CommittedResource, competing)
			require.Equal(t, e, err)
			if e == nil {
				require.NotNil(t, reservation)
			} else {
				require.Nil(t, reservation)
			}
			legacy, legacyErr := loader.ReserveLoadResourceWithSchema(t.Context(), nil)
			require.Equal(t, e, legacyErr)
			require.Equal(t, reservation == nil, legacy == nil)
		}
	})
}
