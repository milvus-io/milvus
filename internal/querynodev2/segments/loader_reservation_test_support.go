//go:build test && dynamic

package segments

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// Patch the additive admission API with mockey where a test needs it.
func (*MockLoader) ReserveLoadResourceWithStatus(context.Context, *schemapb.CollectionSchema, ...*querypb.SegmentLoadInfo) (LoadResourceReservation, LoadResource, error) {
	panic("patch ReserveLoadResourceWithStatus with mockey")
}
