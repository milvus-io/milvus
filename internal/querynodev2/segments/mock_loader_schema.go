//go:build test || dynamic

package segments

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// Patch schema-bound loader calls with mockey in tests.
func (*MockLoader) LoadBloomFilterSetWithSchema(context.Context, *schemapb.CollectionSchema, ...*querypb.SegmentLoadInfo) ([]*pkoracle.BloomFilterSet, error) {
	panic("patch LoadBloomFilterSetWithSchema with mockey")
}

func (*MockLoader) ReserveLoadResourceWithSchema(context.Context, *schemapb.CollectionSchema, ...*querypb.SegmentLoadInfo) (LoadResourceReservation, error) {
	panic("patch ReserveLoadResourceWithSchema with mockey")
}
