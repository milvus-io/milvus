package queryview

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// LoadInfoCatalog retains immutable loading requirements referenced by views.
// Historical snapshot GC is intentionally deferred.
type LoadInfoCatalog interface {
	GetLoadInfo(context.Context, int64, uint64) (*querypb.GetQueryViewLoadInfoResponse, error)
	SaveLoadInfo(context.Context, *querypb.GetQueryViewLoadInfoResponse) error
}
