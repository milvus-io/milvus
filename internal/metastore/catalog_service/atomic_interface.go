// AtomicCapableCatalog is an extension interface for catalog implementations
// that support the atomic composite APIs (CommitCompaction /
// CreateCollectionWithIndex / CommitFlushSegment / GetCommitMarker).
//
// DataCoord callers that want atomic semantics should do a type assertion:
//
//	if atomic, ok := m.catalog.(catalog_service.AtomicCapableCatalog); ok {
//	    _, err := atomic.CommitCompaction(ctx, req)
//	    // ...
//	} else {
//	    // fall back to legacy multi-step path (raw local catalog)
//	}
//
// Only DataCoordAdapter / RootCoordAdapter implement this interface. When
// UseCatalogService=false, DataCoord/RootCoord hold a raw local kv catalog
// that does NOT implement AtomicCapableCatalog, so the type assertion fails
// and callers transparently fall back. This is the rev 2 glue that lets
// feature flags exist without leaking into the metastore interface contract.
package catalog_service

import (
	"context"

	"github.com/milvus-io/milvus-catalog/catalogpb"
)

type AtomicCapableCatalog interface {
	CommitCompaction(ctx context.Context, req *catalogpb.CommitCompactionRequest) (*catalogpb.CommitCompactionResponse, error)
	CreateCollectionWithIndex(ctx context.Context, req *catalogpb.CreateCollectionWithIndexRequest) (*catalogpb.CreateCollectionWithIndexResponse, error)
	CommitFlushSegment(ctx context.Context, req *catalogpb.CommitFlushSegmentRequest) (*catalogpb.CommitFlushSegmentResponse, error)
	GetCommitMarker(ctx context.Context, req *catalogpb.GetCommitMarkerRequest) (*catalogpb.GetCommitMarkerResponse, error)
}
