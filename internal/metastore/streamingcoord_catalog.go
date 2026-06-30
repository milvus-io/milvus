package metastore

import (
	pkgmetastore "github.com/milvus-io/milvus/pkg/v3/metastore"
)

// StreamingCoordCataLog now lives in the shared pkg/v3/metastore package so the
// pooled catalog service can reuse the same persistence contract. This alias
// keeps existing internal call sites (the resource singleton, the
// internal/metastore/kv/streamingcoord implementation, and the internal mocks)
// compiling unchanged.
type StreamingCoordCataLog = pkgmetastore.StreamingCoordCataLog
