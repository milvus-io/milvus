package metastore

import (
	pkgmetastore "github.com/milvus-io/milvus/pkg/v3/metastore"
)

// StreamingNodeCataLog was moved to the shared pkg/v3/metastore module (so the
// pooled catalog service can depend on the streamingnode contract); it is
// re-exported here to keep the internal import path working unchanged.
type StreamingNodeCataLog = pkgmetastore.StreamingNodeCataLog
