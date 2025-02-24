package registry

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/registry"
)

// MustGetBuilder returns the wal builder by name.
func MustGetBuilder(name string) wal.OpenerBuilder {
	b := registry.MustGetBuilder(name)
	return adaptor.AdaptImplsToBuilder(b)
}
