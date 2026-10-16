package growingruntime

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// UpdateIndexMeta prepares vector search parameters, not sealed index files.
// Keep metadata for fields absent from the new configuration: older QueryViews
// can still serve through the same runtime while their leases remain valid.
func (r *Runtime) UpdateIndexMeta(ctx context.Context, indexes []*indexpb.IndexInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return context.Canceled
	}
	meta := segcore.ComposeCollectionIndexMeta(ctx, indexes, r.collection.Schema())
	current := r.collection.IndexMeta()
	fields := make(map[int64]struct{}, len(meta.GetIndexMetas()))
	for _, field := range meta.GetIndexMetas() {
		fields[field.GetFieldID()] = struct{}{}
	}
	for _, field := range current.GetIndexMetas() {
		if _, ok := fields[field.GetFieldID()]; !ok {
			meta.IndexMetas = append(meta.IndexMetas, field)
		}
	}
	if proto.Equal(current, meta) {
		return nil
	}
	return r.collection.UpdateIndexMeta(meta)
}
