package growingruntime

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// PrepareGrowingIndexes adds construction metadata for newly introduced vector
// fields. Existing GrowingSegments keep their encodingIndexMeta snapshot. Query
// planning uses the View's independent read binding, never this write collection.
// AlterIndex and vector-index replacement require release; no hot policy update
// is applied to an existing field or existing segment here.
func (r *Runtime) PrepareGrowingIndexes(ctx context.Context, indexes []*indexpb.IndexInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return context.Canceled
	}
	schema := r.collection.Schema()
	vectorFields := make(map[int64]bool)
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		vectorFields[field.GetFieldID()] = typeutil.IsVectorType(field.GetDataType())
	}
	current := r.collection.IndexMeta()
	existing := make(map[int64]bool)
	for _, field := range current.GetIndexMetas() {
		existing[field.GetFieldID()] = true
	}
	var added []*indexpb.IndexInfo
	for _, index := range indexes {
		if vectorFields[index.GetFieldID()] && !existing[index.GetFieldID()] {
			added = append(added, index)
			existing[index.GetFieldID()] = true
		}
	}
	if len(added) == 0 {
		return nil
	}
	meta := segcore.ComposeCollectionIndexMeta(ctx, added, schema)
	for _, field := range current.GetIndexMetas() {
		meta.IndexMetas = append(meta.IndexMetas, proto.Clone(field).(*segcorepb.FieldIndexMeta))
	}
	return r.collection.UpdateIndexMeta(meta)
}
