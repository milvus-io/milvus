package scheduler

import (
	"context"

	"github.com/milvus-io/milvus/internal/proxy/metacache"
)

// Existing fixtures model a steady-state schema. Transition tests patch this
// boundary with mockey to supply an independent write selector.
func (m *MockCache) GetCollectionWriteInfo(ctx context.Context, database, name string, id int64) (*metacache.CollectionInfo, error) {
	return m.GetCollectionInfo(ctx, database, name, id)
}
