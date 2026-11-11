package ddl

import (
	"context"

	"github.com/milvus-io/milvus/internal/proxy/metacache"
)

// Existing fixtures model a steady-state schema. Transition tests patch this
// boundary with mockey to supply an independent write selector.
func (m *MockCache) GetCollectionWriteInfo(ctx context.Context, database, name string, id int64) (*metacache.CollectionInfo, error) {
	return m.GetCollectionInfo(ctx, database, name, id)
}

func (*MetaCache) GetCollectionWriteInfo(context.Context, string, string, int64) (*metacache.CollectionInfo, error) {
	panic("patch GetCollectionWriteInfo with mockey")
}
