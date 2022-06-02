package proxy

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type getCollectionIDFunc func(ctx context.Context, collectionName string) (typeutil.UniqueID, error)
type getCollectionSchemaFunc func(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error)

type mockCache struct {
	Cache
	getIDFunc     getCollectionIDFunc
	getSchemaFunc getCollectionSchemaFunc
}

func (m *mockCache) GetCollectionID(ctx context.Context, collectionName string) (typeutil.UniqueID, error) {
	if m.getIDFunc != nil {
		return m.getIDFunc(ctx, collectionName)
	}
	return 0, nil
}

func (m *mockCache) GetCollectionSchema(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
	if m.getSchemaFunc != nil {
		return m.getSchemaFunc(ctx, collectionName)
	}
	return nil, nil
}

func (m *mockCache) RemoveCollection(ctx context.Context, collectionName string) {
}

func (m *mockCache) setGetIDFunc(f getCollectionIDFunc) {
	m.getIDFunc = f
}

func (m *mockCache) setGetSchemaFunc(f getCollectionSchemaFunc) {
	m.getSchemaFunc = f
}

func newMockCache() *mockCache {
	return &mockCache{}
}
