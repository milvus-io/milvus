package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type getCollectionIDFunc func(ctx context.Context, collectionName string) (typeutil.UniqueID, error)
type getCollectionNameFunc func(ctx context.Context, collectionID int64) (string, error)
type getCollectionSchemaFunc func(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error)
type getCollectionInfoFunc func(ctx context.Context, collectionName string) (*collectionInfo, error)
type getUserRoleFunc func(username string) []string
type getPartitionIDFunc func(ctx context.Context, collectionName string, partitionName string) (typeutil.UniqueID, error)

type mockCache struct {
	Cache
	getIDFunc          getCollectionIDFunc
	getNameFunc        getCollectionNameFunc
	getSchemaFunc      getCollectionSchemaFunc
	getInfoFunc        getCollectionInfoFunc
	getUserRoleFunc    getUserRoleFunc
	getPartitionIDFunc getPartitionIDFunc
}

func (m *mockCache) GetCollectionID(ctx context.Context, collectionName string) (typeutil.UniqueID, error) {
	if m.getIDFunc != nil {
		return m.getIDFunc(ctx, collectionName)
	}
	return 0, nil
}

func (m *mockCache) GetCollectionName(ctx context.Context, collectionID int64) (string, error) {
	if m.getIDFunc != nil {
		return m.getNameFunc(ctx, collectionID)
	}
	return "", nil
}

func (m *mockCache) GetCollectionSchema(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
	if m.getSchemaFunc != nil {
		return m.getSchemaFunc(ctx, collectionName)
	}
	return nil, nil
}

func (m *mockCache) GetCollectionInfo(ctx context.Context, collectionName string) (*collectionInfo, error) {
	if m.getInfoFunc != nil {
		return m.getInfoFunc(ctx, collectionName)
	}
	return nil, nil
}

func (m *mockCache) RemoveCollection(ctx context.Context, collectionName string) {
}

func (m *mockCache) GetPartitionID(ctx context.Context, collectionName string, partitionName string) (typeutil.UniqueID, error) {
	if m.getPartitionIDFunc != nil {
		return m.getPartitionIDFunc(ctx, collectionName, partitionName)
	}
	return 0, nil
}

func (m *mockCache) GetUserRole(username string) []string {
	if m.getUserRoleFunc != nil {
		return m.getUserRoleFunc(username)
	}
	return []string{}
}

func (m *mockCache) setGetIDFunc(f getCollectionIDFunc) {
	m.getIDFunc = f
}

func (m *mockCache) setGetSchemaFunc(f getCollectionSchemaFunc) {
	m.getSchemaFunc = f
}

func (m *mockCache) setGetInfoFunc(f getCollectionInfoFunc) {
	m.getInfoFunc = f
}

func (m *mockCache) setGetPartitionIDFunc(f getPartitionIDFunc) {
	m.getPartitionIDFunc = f
}

func newMockCache() *mockCache {
	return &mockCache{}
}
