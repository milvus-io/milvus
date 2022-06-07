package table

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Catalog struct {
}

func (tc *Catalog) CreateCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error {
	return nil
}

func (tc *Catalog) GetCollectionByID(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error) {
	return nil, nil
}

func (tc *Catalog) GetCollectionByName(ctx context.Context, collectionName string, ts typeutil.Timestamp) (*model.Collection, error) {
	return nil, nil
}

func (tc *Catalog) ListCollections(ctx context.Context, ts typeutil.Timestamp) (map[string]*model.Collection, error) {
	return nil, nil
}

func (tc *Catalog) CollectionExists(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool {
	return false
}

func (tc *Catalog) DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error {
	return nil
}

func (tc *Catalog) CreatePartition(ctx context.Context, coll *model.Collection, ts typeutil.Timestamp) error {
	return nil
}

func (tc *Catalog) DropPartition(ctx context.Context, collectionInfo *model.Collection, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error {
	return nil
}

func (tc *Catalog) AlterIndex(ctx context.Context, index *model.Index) error {
	return nil
}

func (tc *Catalog) DropIndex(ctx context.Context, collectionInfo *model.Collection, dropIdxID typeutil.UniqueID, ts typeutil.Timestamp) error {
	return nil
}

func (tc *Catalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	return nil, nil
}

func (tc *Catalog) CreateAlias(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	return nil
}

func (tc *Catalog) DropAlias(ctx context.Context, collectionID typeutil.UniqueID, alias string, ts typeutil.Timestamp) error {
	return nil
}

func (tc *Catalog) ListAliases(ctx context.Context) ([]*model.Collection, error) {
	return nil, nil
}

func (tc *Catalog) GetCredential(ctx context.Context, username string) (*model.Credential, error) {
	return nil, nil
}

func (tc *Catalog) CreateCredential(ctx context.Context, credential *model.Credential) error {
	return nil
}

func (tc *Catalog) DropCredential(ctx context.Context, username string) error {
	return nil
}

func (tc *Catalog) ListCredentials(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (tc *Catalog) Close() {

}
