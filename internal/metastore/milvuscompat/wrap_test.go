package milvuscompat

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/catalog"
	"github.com/milvus-io/milvus/pkg/v3/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

type recordingRootCatalog struct {
	metastore.RootCoordCatalog
	db           *model.Database
	ts           uint64
	dbs          []*model.Database
	collection   *model.Collection
	oldColl      *model.Collection
	newColl      *model.Collection
	fileResource *internalpb.FileResourceInfo
}

func (r *recordingRootCatalog) CreateDatabase(ctx context.Context, db *model.Database, ts uint64) error {
	r.db = db
	r.ts = ts
	return nil
}

func (r *recordingRootCatalog) ListDatabases(ctx context.Context, ts uint64) ([]*model.Database, error) {
	r.ts = ts
	return r.dbs, nil
}

func (r *recordingRootCatalog) GetCollectionByID(ctx context.Context, dbID int64, ts uint64, collectionID int64) (*model.Collection, error) {
	r.ts = ts
	return r.collection, nil
}

func (r *recordingRootCatalog) AlterCollectionDB(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts uint64) error {
	r.oldColl = oldColl
	r.newColl = newColl
	r.ts = ts
	return nil
}

func (r *recordingRootCatalog) SaveFileResource(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64) error {
	r.fileResource = resource
	r.ts = version
	return nil
}

func TestWrapMilvusCatalogsCanRoundTripThroughCatalogInterface(t *testing.T) {
	root := &recordingRootCatalog{}
	c := Wrap(Catalogs{RootCoord: root})

	catalogs := New(c)
	db := &model.Database{ID: 10, Name: "db"}
	require.NoError(t, catalogs.RootCoord.CreateDatabase(context.Background(), db, 100))

	require.Same(t, db, root.db)
	require.EqualValues(t, 100, root.ts)
}

func TestWrapDatabasesGetUsesListAndFilters(t *testing.T) {
	root := &recordingRootCatalog{
		dbs: []*model.Database{
			{ID: 1, Name: "default"},
			{ID: 2, Name: "analytics"},
		},
	}
	c := Wrap(Catalogs{RootCoord: root})

	byID, err := c.Metadata().Databases().Get(context.Background(), catalog.DatabaseRef{ID: 2}, catalog.ReadOptions{At: 101})
	require.NoError(t, err)
	require.Equal(t, int64(2), byID.ID)
	require.EqualValues(t, 101, root.ts)

	byName, err := c.Metadata().Databases().Get(context.Background(), catalog.DatabaseRef{Name: "default"}, catalog.ReadOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(1), byName.ID)
}

func TestWrapPartitionsListReadsCollectionPartitions(t *testing.T) {
	partitions := []*model.Partition{{PartitionID: 10, PartitionName: "p1"}}
	root := &recordingRootCatalog{
		collection: &model.Collection{CollectionID: 100, Partitions: partitions},
	}
	c := Wrap(Catalogs{RootCoord: root})

	got, err := c.Metadata().Partitions().List(
		context.Background(),
		catalog.CollectionRef{Database: catalog.DatabaseRef{ID: 1}, ID: 100},
		catalog.ReadOptions{At: 202},
	)
	require.NoError(t, err)
	require.Same(t, partitions[0], got[0])
	require.EqualValues(t, 202, root.ts)
}

func TestWrapCollectionsAlterRoutesDBChangeToAlterCollectionDB(t *testing.T) {
	root := &recordingRootCatalog{}
	c := Wrap(Catalogs{RootCoord: root})

	oldColl := &model.Collection{DBID: 1, DBName: "old_db", CollectionID: 10, Name: "coll", Properties: []*commonpb.KeyValuePair{{Key: "ttl", Value: "60"}}}
	newColl := &model.Collection{DBID: 2, DBName: "new_db", CollectionID: 10, Name: "coll", Properties: []*commonpb.KeyValuePair{{Key: "ttl", Value: "60"}}}

	err := c.Metadata().Collections().Alter(
		context.Background(),
		catalog.AlterCollectionRequest{Old: oldColl, New: newColl},
		catalog.WriteOptions{Timestamp: 303},
	)
	require.NoError(t, err)
	require.Same(t, oldColl, root.oldColl)
	require.Same(t, newColl, root.newColl)
	require.EqualValues(t, 303, root.ts)
}

func TestWrapSnapshotsWithoutOldEquivalentReturnUnsupported(t *testing.T) {
	c := Wrap(Catalogs{DataCoord: &recordingDataCatalog{}})

	_, err := c.MetadataInternal().Snapshots().Get(context.Background(), GetSnapshotRequest{}, catalog.ReadOptions{})
	require.ErrorIs(t, err, catalog.ErrUnsupportedImplementation)

	_, err = c.MetadataInternal().Snapshots().ListManifests(context.Background(), ListManifestsRequest{}, catalog.ReadOptions{})
	require.ErrorIs(t, err, catalog.ErrUnsupportedImplementation)
}

func TestWrapFilesPrefersRootCatalog(t *testing.T) {
	root := &recordingRootCatalog{}
	data := &recordingDataCatalog{}
	c := Wrap(Catalogs{RootCoord: root, DataCoord: data})

	resource := &internalpb.FileResourceInfo{Name: "dictionary"}
	require.NoError(t, c.MetadataInternal().Files().Save(context.Background(), resource, 404, catalog.WriteOptions{}))

	require.Same(t, resource, root.fileResource)
	require.Nil(t, data.fileResource)
	require.EqualValues(t, 404, root.ts)
}

func TestNewOldBoolAPICollapsesErrorsToFalse(t *testing.T) {
	catalogs := New(failingCollectionExistsCatalog{})

	require.False(t, catalogs.RootCoord.CollectionExists(context.Background(), 1, 2, 3))
}

type recordingDataCatalog struct {
	metastore.DataCoordCatalog
	fileResource *internalpb.FileResourceInfo
}

func (r *recordingDataCatalog) SaveFileResource(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64) error {
	r.fileResource = resource
	return nil
}

func TestWrapDatabasesGetReturnsNotFound(t *testing.T) {
	c := Wrap(Catalogs{RootCoord: &recordingRootCatalog{dbs: []*model.Database{{ID: 1, Name: "default"}}}})

	_, err := c.Metadata().Databases().Get(context.Background(), catalog.DatabaseRef{ID: 2}, catalog.ReadOptions{})
	require.True(t, errors.Is(err, catalog.ErrNotFound))
}

type failingCollectionExistsCatalog struct {
	catalog.Catalog
}

func (failingCollectionExistsCatalog) Metadata() catalog.MetadataCatalog {
	return failingCollectionExistsMetadata{}
}

func (failingCollectionExistsCatalog) MetadataInternal() MetadataCatalog {
	return nil
}

func (failingCollectionExistsCatalog) InternalState() MilvusStateCatalog {
	return nil
}

type failingCollectionExistsMetadata struct {
	catalog.MetadataCatalog
}

func (failingCollectionExistsMetadata) Collections() catalog.CollectionCatalog {
	return failingCollectionExistsCollections{}
}

type failingCollectionExistsCollections struct {
	catalog.CollectionCatalog
}

func (failingCollectionExistsCollections) Exists(ctx context.Context, ref catalog.CollectionRef, opts catalog.ReadOptions) (bool, error) {
	return true, errors.New("collection exists failed")
}
