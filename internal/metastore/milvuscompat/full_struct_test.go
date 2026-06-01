package milvuscompat

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/catalog"
	"github.com/milvus-io/milvus/pkg/v3/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
)

type captureRoot struct {
	metastore.RootCoordCatalog
	receivedOld *model.Collection
	receivedNew *model.Collection
}

func (c *captureRoot) AlterCollectionDB(ctx context.Context, oldColl, newColl *model.Collection, ts uint64) error {
	c.receivedOld = oldColl
	c.receivedNew = newColl
	return nil
}

func TestAlterCollectionDBPreservesAllFields(t *testing.T) {
	root := &captureRoot{}
	c := Wrap(Catalogs{RootCoord: root})
	catalogs := New(c)

	oldColl := &model.Collection{
		TenantID:         "tenant-a",
		DBID:             1,
		DBName:           "db_old",
		CollectionID:     100,
		Name:             "users",
		Description:      "user records",
		AutoID:           true,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		CreateTime:       42,
		UpdateTimestamp:  43,
		SchemaVersion:    7,
		State:            pb.CollectionState_CollectionCreated,
		Properties: []*commonpb.KeyValuePair{
			{Key: "collection.ttl.seconds", Value: "3600"},
			{Key: "mmap.enabled", Value: "true"},
		},
		VirtualChannelNames:  []string{"v0", "v1"},
		PhysicalChannelNames: []string{"p0", "p1"},
		ShardsNum:            2,
		EnableDynamicField:   true,
		EnableNamespace:      true,
		Aliases:              []string{"u", "users_alias"},
		FileResourceIds:      []int64{1001, 1002},
		ExternalSource:       "s3://bucket/ext",
		ExternalSpec:         "spec-v1",
	}

	newColl := oldColl.Clone()
	newColl.DBID = 2
	newColl.DBName = "db_new"

	require.NoError(t, catalogs.RootCoord.AlterCollectionDB(context.Background(), oldColl, newColl, 100))

	require.Same(t, oldColl, root.receivedOld, "old collection must be passed through, not reconstructed")
	require.Same(t, newColl, root.receivedNew, "new collection must be passed through, not reconstructed")

	got := root.receivedNew
	require.Equal(t, newColl.TenantID, got.TenantID)
	require.Equal(t, newColl.DBID, got.DBID)
	require.Equal(t, newColl.DBName, got.DBName)
	require.Equal(t, newColl.CollectionID, got.CollectionID)
	require.Equal(t, newColl.Name, got.Name)
	require.Equal(t, newColl.Description, got.Description)
	require.Equal(t, newColl.AutoID, got.AutoID)
	require.Equal(t, newColl.ConsistencyLevel, got.ConsistencyLevel)
	require.Equal(t, newColl.CreateTime, got.CreateTime)
	require.Equal(t, newColl.UpdateTimestamp, got.UpdateTimestamp)
	require.Equal(t, newColl.SchemaVersion, got.SchemaVersion)
	require.Equal(t, newColl.State, got.State)
	require.Equal(t, newColl.Properties, got.Properties)
	require.Equal(t, newColl.VirtualChannelNames, got.VirtualChannelNames)
	require.Equal(t, newColl.PhysicalChannelNames, got.PhysicalChannelNames)
	require.Equal(t, newColl.ShardsNum, got.ShardsNum)
	require.Equal(t, newColl.Aliases, got.Aliases)
	require.Equal(t, newColl.EnableDynamicField, got.EnableDynamicField)
	require.Equal(t, newColl.EnableNamespace, got.EnableNamespace)
	require.Equal(t, newColl.FileResourceIds, got.FileResourceIds)
	require.Equal(t, newColl.ExternalSource, got.ExternalSource)
	require.Equal(t, newColl.ExternalSpec, got.ExternalSpec)
}

type partitionFilterData struct {
	metastore.DataCoordCatalog
	segments []*datapb.SegmentInfo
}

func (d *partitionFilterData) ListSegments(ctx context.Context, collectionID int64) ([]*datapb.SegmentInfo, error) {
	return d.segments, nil
}

func TestListSegmentsFiltersByPartitionID(t *testing.T) {
	data := &partitionFilterData{
		segments: []*datapb.SegmentInfo{
			{ID: 1, CollectionID: 100, PartitionID: 10},
			{ID: 2, CollectionID: 100, PartitionID: 20},
			{ID: 3, CollectionID: 100, PartitionID: 10},
			{ID: 4, CollectionID: 100, PartitionID: 30},
		},
	}
	c := Wrap(Catalogs{DataCoord: data})

	got, err := c.MetadataInternal().Segments().List(
		context.Background(),
		ListSegmentsRequest{CollectionID: 100, PartitionID: 10},
		catalog.ReadOptions{},
	)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, int64(1), got[0].GetID())
	require.Equal(t, int64(3), got[1].GetID())

	got, err = c.MetadataInternal().Segments().List(
		context.Background(),
		ListSegmentsRequest{CollectionID: 100, PartitionID: 10, SegmentIDs: []int64{3, 4}},
		catalog.ReadOptions{},
	)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, int64(3), got[0].GetID())
}
