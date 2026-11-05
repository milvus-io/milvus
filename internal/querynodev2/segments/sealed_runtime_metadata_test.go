package segments

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestSealedRuntimeMetadataOutlivesPreparingCollection(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	collection := &Collection{dbName: "db", resourceGroup: "rg"}
	collection.setSchema(schema, 1, 0, 1)
	local := &LocalSegment{baseSegment: newTestBaseSegment(10, 1)}
	local.collection = collection
	require.Error(t, local.DetachPreparingCollection())
	require.Same(t, collection, local.GetCollection(), "failed finalization must keep its owner for cleanup")
	usage := ResourceUsage{}
	local.resourceUsageCache.Store(&usage)
	require.NoError(t, local.DetachPreparingCollection())
	require.Nil(t, local.GetCollection())
	require.Equal(t, "db", local.DatabaseName())
	require.Equal(t, "rg", local.ResourceGroup())
	require.Equal(t, usage, local.ResourceUsageEstimate())
	// No collection manager or native collection is needed to consume later
	// historical deletes. Changing the old protobuf cannot affect PK decoding.
	schema.Fields[0].DataType = schemapb.DataType_VarChar
	require.Equal(t, schemapb.DataType_Int64, local.sealedMetadata.delta.primaryKeyType)
	called := false
	mockey.PatchConvey("load deletes after detaching the preparing owner", t, func() {
		mockey.Mock((*LocalSegment).LoadDeltaData).To(func(_ *LocalSegment, _ context.Context, data *storage.DeltaData) error {
			called = true
			require.Zero(t, data.DeleteRowCount())
			return nil
		}).Build()
		require.NoError(t, (&segmentLoader{}).loadDeltalogs(context.Background(), local, &querypb.SegmentLoadInfo{}))
	})
	require.True(t, called)
}
