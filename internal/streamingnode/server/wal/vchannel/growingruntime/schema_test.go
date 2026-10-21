package growingruntime

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestAlterSchemaPreservesSegmentEncodingAndUpdatesCollection(t *testing.T) {
	initSegcoreForRuntimeTest(t)
	schema := mock_segcore.GenTestCollectionSchema("schema-handoff", schemapb.DataType_Int64, false)
	c, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{CollectionID: 1, Schema: schema})
	require.NoError(t, err)
	r := newRuntime()
	r.collection = c
	defer r.Close()
	oldSegment := r.getOrCreateSegment(100, 1)
	require.NoError(t, oldSegment.ensureCSegment(50))
	updated := proto.Clone(schema).(*schemapb.CollectionSchema)
	updated.Version = 1
	updated.Fields = append(updated.Fields, &schemapb.FieldSchema{FieldID: 200, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true})
	msg := message.NewAlterCollectionMessageBuilderV2().WithVChannel("p_1v0").WithHeader(&messagespb.AlterCollectionMessageHeader{CollectionId: 1, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}}}).WithBody(&messagespb.AlterCollectionMessageBody{Updates: &messagespb.AlterCollectionMessageUpdates{Schema: updated}}).MustBuildMutable().WithTimeTick(100).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(100))
	// The collection schema-cache/native update implementation is a separate
	// prerequisite. Verify the caller contract without importing that implementation.
	mockey.PatchConvey("ALTER forwards the schema and keeps existing segment encoding", t, func() {
		update := mockey.Mock((*segcore.CCollection).UpdateSchema).To(func(collection *segcore.CCollection, next *schemapb.CollectionSchema, tick uint64) error {
			require.Same(t, c, collection)
			require.True(t, proto.Equal(updated, next))
			require.EqualValues(t, 100, tick)
			return nil
		}).Build()
		r.ApplyLiveEvent(t.Context(), walview.VChannelResourceEvent{Message: msg})
		require.Equal(t, 1, update.Times())
		require.EqualValues(t, 0, oldSegment.encodingSchema.GetVersion())
		require.True(t, proto.Equal(schema, oldSegment.collection.Schema()), "ALTER must preserve the segment encoding schema")
		require.Equal(t, uint64(100), r.AppliedGrowingTimeTick())
	})
}

func TestRecoveredGrowingSegmentUsesItsOwnSchema(t *testing.T) {
	initSegcoreForRuntimeTest(t)
	original := mock_segcore.GenTestCollectionSchema("recovered-encoding", schemapb.DataType_Int64, false)
	latest := proto.Clone(original).(*schemapb.CollectionSchema)
	latest.Version = 2
	latest.Fields = append(latest.Fields, &schemapb.FieldSchema{FieldID: 200, Name: "later", DataType: schemapb.DataType_Int64, Nullable: true})
	collection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{CollectionID: 1, Schema: latest})
	require.NoError(t, err)
	defer collection.Release()
	segment, err := newGrowingSegmentFromVisible(t.Context(), collection, walview.VisibleSegment{SegmentID: 10, PartitionID: 1, Schema: original})
	require.NoError(t, err)
	defer segment.release()
	require.NoError(t, segment.ensureCSegment(50))
	require.True(t, proto.Equal(original, segment.collection.Schema()))
	require.NotSame(t, collection, segment.collection)
}
