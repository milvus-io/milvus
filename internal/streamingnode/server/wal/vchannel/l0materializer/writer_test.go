package l0materializer

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func deleteEntry(tt uint64, partition int64, ids *schemapb.IDs) *streamingpb.TransformLogEntry {
	return &streamingpb.TransformLogEntry{TimeTick: tt, Entry: &streamingpb.TransformLogEntry_Delete{Delete: &streamingpb.TransformDeleteEntry{
		Blocks: []*streamingpb.TransformDeleteBlock{{PartitionId: partition, PrimaryKeys: ids}},
	}}}
}

func TestWriterSplitsOutputWithoutSplittingCursorCommit(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	req := MaterializeRequest{VChannel: "p1_1v0", TargetTimeTick: 300, MaxRows: 1, Entries: []*streamingpb.TransformLogEntry{
		deleteEntry(100, 10, &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}}),
		deleteEntry(200, 20, &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", "b"}}}}),
		deleteEntry(300, 20, &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}}),
	}}
	groups := splitMaterializeGroups(req)
	require.Len(t, groups, 4)
	for _, g := range groups {
		require.Len(t, g.pks, 1)
		require.Equal(t, []uint64{g.fromTimeTick}, g.timestamps)
	}
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	writer := NewSyncMaterializer(cm, allocator.NewLocalAllocator(1, 100), syncmgr.BrokerMetaWriter(nil, 1))
	calls := 0
	fail := true
	patch := mockey.Mock((*syncmgr.SyncTask).Run).To(func(task *syncmgr.SyncTask, _ context.Context) error {
		require.Positive(t, task.SegmentID())
		calls++
		if fail && calls == 2 {
			return context.DeadlineExceeded
		}
		return nil
	}).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, writer.Materialize(ctx, req), context.DeadlineExceeded)
	require.Equal(t, 2, calls, "failure must stop a partial output batch")
	fail = false
	require.NoError(t, writer.Materialize(ctx, req))
	require.Equal(t, 6, calls, "retry registers every required output before allowing cursor advancement")
	writer.allocator = allocator.NewLocalAllocator(1, 1)
	require.Error(t, writer.Materialize(ctx, req))
}

func TestWriterByteCapsAndPKRepresentation(t *testing.T) {
	ints := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}}
	strings := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", "b"}}}}
	req := MaterializeRequest{Entries: []*streamingpb.TransformLogEntry{deleteEntry(100, 1, ints), deleteEntry(200, 1, strings)}}
	groups := splitMaterializeGroups(req)
	require.Len(t, groups, 2, "PK representations require separate output schemas")
	require.Equal(t, schemapb.DataType_Int64, groups[0].pkType)
	require.Equal(t, schemapb.DataType_VarChar, groups[1].pkType)
	require.Equal(t, []uint64{100, 100}, groups[0].timestamps)
	req.MaxBytes = 1
	require.Len(t, splitMaterializeGroups(req), 4, "one oversized key is allowed, subsequent keys start another output")
}

func TestWriterRejectsMissingDependencies(t *testing.T) {
	ctx := context.Background()
	writer := &SyncMaterializer{}
	require.NoError(t, writer.Materialize(ctx, MaterializeRequest{}))
	req := MaterializeRequest{Entries: []*streamingpb.TransformLogEntry{deleteEntry(100, 1, nil)}}
	require.ErrorContains(t, writer.Materialize(ctx, req), "chunk manager")
	writer.chunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	require.ErrorContains(t, writer.Materialize(ctx, req), "id allocator")
	writer.allocator = allocator.NewLocalAllocator(1, 100)
	require.ErrorContains(t, writer.Materialize(ctx, req), "meta writer")
	writer.metaWriter = syncmgr.BrokerMetaWriter(nil, 1)
	require.ErrorContains(t, writer.Materialize(ctx, req), "invalid vchannel")
}
