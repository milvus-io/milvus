package l0materializer

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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
	req.DeletePositions = map[uint64]*msgpb.MsgPosition{
		100: utility.NewMessagePosition(walDelete(100), req.VChannel),
		200: utility.NewMessagePosition(walDelete(200), req.VChannel),
	}
	groups := splitMaterializeGroups(req)
	require.Len(t, groups, 2)
	for _, g := range groups {
		require.Len(t, g.pks, 2)
		require.Equal(t, []uint64{g.fromTimeTick, g.fromTimeTick}, g.timestamps)
	}
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	writer := NewSyncMaterializer(cm, allocator.NewLocalAllocator(1, 100), syncmgr.BrokerMetaWriter(nil, 1))
	calls := 0
	fail := true
	patch := mockey.Mock((*syncmgr.SyncTask).Run).To(func(task *syncmgr.SyncTask, _ context.Context) error {
		require.Positive(t, task.SegmentID())
		require.True(t, proto.Equal(req.DeletePositions[task.StartPosition().GetTimestamp()], task.StartPosition()), "each split output uses its own first Delete position")
		require.True(t, proto.Equal(req.DeletePositions[task.Checkpoint().GetTimestamp()], task.Checkpoint()), "each output ends at its own last Delete position")
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
	require.Equal(t, 4, calls, "retry registers every required output before allowing cursor advancement")
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
	require.Len(t, splitMaterializeGroups(req), 2, "a TimeTick stays whole even when its keys exceed the byte limit")
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

func TestWriterTransactionBlocksShareByteBudget(t *testing.T) {
	ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}
	entry := deleteEntry(100, 10, ids)
	entry.GetDelete().Blocks = append(entry.GetDelete().Blocks,
		&streamingpb.TransformDeleteBlock{PartitionId: 10, PrimaryKeys: ids})
	req := MaterializeRequest{Entries: []*streamingpb.TransformLogEntry{entry}, MaxRows: 100, MaxBytes: uint64(proto.Size(entry))}
	groups := splitMaterializeGroups(req)
	require.Len(t, groups, 1, "one transaction fits the byte budget")
	require.Len(t, groups[0].pks, 2)
	require.Equal(t, []uint64{100, 100}, groups[0].timestamps)
	req.MaxBytes = uint64(proto.Size(entry.GetDelete().Blocks[0]))
	require.Len(t, splitMaterializeGroups(req), 1, "all blocks of one partition/TimeTick stay whole beyond the soft byte limit")
}

func TestWriterKeepsPartitionTimeTickWhole(t *testing.T) {
	ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}
	entry := deleteEntry(100, 10, ids)
	entry.GetDelete().Blocks = append(entry.GetDelete().Blocks,
		&streamingpb.TransformDeleteBlock{PartitionId: 20, PrimaryKeys: ids},
		&streamingpb.TransformDeleteBlock{PartitionId: 10, PrimaryKeys: ids})
	for _, tc := range []struct {
		name  string
		rows  uint64
		bytes uint64
	}{
		{name: "row limit", rows: 2},
		{name: "byte limit", bytes: uint64(proto.Size(entry.GetDelete().Blocks[0])) * 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			groups := splitMaterializeGroups(MaterializeRequest{
				Entries: []*streamingpb.TransformLogEntry{deleteEntry(50, 10, ids), entry, deleteEntry(200, 10, ids)},
				MaxRows: tc.rows, MaxBytes: tc.bytes,
			})
			var partitionGroups []materializeGroup
			for _, group := range groups {
				if group.partitionID == 10 {
					partitionGroups = append(partitionGroups, group)
				} else {
					require.EqualValues(t, 20, group.partitionID)
					require.Equal(t, []uint64{100}, group.timestamps)
				}
			}
			require.Len(t, partitionGroups, 3)
			require.Equal(t, []uint64{50}, partitionGroups[0].timestamps)
			require.Equal(t, []uint64{100, 100}, partitionGroups[1].timestamps, "preflight all blocks before admitting this TimeTick")
			require.Equal(t, []uint64{200}, partitionGroups[2].timestamps)
		})
	}
	groups := splitMaterializeGroups(MaterializeRequest{
		Entries: []*streamingpb.TransformLogEntry{deleteEntry(100, 10, ids), deleteEntry(100, 10, ids), deleteEntry(200, 10, ids)},
		MaxRows: 1,
	})
	require.Len(t, groups, 2)
	require.Equal(t, []uint64{100, 100}, groups[0].timestamps, "equal-TimeTick entries also remain indivisible")
	require.Equal(t, []uint64{200}, groups[1].timestamps)
}

func TestWriterPublishesEachL0OwnLastDeletePosition(t *testing.T) {
	paramtable.Init()
	ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}
	for _, walPositions := range []bool{true, false} {
		t.Run(map[bool]string{true: "WAL", false: "Summary"}[walPositions], func(t *testing.T) {
			req := MaterializeRequest{VChannel: "p1_1v0", TargetTimeTick: 200, MaxRows: 2, Entries: []*streamingpb.TransformLogEntry{
				deleteEntry(100, 10, ids), deleteEntry(120, 10, ids), deleteEntry(150, 10, ids),
			}}
			if walPositions {
				req.DeletePositions = make(map[uint64]*msgpb.MsgPosition)
				for _, entry := range req.Entries {
					req.DeletePositions[entry.GetTimeTick()] = utility.NewMessagePosition(walDelete(entry.GetTimeTick()), req.VChannel)
				}
			}
			b := broker.NewCoordBroker(nil, 1)
			var registrations []*datapb.SaveBinlogPathsRequest
			save := mockey.Mock(mockey.GetMethod(b, "SaveBinlogPaths")).To(func(_ context.Context, registered *datapb.SaveBinlogPathsRequest) error {
				registrations = append(registrations, proto.Clone(registered).(*datapb.SaveBinlogPathsRequest))
				return nil
			}).Build()
			defer save.UnPatch()
			writer := NewSyncMaterializer(storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())), allocator.NewLocalAllocator(1, 10000), syncmgr.BrokerMetaWriter(b, 1))
			require.NoError(t, writer.Materialize(t.Context(), req))
			require.Len(t, registrations, 2)
			for i, expected := range []struct{ from, to uint64 }{{100, 120}, {150, 150}} {
				registered := registrations[i]
				require.Len(t, registered.GetCheckPoints(), 1)
				require.Len(t, registered.GetStartPositions(), 1)
				start := registered.GetStartPositions()[0].GetStartPosition()
				end := registered.GetCheckPoints()[0].GetPosition()
				require.Equal(t, expected.from, start.GetTimestamp())
				require.Equal(t, expected.to, end.GetTimestamp(), "batch boundary 200 must not become a Segment DmlPosition")
				if walPositions {
					require.True(t, proto.Equal(req.DeletePositions[expected.from], start))
					require.True(t, proto.Equal(req.DeletePositions[expected.to], end))
				}
			}
		})
	}
}
