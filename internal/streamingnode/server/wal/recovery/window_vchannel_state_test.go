package recovery

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func writeTestBootstrapPChannelWindowMeta(
	t require.TestingT,
	ctx context.Context,
	pchannel string,
	chunkManager storage.ChunkManager,
	checkpoint *utility.WALCheckpoint,
) *streamingpb.PChannelWindowMeta {
	if helper, ok := t.(interface{ Helper() }); ok {
		helper.Helper()
	}
	payload, footer, _, err := marshalPChannelWindowChunk(pchannel, 0, checkpoint, nil)
	require.NoError(t, err)
	key := buildPChannelWindowChunkKey(pchannel, footer.Generation)
	require.NoError(t, chunkManager.Write(ctx, key, payload))
	return newPChannelWindowStoreMetaFromChunk(pchannel, footer, 0, 0).intoCatalogMeta()
}

func writeTestPChannelWindowChunk(
	t require.TestingT,
	ctx context.Context,
	pchannel string,
	generation uint64,
	chunkManager storage.ChunkManager,
	checkpoint *utility.WALCheckpoint,
	records map[string][]committedWriteRecord,
) (*pchannelWindowChunkFooter, string, string) {
	if helper, ok := t.(interface{ Helper() }); ok {
		helper.Helper()
	}
	payload, footer, checksum, err := marshalPChannelWindowChunk(pchannel, generation, checkpoint, records)
	require.NoError(t, err)
	key := buildPChannelWindowChunkKey(pchannel, generation)
	require.NoError(t, chunkManager.Write(ctx, key, payload))
	return footer, key, checksum
}

func recoverTestIdempotencyWindows(t require.TestingT, ctx context.Context, rs *recoveryStorageImpl, pchannel string, allowBootstrap bool) {
	if helper, ok := t.(interface{ Helper() }); ok {
		helper.Helper()
	}
	require.NoError(t, recoverTestIdempotencyWindowsWithError(ctx, rs, pchannel, allowBootstrap))
}

func recoverTestIdempotencyWindowsWithError(ctx context.Context, rs *recoveryStorageImpl, pchannel string, allowBootstrap bool) error {
	info, err := rs.windowManager.loadWindowInfoFromMeta(ctx, pchannel, allowBootstrap, rs.checkpoint)
	if err != nil {
		return err
	}
	rs.windowManager.initializeIdempotencyWindowsFromMeta(rs.vchannels, info.storeMeta.SourceCheckpoint, info.windowMetas)
	rewound, err := rs.windowManager.recoverWindowStoreFromSnapshot(ctx, info, rs.checkpoint, rs.vchannels)
	if err != nil {
		return err
	}
	// Mirror RecoverRecoveryStorage: apply the (possibly rewound) checkpoint.
	rs.checkpoint = rewound
	return nil
}

type testPChannelWindowCatalogState struct {
	windowMetas map[string]*streamingpb.VChannelWindowMeta
	storeMeta   *streamingpb.PChannelWindowMeta
	operations  []string
}

func newTestPChannelWindowCatalog(t *testing.T) (*mock_metastore.MockStreamingNodeCataLog, *testPChannelWindowCatalogState) {
	params := paramtable.Get()
	params.Save(params.MinioCfg.RootPath.Key, t.TempDir())
	t.Cleanup(func() {
		params.Reset(params.MinioCfg.RootPath.Key)
	})
	state := &testPChannelWindowCatalogState{
		windowMetas: make(map[string]*streamingpb.VChannelWindowMeta),
	}
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().ListVChannelWindowMetas(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, viewType string) ([]*streamingpb.VChannelWindowMeta, error) {
		values := make([]*streamingpb.VChannelWindowMeta, 0, len(state.windowMetas))
		for _, meta := range state.windowMetas {
			values = append(values, proto.Clone(meta).(*streamingpb.VChannelWindowMeta))
		}
		return values, nil
	}).Maybe()
	catalog.EXPECT().SaveVChannelWindowMetas(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, viewType string, saved map[string]*streamingpb.VChannelWindowMeta) error {
		state.operations = append(state.operations, "vchannel-window-meta")
		for key, meta := range saved {
			state.windowMetas[key] = proto.Clone(meta).(*streamingpb.VChannelWindowMeta)
		}
		return nil
	}).Maybe()
	catalog.EXPECT().GetPChannelWindowMeta(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string) (*streamingpb.PChannelWindowMeta, error) {
		if state.storeMeta == nil {
			return nil, nil
		}
		return proto.Clone(state.storeMeta).(*streamingpb.PChannelWindowMeta), nil
	}).Maybe()
	catalog.EXPECT().SavePChannelWindowMeta(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, meta *streamingpb.PChannelWindowMeta) error {
		state.operations = append(state.operations, "pchannel-window-meta")
		state.storeMeta = proto.Clone(meta).(*streamingpb.PChannelWindowMeta)
		return nil
	}).Maybe()
	catalog.EXPECT().RemoveVChannelWindowMetas(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, viewType string, vchannels []string) error {
		state.operations = append(state.operations, "remove-vchannel-window-metas")
		for _, vchannel := range vchannels {
			delete(state.windowMetas, vchannel)
		}
		return nil
	}).Maybe()
	catalog.EXPECT().RemovePChannelWindowMeta(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string) error {
		state.operations = append(state.operations, "remove-pchannel-window-meta")
		state.storeMeta = nil
		return nil
	}).Maybe()
	return catalog, state
}

func TestPersistPChannelWindowRetriesTransientMetaLoad(t *testing.T) {
	ctx := context.Background()
	params := paramtable.Get()
	params.Save(params.MinioCfg.RootPath.Key, t.TempDir())
	t.Cleanup(func() { params.Reset(params.MinioCfg.RootPath.Key) })

	state := &testPChannelWindowCatalogState{windowMetas: make(map[string]*streamingpb.VChannelWindowMeta)}
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().ListVChannelWindowMetas(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, viewType string) ([]*streamingpb.VChannelWindowMeta, error) {
		return nil, nil
	}).Maybe()
	catalog.EXPECT().SaveVChannelWindowMetas(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, viewType string, saved map[string]*streamingpb.VChannelWindowMeta) error {
		for key, meta := range saved {
			state.windowMetas[key] = proto.Clone(meta).(*streamingpb.VChannelWindowMeta)
		}
		return nil
	}).Maybe()
	catalog.EXPECT().SavePChannelWindowMeta(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, meta *streamingpb.PChannelWindowMeta) error {
		state.storeMeta = proto.Clone(meta).(*streamingpb.PChannelWindowMeta)
		return nil
	}).Maybe()
	// GetPChannelWindowMeta fails on its first call -- a transient etcd blip on the
	// one persist-path call that was not wrapped in retry -- then succeeds.
	getCalls := 0
	catalog.EXPECT().GetPChannelWindowMeta(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string) (*streamingpb.PChannelWindowMeta, error) {
		getCalls++
		if getCalls == 1 {
			return nil, errors.New("transient etcd error")
		}
		if state.storeMeta == nil {
			return nil, nil
		}
		return proto.Clone(state.storeMeta).(*streamingpb.PChannelWindowMeta), nil
	}).Maybe()

	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 1})
	rs.SetLogger(resource.Resource().Logger())
	rs.windowManager.SetLogger(resource.Resource().Logger())

	records := map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:                    "key-1",
				CommitTimetick:         99,
				MessageId:              rmq.NewRmqID(99).IntoProto(),
				LastConfirmedMessageId: rmq.NewRmqID(98).IntoProto(),
			}),
		},
	}
	sourceCheckpoint := &utility.WALCheckpoint{MessageID: rmq.NewRmqID(120), TimeTick: 120}

	// A transient meta-load error must be retried internally, not propagated: a
	// propagated error kills the window background task and stalls idempotency
	// durability (the windows then grow unbounded until OOM).
	_, _, err := rs.windowManager.persistPChannelWindow(ctx, resource.Resource().Logger(), records, nil, sourceCheckpoint)
	require.NoError(t, err)
	require.GreaterOrEqual(t, getCalls, 2, "transient GetPChannelWindowMeta error should have been retried")
	require.NotNil(t, state.storeMeta)
}

func TestWindowSnapshotSerdeRoundTrip(t *testing.T) {
	snapshot := &streamingpb.WindowSnapshot{
		Pchannel:                   "p1",
		Vchannel:                   "v1",
		SnapshotCheckpointTimetick: 100,
		EvictedWatermarkTimetick:   90,
		SnapshotCheckpointMessageId: &commonpb.MessageID{
			WALName: commonpb.WALName_Test,
			Id:      "100",
		},
		Entries: []*streamingpb.WindowEntry{
			{
				Key:            "key-2",
				CommitTimetick: 95,
				MessageId:      &commonpb.MessageID{WALName: commonpb.WALName_Test, Id: "95"},
				IdempotentResult: &messagespb.IdempotentInsertResult{
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{11}}},
					},
				},
			},
			{
				Key:            "key-1",
				CommitTimetick: 91,
				MessageId:      &commonpb.MessageID{WALName: commonpb.WALName_Test, Id: "91"},
				IdempotentResult: &messagespb.IdempotentInsertResult{
					RowOffsets: []uint32{2, 0},
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{12, 10}}},
					},
				},
			},
		},
	}

	payload, err := proto.Marshal(snapshot)
	require.NoError(t, err)

	decoded := &streamingpb.WindowSnapshot{}
	require.NoError(t, proto.Unmarshal(payload, decoded))
	require.Equal(t, snapshot.GetPchannel(), decoded.GetPchannel())
	require.Equal(t, snapshot.GetVchannel(), decoded.GetVchannel())
	require.Equal(t, snapshot.GetSnapshotCheckpointTimetick(), decoded.GetSnapshotCheckpointTimetick())
	require.Len(t, decoded.GetEntries(), 2)
}

func TestDIDWindowRecoveryStateFromSnapshot(t *testing.T) {
	snapshot := &streamingpb.WindowSnapshot{
		Pchannel:                   "p1",
		Vchannel:                   "v1",
		SnapshotCheckpointTimetick: 100,
		Entries: []*streamingpb.WindowEntry{
			{Key: "key-2", CommitTimetick: 95},
			{Key: "key-1", CommitTimetick: 90},
		},
	}

	state, err := newVChannelWindowFromSnapshot(snapshot)
	require.NoError(t, err)
	roundTrip := state.snapshot()
	require.Equal(t, uint64(100), roundTrip.GetSnapshotCheckpointTimetick())
	require.Equal(t, uint64(90), roundTrip.GetEvictedWatermarkTimetick())
	require.Equal(t, "key-1", roundTrip.GetEntries()[0].GetKey())
	require.Equal(t, "key-2", roundTrip.GetEntries()[1].GetKey())
}

func TestCommittedWriteRecordFromMessageWithIdempotency(t *testing.T) {
	extra := &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{2, 0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{11, 10}}},
		},
	}
	msg := newTestIdempotentInsertMessage(t, "v1", "key-1", extra).
		WithTimeTick(120).
		WithLastConfirmed(rmq.NewRmqID(119)).
		IntoImmutableMessage(rmq.NewRmqID(120))

	record, ok := newCommittedWriteRecordFromMessage("p1", msg)
	require.True(t, ok)
	require.Equal(t, "p1", record.SourcePChannel)
	require.Equal(t, "v1", record.VChannel)
	require.Equal(t, uint64(120), record.SourceTimeTick)
	require.True(t, message.MustUnmarshalMessageID(record.SourceMessageID).EQ(rmq.NewRmqID(120)))
	require.True(t, message.MustUnmarshalMessageID(record.LastConfirmedMessageID).EQ(rmq.NewRmqID(119)))
	require.NotNil(t, record.Idempotency)
	require.Equal(t, "key-1", record.Idempotency.Key)
	require.Len(t, record.Rows, 2)
	require.Equal(t, uint32(2), record.Rows[0].RowOffset)
	require.Equal(t, committedWritePrimaryKeyTypeInt64, record.Rows[0].PrimaryKeyType)
	require.Equal(t, int64(11), record.Rows[0].Int64PrimaryKeyValue)
	require.Equal(t, uint32(0), record.Rows[1].RowOffset)
	require.Equal(t, int64(10), record.Rows[1].Int64PrimaryKeyValue)

	entry := record.WindowEntry()
	require.NotNil(t, entry)
	require.Equal(t, "key-1", entry.GetKey())
	require.Equal(t, []uint32{2, 0}, entry.GetIdempotentResult().GetRowOffsets())
	require.Equal(t, []int64{11, 10}, entry.GetIdempotentResult().GetIds().GetIntId().GetData())
}

func TestCommittedWriteRecordFromTxnMessageWithIdempotency(t *testing.T) {
	txnCtx := message.TxnContext{TxnID: 1, Keepalive: 10}
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(100).
		WithLastConfirmed(rmq.NewRmqID(99)).
		IntoImmutableMessage(rmq.NewRmqID(100))
	beginMsg, err := message.AsImmutableBeginTxnMessageV2(begin)
	require.NoError(t, err)

	body1 := newTestIdempotentInsertMessage(t, "v1", "", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"pk-0"}}},
		},
	}).WithTxnContext(txnCtx).WithTimeTick(101).IntoImmutableMessage(rmq.NewRmqID(101))
	body2 := newTestIdempotentInsertMessage(t, "v1", "", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{2, 1},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"pk-2", "pk-1"}}},
		},
	}).WithTxnContext(txnCtx).WithTimeTick(102).IntoImmutableMessage(rmq.NewRmqID(102))
	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{IdempotencyKey: "txn-key"}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(103).
		WithLastConfirmed(rmq.NewRmqID(102)).
		IntoImmutableMessage(rmq.NewRmqID(103))
	commitMsg, err := message.AsImmutableCommitTxnMessageV2(commit)
	require.NoError(t, err)

	txnMsg, err := message.NewImmutableTxnMessageBuilder(beginMsg).Add(body1).Add(body2).Build(commitMsg)
	require.NoError(t, err)
	record, ok := newCommittedWriteRecordFromMessage("p1", txnMsg)
	require.True(t, ok)
	require.Equal(t, "txn-key", record.Idempotency.Key)
	require.Equal(t, []uint32{0, 2, 1}, record.DuplicateResponse.GetRowOffsets())
	require.Equal(t, []string{"pk-0", "pk-2", "pk-1"}, record.DuplicateResponse.GetIds().GetStrId().GetData())
	require.True(t, message.MustUnmarshalMessageID(record.SourceMessageID).EQ(rmq.NewRmqID(103)))
}

// testReplicateHeader marks a message as replicated from another cluster.
func testReplicateHeader(msgID int64) *message.ReplicateHeader {
	return &message.ReplicateHeader{
		ClusterID:              "source-cluster",
		MessageID:              rmq.NewRmqID(msgID),
		LastConfirmedMessageID: rmq.NewRmqID(msgID - 1),
		TimeTick:               uint64(msgID),
		VChannel:               "v1",
	}
}

func TestVChannelWindowSkipsReplicatedIdempotencyKey(t *testing.T) {
	state := newEmptyVChannelWindow("p1", "v1", testRecoveryCheckpoint(1, 1))

	// A native keyed insert materializes a window entry.
	state.observeMessage(newTestIdempotentCommittedInsertMessage(t, "v1", "native-key", 10))
	require.Contains(t, state.entries, "native-key")

	// A replicated insert preserves the SOURCE cluster's key AND insert result;
	// it must be recorded as a keyless committed write: no window entry, while
	// checkpoint bookkeeping still advances.
	replicated := newTestIdempotentInsertMessage(t, "v1", "replicated-key", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{100}}},
		},
	}).
		WithReplicateHeader(testReplicateHeader(5)).
		WithTimeTick(20).
		WithLastConfirmed(rmq.NewRmqID(19)).
		IntoImmutableMessage(rmq.NewRmqID(20))
	state.observeMessage(replicated)

	require.NotContains(t, state.entries, "replicated-key")
	require.Len(t, state.entries, 1)
	require.Equal(t, uint64(20), state.snapshotCheckpointTimetick)
	// The keyless record is still staged for persistence, so a restart replays
	// it without materializing an entry either. The source cluster's insert
	// result must not tag along: it could never be served as a duplicate
	// response, so persisting its rows would be pure write amplification.
	require.NotEmpty(t, state.pendingRecords)
	last := state.pendingRecords[len(state.pendingRecords)-1]
	require.Nil(t, last.Idempotency)
	require.Empty(t, last.Rows)
	require.Nil(t, last.DuplicateResponse)
}

func TestCommittedWriteRecordSkipsReplicatedTxnCommitKey(t *testing.T) {
	txnCtx := message.TxnContext{TxnID: 1, Keepalive: 10}
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(100).
		WithLastConfirmed(rmq.NewRmqID(99)).
		IntoImmutableMessage(rmq.NewRmqID(100))
	beginMsg, err := message.AsImmutableBeginTxnMessageV2(begin)
	require.NoError(t, err)

	// The replicated body carries the SOURCE cluster's insert result in its
	// header, just like the commit carries the source's key.
	body := newTestIdempotentInsertMessage(t, "v1", "", &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{100}}},
		},
	}).
		WithReplicateHeader(testReplicateHeader(101)).
		WithTxnContext(txnCtx).WithTimeTick(101).IntoImmutableMessage(rmq.NewRmqID(101))
	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{IdempotencyKey: "txn-key"}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithReplicateHeader(testReplicateHeader(102)).
		WithTimeTick(103).
		WithLastConfirmed(rmq.NewRmqID(102)).
		IntoImmutableMessage(rmq.NewRmqID(103))
	commitMsg, err := message.AsImmutableCommitTxnMessageV2(commit)
	require.NoError(t, err)

	txnMsg, err := message.NewImmutableTxnMessageBuilder(beginMsg).Add(body).Build(commitMsg)
	require.NoError(t, err)

	// Neither the replicated commit's key nor the replicated bodies' insert
	// results may surface on the committed-write record.
	record, ok := newCommittedWriteRecordFromMessage("p1", txnMsg)
	require.True(t, ok)
	require.Nil(t, record.Idempotency)
	require.Empty(t, record.Rows)
	require.Nil(t, record.DuplicateResponse)
}

func TestCommittedWriteRecordWithoutDIDDoesNotEnterDIDWindow(t *testing.T) {
	extra := &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{1},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"pk-1"}}},
		},
	}
	msg := newTestIdempotentInsertMessage(t, "v1", "", extra).
		WithTimeTick(121).
		WithLastConfirmed(rmq.NewRmqID(120)).
		IntoImmutableMessage(rmq.NewRmqID(121))

	record, ok := newCommittedWriteRecordFromMessage("p1", msg)
	require.True(t, ok)
	require.Nil(t, record.Idempotency)
	require.Len(t, record.Rows, 1)
	require.Equal(t, committedWritePrimaryKeyTypeString, record.Rows[0].PrimaryKeyType)
	require.Equal(t, "pk-1", record.Rows[0].StringPrimaryKeyValue)
	require.Nil(t, record.WindowEntry())
}

func TestRecoveryStorageRegistersRuntimeVChannelForIdempotencyWindow(t *testing.T) {
	resource.InitForTest(t)
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())
	rs.vchannels = make(map[string]*vchannelRecoveryInfo)
	rs.segments = make(map[int64]*segmentRecoveryInfo)
	rs.windowManager.resetIdempotencyWindows()

	msg := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 100,
			PartitionIds: []int64{10},
		}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		MustBuildMutable().
		WithTimeTick(10).
		WithLastConfirmed(rmq.NewRmqID(9)).
		IntoImmutableMessage(rmq.NewRmqID(10))

	require.NoError(t, rs.ObserveMessage(context.Background(), msg))
	window := rs.windowManager.idempotencyWindows()["v1"]
	require.NotNil(t, window)
	require.False(t, window.dirty)
	require.Equal(t, uint64(10), window.snapshotCheckpointTimetick)
	require.Equal(t, uint64(10), rs.windowManager.getPChannelWindowSnapshotCheckpointUnsafe().TimeTick)
}

func TestConsumeDirtySnapshotDoesNotConsumeIdempotencyWindows(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(10, 10))
	window := newEmptyVChannelWindow("p1", "v1", testRecoveryCheckpoint(10, 10))
	require.NoError(t, window.applyCommittedWriteRecord(*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
		Key:            "key-1",
		CommitTimetick: 20,
		MessageId:      rmq.NewRmqID(20).IntoProto(),
	}), true))
	rs.windowManager.setIdempotencyWindows(map[string]*vchannelWindow{"v1": window})
	rs.dirtyCounter = 1

	snapshot := rs.consumeDirtySnapshot()
	require.NotNil(t, snapshot)
	require.Nil(t, snapshot.pchannelWindowSourceCheckpoint)
	require.True(t, window.dirty)
	require.Equal(t, 0, rs.dirtyCounter)
}

func TestConsumeIdempotencySnapshotDoesNotConsumeRecoveryState(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(10, 10))
	window := newEmptyVChannelWindow("p1", "v1", testRecoveryCheckpoint(10, 10))
	require.NoError(t, window.applyCommittedWriteRecord(*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
		Key:            "key-1",
		CommitTimetick: 20,
		MessageId:      rmq.NewRmqID(20).IntoProto(),
	}), true))
	rs.windowManager.setIdempotencyWindows(map[string]*vchannelWindow{"v1": window})
	rs.windowManager.advancePChannelWindowSnapshotCheckpoint(testRecoveryCheckpoint(20, 20))
	rs.dirtyCounter = 1

	snapshot := rs.windowManager.consumeIdempotencySnapshot()
	require.NotNil(t, snapshot)
	require.NotNil(t, snapshot.pchannelWindowSourceCheckpoint)
	require.Equal(t, uint64(20), snapshot.pchannelWindowSourceCheckpoint.TimeTick)
	require.False(t, window.dirty)
	require.Equal(t, 1, rs.dirtyCounter)
}

func TestCommittedWriteRecordFromWindowEntry(t *testing.T) {
	entry := &streamingpb.WindowEntry{
		Key:                    "key-1",
		CommitTimetick:         130,
		MessageId:              rmq.NewRmqID(130).IntoProto(),
		LastConfirmedMessageId: rmq.NewRmqID(129).IntoProto(),
		IdempotentResult: &messagespb.IdempotentInsertResult{
			RowOffsets: []uint32{3, 1},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"pk-3", "pk-1"}}},
			},
		},
	}

	record := committedWriteRecordFromWindowEntry("p1", "v1", entry)
	require.NotNil(t, record)
	require.Equal(t, "p1", record.SourcePChannel)
	require.Equal(t, "v1", record.VChannel)
	require.Equal(t, uint64(130), record.SourceTimeTick)
	require.NotNil(t, record.Idempotency)
	require.Equal(t, "key-1", record.Idempotency.Key)
	require.Len(t, record.Rows, 2)
	require.Equal(t, uint32(3), record.Rows[0].RowOffset)
	require.Equal(t, committedWritePrimaryKeyTypeString, record.Rows[0].PrimaryKeyType)
	require.Equal(t, "pk-3", record.Rows[0].StringPrimaryKeyValue)
	require.Equal(t, uint32(1), record.Rows[1].RowOffset)
	require.Equal(t, "pk-1", record.Rows[1].StringPrimaryKeyValue)

	roundTrip := record.WindowEntry()
	require.Equal(t, entry.GetKey(), roundTrip.GetKey())
	require.Equal(t, entry.GetCommitTimetick(), roundTrip.GetCommitTimetick())
	require.Equal(t, entry.GetIdempotentResult().GetRowOffsets(), roundTrip.GetIdempotentResult().GetRowOffsets())
	require.Equal(t, entry.GetIdempotentResult().GetIds().GetStrId().GetData(), roundTrip.GetIdempotentResult().GetIds().GetStrId().GetData())
}

func TestDIDWindowMaterializerApplyCommittedWriteRecords(t *testing.T) {
	state := newEmptyVChannelWindow("p1", "v1", nil)
	record := *committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
		Key:                    "key-1",
		CommitTimetick:         110,
		MessageId:              rmq.NewRmqID(110).IntoProto(),
		LastConfirmedMessageId: rmq.NewRmqID(109).IntoProto(),
		IdempotentResult: &messagespb.IdempotentInsertResult{
			RowOffsets: []uint32{0},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}},
			},
		},
	})
	noDIDRecord := committedWriteRecord{
		SourcePChannel:         "p1",
		SourceMessageID:        rmq.NewRmqID(112).IntoProto(),
		SourceTimeTick:         112,
		VChannel:               "v1",
		LastConfirmedMessageID: rmq.NewRmqID(111).IntoProto(),
		Rows: []committedWriteRow{
			{
				RowOffset:            0,
				PrimaryKeyType:       committedWritePrimaryKeyTypeInt64,
				Int64PrimaryKeyValue: 12,
			},
		},
	}

	require.NoError(t, state.applyCommittedWriteRecordsAtGeneration([]committedWriteRecord{noDIDRecord, record, record}, 0))
	require.False(t, state.dirty)
	require.Empty(t, state.pendingEntries)

	snapshot := state.snapshot()
	require.Len(t, snapshot.GetEntries(), 1)
	require.Equal(t, "key-1", snapshot.GetEntries()[0].GetKey())
	require.Equal(t, []int64{10}, snapshot.GetEntries()[0].GetIdempotentResult().GetIds().GetIntId().GetData())
	require.Equal(t, uint64(112), snapshot.GetSnapshotCheckpointTimetick())
	require.True(t, message.MustUnmarshalMessageID(snapshot.GetSnapshotCheckpointMessageId()).EQ(rmq.NewRmqID(111)))
}

func TestDIDWindowConsumesPendingCommittedWriteRecords(t *testing.T) {
	state := newEmptyVChannelWindow("p1", "v1", nil)
	didRecord := *committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
		Key:                    "key-1",
		CommitTimetick:         110,
		MessageId:              rmq.NewRmqID(110).IntoProto(),
		LastConfirmedMessageId: rmq.NewRmqID(109).IntoProto(),
	})
	noDIDRecord := committedWriteRecord{
		SourcePChannel:         "p1",
		SourceMessageID:        rmq.NewRmqID(111).IntoProto(),
		SourceTimeTick:         111,
		VChannel:               "v1",
		LastConfirmedMessageID: rmq.NewRmqID(110).IntoProto(),
	}

	require.NoError(t, state.applyCommittedWriteRecord(didRecord, true))
	require.NoError(t, state.applyCommittedWriteRecord(noDIDRecord, true))

	pending, metaUpdate := state.consumePendingCommittedWriteRecords()
	require.Len(t, pending, 2)
	require.NotNil(t, metaUpdate)
	require.Equal(t, "key-1", pending[0].Idempotency.Key)
	require.Nil(t, pending[1].Idempotency)
	require.False(t, state.dirty)
	require.Empty(t, state.pendingEntries)
	require.Empty(t, state.pendingRecords)
	pending, metaUpdate = state.consumePendingCommittedWriteRecords()
	require.Nil(t, pending)
	require.Nil(t, metaUpdate)
}

func TestDIDWindowCheckpointOnlyDoesNotForceMetaUpdate(t *testing.T) {
	state := newEmptyVChannelWindow("p1", "v1", &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(10),
		TimeTick:  10,
	})
	state.advanceCheckpoint(message.CreateTestTimeTickSyncMessage(t, 20, 20, rmq.NewRmqID(20)).IntoImmutableMessage(rmq.NewRmqID(20)))

	require.False(t, state.dirty)
	require.Equal(t, uint64(20), state.snapshotCheckpointTimetick)
	pending, metaUpdate := state.consumePendingCommittedWriteRecords()
	require.Nil(t, pending)
	require.Nil(t, metaUpdate)
}

func TestDIDWindowMaterializerApplyCommittedWriteRecordDuplicate(t *testing.T) {
	state := newEmptyVChannelWindow("p1", "v1", nil)
	first := *committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
		Key:            "key-1",
		CommitTimetick: 100,
		MessageId:      rmq.NewRmqID(100).IntoProto(),
	})
	duplicate := *committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
		Key:            "key-1",
		CommitTimetick: 101,
		MessageId:      rmq.NewRmqID(101).IntoProto(),
	})

	err := state.applyCommittedWriteRecordsAtGeneration([]committedWriteRecord{first, duplicate}, 0)
	require.NoError(t, err)

	snapshot := state.snapshot()
	require.Len(t, snapshot.GetEntries(), 1)
	require.Equal(t, uint64(101), snapshot.GetSnapshotCheckpointTimetick())
}

func TestPChannelWindowChunkCodecRoundTrip(t *testing.T) {
	records := map[string][]committedWriteRecord{
		"v2": {
			*committedWriteRecordFromWindowEntry("p1", "v2", &streamingpb.WindowEntry{
				Key:            "key-2",
				CommitTimetick: 119,
				MessageId:      rmq.NewRmqID(119).IntoProto(),
			}),
		},
		"v1": {
			{
				SourcePChannel:  "p1",
				SourceMessageID: rmq.NewRmqID(105).IntoProto(),
				SourceTimeTick:  105,
				VChannel:        "v1",
				Rows: []committedWriteRow{
					{
						RowOffset:             0,
						PrimaryKeyType:        committedWritePrimaryKeyTypeString,
						StringPrimaryKeyValue: "pk-105",
					},
				},
			},
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:            "key-1",
				CommitTimetick: 110,
				MessageId:      rmq.NewRmqID(110).IntoProto(),
			}),
		},
	}

	payload, footer, checksum, err := marshalPChannelWindowChunk("p1", 7, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, records)
	require.NoError(t, err)
	require.NotEmpty(t, checksum)
	require.Equal(t, uint64(7), footer.Generation)
	require.Equal(t, uint64(120), footer.SourceCheckpointTimetick)
	require.Len(t, footer.Chunks, 2)
	require.Equal(t, "v1", footer.Chunks[0].VChannel)
	require.Equal(t, uint64(2), footer.Chunks[0].RecordCount)
	require.True(t, message.MustUnmarshalMessageID(footer.Chunks[0].SourceStartMessageID).EQ(rmq.NewRmqID(105)))
	require.Equal(t, "v2", footer.Chunks[1].VChannel)
	require.Equal(t, uint64(1), footer.Chunks[1].RecordCount)
	require.True(t, message.MustUnmarshalMessageID(footer.SourceStartMessageID).EQ(rmq.NewRmqID(105)))
	require.True(t, message.MustUnmarshalMessageID(footer.SourceEndMessageID).EQ(rmq.NewRmqID(119)))

	decoded, _, decodedChecksum, err := unmarshalPChannelWindowChunk(payload)
	require.NoError(t, err)
	require.Equal(t, checksum, decodedChecksum)
	require.Len(t, decoded, 2)
	require.Nil(t, decoded["v1"][0].Idempotency)
	require.Equal(t, "pk-105", decoded["v1"][0].Rows[0].StringPrimaryKeyValue)
	require.Equal(t, "key-1", decoded["v1"][1].Idempotency.Key)
	require.Equal(t, "key-2", decoded["v2"][0].Idempotency.Key)
}

func TestPChannelWindowChunkCodecHasNoViewTypeAndKeepsPayloadGeneration(t *testing.T) {
	records := map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:            "key-1",
				CommitTimetick: 110,
				MessageId:      rmq.NewRmqID(110).IntoProto(),
			}),
		},
	}

	payload, footer, _, err := marshalPChannelWindowChunk("p1", 9, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, records)
	require.NoError(t, err)
	require.NotContains(t, string(payload), "view_type")
	require.Len(t, footer.Chunks, 1)
	chunkIndex := footer.Chunks[0]
	chunkPayload := payload[int(chunkIndex.Offset):int(chunkIndex.Offset+chunkIndex.Length)]
	require.NotContains(t, string(chunkPayload), "view_type")
	decodedChunk, err := unmarshalVChannelWindowChunk(chunkPayload)
	require.NoError(t, err)
	require.Equal(t, uint64(9), decodedChunk.Generation)
	require.Equal(t, "p1", decodedChunk.PChannel)
	require.Equal(t, "v1", decodedChunk.VChannel)
}

func TestPChannelWindowChunkCodecCheckpointOnlyRoundTrip(t *testing.T) {
	payload, footer, checksum, err := marshalPChannelWindowChunk("p1", 8, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(200),
		TimeTick:  200,
	}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, payload)
	require.NotEmpty(t, checksum)
	require.Equal(t, uint64(8), footer.Generation)
	require.Equal(t, uint64(200), footer.SourceCheckpointTimetick)
	require.Empty(t, footer.Chunks)

	decodedRecords, decodedFooter, decodedChecksum, err := unmarshalPChannelWindowChunk(payload)
	require.NoError(t, err)
	require.Equal(t, checksum, decodedChecksum)
	require.Empty(t, decodedRecords)
	require.Equal(t, uint64(8), decodedFooter.Generation)
	require.Equal(t, uint64(200), decodedFooter.SourceCheckpointTimetick)
}

func TestPChannelWindowChunkCodecChecksumMismatch(t *testing.T) {
	records := map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:            "key-1",
				CommitTimetick: 100,
				MessageId:      rmq.NewRmqID(100).IntoProto(),
			}),
		},
	}
	payload, _, _, err := marshalPChannelWindowChunk("p1", 1, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
	}, records)
	require.NoError(t, err)
	payload[len(pchannelWindowChunkHeaderMagic)] ^= 0x01

	_, _, _, err = unmarshalPChannelWindowChunk(payload)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPChannelWindowStoreCorrupted)
}

func TestPChannelWindowChunkCodecDetectsFooterChecksumMismatch(t *testing.T) {
	payload, _, _, err := marshalPChannelWindowChunk("p1", 1, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:            "key-1",
				CommitTimetick: 100,
				MessageId:      rmq.NewRmqID(100).IntoProto(),
			}),
		},
	})
	require.NoError(t, err)

	payload = rewritePChannelWindowFooterPayload(t, payload, func(footer *pchannelWindowChunkFooter) {
		footer.SourceEndTimetick++
	})

	_, _, _, err = unmarshalPChannelWindowChunk(payload)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPChannelWindowStoreCorrupted)
	require.Contains(t, err.Error(), "pchannel window chunk footer checksum mismatch")
}

func TestPChannelWindowChunkCodecDetectsVChannelBlockChecksumMismatch(t *testing.T) {
	payload, footer, _, err := marshalPChannelWindowChunk("p1", 1, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:            "key-1",
				CommitTimetick: 100,
				MessageId:      rmq.NewRmqID(100).IntoProto(),
			}),
		},
	})
	require.NoError(t, err)
	require.Len(t, footer.Chunks, 1)
	mutated := append([]byte(nil), payload...)
	mutated[int(footer.Chunks[0].Offset)] ^= 0x01

	_, _, _, err = unmarshalPChannelWindowChunk(mutated)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPChannelWindowStoreCorrupted)
	require.Contains(t, err.Error(), "vchannel window chunk checksum mismatch")
}

func TestPChannelWindowChunkKeyIsDeterministic(t *testing.T) {
	key := buildPChannelWindowChunkKey("by-dev-rootcoord-dml_0", 42)
	require.Contains(t, key, "/streamingnode/window-store/by-dev-rootcoord-dml_0/chunks/chunk.42.pwc")
	require.NotContains(t, key, "manifests")
	require.NotContains(t, key, "checksum")
}

func rewritePChannelWindowFooterPayload(
	t *testing.T,
	payload []byte,
	mutate func(*pchannelWindowChunkFooter),
) []byte {
	t.Helper()
	footerMagicStart := len(payload) - len(pchannelWindowChunkFooterMagic)
	require.GreaterOrEqual(t, footerMagicStart, pchannelWindowChunkHeaderSize+pchannelWindowChunkChecksumSize+4)
	require.Equal(t, pchannelWindowChunkFooterMagic, payload[footerMagicStart:])
	footerLenStart := footerMagicStart - 4
	footerChecksumStart := footerLenStart - pchannelWindowChunkChecksumSize
	footerLen := int(binary.BigEndian.Uint32(payload[footerLenStart:footerMagicStart]))
	footerStart := footerChecksumStart - footerLen
	require.GreaterOrEqual(t, footerStart, pchannelWindowChunkHeaderSize)
	// Preserve the original (now stale) trailer checksum so mutating the footer
	// body is detected as corruption.
	staleChecksum := payload[footerChecksumStart:footerLenStart]
	footer := &pchannelWindowChunkFooter{}
	require.NoError(t, json.Unmarshal(payload[footerStart:footerChecksumStart], footer))
	mutate(footer)
	footerPayload, err := json.Marshal(footer)
	require.NoError(t, err)
	mutated := make([]byte, 0, footerStart+len(footerPayload)+pchannelWindowChunkChecksumSize+4+len(pchannelWindowChunkFooterMagic))
	mutated = append(mutated, payload[:footerStart]...)
	mutated = append(mutated, footerPayload...)
	mutated = append(mutated, staleChecksum...)
	footerLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(footerLenBytes, uint32(len(footerPayload)))
	mutated = append(mutated, footerLenBytes...)
	mutated = append(mutated, pchannelWindowChunkFooterMagic...)
	return mutated
}

func TestPChannelWindowPersistRecover(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())
	records := map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:                    "key-1",
				CommitTimetick:         99,
				MessageId:              rmq.NewRmqID(99).IntoProto(),
				LastConfirmedMessageId: rmq.NewRmqID(98).IntoProto(),
			}),
		},
	}
	sourceCheckpoint := &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}

	_, _, err := rs.windowManager.persistPChannelWindow(ctx, resource.Resource().Logger(), records, nil, sourceCheckpoint)
	require.NoError(t, err)
	meta := catalogState.storeMeta
	require.NotNil(t, meta)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", meta.GetLatestGeneration(), true)
	require.Equal(t, uint64(120), meta.GetSourceCheckpointTimetick())
	require.Equal(t, uint64(0), meta.GetLatestGeneration())
	require.Equal(t, uint64(0), meta.GetMinAvailableGeneration())

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
		{Vchannel: "v2", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	recovered.SetLogger(resource.Resource().Logger())
	recoverTestIdempotencyWindows(t, ctx, recovered, "p1", false)
	require.Len(t, recovered.windowManager.idempotencyWindows(), 2)
	v1 := recovered.windowManager.idempotencyWindows()["v1"].snapshot()
	require.Len(t, v1.GetEntries(), 1)
	require.Equal(t, "key-1", v1.GetEntries()[0].GetKey())
	require.Equal(t, uint64(120), v1.GetSnapshotCheckpointTimetick())
	v2 := recovered.windowManager.idempotencyWindows()["v2"].snapshot()
	require.Empty(t, v2.GetEntries())
	require.Equal(t, uint64(120), v2.GetSnapshotCheckpointTimetick())
}

func TestPChannelWindowRecoverWithContinuousChunks(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	records0 := map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:                    "key-from-generation-0",
				CommitTimetick:         110,
				MessageId:              rmq.NewRmqID(110).IntoProto(),
				LastConfirmedMessageId: rmq.NewRmqID(109).IntoProto(),
			}),
		},
	}
	writeTestPChannelWindowChunk(t, ctx, "p1", 0, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, records0)

	records1 := map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:                    "key-from-generation-1",
				CommitTimetick:         130,
				MessageId:              rmq.NewRmqID(130).IntoProto(),
				LastConfirmedMessageId: rmq.NewRmqID(129).IntoProto(),
			}),
		},
	}
	footer, _, _ := writeTestPChannelWindowChunk(t, ctx, "p1", 1, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(140),
		TimeTick:  140,
	}, records1)
	catalogState.storeMeta = newPChannelWindowStoreMetaFromChunk("p1", footer, 0, 0).intoCatalogMeta()

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	recovered.SetLogger(resource.Resource().Logger())
	recoverTestIdempotencyWindows(t, ctx, recovered, "p1", false)

	window := recovered.windowManager.idempotencyWindows()["v1"].snapshot()
	require.Len(t, window.GetEntries(), 2)
	require.Equal(t, "key-from-generation-0", window.GetEntries()[0].GetKey())
	require.Equal(t, "key-from-generation-1", window.GetEntries()[1].GetKey())
	require.Equal(t, uint64(140), window.GetSnapshotCheckpointTimetick())
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinAvailableGeneration())
}

func TestPChannelWindowRecoverySkipsGenerationBelowViewMinRequired(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	records0 := map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:            "key-evicted",
				CommitTimetick: 110,
				MessageId:      rmq.NewRmqID(110).IntoProto(),
			}),
		},
	}
	writeTestPChannelWindowChunk(t, ctx, "p1", 0, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, records0)

	records1 := map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:            "key-retained",
				CommitTimetick: 130,
				MessageId:      rmq.NewRmqID(130).IntoProto(),
			}),
		},
	}
	footer, _, _ := writeTestPChannelWindowChunk(t, ctx, "p1", 1, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(140),
		TimeTick:  140,
	}, records1)
	catalogState.storeMeta = newPChannelWindowStoreMetaFromChunk("p1", footer, 0, 1).intoCatalogMeta()
	catalogState.windowMetas["v1"] = &streamingpb.VChannelWindowMeta{
		Pchannel:                    "p1",
		Vchannel:                    "v1",
		ViewType:                    common.VChannelWindowViewTypeIdempotency,
		SnapshotCheckpointMessageId: rmq.NewRmqID(20).IntoProto(),
		SnapshotCheckpointTimetick:  20,
		LatestAppliedGeneration:     1,
		MinRequiredGeneration:       1,
	}

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	recovered.SetLogger(resource.Resource().Logger())
	recoverTestIdempotencyWindows(t, ctx, recovered, "p1", false)

	window := recovered.windowManager.idempotencyWindows()["v1"].snapshot()
	require.Len(t, window.GetEntries(), 1)
	require.Equal(t, "key-retained", window.GetEntries()[0].GetKey())
	require.Equal(t, uint64(1), recovered.windowManager.idempotencyWindows()["v1"].minRequiredGeneration)
}

func TestPChannelWindowRecoverFailsWhenGenerationHasHole(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	writeTestPChannelWindowChunk(t, ctx, "p1", 0, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, nil)
	footer, _, _ := writeTestPChannelWindowChunk(t, ctx, "p1", 2, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(140),
		TimeTick:  140,
	}, nil)
	catalogState.storeMeta = newPChannelWindowStoreMetaFromChunk("p1", footer, 0, 0).intoCatalogMeta()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	recovered.SetLogger(resource.Resource().Logger())

	err := recoverTestIdempotencyWindowsWithError(ctx, recovered, "p1", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read pchannel window chunk")
	require.Contains(t, err.Error(), "chunk.1.pwc")
}

func TestPChannelWindowRecoverFailsWhenChunkMissing(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	catalogState.storeMeta = &streamingpb.PChannelWindowMeta{
		Pchannel:                  "p1",
		LatestGeneration:          0,
		MinAvailableGeneration:    0,
		SourceCheckpointMessageId: rmq.NewRmqID(120).IntoProto(),
		SourceCheckpointTimetick:  120,
		CodecVersion:              uint32(pchannelWindowCodecVersion),
	}
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	recovered.SetLogger(resource.Resource().Logger())

	err := recoverTestIdempotencyWindowsWithError(ctx, recovered, "p1", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read pchannel window chunk")
}

func TestPChannelWindowRecoveryRepairsLaggingPChannelMeta(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	initialCheckpoint := &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(10),
		TimeTick:  10,
	}
	catalogState.storeMeta = writeTestBootstrapPChannelWindowMeta(t, ctx, "p1", chunkManager, initialCheckpoint)

	records := map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:                    "key-orphan",
				CommitTimetick:         110,
				MessageId:              rmq.NewRmqID(110).IntoProto(),
				LastConfirmedMessageId: rmq.NewRmqID(109).IntoProto(),
			}),
		},
	}
	writeTestPChannelWindowChunk(t, ctx, "p1", 1, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, records)
	catalogState.windowMetas["v1"] = &streamingpb.VChannelWindowMeta{
		Pchannel:                    "p1",
		Vchannel:                    "v1",
		ViewType:                    common.VChannelWindowViewTypeIdempotency,
		SnapshotCheckpointMessageId: rmq.NewRmqID(20).IntoProto(),
		SnapshotCheckpointTimetick:  20,
		LatestAppliedGeneration:     1,
		MinRequiredGeneration:       1,
	}

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, initialCheckpoint)
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	recovered.SetLogger(resource.Resource().Logger())
	recoverTestIdempotencyWindows(t, ctx, recovered, "p1", false)

	window := recovered.windowManager.idempotencyWindows()["v1"].snapshot()
	require.Len(t, window.GetEntries(), 1)
	require.Equal(t, "key-orphan", window.GetEntries()[0].GetKey())
	require.Equal(t, uint64(120), window.GetSnapshotCheckpointTimetick())
	require.Equal(t, uint64(1), catalogState.storeMeta.GetLatestGeneration())
	require.Equal(t, uint64(120), catalogState.storeMeta.GetSourceCheckpointTimetick())
}

func TestPChannelWindowRecoveryDropsCorruptOrphanChunkAboveLatest(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	initialCheckpoint := &utility.WALCheckpoint{MessageID: rmq.NewRmqID(10), TimeTick: 10}
	catalogState.storeMeta = writeTestBootstrapPChannelWindowMeta(t, ctx, "p1", chunkManager, initialCheckpoint)
	require.Equal(t, uint64(0), catalogState.storeMeta.GetLatestGeneration())

	// A persist wrote chunk generation 1 but crashed before advancing the meta
	// (still LatestGeneration=0), and the chunk on disk is corrupt/truncated.
	payload, _, _, err := marshalPChannelWindowChunk("p1", 1, &utility.WALCheckpoint{MessageID: rmq.NewRmqID(120), TimeTick: 120}, nil)
	require.NoError(t, err)
	corruptPayload := rewritePChannelWindowFooterPayload(t, payload, func(footer *pchannelWindowChunkFooter) {
		footer.SourceCheckpointTimetick = 999999
	})
	orphanKey := buildPChannelWindowChunkKey("p1", 1)
	require.NoError(t, chunkManager.Write(ctx, orphanKey, corruptPayload))

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, initialCheckpoint)
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	recovered.SetLogger(resource.Resource().Logger())

	// Recovery must not fail on the corrupt orphan; the data is still in the WAL.
	require.NoError(t, recoverTestIdempotencyWindowsWithError(ctx, recovered, "p1", false))

	// The corrupt orphan must be deleted so the next persist can rewrite generation
	// 1 instead of wedging on a byte-mismatch forever.
	exists, err := chunkManager.Exist(ctx, orphanKey)
	require.NoError(t, err)
	require.False(t, exists, "corrupt orphan chunk above latest generation must be deleted")
	require.Equal(t, uint64(0), catalogState.storeMeta.GetLatestGeneration())
}

func TestPChannelWindowRecoveryRewindsCheckpointByStore(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())
	_, _, err := rs.windowManager.persistPChannelWindow(ctx, resource.Resource().Logger(), nil, nil, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	})
	require.NoError(t, err)

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(150),
		TimeTick:  150,
	})
	recovered.SetLogger(resource.Resource().Logger())

	recoverTestIdempotencyWindows(t, ctx, recovered, "p1", false)
	require.True(t, recovered.checkpoint.MessageID.EQ(rmq.NewRmqID(120)))
	require.Equal(t, uint64(120), recovered.checkpoint.TimeTick)
}

func TestPChannelWindowRecoveryRewindsCheckpointByStoreAndFlusher(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())
	records := map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:                    "key-1",
				CommitTimetick:         99,
				MessageId:              rmq.NewRmqID(99).IntoProto(),
				LastConfirmedMessageId: rmq.NewRmqID(98).IntoProto(),
			}),
		},
	}
	_, _, err := rs.windowManager.persistPChannelWindow(ctx, resource.Resource().Logger(), records, nil, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	})
	require.NoError(t, err)

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(150),
		TimeTick:  150,
	})
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{
			Vchannel:       "v1",
			State:          streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: 1},
		},
	})
	recovered.vchannels["v1"].flusherCheckpoint = &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(90),
		TimeTick:  90,
	}
	recovered.SetLogger(resource.Resource().Logger())

	recoverTestIdempotencyWindows(t, ctx, recovered, "p1", false)
	require.True(t, recovered.checkpoint.MessageID.EQ(rmq.NewRmqID(90)))
	require.Equal(t, uint64(90), recovered.checkpoint.TimeTick)
}

func TestPChannelWindowRecoveryReplayTailIdempotently(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	msg100 := newTestIdempotentInsertMessage(t, "v1", "key-1", nil).
		WithTimeTick(100).
		WithLastConfirmed(rmq.NewRmqID(99)).
		IntoImmutableMessage(rmq.NewRmqID(100))
	record100, ok := newCommittedWriteRecordFromMessage("p1", msg100)
	require.True(t, ok)
	msg130 := newTestIdempotentInsertMessage(t, "v1", "key-2", nil).
		WithTimeTick(130).
		WithLastConfirmed(rmq.NewRmqID(129)).
		IntoImmutableMessage(rmq.NewRmqID(130))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())
	_, _, err := rs.windowManager.persistPChannelWindow(ctx, resource.Resource().Logger(), map[string][]committedWriteRecord{
		"v1": {*record100},
	}, nil, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	})
	require.NoError(t, err)

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(80),
		TimeTick:  80,
	})
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{
			Vchannel:       "v1",
			State:          streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: 1},
		},
	})
	recovered.segments = make(map[int64]*segmentRecoveryInfo)
	recovered.SetLogger(resource.Resource().Logger())
	recoverTestIdempotencyWindows(t, ctx, recovered, "p1", false)
	require.True(t, recovered.checkpoint.MessageID.EQ(rmq.NewRmqID(80)))
	require.Equal(t, uint64(80), recovered.checkpoint.TimeTick)

	builder := &streamBuilder{
		channel:   types.PChannelInfo{Name: "p1"},
		histories: []message.ImmutableMessage{msg100, msg130},
	}
	lastTimeTick := message.CreateTestTimeTickSyncMessage(t, 130, 130, rmq.NewRmqID(130)).IntoImmutableMessage(rmq.NewRmqID(130))
	snapshot, err := recovered.recoverFromStream(ctx, builder, lastTimeTick)
	require.NoError(t, err)
	require.NotNil(t, snapshot)

	window := snapshot.IdempotencyWindows["v1"]
	require.NotNil(t, window)
	require.Len(t, window.GetEntries(), 2)
	require.Equal(t, "key-1", window.GetEntries()[0].GetKey())
	require.Equal(t, "key-2", window.GetEntries()[1].GetKey())
	require.Equal(t, uint64(130), window.GetSnapshotCheckpointTimetick())
}

func TestPChannelWindowCrashBeforeChunkFallsBackToWALReplay(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	initialCheckpoint := &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(10),
		TimeTick:  10,
	}
	catalogState.storeMeta = writeTestBootstrapPChannelWindowMeta(t, ctx, "p1", chunkManager, initialCheckpoint)
	msg100 := newTestIdempotentCommittedInsertMessage(t, "v1", "key-from-wal", 100)

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, initialCheckpoint)
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{
			Vchannel:       "v1",
			State:          streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: 1},
		},
	})
	recovered.segments = make(map[int64]*segmentRecoveryInfo)
	recovered.SetLogger(resource.Resource().Logger())
	recoverTestIdempotencyWindows(t, ctx, recovered, "p1", false)

	snapshot, err := recovered.recoverFromStream(ctx, &streamBuilder{
		channel:   types.PChannelInfo{Name: "p1"},
		histories: []message.ImmutableMessage{msg100},
	}, message.CreateTestTimeTickSyncMessage(t, 100, 100, rmq.NewRmqID(100)).IntoImmutableMessage(rmq.NewRmqID(100)))
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	window := snapshot.IdempotencyWindows["v1"]
	require.NotNil(t, window)
	require.Len(t, window.GetEntries(), 1)
	require.Equal(t, "key-from-wal", window.GetEntries()[0].GetKey())
	require.Equal(t, uint64(0), catalogState.storeMeta.GetLatestGeneration())
}

func TestPChannelWindowCrashAfterConsumeCheckpointRecoversFromChunkWithoutReplay(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	msg100 := newTestIdempotentCommittedInsertMessage(t, "v1", "key-from-chunk", 100)
	record100, ok := newCommittedWriteRecordFromMessage("p1", msg100)
	require.True(t, ok)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(10),
		TimeTick:  10,
	})
	rs.SetLogger(resource.Resource().Logger())
	_, _, err := rs.windowManager.persistPChannelWindow(ctx, resource.Resource().Logger(), map[string][]committedWriteRecord{
		"v1": {*record100},
	}, nil, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	})
	require.NoError(t, err)

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	})
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{
			Vchannel:       "v1",
			State:          streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: 1},
		},
	})
	recovered.segments = make(map[int64]*segmentRecoveryInfo)
	recovered.SetLogger(resource.Resource().Logger())
	recoverTestIdempotencyWindows(t, ctx, recovered, "p1", false)

	snapshot, err := recovered.recoverFromStream(ctx, &streamBuilder{
		channel:   types.PChannelInfo{Name: "p1"},
		histories: []message.ImmutableMessage{msg100},
	}, message.CreateTestTimeTickSyncMessage(t, 120, 120, rmq.NewRmqID(120)).IntoImmutableMessage(rmq.NewRmqID(120)))
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	window := snapshot.IdempotencyWindows["v1"]
	require.NotNil(t, window)
	require.Len(t, window.GetEntries(), 1)
	require.Equal(t, "key-from-chunk", window.GetEntries()[0].GetKey())
	require.Equal(t, uint64(120), window.GetSnapshotCheckpointTimetick())
}

func newTestIdempotentCommittedInsertMessage(t *testing.T, vchannel string, key string, id int64) message.ImmutableMessage {
	t.Helper()
	return newTestIdempotentInsertMessage(t, vchannel, key, nil).
		WithTimeTick(uint64(id)).
		WithLastConfirmed(rmq.NewRmqID(id - 1)).
		IntoImmutableMessage(rmq.NewRmqID(id))
}

func newTestIdempotentInsertMessage(t *testing.T, vchannel string, key string, extra *messagespb.IdempotentInsertResult) message.MutableMessage {
	t.Helper()
	header := &message.InsertMessageHeader{
		CollectionId:   1,
		IdempotencyKey: proto.String(key),
	}
	message.SetInsertHeaderIdempotentInsertResult(header, extra)
	return message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(header).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable()
}

func TestPChannelWindowRecoverFailsWhenLatestGenerationChunkMissing(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())
	_, _, err := rs.windowManager.persistPChannelWindow(ctx, resource.Resource().Logger(), nil, nil, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	})
	require.NoError(t, err)
	catalogState.storeMeta.LatestGeneration = 2

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(150),
		TimeTick:  150,
	})
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	recovered.SetLogger(resource.Resource().Logger())

	err = recoverTestIdempotencyWindowsWithError(ctx, recovered, "p1", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read pchannel window chunk")
	require.Contains(t, err.Error(), "chunk.1.pwc")
}

func TestPChannelWindowBootstrapCreatesGenerationZeroChunk(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(10),
		TimeTick:  10,
	})
	rs.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	rs.SetLogger(resource.Resource().Logger())

	recoverTestIdempotencyWindows(t, ctx, rs, "p1", true)
	meta := catalogState.storeMeta
	require.NotNil(t, meta)
	require.Equal(t, uint64(0), meta.GetLatestGeneration())
	require.Equal(t, uint64(0), meta.GetMinAvailableGeneration())
	require.Equal(t, uint64(0), meta.GetMinInUseGeneration())
	require.Equal(t, uint64(10), meta.GetSourceCheckpointTimetick())

	payload, err := chunkManager.Read(ctx, buildPChannelWindowChunkKey("p1", meta.GetLatestGeneration()))
	require.NoError(t, err)
	records, footer, _, err := unmarshalPChannelWindowChunk(payload)
	require.NoError(t, err)
	require.Equal(t, uint64(0), footer.Generation)
	require.Equal(t, uint64(10), footer.SourceCheckpointTimetick)
	require.Empty(t, records)
	require.Len(t, rs.windowManager.idempotencyWindows(), 1)
}

func TestPChannelWindowMissingMetaFailsWithoutBootstrap(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelWindowCatalog(t)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	rs.SetLogger(resource.Resource().Logger())

	err := recoverTestIdempotencyWindowsWithError(ctx, rs, "p1", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pchannel window meta missing")
}

func TestPChannelWindowPersistsCheckpointOnlyGeneration(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())
	sourceCheckpoint := &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(200),
		TimeTick:  200,
	}

	_, generation, err := rs.windowManager.persistPChannelWindow(ctx, resource.Resource().Logger(), nil, nil, sourceCheckpoint)
	require.NoError(t, err)
	require.Equal(t, uint64(0), generation)
	meta := catalogState.storeMeta
	require.NotNil(t, meta)
	require.Equal(t, uint64(200), meta.GetSourceCheckpointTimetick())
	require.Equal(t, uint64(0), meta.GetLatestGeneration())
	require.Equal(t, uint64(0), meta.GetMinAvailableGeneration())
	require.Equal(t, uint64(0), meta.GetMinInUseGeneration())

	payload, err := chunkManager.Read(ctx, buildPChannelWindowChunkKey("p1", meta.GetLatestGeneration()))
	require.NoError(t, err)
	records, footer, _, err := unmarshalPChannelWindowChunk(payload)
	require.NoError(t, err)
	require.Equal(t, uint64(0), footer.Generation)
	require.Equal(t, uint64(200), footer.SourceCheckpointTimetick)
	require.Empty(t, records)
	require.Equal(t, []string{"pchannel-window-meta"}, catalogState.operations)
}

func TestPChannelWindowPersistWritesContinuousGenerationsWhenCheckpointAdvances(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())

	for idx, timetick := range []uint64{100, 120, 140} {
		_, generation, err := rs.windowManager.persistPChannelWindow(ctx, resource.Resource().Logger(), nil, nil, &utility.WALCheckpoint{
			MessageID: rmq.NewRmqID(int64(timetick)),
			TimeTick:  timetick,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(idx), generation)
		require.Equal(t, uint64(idx), catalogState.storeMeta.GetLatestGeneration())
		require.Equal(t, timetick, catalogState.storeMeta.GetSourceCheckpointTimetick())
	}

	for generation := uint64(0); generation <= 2; generation++ {
		chunkKey := buildPChannelWindowChunkKey("p1", generation)
		exists, err := chunkManager.Exist(ctx, chunkKey)
		require.NoError(t, err)
		require.True(t, exists)
		payload, err := chunkManager.Read(ctx, chunkKey)
		require.NoError(t, err)
		records, footer, _, err := unmarshalPChannelWindowChunk(payload)
		require.NoError(t, err)
		require.Empty(t, records)
		require.Equal(t, generation, footer.Generation)
	}
	exists, err := chunkManager.Exist(ctx, buildPChannelWindowChunkKey("p1", 3))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestPChannelWindowPersistEmptyActiveWindowAdvancesMinInUse(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.windowManager.setIdempotencyWindows(map[string]*vchannelWindow{
		"v1": newEmptyVChannelWindow("p1", "v1", &utility.WALCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  1,
		}),
	})
	rs.SetLogger(resource.Resource().Logger())

	for idx, timetick := range []uint64{100, 120} {
		_, generation, err := rs.windowManager.persistPChannelWindow(ctx, resource.Resource().Logger(), nil, nil, &utility.WALCheckpoint{
			MessageID: rmq.NewRmqID(int64(timetick)),
			TimeTick:  timetick,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(idx), generation)
	}

	require.Equal(t, uint64(1), catalogState.storeMeta.GetLatestGeneration())
	require.Equal(t, uint64(1), catalogState.storeMeta.GetMinInUseGeneration())
	rs.windowManager.markVChannelWindowsPersisted(nil, nil, 1, nil)
	require.Equal(t, uint64(1), rs.windowManager.idempotencyWindows()["v1"].windowMeta().GetMinRequiredGeneration())
}

func TestPChannelWindowPersistSavesViewMetaBeforePChannelMeta(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	window := newEmptyVChannelWindow("p1", "v1", nil)
	record := *committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
		Key:            "key-1",
		CommitTimetick: 210,
		MessageId:      rmq.NewRmqID(210).IntoProto(),
	})
	require.NoError(t, window.applyCommittedWriteRecord(record, true))
	records, metaUpdate := window.consumePendingCommittedWriteRecords()

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(200),
		TimeTick:  200,
	})
	rs.windowManager.setIdempotencyWindows(map[string]*vchannelWindow{"v1": window})
	rs.SetLogger(resource.Resource().Logger())

	windowMetas, generation, err := rs.windowManager.persistPChannelWindow(ctx, resource.Resource().Logger(), map[string][]committedWriteRecord{
		"v1": records,
	}, map[string]*idempotencyWindowMetaUpdate{
		"v1": metaUpdate,
	}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(220),
		TimeTick:  220,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(0), generation)
	require.Equal(t, uint64(0), windowMetas["v1"].GetMinRequiredGeneration())
	require.Equal(t, []string{"vchannel-window-meta", "pchannel-window-meta"}, catalogState.operations)
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinInUseGeneration())
}

func TestPChannelWindowPersistRetryDoesNotAllocateNextGenerationWhenCheckpointCovered(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
	})
	rs.SetLogger(resource.Resource().Logger())
	records := map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:            "key-1",
				CommitTimetick: 110,
				MessageId:      rmq.NewRmqID(110).IntoProto(),
			}),
		},
	}
	sourceCheckpoint := &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}

	_, generation, err := rs.windowManager.persistPChannelWindow(ctx, resource.Resource().Logger(), records, nil, sourceCheckpoint)
	require.NoError(t, err)
	require.Equal(t, uint64(0), generation)
	_, generation, err = rs.windowManager.persistPChannelWindow(ctx, resource.Resource().Logger(), records, nil, sourceCheckpoint)
	require.NoError(t, err)
	require.Equal(t, uint64(0), generation)
	require.Equal(t, uint64(0), catalogState.storeMeta.GetLatestGeneration())
	exists, err := chunkManager.Exist(ctx, buildPChannelWindowChunkKey("p1", 1))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestPChannelWindowMetaMinInUseIncludesNonDirtyWindows(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	footer, _, _ := writeTestPChannelWindowChunk(t, ctx, "p1", 4, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(400),
		TimeTick:  400,
	}, nil)
	catalogState.storeMeta = newPChannelWindowStoreMetaFromChunk("p1", footer, 0, 4).intoCatalogMeta()

	oldWindow := newEmptyVChannelWindow("p1", "v-old", nil)
	require.NoError(t, oldWindow.applyCommittedWriteRecordsAtGeneration([]committedWriteRecord{
		*committedWriteRecordFromWindowEntry("p1", "v-old", &streamingpb.WindowEntry{
			Key:            "key-old",
			CommitTimetick: 210,
			MessageId:      rmq.NewRmqID(210).IntoProto(),
		}),
	}, 2))
	newWindow := newEmptyVChannelWindow("p1", "v-new", nil)
	require.NoError(t, newWindow.applyCommittedWriteRecord(*committedWriteRecordFromWindowEntry("p1", "v-new", &streamingpb.WindowEntry{
		Key:            "key-new",
		CommitTimetick: 410,
		MessageId:      rmq.NewRmqID(410).IntoProto(),
	}), true))
	records, metaUpdate := newWindow.consumePendingCommittedWriteRecords()

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(400),
		TimeTick:  400,
	})
	rs.windowManager.setIdempotencyWindows(map[string]*vchannelWindow{
		"v-old": oldWindow,
		"v-new": newWindow,
	})
	rs.SetLogger(resource.Resource().Logger())

	windowMetas, generation, err := rs.windowManager.persistPChannelWindow(ctx, resource.Resource().Logger(), map[string][]committedWriteRecord{
		"v-new": records,
	}, map[string]*idempotencyWindowMetaUpdate{
		"v-new": metaUpdate,
	}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(500),
		TimeTick:  500,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(5), generation)
	require.Equal(t, uint64(5), windowMetas["v-new"].GetMinRequiredGeneration())
	require.Equal(t, uint64(2), catalogState.storeMeta.GetMinInUseGeneration())
}

func TestPChannelWindowRejectsVChannelOnlyMeta(t *testing.T) {
	ctx := context.Background()
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().ListVChannelWindowMetas(mock.Anything, "p1", common.VChannelWindowViewTypeIdempotency).Return([]*streamingpb.VChannelWindowMeta{
		{
			Pchannel:                    "p1",
			Vchannel:                    "v1",
			ViewType:                    "idempotency",
			SnapshotCheckpointTimetick:  100,
			SnapshotCheckpointMessageId: rmq.NewRmqID(100).IntoProto(),
		},
	}, nil)
	catalog.EXPECT().GetPChannelWindowMeta(mock.Anything, "p1").Return(nil, nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	rs.SetLogger(resource.Resource().Logger())

	err := recoverTestIdempotencyWindowsWithError(ctx, rs, "p1", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pchannel window meta missing")
}
