package recovery

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
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
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// The persisted-meta projection must honor the durable-retention ledger the
// same way summaryMetaAtGeneration does: after evictPersisted clears the staging
// summary, a later persist cycle carrying only keyless committed writes
// (EntryCount == 0) must not project MinRequiredGeneration forward past a
// generation the ledger still pins — that poisoned meta would survive restart
// and make the loss of in-TTL keys irreversible once chunk GC runs.
func TestWithPersistedGenerationHonorsRetentionLedger(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", testRecoveryCheckpoint(1, 1))
	state.evictionCfg = summaryEvictionConfig{entryTTL: 10 * time.Minute}

	// Generation 1 persists a keyed insert; the staging summary is then cleared.
	keyed := committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
		Key:            "key-1",
		CommitTimetick: tsoutil.ComposeTS(600_000, 0),
		MessageId:      rmq.NewRmqID(600_000).IntoProto(),
	})
	require.NoError(t, state.applyCommittedWriteRecord(keyed, true))
	state.markCommittedWriteRecordsPersisted([]*streamingpb.CommittedWriteRecord{keyed}, 1)
	state.evictPersisted()
	state.consumePendingCommittedWriteRecords()
	require.Equal(t, uint64(1), state.minRequiredGeneration)

	// The next cycle carries only a keyless committed write (delete/replicated),
	// so the staged meta reports EntryCount == 0 while the ledger still pins 1.
	keyless := &streamingpb.CommittedWriteRecord{
		SourceTimetick: tsoutil.ComposeTS(660_000, 0),
	}
	require.NoError(t, state.applyCommittedWriteRecord(keyless, true))
	_, update := state.consumePendingCommittedWriteRecords()
	require.NotNil(t, update)

	meta := update.WithPersistedGeneration(2)
	require.Equal(t, uint64(2), meta.GetLatestAppliedGeneration())
	require.Equal(t, uint64(1), meta.GetMinRequiredGeneration(),
		"persisted meta must keep the ledger-pinned generation, not project it forward")
}

// The durable-retention ledger must keep chunk generations recoverable for the
// full retention policy even after evictPersisted cleared the staging memory.
// Previously minRequiredGeneration was derived from materialized entries only,
// so every persist cycle collapsed it to the latest generation, chunk GC
// trimmed everything below, and a restart could rebuild only ~one snapshot
// interval of the summary instead of a TTL's worth.
func TestMinRequiredGenerationSurvivesEvictPersisted(t *testing.T) {
	entryAt := func(vchannel, key string, physicalMs int64) *streamingpb.CommittedWriteRecord {
		return committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
			Key:            key,
			CommitTimetick: tsoutil.ComposeTS(physicalMs, 0),
			MessageId:      rmq.NewRmqID(physicalMs).IntoProto(),
		})
	}

	state := newEmptyVChannelSummary("p1", "v1", testRecoveryCheckpoint(1, 1))
	state.evictionCfg = summaryEvictionConfig{entryTTL: 10 * time.Minute}

	// Three persist cycles at generations 1..3, entries one minute apart; each
	// cycle clears the staging memory like the summary background task does.
	for i, gen := range []uint64{1, 2, 3} {
		rec := entryAt("v1", fmt.Sprintf("key-%d", gen), int64(600_000+i*60_000))
		require.NoError(t, state.applyCommittedWriteRecord(rec, true))
		state.markCommittedWriteRecordsPersisted([]*streamingpb.CommittedWriteRecord{rec}, gen)
		state.evictPersisted()
	}
	require.Empty(t, state.entries)

	// All entries are within TTL: generation 1 stays pinned despite the empty
	// staging summary, and the persisted meta projection must not override it.
	state.snapshotCheckpointTimetick = tsoutil.ComposeTS(600_000+3*60_000, 0)
	state.refreshMinRequiredGeneration()
	require.Equal(t, uint64(1), state.minRequiredGeneration)
	meta := state.summaryMetaAtGeneration(4)
	require.Equal(t, uint64(1), meta.GetMinRequiredGeneration())
	require.Equal(t, uint64(4), meta.GetLatestAppliedGeneration())

	// Time passing expires generations 1 and 2 (TTL 10m, no floor here), and the
	// boundary advances so chunk GC can reclaim them.
	state.snapshotCheckpointTimetick = tsoutil.ComposeTS(600_000+60_000+10*60_000+30_000, 0)
	state.refreshMinRequiredGeneration()
	require.Equal(t, uint64(3), state.minRequiredGeneration)

	// A byte cap releases generations beyond the cap even within TTL.
	capped := newEmptyVChannelSummary("p1", "v2", testRecoveryCheckpoint(1, 1))
	capped.evictionCfg = summaryEvictionConfig{entryTTL: 10 * time.Minute}
	var capBytes int
	for i, gen := range []uint64{1, 2} {
		rec := entryAt("v2", fmt.Sprintf("cap-key-%d", gen), int64(600_000+i*60_000))
		if capBytes == 0 {
			capBytes = proto.Size(summaryEntryOfCommittedWriteRecord(rec))
			capped.evictionCfg.maxBytes = capBytes
		}
		require.NoError(t, capped.applyCommittedWriteRecord(rec, true))
		capped.markCommittedWriteRecordsPersisted([]*streamingpb.CommittedWriteRecord{rec}, gen)
		capped.evictPersisted()
	}
	capped.snapshotCheckpointTimetick = tsoutil.ComposeTS(600_000+2*60_000, 0)
	capped.refreshMinRequiredGeneration()
	require.Equal(t, uint64(2), capped.minRequiredGeneration)
}

func TestMinRequiredGenerationHonorsByteOnlyRetention(t *testing.T) {
	entryAt := func(key string, physicalMs int64) *streamingpb.CommittedWriteRecord {
		return committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
			Key:            key,
			CommitTimetick: tsoutil.ComposeTS(physicalMs, 0),
			MessageId:      rmq.NewRmqID(physicalMs).IntoProto(),
		})
	}

	state := newEmptyVChannelSummary("p1", "v1", testRecoveryCheckpoint(1, 1))

	totalBytes := 0
	latestBytes := 0
	for _, gen := range []uint64{1, 2} {
		rec := entryAt(fmt.Sprintf("byte-key-%d", gen), int64(600_000+gen*60_000))
		size := proto.Size(summaryEntryOfCommittedWriteRecord(rec))
		totalBytes += size
		latestBytes = size
		require.NoError(t, state.applyCommittedWriteRecord(rec, true))
		state.markCommittedWriteRecordsPersisted([]*streamingpb.CommittedWriteRecord{rec}, gen)
		state.evictPersisted()
	}
	require.Empty(t, state.entries)

	state.evictionCfg.maxBytes = totalBytes
	state.refreshMinRequiredGeneration()
	require.Equal(t, uint64(1), state.minRequiredGeneration)

	state.evictionCfg.maxBytes = latestBytes
	state.refreshMinRequiredGeneration()
	require.Equal(t, uint64(2), state.minRequiredGeneration)
}

func writeTestBootstrapPChannelSummaryMeta(
	ctx context.Context,
	t require.TestingT,
	pchannel string,
	chunkManager storage.ChunkManager,
	checkpoint *utility.WALCheckpoint,
) *streamingpb.PChannelSummaryMeta {
	if helper, ok := t.(interface{ Helper() }); ok {
		helper.Helper()
	}
	payload, footer, _, err := marshalPChannelSummaryChunk(pchannel, 0, 0, checkpoint, nil)
	require.NoError(t, err)
	key := buildPChannelSummaryChunkKey(pchannel, footer.Generation, footer.Term)
	require.NoError(t, chunkManager.Write(ctx, key, payload))
	return newPChannelSummaryStoreMetaFromChunk(pchannel, footer, 0, 0).intoCatalogMeta()
}

func writeTestPChannelSummaryChunk(
	ctx context.Context,
	t require.TestingT,
	pchannel string,
	generation uint64,
	chunkManager storage.ChunkManager,
	checkpoint *utility.WALCheckpoint,
	records map[string][]*streamingpb.CommittedWriteRecord,
) (*streamingpb.PChannelSummaryChunkFooter, string, string) {
	return writeTestPChannelSummaryChunkWithTerm(ctx, t, pchannel, generation, 0, chunkManager, checkpoint, records)
}

func writeTestPChannelSummaryChunkWithTerm(
	ctx context.Context,
	t require.TestingT,
	pchannel string,
	generation uint64,
	term int64,
	chunkManager storage.ChunkManager,
	checkpoint *utility.WALCheckpoint,
	records map[string][]*streamingpb.CommittedWriteRecord,
) (*streamingpb.PChannelSummaryChunkFooter, string, string) {
	if helper, ok := t.(interface{ Helper() }); ok {
		helper.Helper()
	}
	payload, footer, checksum, err := marshalPChannelSummaryChunk(pchannel, generation, term, checkpoint, records)
	require.NoError(t, err)
	key := buildPChannelSummaryChunkKey(pchannel, generation, term)
	require.NoError(t, chunkManager.Write(ctx, key, payload))
	return footer, key, checksum
}

func recoverTestSummaries(ctx context.Context, t require.TestingT, rs *recoveryStorageImpl, pchannel string, allowBootstrap bool) {
	if helper, ok := t.(interface{ Helper() }); ok {
		helper.Helper()
	}
	require.NoError(t, recoverTestSummariesWithError(ctx, rs, pchannel, allowBootstrap))
}

func recoverTestSummariesWithError(ctx context.Context, rs *recoveryStorageImpl, pchannel string, allowBootstrap bool) error {
	rs.summaryManager.cfg.idempotencyEnabled = true
	info, err := rs.summaryManager.loadSummaryInfoFromMeta(ctx, pchannel, allowBootstrap, rs.checkpoint)
	if err != nil {
		return err
	}
	rs.summaryManager.initializeSummariesFromMeta(rs.vchannels, info.storeMeta.SourceCheckpoint, info.summaryMetas)
	rewound, err := rs.summaryManager.recoverSummaryStoreFromSnapshot(ctx, info, rs.checkpoint, rs.vchannels)
	if err != nil {
		return err
	}
	// Mirror RecoverRecoveryStorage: apply the (possibly rewound) checkpoint.
	rs.checkpoint = rewound
	return nil
}

type testPChannelSummaryCatalogState struct {
	summaryMetas map[string]*streamingpb.VChannelSummaryMeta
	storeMeta    *streamingpb.PChannelSummaryMeta
	operations   []string
}

type testPChannelSummaryCASCatalog struct {
	*mock_metastore.MockStreamingNodeCataLog
	state *testPChannelSummaryCatalogState
}

func newTestPChannelSummaryCASCatalog(t *testing.T) (*testPChannelSummaryCASCatalog, *testPChannelSummaryCatalogState) {
	catalog, state := newTestPChannelSummaryCatalog(t)
	return &testPChannelSummaryCASCatalog{
		MockStreamingNodeCataLog: catalog,
		state:                    state,
	}, state
}

func (c *testPChannelSummaryCASCatalog) CompareAndSwapPChannelSummaryMeta(ctx context.Context, pchannelName string, expected *streamingpb.PChannelSummaryMeta, target *streamingpb.PChannelSummaryMeta) (bool, error) {
	if target == nil {
		return true, nil
	}
	if expected == nil {
		if c.state.storeMeta != nil {
			return false, nil
		}
	} else if c.state.storeMeta == nil || !proto.Equal(c.state.storeMeta, expected) {
		return false, nil
	}
	c.state.operations = append(c.state.operations, "pchannel-summary-meta")
	c.state.storeMeta = proto.Clone(target).(*streamingpb.PChannelSummaryMeta)
	return true, nil
}

func newTestPChannelSummaryCatalog(t *testing.T) (*mock_metastore.MockStreamingNodeCataLog, *testPChannelSummaryCatalogState) {
	params := paramtable.Get()
	params.Save(params.MinioCfg.RootPath.Key, t.TempDir())
	t.Cleanup(func() {
		params.Reset(params.MinioCfg.RootPath.Key)
	})
	state := &testPChannelSummaryCatalogState{
		summaryMetas: make(map[string]*streamingpb.VChannelSummaryMeta),
	}
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().ListVChannelSummaryMetas(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, viewType string) ([]*streamingpb.VChannelSummaryMeta, error) {
		values := make([]*streamingpb.VChannelSummaryMeta, 0, len(state.summaryMetas))
		for _, meta := range state.summaryMetas {
			values = append(values, proto.Clone(meta).(*streamingpb.VChannelSummaryMeta))
		}
		return values, nil
	}).Maybe()
	catalog.EXPECT().SaveVChannelSummaryMetas(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, viewType string, saved map[string]*streamingpb.VChannelSummaryMeta) error {
		state.operations = append(state.operations, "vchannel-summary-meta")
		for key, meta := range saved {
			state.summaryMetas[key] = proto.Clone(meta).(*streamingpb.VChannelSummaryMeta)
		}
		return nil
	}).Maybe()
	catalog.EXPECT().GetPChannelSummaryMeta(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string) (*streamingpb.PChannelSummaryMeta, error) {
		if state.storeMeta == nil {
			return nil, nil
		}
		return proto.Clone(state.storeMeta).(*streamingpb.PChannelSummaryMeta), nil
	}).Maybe()
	catalog.EXPECT().SavePChannelSummaryMeta(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, meta *streamingpb.PChannelSummaryMeta) error {
		state.operations = append(state.operations, "pchannel-summary-meta")
		state.storeMeta = proto.Clone(meta).(*streamingpb.PChannelSummaryMeta)
		return nil
	}).Maybe()
	catalog.EXPECT().RemoveVChannelSummaryMetas(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, viewType string, vchannels []string) error {
		state.operations = append(state.operations, "remove-vchannel-summary-metas")
		for _, vchannel := range vchannels {
			delete(state.summaryMetas, vchannel)
		}
		return nil
	}).Maybe()
	catalog.EXPECT().RemovePChannelSummaryMeta(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string) error {
		state.operations = append(state.operations, "remove-pchannel-summary-meta")
		state.storeMeta = nil
		return nil
	}).Maybe()
	return catalog, state
}

func TestPersistPChannelSummaryRetriesTransientMetaLoad(t *testing.T) {
	ctx := context.Background()
	params := paramtable.Get()
	params.Save(params.MinioCfg.RootPath.Key, t.TempDir())
	t.Cleanup(func() { params.Reset(params.MinioCfg.RootPath.Key) })

	state := &testPChannelSummaryCatalogState{summaryMetas: make(map[string]*streamingpb.VChannelSummaryMeta)}
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().ListVChannelSummaryMetas(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, viewType string) ([]*streamingpb.VChannelSummaryMeta, error) {
		return nil, nil
	}).Maybe()
	catalog.EXPECT().SaveVChannelSummaryMetas(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, viewType string, saved map[string]*streamingpb.VChannelSummaryMeta) error {
		for key, meta := range saved {
			state.summaryMetas[key] = proto.Clone(meta).(*streamingpb.VChannelSummaryMeta)
		}
		return nil
	}).Maybe()
	catalog.EXPECT().SavePChannelSummaryMeta(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, meta *streamingpb.PChannelSummaryMeta) error {
		state.storeMeta = proto.Clone(meta).(*streamingpb.PChannelSummaryMeta)
		return nil
	}).Maybe()
	// GetPChannelSummaryMeta fails on its first call -- a transient etcd blip on the
	// one persist-path call that was not wrapped in retry -- then succeeds.
	getCalls := 0
	catalog.EXPECT().GetPChannelSummaryMeta(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string) (*streamingpb.PChannelSummaryMeta, error) {
		getCalls++
		if getCalls == 1 {
			return nil, errors.New("transient etcd error")
		}
		if state.storeMeta == nil {
			return nil, nil
		}
		return proto.Clone(state.storeMeta).(*streamingpb.PChannelSummaryMeta), nil
	}).Maybe()

	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 1})
	rs.SetLogger(resource.Resource().Logger())
	rs.summaryManager.SetLogger(resource.Resource().Logger())

	records := map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {
			committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
				Key:                    "key-1",
				CommitTimetick:         99,
				MessageId:              rmq.NewRmqID(99).IntoProto(),
				LastConfirmedMessageId: rmq.NewRmqID(98).IntoProto(),
			}),
		},
	}
	sourceCheckpoint := &utility.WALCheckpoint{MessageID: rmq.NewRmqID(120), TimeTick: 120}

	// A transient meta-load error must be retried internally, not propagated: a
	// propagated error kills the summary background task and stalls idempotency
	// durability (the summaries then grow unbounded until OOM).
	_, _, err := rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), records, nil, sourceCheckpoint)
	require.NoError(t, err)
	require.GreaterOrEqual(t, getCalls, 2, "transient GetPChannelSummaryMeta error should have been retried")
	require.NotNil(t, state.storeMeta)
}

func TestSummarySnapshotSerdeRoundTrip(t *testing.T) {
	snapshot := &streamingpb.SummarySnapshot{
		Pchannel:                   "p1",
		Vchannel:                   "v1",
		SnapshotCheckpointTimetick: 100,
		EvictedWatermarkTimetick:   90,
		Entries: []*streamingpb.SummaryEntry{
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

	decoded := &streamingpb.SummarySnapshot{}
	require.NoError(t, proto.Unmarshal(payload, decoded))
	require.Equal(t, snapshot.GetPchannel(), decoded.GetPchannel())
	require.Equal(t, snapshot.GetVchannel(), decoded.GetVchannel())
	require.Equal(t, snapshot.GetSnapshotCheckpointTimetick(), decoded.GetSnapshotCheckpointTimetick())
	require.Len(t, decoded.GetEntries(), 2)
}

func TestSummaryRecoveryStateFromSnapshot(t *testing.T) {
	snapshot := &streamingpb.SummarySnapshot{
		Pchannel:                   "p1",
		Vchannel:                   "v1",
		SnapshotCheckpointTimetick: 100,
		Entries: []*streamingpb.SummaryEntry{
			{Key: "key-2", CommitTimetick: 95},
			{Key: "key-1", CommitTimetick: 90},
		},
	}

	state, err := newVChannelSummaryFromSnapshot(snapshot)
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
	require.Equal(t, uint64(120), record.SourceTimetick)
	require.True(t, message.MustUnmarshalMessageID(record.SourceMessageId).EQ(rmq.NewRmqID(120)))
	require.True(t, message.MustUnmarshalMessageID(record.LastConfirmedMessageId).EQ(rmq.NewRmqID(119)))
	require.NotEmpty(t, record.IdempotencyKey)
	require.Equal(t, "key-1", record.GetIdempotencyKey())
	require.Equal(t, []uint32{2, 0}, record.IdempotentResult.GetRowOffsets())
	require.Equal(t, []int64{11, 10}, record.IdempotentResult.GetIds().GetIntId().GetData())

	entry := summaryEntryOfCommittedWriteRecord(record)
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
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		WithIdempotencyKey("txn-key").
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
	require.Equal(t, "txn-key", record.GetIdempotencyKey())
	require.Equal(t, []uint32{0, 2, 1}, record.IdempotentResult.GetRowOffsets())
	require.Equal(t, []string{"pk-0", "pk-2", "pk-1"}, record.IdempotentResult.GetIds().GetStrId().GetData())
	require.True(t, message.MustUnmarshalMessageID(record.SourceMessageId).EQ(rmq.NewRmqID(103)))
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

func TestVChannelSummarySkipsReplicatedIdempotencyKey(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", testRecoveryCheckpoint(1, 1))

	// A native keyed insert materializes a summary entry.
	state.observeMessage(newTestIdempotentCommittedInsertMessage(t, "v1", "native-key", 10))
	require.Contains(t, state.entries, "native-key")

	// A replicated insert preserves the SOURCE cluster's key AND insert result;
	// it must be recorded as a keyless committed write: no summary entry, while
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
	require.Empty(t, last.IdempotencyKey)
	require.Nil(t, last.IdempotentResult)
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
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		WithIdempotencyKey("txn-key").
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
	require.Empty(t, record.IdempotencyKey)
	require.Nil(t, record.IdempotentResult)
}

func TestCommittedWriteRecordWithoutKeyDoesNotEnterSummary(t *testing.T) {
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
	require.Empty(t, record.IdempotencyKey)
	require.Equal(t, []string{"pk-1"}, record.IdempotentResult.GetIds().GetStrId().GetData())
	require.Nil(t, summaryEntryOfCommittedWriteRecord(record))
}

func TestRecoveryStorageRegistersRuntimeVChannelForIdempotencySummary(t *testing.T) {
	enableRecoveryIdempotency(t)
	resource.InitForTest(t)
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())
	rs.vchannels = make(map[string]*vchannelRecoveryInfo)
	rs.segments = make(map[int64]*segmentRecoveryInfo)
	rs.summaryManager.resetSummaries()

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
	summary := rs.summaryManager.summaries()["v1"]
	require.NotNil(t, summary)
	require.False(t, summary.dirty)
	require.Equal(t, uint64(10), summary.snapshotCheckpointTimetick)
	require.Equal(t, uint64(10), rs.summaryManager.getPChannelSummarySnapshotCheckpointUnsafe().TimeTick)
}

func TestRecoveryStorageAdvancesOnlyTargetSummaryForOrdinaryMessages(t *testing.T) {
	enableRecoveryIdempotency(t)
	resource.InitForTest(t)
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(10, 10))
	rs.SetLogger(resource.Resource().Logger())
	rs.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
		{Vchannel: "v2", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	v1 := newEmptyVChannelSummary("p1", "v1", testRecoveryCheckpoint(10, 10))
	v2 := newEmptyVChannelSummary("p1", "v2", testRecoveryCheckpoint(10, 10))
	rs.summaryManager.setSummaries(map[string]*vchannelSummary{
		"v1": v1,
		"v2": v2,
	})

	ordinary := message.NewDeleteMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.DeleteRequest{CollectionID: 1}).
		MustBuildMutable().
		WithTimeTick(20).
		WithLastConfirmed(rmq.NewRmqID(19)).
		IntoImmutableMessage(rmq.NewRmqID(20))
	require.NoError(t, rs.ObserveMessage(context.Background(), ordinary))
	require.Equal(t, uint64(20), v1.snapshotCheckpointTimetick)
	require.Equal(t, uint64(10), v2.snapshotCheckpointTimetick)
	require.Equal(t, uint64(20), rs.summaryManager.getPChannelSummarySnapshotCheckpointUnsafe().TimeTick)

	require.NoError(t, rs.ObserveMessage(context.Background(), buildTimeTickMessage(t, 30)))
	require.Equal(t, uint64(30), v1.snapshotCheckpointTimetick)
	require.Equal(t, uint64(30), v2.snapshotCheckpointTimetick)
	require.Equal(t, uint64(30), rs.summaryManager.getPChannelSummarySnapshotCheckpointUnsafe().TimeTick)
}

func TestConsumeDirtySnapshotDoesNotConsumeIdempotencySummaries(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(10, 10))
	summary := newEmptyVChannelSummary("p1", "v1", testRecoveryCheckpoint(10, 10))
	require.NoError(t, summary.applyCommittedWriteRecord(committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
		Key:            "key-1",
		CommitTimetick: 20,
		MessageId:      rmq.NewRmqID(20).IntoProto(),
	}), true))
	rs.summaryManager.setSummaries(map[string]*vchannelSummary{"v1": summary})
	rs.dirtyCounter = 1

	snapshot := rs.consumeDirtySnapshot()
	require.NotNil(t, snapshot)
	require.Nil(t, snapshot.pchannelSummarySourceCheckpoint)
	require.True(t, summary.dirty)
	require.Equal(t, 0, rs.dirtyCounter)
}

func TestConsumeIdempotencySnapshotDoesNotConsumeRecoveryState(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, testRecoveryCheckpoint(10, 10))
	summary := newEmptyVChannelSummary("p1", "v1", testRecoveryCheckpoint(10, 10))
	require.NoError(t, summary.applyCommittedWriteRecord(committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
		Key:            "key-1",
		CommitTimetick: 20,
		MessageId:      rmq.NewRmqID(20).IntoProto(),
	}), true))
	rs.summaryManager.setSummaries(map[string]*vchannelSummary{"v1": summary})
	rs.summaryManager.advancePChannelSummarySnapshotCheckpoint(testRecoveryCheckpoint(20, 20))
	rs.dirtyCounter = 1

	snapshot := rs.summaryManager.consumeIdempotencySnapshot()
	require.NotNil(t, snapshot)
	require.NotNil(t, snapshot.pchannelSummarySourceCheckpoint)
	require.Equal(t, uint64(20), snapshot.pchannelSummarySourceCheckpoint.TimeTick)
	require.False(t, summary.dirty)
	require.Equal(t, 1, rs.dirtyCounter)
}

func TestCommittedWriteRecordFromSummaryEntry(t *testing.T) {
	entry := &streamingpb.SummaryEntry{
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

	record := committedWriteRecordFromSummaryEntry(entry)
	require.NotNil(t, record)
	require.Equal(t, uint64(130), record.SourceTimetick)
	require.NotEmpty(t, record.IdempotencyKey)
	require.Equal(t, "key-1", record.GetIdempotencyKey())
	require.Equal(t, []uint32{3, 1}, record.IdempotentResult.GetRowOffsets())
	require.Equal(t, []string{"pk-3", "pk-1"}, record.IdempotentResult.GetIds().GetStrId().GetData())

	roundTrip := summaryEntryOfCommittedWriteRecord(record)
	require.Equal(t, entry.GetKey(), roundTrip.GetKey())
	require.Equal(t, entry.GetCommitTimetick(), roundTrip.GetCommitTimetick())
	require.Equal(t, entry.GetIdempotentResult().GetRowOffsets(), roundTrip.GetIdempotentResult().GetRowOffsets())
	require.Equal(t, entry.GetIdempotentResult().GetIds().GetStrId().GetData(), roundTrip.GetIdempotentResult().GetIds().GetStrId().GetData())
}

func TestSummaryMaterializerApplyCommittedWriteRecords(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", nil)
	record := committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
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
	keylessRecord := &streamingpb.CommittedWriteRecord{
		SourceMessageId:        rmq.NewRmqID(112).IntoProto(),
		SourceTimetick:         112,
		LastConfirmedMessageId: rmq.NewRmqID(111).IntoProto(),
		IdempotentResult: message.NewIdempotentInsertResult(
			[]uint32{0},
			&schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{12}}}},
		),
	}

	require.NoError(t, state.applyCommittedWriteRecordsAtGeneration([]*streamingpb.CommittedWriteRecord{keylessRecord, record, record}, 0))
	require.False(t, state.dirty)
	require.Empty(t, state.pendingEntries)

	snapshot := state.snapshot()
	require.Len(t, snapshot.GetEntries(), 1)
	require.Equal(t, "key-1", snapshot.GetEntries()[0].GetKey())
	require.Equal(t, []int64{10}, snapshot.GetEntries()[0].GetIdempotentResult().GetIds().GetIntId().GetData())
	require.Equal(t, uint64(112), snapshot.GetSnapshotCheckpointTimetick())
}

func TestSummaryConsumesPendingCommittedWriteRecords(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", nil)
	didRecord := committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
		Key:                    "key-1",
		CommitTimetick:         110,
		MessageId:              rmq.NewRmqID(110).IntoProto(),
		LastConfirmedMessageId: rmq.NewRmqID(109).IntoProto(),
	})
	keylessRecord := &streamingpb.CommittedWriteRecord{
		SourceMessageId:        rmq.NewRmqID(111).IntoProto(),
		SourceTimetick:         111,
		LastConfirmedMessageId: rmq.NewRmqID(110).IntoProto(),
	}

	require.NoError(t, state.applyCommittedWriteRecord(didRecord, true))
	require.NoError(t, state.applyCommittedWriteRecord(keylessRecord, true))

	pending, metaUpdate := state.consumePendingCommittedWriteRecords()
	require.Len(t, pending, 2)
	require.NotNil(t, metaUpdate)
	require.Equal(t, "key-1", pending[0].IdempotencyKey)
	require.Empty(t, pending[1].IdempotencyKey)
	require.False(t, state.dirty)
	require.Empty(t, state.pendingEntries)
	require.Empty(t, state.pendingRecords)
	pending, metaUpdate = state.consumePendingCommittedWriteRecords()
	require.Nil(t, pending)
	require.Nil(t, metaUpdate)
}

func TestSummaryCheckpointOnlyDoesNotForceMetaUpdate(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", &utility.WALCheckpoint{
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

func TestSummaryMaterializerApplyCommittedWriteRecordDuplicate(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", nil)
	first := committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
		Key:            "key-1",
		CommitTimetick: 100,
		MessageId:      rmq.NewRmqID(100).IntoProto(),
	})
	duplicate := committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
		Key:            "key-1",
		CommitTimetick: 101,
		MessageId:      rmq.NewRmqID(101).IntoProto(),
	})

	err := state.applyCommittedWriteRecordsAtGeneration([]*streamingpb.CommittedWriteRecord{first, duplicate}, 0)
	require.NoError(t, err)

	snapshot := state.snapshot()
	require.Len(t, snapshot.GetEntries(), 1)
	require.Equal(t, uint64(100), snapshot.GetEntries()[0].GetCommitTimetick())
	require.Equal(t, uint64(101), snapshot.GetSnapshotCheckpointTimetick())
}

func TestSummaryMaterializerReplacesDuplicateAfterTTL(t *testing.T) {
	state := newEmptyVChannelSummary("p1", "v1", nil)
	state.evictionCfg = summaryEvictionConfig{entryTTL: time.Second}
	firstTT := tsoutil.ComposeTS(100_000, 0)
	reusedTT := tsoutil.ComposeTS(102_000, 0)
	first := committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
		Key:            "key-1",
		CommitTimetick: firstTT,
		MessageId:      rmq.NewRmqID(100).IntoProto(),
	})
	reused := committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
		Key:            "key-1",
		CommitTimetick: reusedTT,
		MessageId:      rmq.NewRmqID(101).IntoProto(),
	})

	require.NoError(t, state.applyCommittedWriteRecordsAtGeneration([]*streamingpb.CommittedWriteRecord{first}, 0))
	require.NoError(t, state.applyCommittedWriteRecordsAtGeneration([]*streamingpb.CommittedWriteRecord{reused}, 1))

	snapshot := state.snapshot()
	require.Len(t, snapshot.GetEntries(), 1)
	require.Equal(t, reusedTT, snapshot.GetEntries()[0].GetCommitTimetick())
	require.Equal(t, uint64(1), state.latestAppliedGeneration)
	_, oldGenerationPinned := state.generationStats[0]
	require.False(t, oldGenerationPinned)
}

func TestPChannelSummaryChunkCodecRoundTrip(t *testing.T) {
	records := map[string][]*streamingpb.CommittedWriteRecord{
		"v2": {
			committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
				Key:            "key-2",
				CommitTimetick: 119,
				MessageId:      rmq.NewRmqID(119).IntoProto(),
			}),
		},
		"v1": {
			{
				SourceMessageId: rmq.NewRmqID(105).IntoProto(),
				SourceTimetick:  105,
				IdempotentResult: message.NewIdempotentInsertResult(
					[]uint32{0},
					&schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"pk-105"}}}},
				),
			},
			committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
				Key:            "key-1",
				CommitTimetick: 110,
				MessageId:      rmq.NewRmqID(110).IntoProto(),
			}),
		},
	}

	payload, footer, checksum, err := marshalPChannelSummaryChunk("p1", 7, 0, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, records)
	require.NoError(t, err)
	require.NotEmpty(t, checksum)
	require.Equal(t, uint64(7), footer.Generation)
	require.Equal(t, uint64(120), footer.SourceCheckpointTimetick)
	require.Len(t, footer.Chunks, 2)
	require.Equal(t, "v1", footer.Chunks[0].Vchannel)
	require.Equal(t, uint64(2), footer.Chunks[0].RecordCount)
	// The source span is a timetick range: nothing below the pchannel level
	// records a physical WAL position.
	require.Equal(t, uint64(105), footer.Chunks[0].SourceStartTimetick)
	require.Equal(t, uint64(110), footer.Chunks[0].SourceEndTimetick)
	require.Equal(t, "v2", footer.Chunks[1].Vchannel)
	require.Equal(t, uint64(1), footer.Chunks[1].RecordCount)
	require.Equal(t, uint64(105), footer.SourceStartTimetick)
	require.Equal(t, uint64(119), footer.SourceEndTimetick)

	decoded, _, decodedChecksum, err := unmarshalPChannelSummaryChunk(payload)
	require.NoError(t, err)
	require.Equal(t, checksum, decodedChecksum)
	require.Len(t, decoded, 2)
	require.Empty(t, decoded["v1"][0].IdempotencyKey)
	require.Equal(t, []string{"pk-105"}, decoded["v1"][0].IdempotentResult.GetIds().GetStrId().GetData())
	require.Equal(t, "key-1", decoded["v1"][1].IdempotencyKey)
	require.Equal(t, "key-2", decoded["v2"][0].IdempotencyKey)
}

func TestPChannelSummaryChunkCodecHasNoViewTypeAndTakesIdentityFromFooter(t *testing.T) {
	records := map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {
			committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
				Key:            "key-1",
				CommitTimetick: 110,
				MessageId:      rmq.NewRmqID(110).IntoProto(),
			}),
		},
	}

	payload, footer, _, err := marshalPChannelSummaryChunk("p1", 9, 0, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, records)
	require.NoError(t, err)
	// The chunk store is view-agnostic: a view type belongs to the etcd meta, not
	// to the physical payload.
	require.NotContains(t, string(payload), "view_type")
	require.Len(t, footer.Chunks, 1)
	chunkIndex := footer.Chunks[0]
	require.Equal(t, "v1", chunkIndex.Vchannel)
	require.Equal(t, uint64(9), footer.Generation)
	chunkPayload := payload[int(chunkIndex.Offset):int(chunkIndex.Offset+chunkIndex.Length)]
	require.NotContains(t, string(chunkPayload), "view_type")

	// The payload holds records only. Where they belong is the footer's to say,
	// so decoding takes the destination from the index rather than the bytes.
	decodedChunk, err := unmarshalVChannelSummaryChunk(chunkPayload)
	require.NoError(t, err)
	require.Len(t, decodedChunk.Records, 1)
	// The destination lives on the chunk, once, not on every record.
	require.Equal(t, "v1", decodedChunk.GetVchannel())
	require.Equal(t, "p1", footer.GetPchannel())
}

func TestPChannelSummaryChunkCodecCheckpointOnlyRoundTrip(t *testing.T) {
	payload, footer, checksum, err := marshalPChannelSummaryChunk("p1", 8, 0, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(200),
		TimeTick:  200,
	}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, payload)
	require.NotEmpty(t, checksum)
	require.Equal(t, uint64(8), footer.Generation)
	require.Equal(t, uint64(200), footer.SourceCheckpointTimetick)
	require.Empty(t, footer.Chunks)

	decodedRecords, decodedFooter, decodedChecksum, err := unmarshalPChannelSummaryChunk(payload)
	require.NoError(t, err)
	require.Equal(t, checksum, decodedChecksum)
	require.Empty(t, decodedRecords)
	require.Equal(t, uint64(8), decodedFooter.Generation)
	require.Equal(t, uint64(200), decodedFooter.SourceCheckpointTimetick)
}

func TestPChannelSummaryChunkCodecChecksumMismatch(t *testing.T) {
	records := map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {
			committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
				Key:            "key-1",
				CommitTimetick: 100,
				MessageId:      rmq.NewRmqID(100).IntoProto(),
			}),
		},
	}
	payload, _, _, err := marshalPChannelSummaryChunk("p1", 1, 0, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
	}, records)
	require.NoError(t, err)
	payload[len(pchannelSummaryChunkHeaderMagic)] ^= 0x01

	_, _, _, err = unmarshalPChannelSummaryChunk(payload)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
}

func TestPChannelSummaryChunkCodecDetectsFooterChecksumMismatch(t *testing.T) {
	payload, _, _, err := marshalPChannelSummaryChunk("p1", 1, 0, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {
			committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
				Key:            "key-1",
				CommitTimetick: 100,
				MessageId:      rmq.NewRmqID(100).IntoProto(),
			}),
		},
	})
	require.NoError(t, err)

	payload = rewritePChannelSummaryFooterPayload(t, payload, func(footer *streamingpb.PChannelSummaryChunkFooter) {
		footer.SourceEndTimetick++
	})

	_, _, _, err = unmarshalPChannelSummaryChunk(payload)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
	require.Contains(t, err.Error(), "pchannel summary chunk footer checksum mismatch")
}

func TestPChannelSummaryChunkCodecDetectsVChannelBlockChecksumMismatch(t *testing.T) {
	payload, footer, _, err := marshalPChannelSummaryChunk("p1", 1, 0, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {
			committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
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

	_, _, _, err = unmarshalPChannelSummaryChunk(mutated)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
	require.Contains(t, err.Error(), "vchannel summary chunk checksum mismatch")
}

func TestPChannelSummaryChunkKeyIsDeterministic(t *testing.T) {
	key := buildPChannelSummaryChunkKey("by-dev-rootcoord-dml_0", 42, 7)
	require.Contains(t, key, "/streamingnode/summary-store/by-dev-rootcoord-dml_0/chunks/chunk.42.term7.psc")
	require.NotContains(t, key, "manifests")
	require.NotContains(t, key, "checksum")
}

func rewritePChannelSummaryFooterPayload(
	t *testing.T,
	payload []byte,
	mutate func(*streamingpb.PChannelSummaryChunkFooter),
) []byte {
	t.Helper()
	footerMagicStart := len(payload) - len(pchannelSummaryChunkFooterMagic)
	require.GreaterOrEqual(t, footerMagicStart, pchannelSummaryChunkHeaderSize+pchannelSummaryChunkChecksumSize+4)
	require.Equal(t, pchannelSummaryChunkFooterMagic, payload[footerMagicStart:])
	footerLenStart := footerMagicStart - 4
	footerChecksumStart := footerLenStart - pchannelSummaryChunkChecksumSize
	footerLen := int(binary.BigEndian.Uint32(payload[footerLenStart:footerMagicStart]))
	footerStart := footerChecksumStart - footerLen
	require.GreaterOrEqual(t, footerStart, pchannelSummaryChunkHeaderSize)
	// Preserve the original (now stale) trailer checksum so mutating the footer
	// body is detected as corruption.
	staleChecksum := payload[footerChecksumStart:footerLenStart]
	// Go through the package's own codec rather than hand-rolling the encoding,
	// so this helper cannot drift from the format under test.
	footer, err := unmarshalPChannelSummaryChunkFooter(payload[footerStart:footerChecksumStart])
	require.NoError(t, err)
	mutate(footer)
	footerPayload, err := marshalPChannelSummaryChunkFooter(footer)
	require.NoError(t, err)
	mutated := make([]byte, 0, footerStart+len(footerPayload)+pchannelSummaryChunkChecksumSize+4+len(pchannelSummaryChunkFooterMagic))
	mutated = append(mutated, payload[:footerStart]...)
	mutated = append(mutated, footerPayload...)
	mutated = append(mutated, staleChecksum...)
	footerLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(footerLenBytes, uint32(len(footerPayload)))
	mutated = append(mutated, footerLenBytes...)
	mutated = append(mutated, pchannelSummaryChunkFooterMagic...)
	return mutated
}

func TestPChannelSummaryPersistRecover(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())
	records := map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {
			committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
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

	_, _, err := rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), records, nil, sourceCheckpoint)
	require.NoError(t, err)
	meta := catalogState.storeMeta
	require.NotNil(t, meta)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", meta.GetLatestGeneration(), true)
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
	recoverTestSummaries(ctx, t, recovered, "p1", false)
	require.Len(t, recovered.summaryManager.summaries(), 2)
	v1 := recovered.summaryManager.summaries()["v1"].snapshot()
	require.Len(t, v1.GetEntries(), 1)
	require.Equal(t, "key-1", v1.GetEntries()[0].GetKey())
	require.Equal(t, uint64(120), v1.GetSnapshotCheckpointTimetick())
	v2 := recovered.summaryManager.summaries()["v2"].snapshot()
	require.Empty(t, v2.GetEntries())
	require.Equal(t, uint64(120), v2.GetSnapshotCheckpointTimetick())
}

func TestPChannelSummaryRecoverWithContinuousChunks(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	records0 := map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {
			committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
				Key:                    "key-from-generation-0",
				CommitTimetick:         110,
				MessageId:              rmq.NewRmqID(110).IntoProto(),
				LastConfirmedMessageId: rmq.NewRmqID(109).IntoProto(),
			}),
		},
	}
	writeTestPChannelSummaryChunk(ctx, t, "p1", 0, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, records0)

	records1 := map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {
			committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
				Key:                    "key-from-generation-1",
				CommitTimetick:         130,
				MessageId:              rmq.NewRmqID(130).IntoProto(),
				LastConfirmedMessageId: rmq.NewRmqID(129).IntoProto(),
			}),
		},
	}
	footer, _, _ := writeTestPChannelSummaryChunk(ctx, t, "p1", 1, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(140),
		TimeTick:  140,
	}, records1)
	catalogState.storeMeta = newPChannelSummaryStoreMetaFromChunk("p1", footer, 0, 0).intoCatalogMeta()

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	recovered.SetLogger(resource.Resource().Logger())
	recoverTestSummaries(ctx, t, recovered, "p1", false)

	summary := recovered.summaryManager.summaries()["v1"].snapshot()
	require.Len(t, summary.GetEntries(), 2)
	require.Equal(t, "key-from-generation-0", summary.GetEntries()[0].GetKey())
	require.Equal(t, "key-from-generation-1", summary.GetEntries()[1].GetKey())
	require.Equal(t, uint64(140), summary.GetSnapshotCheckpointTimetick())
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinAvailableGeneration())
}

func TestPChannelSummaryRecoveryIgnoresStaleViewMinRequired(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	records0 := map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {
			committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
				Key:            "key-evicted",
				CommitTimetick: 110,
				MessageId:      rmq.NewRmqID(110).IntoProto(),
			}),
		},
	}
	writeTestPChannelSummaryChunk(ctx, t, "p1", 0, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, records0)

	records1 := map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {
			committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
				Key:            "key-retained",
				CommitTimetick: 130,
				MessageId:      rmq.NewRmqID(130).IntoProto(),
			}),
		},
	}
	footer, _, _ := writeTestPChannelSummaryChunk(ctx, t, "p1", 1, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(140),
		TimeTick:  140,
	}, records1)
	catalogState.storeMeta = newPChannelSummaryStoreMetaFromChunk("p1", footer, 0, 0).intoCatalogMeta()
	catalogState.summaryMetas["v1"] = &streamingpb.VChannelSummaryMeta{
		Pchannel:                   "p1",
		Vchannel:                   "v1",
		ViewType:                   common.VChannelSummaryViewTypeIdempotency,
		SnapshotCheckpointTimetick: 20,
		LatestAppliedGeneration:    1,
		MinRequiredGeneration:      1,
	}

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	recovered.SetLogger(resource.Resource().Logger())
	recoverTestSummaries(ctx, t, recovered, "p1", false)

	summary := recovered.summaryManager.summaries()["v1"].snapshot()
	require.Len(t, summary.GetEntries(), 2)
	require.Equal(t, "key-evicted", summary.GetEntries()[0].GetKey())
	require.Equal(t, "key-retained", summary.GetEntries()[1].GetKey())
	require.Equal(t, uint64(0), recovered.summaryManager.summaries()["v1"].minRequiredGeneration)
}

func TestPChannelSummaryRecoverFailsWhenGenerationHasHole(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	writeTestPChannelSummaryChunk(ctx, t, "p1", 0, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, nil)
	footer, _, _ := writeTestPChannelSummaryChunk(ctx, t, "p1", 2, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(140),
		TimeTick:  140,
	}, nil)
	catalogState.storeMeta = newPChannelSummaryStoreMetaFromChunk("p1", footer, 0, 0).intoCatalogMeta()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	recovered.SetLogger(resource.Resource().Logger())

	err := recoverTestSummariesWithError(ctx, recovered, "p1", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read pchannel summary chunk")
	require.Contains(t, err.Error(), "chunk.1.term0.psc")
}

func TestPChannelSummaryRecoverFailsWhenChunkMissing(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	catalogState.storeMeta = &streamingpb.PChannelSummaryMeta{
		Pchannel:                  "p1",
		LatestGeneration:          0,
		MinAvailableGeneration:    0,
		SourceCheckpointMessageId: rmq.NewRmqID(120).IntoProto(),
		SourceCheckpointTimetick:  120,
		CodecVersion:              uint32(pchannelSummaryCodecVersion),
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

	err := recoverTestSummariesWithError(ctx, recovered, "p1", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read pchannel summary chunk")
}

func TestPChannelSummaryRecoveryRepairsLaggingPChannelMeta(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	initialCheckpoint := &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(10),
		TimeTick:  10,
	}
	catalogState.storeMeta = writeTestBootstrapPChannelSummaryMeta(ctx, t, "p1", chunkManager, initialCheckpoint)

	records := map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {
			committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
				Key:                    "key-orphan",
				CommitTimetick:         110,
				MessageId:              rmq.NewRmqID(110).IntoProto(),
				LastConfirmedMessageId: rmq.NewRmqID(109).IntoProto(),
			}),
		},
	}
	writeTestPChannelSummaryChunk(ctx, t, "p1", 1, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, records)
	catalogState.summaryMetas["v1"] = &streamingpb.VChannelSummaryMeta{
		Pchannel:                   "p1",
		Vchannel:                   "v1",
		ViewType:                   common.VChannelSummaryViewTypeIdempotency,
		SnapshotCheckpointTimetick: 20,
		LatestAppliedGeneration:    1,
		MinRequiredGeneration:      1,
	}

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, initialCheckpoint)
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	recovered.SetLogger(resource.Resource().Logger())
	recoverTestSummaries(ctx, t, recovered, "p1", false)

	summary := recovered.summaryManager.summaries()["v1"].snapshot()
	require.Len(t, summary.GetEntries(), 1)
	require.Equal(t, "key-orphan", summary.GetEntries()[0].GetKey())
	require.Equal(t, uint64(120), summary.GetSnapshotCheckpointTimetick())
	require.Equal(t, uint64(1), catalogState.storeMeta.GetLatestGeneration())
	require.Equal(t, uint64(120), catalogState.storeMeta.GetSourceCheckpointTimetick())
}

func TestPChannelSummaryRecoveryDropsCorruptOrphanChunkAboveLatest(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	initialCheckpoint := &utility.WALCheckpoint{MessageID: rmq.NewRmqID(10), TimeTick: 10}
	catalogState.storeMeta = writeTestBootstrapPChannelSummaryMeta(ctx, t, "p1", chunkManager, initialCheckpoint)
	require.Equal(t, uint64(0), catalogState.storeMeta.GetLatestGeneration())

	// A persist wrote chunk generation 1 but crashed before advancing the meta
	// (still LatestGeneration=0), and the chunk on disk is corrupt/truncated.
	payload, _, _, err := marshalPChannelSummaryChunk("p1", 1, 0, &utility.WALCheckpoint{MessageID: rmq.NewRmqID(120), TimeTick: 120}, nil)
	require.NoError(t, err)
	corruptPayload := rewritePChannelSummaryFooterPayload(t, payload, func(footer *streamingpb.PChannelSummaryChunkFooter) {
		footer.SourceCheckpointTimetick = 999999
	})
	orphanKey := buildPChannelSummaryChunkKey("p1", 1, 0)
	require.NoError(t, chunkManager.Write(ctx, orphanKey, corruptPayload))

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, initialCheckpoint)
	recovered.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	recovered.SetLogger(resource.Resource().Logger())

	// Recovery must not fail on the corrupt orphan; the data is still in the WAL.
	require.NoError(t, recoverTestSummariesWithError(ctx, recovered, "p1", false))

	// The corrupt orphan must be deleted so the next persist can rewrite generation
	// 1 instead of wedging on a byte-mismatch forever.
	exists, err := chunkManager.Exist(ctx, orphanKey)
	require.NoError(t, err)
	require.False(t, exists, "corrupt orphan chunk above latest generation must be deleted")
	require.Equal(t, uint64(0), catalogState.storeMeta.GetLatestGeneration())
}

func TestPChannelSummaryRecoveryRewindsCheckpointByStore(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())
	_, _, err := rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), nil, nil, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	})
	require.NoError(t, err)

	recovered := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(150),
		TimeTick:  150,
	})
	recovered.SetLogger(resource.Resource().Logger())

	recoverTestSummaries(ctx, t, recovered, "p1", false)
	require.True(t, recovered.checkpoint.MessageID.EQ(rmq.NewRmqID(120)))
	require.Equal(t, uint64(120), recovered.checkpoint.TimeTick)
}

func TestPChannelSummaryRecoveryRewindsCheckpointByStoreAndFlusher(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())
	records := map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {
			committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
				Key:                    "key-1",
				CommitTimetick:         99,
				MessageId:              rmq.NewRmqID(99).IntoProto(),
				LastConfirmedMessageId: rmq.NewRmqID(98).IntoProto(),
			}),
		},
	}
	_, _, err := rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), records, nil, &utility.WALCheckpoint{
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

	recoverTestSummaries(ctx, t, recovered, "p1", false)
	require.True(t, recovered.checkpoint.MessageID.EQ(rmq.NewRmqID(90)))
	require.Equal(t, uint64(90), recovered.checkpoint.TimeTick)
}

func TestPChannelSummaryRecoveryReplayTailIdempotently(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelSummaryCatalog(t)
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
	_, _, err := rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {record100},
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
	recoverTestSummaries(ctx, t, recovered, "p1", false)
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

	summary := snapshot.SummarySnapshots["v1"]
	require.NotNil(t, summary)
	require.Len(t, summary.GetEntries(), 2)
	require.Equal(t, "key-1", summary.GetEntries()[0].GetKey())
	require.Equal(t, "key-2", summary.GetEntries()[1].GetKey())
	require.Equal(t, uint64(130), summary.GetSnapshotCheckpointTimetick())
}

func TestPChannelSummaryCrashBeforeChunkFallsBackToWALReplay(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	initialCheckpoint := &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(10),
		TimeTick:  10,
	}
	catalogState.storeMeta = writeTestBootstrapPChannelSummaryMeta(ctx, t, "p1", chunkManager, initialCheckpoint)
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
	recoverTestSummaries(ctx, t, recovered, "p1", false)

	snapshot, err := recovered.recoverFromStream(ctx, &streamBuilder{
		channel:   types.PChannelInfo{Name: "p1"},
		histories: []message.ImmutableMessage{msg100},
	}, message.CreateTestTimeTickSyncMessage(t, 100, 100, rmq.NewRmqID(100)).IntoImmutableMessage(rmq.NewRmqID(100)))
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	summary := snapshot.SummarySnapshots["v1"]
	require.NotNil(t, summary)
	require.Len(t, summary.GetEntries(), 1)
	require.Equal(t, "key-from-wal", summary.GetEntries()[0].GetKey())
	require.Equal(t, uint64(0), catalogState.storeMeta.GetLatestGeneration())
}

func TestPChannelSummaryCrashAfterConsumeCheckpointRecoversFromChunkWithoutReplay(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelSummaryCatalog(t)
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
	_, _, err := rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {record100},
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
	recoverTestSummaries(ctx, t, recovered, "p1", false)

	snapshot, err := recovered.recoverFromStream(ctx, &streamBuilder{
		channel:   types.PChannelInfo{Name: "p1"},
		histories: []message.ImmutableMessage{msg100},
	}, message.CreateTestTimeTickSyncMessage(t, 120, 120, rmq.NewRmqID(120)).IntoImmutableMessage(rmq.NewRmqID(120)))
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	summary := snapshot.SummarySnapshots["v1"]
	require.NotNil(t, summary)
	require.Len(t, summary.GetEntries(), 1)
	require.Equal(t, "key-from-chunk", summary.GetEntries()[0].GetKey())
	require.Equal(t, uint64(120), summary.GetSnapshotCheckpointTimetick())
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
		CollectionId: 1,
	}
	message.SetInsertHeaderIdempotentInsertResult(header, extra)
	return message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(header).
		WithBody(&msgpb.InsertRequest{}).
		WithIdempotencyKey(key).
		MustBuildMutable()
}

func TestPChannelSummaryRecoverFailsWhenLatestGenerationChunkMissing(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())
	_, _, err := rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), nil, nil, &utility.WALCheckpoint{
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

	err = recoverTestSummariesWithError(ctx, recovered, "p1", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pchannel summary chunk manifest misses latest generation 2")
}

func TestPChannelSummaryBootstrapCreatesGenerationZeroChunk(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
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

	recoverTestSummaries(ctx, t, rs, "p1", true)
	meta := catalogState.storeMeta
	require.NotNil(t, meta)
	require.Equal(t, uint64(0), meta.GetLatestGeneration())
	require.Equal(t, uint64(0), meta.GetMinAvailableGeneration())
	require.Equal(t, uint64(0), meta.GetMinInUseGeneration())
	require.Equal(t, uint64(10), meta.GetSourceCheckpointTimetick())

	payload, err := chunkManager.Read(ctx, buildPChannelSummaryChunkKey("p1", meta.GetLatestGeneration(), meta.GetTerm()))
	require.NoError(t, err)
	records, footer, _, err := unmarshalPChannelSummaryChunk(payload)
	require.NoError(t, err)
	require.Equal(t, uint64(0), footer.Generation)
	require.Equal(t, uint64(10), footer.SourceCheckpointTimetick)
	require.Empty(t, records)
	require.Len(t, rs.summaryManager.summaries(), 1)
}

func TestPChannelSummaryMissingMetaFailsWithoutBootstrap(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelSummaryCatalog(t)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	rs.SetLogger(resource.Resource().Logger())

	err := recoverTestSummariesWithError(ctx, rs, "p1", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pchannel summary meta missing")
}

func TestPChannelSummaryPersistsCheckpointOnlyGeneration(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
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

	_, generation, err := rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), nil, nil, sourceCheckpoint)
	require.NoError(t, err)
	require.Equal(t, uint64(0), generation)
	meta := catalogState.storeMeta
	require.NotNil(t, meta)
	require.Equal(t, uint64(200), meta.GetSourceCheckpointTimetick())
	require.Equal(t, uint64(0), meta.GetLatestGeneration())
	require.Equal(t, uint64(0), meta.GetMinAvailableGeneration())
	require.Equal(t, uint64(0), meta.GetMinInUseGeneration())

	payload, err := chunkManager.Read(ctx, buildPChannelSummaryChunkKey("p1", meta.GetLatestGeneration(), meta.GetTerm()))
	require.NoError(t, err)
	records, footer, _, err := unmarshalPChannelSummaryChunk(payload)
	require.NoError(t, err)
	require.Equal(t, uint64(0), footer.Generation)
	require.Equal(t, uint64(200), footer.SourceCheckpointTimetick)
	require.Empty(t, records)
	require.Equal(t, []string{"pchannel-summary-meta"}, catalogState.operations)
}

func TestForcePersistIdempotencySummaryToTimeTickPersistsCleanCheckpoint(t *testing.T) {
	enableRecoveryIdempotency(t)
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCASCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())

	checkpoint100 := &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
	}
	rs.summaryManager.setPChannelSummarySnapshotCheckpoint(checkpoint100)
	_, generation, err := rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), nil, nil, checkpoint100)
	require.NoError(t, err)
	require.Equal(t, uint64(0), generation)
	rs.summaryManager.markPChannelSummarySnapshotCheckpointPersisted(checkpoint100)

	checkpoint150 := &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(150),
		TimeTick:  150,
	}
	rs.summaryManager.advancePChannelSummarySnapshotCheckpoint(checkpoint150)
	persisted, err := rs.ForcePersistSummaryToTimeTick(ctx, checkpoint150.TimeTick)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	require.Equal(t, uint64(150), persisted.TimeTick)
	require.NotNil(t, catalogState.storeMeta)
	require.Equal(t, uint64(1), catalogState.storeMeta.GetLatestGeneration())
	require.Equal(t, uint64(150), catalogState.storeMeta.GetSourceCheckpointTimetick())
	require.Equal(t, int64(0), catalogState.storeMeta.GetTerm())

	payload, err := chunkManager.Read(ctx, buildPChannelSummaryChunkKey("p1", 1, 0))
	require.NoError(t, err)
	records, footer, _, err := unmarshalPChannelSummaryChunk(payload)
	require.NoError(t, err)
	require.Empty(t, records)
	require.Equal(t, uint64(1), footer.Generation)
	require.Equal(t, uint64(150), footer.SourceCheckpointTimetick)
}

func TestForcePersistIdempotencySummaryToTimeTickNoopsWhenDisabled(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.StreamingCfg.IdempotencyEnabled.Key, "false")
	t.Cleanup(func() { params.Reset(params.StreamingCfg.IdempotencyEnabled.Key) })

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})

	persisted, err := rs.ForcePersistSummaryToTimeTick(context.Background(), 150)
	require.NoError(t, err)
	require.NotNil(t, persisted)
	require.Equal(t, uint64(150), persisted.TimeTick)
}

func TestPChannelSummaryPersistWritesContinuousGenerationsWhenCheckpointAdvances(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.SetLogger(resource.Resource().Logger())

	for idx, timetick := range []uint64{100, 120, 140} {
		_, generation, err := rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), nil, nil, &utility.WALCheckpoint{
			MessageID: rmq.NewRmqID(int64(timetick)),
			TimeTick:  timetick,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(idx), generation)
		require.Equal(t, uint64(idx), catalogState.storeMeta.GetLatestGeneration())
		require.Equal(t, timetick, catalogState.storeMeta.GetSourceCheckpointTimetick())
	}

	for generation := uint64(0); generation <= 2; generation++ {
		chunkKey := buildPChannelSummaryChunkKey("p1", generation, 0)
		exists, err := chunkManager.Exist(ctx, chunkKey)
		require.NoError(t, err)
		require.True(t, exists)
		payload, err := chunkManager.Read(ctx, chunkKey)
		require.NoError(t, err)
		records, footer, _, err := unmarshalPChannelSummaryChunk(payload)
		require.NoError(t, err)
		require.Empty(t, records)
		require.Equal(t, generation, footer.Generation)
	}
	exists, err := chunkManager.Exist(ctx, buildPChannelSummaryChunkKey("p1", 3, 0))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestPChannelSummaryPersistEmptyActiveSummaryAdvancesMinInUse(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.summaryManager.setSummaries(map[string]*vchannelSummary{
		"v1": newEmptyVChannelSummary("p1", "v1", &utility.WALCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  1,
		}),
	})
	rs.SetLogger(resource.Resource().Logger())

	for idx, timetick := range []uint64{100, 120} {
		_, generation, err := rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), nil, nil, &utility.WALCheckpoint{
			MessageID: rmq.NewRmqID(int64(timetick)),
			TimeTick:  timetick,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(idx), generation)
	}

	require.Equal(t, uint64(1), catalogState.storeMeta.GetLatestGeneration())
	require.Equal(t, uint64(1), catalogState.storeMeta.GetMinInUseGeneration())
	rs.summaryManager.markVChannelSummariesPersisted(nil, nil, 1, nil)
	require.Equal(t, uint64(1), rs.summaryManager.summaries()["v1"].summaryMeta().GetMinRequiredGeneration())
}

func TestPChannelSummaryPersistSavesViewMetaAfterPChannelMeta(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	summary := newEmptyVChannelSummary("p1", "v1", nil)
	record := committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
		Key:            "key-1",
		CommitTimetick: 210,
		MessageId:      rmq.NewRmqID(210).IntoProto(),
	})
	require.NoError(t, summary.applyCommittedWriteRecord(record, true))
	records, metaUpdate := summary.consumePendingCommittedWriteRecords()

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(200),
		TimeTick:  200,
	})
	rs.summaryManager.setSummaries(map[string]*vchannelSummary{"v1": summary})
	rs.SetLogger(resource.Resource().Logger())

	summaryMetas, generation, err := rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), map[string][]*streamingpb.CommittedWriteRecord{
		"v1": records,
	}, map[string]*summaryMetaUpdate{
		"v1": metaUpdate,
	}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(220),
		TimeTick:  220,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(0), generation)
	require.Equal(t, uint64(0), summaryMetas["v1"].GetMinRequiredGeneration())
	require.Equal(t, []string{"pchannel-summary-meta", "vchannel-summary-meta"}, catalogState.operations)
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinInUseGeneration())
}

func TestPChannelSummaryPersistRetryDoesNotAllocateNextGenerationWhenCheckpointCovered(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
	})
	rs.SetLogger(resource.Resource().Logger())
	records := map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {
			committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
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

	_, generation, err := rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), records, nil, sourceCheckpoint)
	require.NoError(t, err)
	require.Equal(t, uint64(0), generation)
	_, generation, err = rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), records, nil, sourceCheckpoint)
	require.NoError(t, err)
	require.Equal(t, uint64(0), generation)
	require.Equal(t, uint64(0), catalogState.storeMeta.GetLatestGeneration())
	exists, err := chunkManager.Exist(ctx, buildPChannelSummaryChunkKey("p1", 1, 0))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestPChannelSummaryMetaMinInUseIncludesNonDirtySummaries(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	footer, _, _ := writeTestPChannelSummaryChunk(ctx, t, "p1", 4, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(400),
		TimeTick:  400,
	}, nil)
	catalogState.storeMeta = newPChannelSummaryStoreMetaFromChunk("p1", footer, 0, 4).intoCatalogMeta()

	oldSummary := newEmptyVChannelSummary("p1", "v-old", nil)
	require.NoError(t, oldSummary.applyCommittedWriteRecordsAtGeneration([]*streamingpb.CommittedWriteRecord{
		committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
			Key:            "key-old",
			CommitTimetick: 210,
			MessageId:      rmq.NewRmqID(210).IntoProto(),
		}),
	}, 2))
	newSummary := newEmptyVChannelSummary("p1", "v-new", nil)
	require.NoError(t, newSummary.applyCommittedWriteRecord(committedWriteRecordFromSummaryEntry(&streamingpb.SummaryEntry{
		Key:            "key-new",
		CommitTimetick: 410,
		MessageId:      rmq.NewRmqID(410).IntoProto(),
	}), true))
	records, metaUpdate := newSummary.consumePendingCommittedWriteRecords()

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(400),
		TimeTick:  400,
	})
	rs.summaryManager.setSummaries(map[string]*vchannelSummary{
		"v-old": oldSummary,
		"v-new": newSummary,
	})
	rs.SetLogger(resource.Resource().Logger())

	summaryMetas, generation, err := rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), map[string][]*streamingpb.CommittedWriteRecord{
		"v-new": records,
	}, map[string]*summaryMetaUpdate{
		"v-new": metaUpdate,
	}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(500),
		TimeTick:  500,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(5), generation)
	require.Equal(t, uint64(5), summaryMetas["v-new"].GetMinRequiredGeneration())
	require.Equal(t, uint64(2), catalogState.storeMeta.GetMinInUseGeneration())
}

func TestPChannelSummaryRejectsVChannelOnlyMeta(t *testing.T) {
	ctx := context.Background()
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().ListVChannelSummaryMetas(mock.Anything, "p1", common.VChannelSummaryViewTypeIdempotency).Return([]*streamingpb.VChannelSummaryMeta{
		{
			Pchannel:                   "p1",
			Vchannel:                   "v1",
			ViewType:                   "idempotency",
			SnapshotCheckpointTimetick: 100,
		},
	}, nil)
	catalog.EXPECT().GetPChannelSummaryMeta(mock.Anything, "p1").Return(nil, nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1"}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1,
	})
	rs.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	rs.SetLogger(resource.Resource().Logger())

	err := recoverTestSummariesWithError(ctx, rs, "p1", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pchannel summary meta missing")
}
