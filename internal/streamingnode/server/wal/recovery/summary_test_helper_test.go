package recovery

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// enableRecoveryIdempotency turns the summary store on for a test and restores
// the parameter afterwards.
func enableRecoveryIdempotency(t *testing.T) {
	t.Helper()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.StreamingCfg.IdempotencyEnabled.Key, "true"))
	t.Cleanup(func() { _ = params.Reset(params.StreamingCfg.IdempotencyEnabled.Key) })
}

// newTestSummaryStoreChunkManager roots the store at a per-test temp directory.
// The root MUST come straight from t.TempDir(): chunk keys are built from the
// chunk manager's own root and LocalChunkManager writes a key verbatim, so a
// relative root would drop the chunk files into the package directory.
func newTestSummaryStoreChunkManager(t *testing.T) storage.ChunkManager {
	t.Helper()
	return storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
}

func requirePChannelSummaryChunkExists(
	t *testing.T,
	ctx context.Context,
	chunkManager storage.ChunkManager,
	pchannel string,
	generation uint64,
	term int64,
	expected bool,
) {
	t.Helper()
	exists, err := chunkManager.Exist(ctx, buildPChannelSummaryChunkKey(chunkManager, pchannel, generation, term))
	require.NoError(t, err)
	require.Equal(t, expected, exists)
}

func testRecoveryCheckpoint(messageID int64, timetick uint64) *WALCheckpoint {
	return &WALCheckpoint{
		MessageID: rmq.NewRmqID(messageID),
		TimeTick:  timetick,
	}
}

// newTestSummaryRecord builds a record the way the write path does: a key, the
// primary keys it produced, and the offsets that map them back into the client's
// request. Those three land in different chunk sections, so a test that uses
// this exercises the split rather than one section of it.
func newTestSummaryRecord(key string, timetick uint64, ids ...int64) *SummaryRecord {
	record := &SummaryRecord{
		SourceMessageID:        rmq.NewRmqID(int64(timetick)).IntoProto(),
		SourceTimeTick:         timetick,
		LastConfirmedMessageID: rmq.NewRmqID(int64(timetick) - 1).IntoProto(),
		IdempotencyKey:         key,
	}
	if len(ids) > 0 {
		rowOffsets := make([]uint32, 0, len(ids))
		for i := range ids {
			rowOffsets = append(rowOffsets, uint32(i))
		}
		record.InsertResult = &messagespb.IdempotentInsertResult{
			RowOffsets: rowOffsets,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}},
			},
		}
	}
	return record
}

// newTestSummaryRecords builds count records for one vchannel, each carrying a
// single primary key, with timeticks starting at baseTimetick.
func newTestSummaryRecords(keyPrefix string, baseTimetick uint64, count int) []*SummaryRecord {
	records := make([]*SummaryRecord, 0, count)
	for i := 0; i < count; i++ {
		timetick := baseTimetick + uint64(i)
		records = append(records, newTestSummaryRecord(fmt.Sprintf("%s-%d", keyPrefix, i), timetick, int64(timetick)))
	}
	return records
}

// writeTestPChannelSummaryChunk frames and stores one chunk directly, bypassing
// the persist path, so a recovery test can set up the durable state it wants.
func writeTestPChannelSummaryChunk(
	ctx context.Context,
	t *testing.T,
	chunkManager storage.ChunkManager,
	pchannel string,
	generation uint64,
	term int64,
	recordsByVChannel map[string][]*SummaryRecord,
) *streamingpb.PChannelSummaryChunkIndexEntry {
	t.Helper()
	payload, footer, err := marshalPChannelSummaryChunk(pchannel, generation, term, recordsByVChannel)
	require.NoError(t, err)
	key := buildPChannelSummaryChunkKey(chunkManager, pchannel, generation, term)
	require.NoError(t, chunkManager.Write(ctx, key, payload))
	return chunkIndexEntryFromFooter(footer, uint64(len(payload)))
}

// writeTestPChannelSummaryManifest stores a manifest for one term directly.
func writeTestPChannelSummaryManifest(
	ctx context.Context,
	t *testing.T,
	pchannel string,
	term int64,
	entries ...*streamingpb.PChannelSummaryChunkIndexEntry,
) {
	t.Helper()
	manifest := &streamingpb.PChannelSummaryManifest{Chunks: entries}
	require.NoError(t, writePChannelSummaryManifest(ctx, pchannel, term, manifest))
}

type testPChannelSummaryCatalogState struct {
	storeMeta  *streamingpb.PChannelSummaryMeta
	operations []string
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

func (c *testPChannelSummaryCASCatalog) CompareAndSwapPChannelSummaryMeta(
	ctx context.Context,
	pchannelName string,
	expected *streamingpb.PChannelSummaryMeta,
	target *streamingpb.PChannelSummaryMeta,
) (bool, error) {
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
	state := &testPChannelSummaryCatalogState{}
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
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
	catalog.EXPECT().RemovePChannelSummaryMeta(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string) error {
		state.operations = append(state.operations, "remove-pchannel-summary-meta")
		state.storeMeta = nil
		return nil
	}).Maybe()
	return catalog, state
}

// newTestSummaryManager builds a manager wired to a config, without the recovery
// bootstrap, for tests that drive the persist/retention paths directly.
func newTestSummaryManager(t *testing.T, pchannel string, term int64, cfg *config) *summaryManager {
	t.Helper()
	manager := newSummaryManager(pchannel, term, cfg, newRecoveryStorageMetrics(types.PChannelInfo{Name: pchannel, Term: term}), summaryEvictionConfig{})
	manager.SetLogger(resource.Resource().Logger())
	t.Cleanup(func() { manager.metrics.Close() })
	return manager
}

func newTestSummaryConfig() *config {
	return &config{
		idempotencyEnabled:           true,
		idempotencyMinRetainedBytes:  1 << 20,
		idempotencyMaxRetainedChunks: 256,
		idempotencyRetentionTTL:      0,
		idempotencyGCInterval:        0,
		gracefulTimeout:              0,
	}
}
