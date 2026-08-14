//go:build test
// +build test

package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// The chunk store's Exist->Write is not atomic, so two split-brain owners can
// both pass the absence check for the same generation. The footer term must
// arbitrate: the stale owner is fenced, the newer owner overwrites, and only a
// same-term payload mismatch remains corruption.
func TestWritePChannelSummaryChunkIfAbsentArbitratesByTerm(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	checkpoint := &utility.WALCheckpoint{MessageID: rmq.NewRmqID(100), TimeTick: 100}
	stalePayload, staleFooter, _, err := marshalPChannelSummaryChunk("p1", 7, 3, checkpoint, nil)
	require.NoError(t, err)
	currentPayload, currentFooter, _, err := marshalPChannelSummaryChunk("p1", 7, 5, checkpoint, nil)
	require.NoError(t, err)
	chunkKey := buildPChannelSummaryChunkKey("p1", 7)

	// A stale owner (term 3) must not overwrite the newer owner's chunk (term 5).
	require.NoError(t, chunkManager.Write(ctx, chunkKey, currentPayload))
	err = writePChannelSummaryChunkIfAbsent(ctx, chunkKey, stalePayload, staleFooter, 3)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreFenced)
	stored, err := chunkManager.Read(ctx, chunkKey)
	require.NoError(t, err)
	require.Equal(t, currentPayload, stored)

	// The newer owner overwrites a stale owner's leftover chunk.
	require.NoError(t, chunkManager.Write(ctx, chunkKey, stalePayload))
	require.NoError(t, writePChannelSummaryChunkIfAbsent(ctx, chunkKey, currentPayload, currentFooter, 5))
	stored, err = chunkManager.Read(ctx, chunkKey)
	require.NoError(t, err)
	require.Equal(t, currentPayload, stored)

	// Same term, different content: undecidable — corruption, as before.
	conflictPayload, conflictFooter, _, err := marshalPChannelSummaryChunk("p1", 7, 5, &utility.WALCheckpoint{MessageID: rmq.NewRmqID(200), TimeTick: 200}, nil)
	require.NoError(t, err)
	err = writePChannelSummaryChunkIfAbsent(ctx, chunkKey, conflictPayload, conflictFooter, 5)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
}

// A same-term retry of an identical chunk must stay idempotent even when the
// stored bytes differ. The payload encoding is protobuf, which is not guaranteed
// byte-stable across library versions, so a retry that spans a binary upgrade
// re-encodes the same records differently; arbitration must compare the decoded
// content, not the raw bytes, or a healthy retry would be reported as corruption.
func TestWritePChannelSummaryChunkIfAbsentAcceptsByteDifferentSameContentRetry(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	checkpoint := &utility.WALCheckpoint{MessageID: rmq.NewRmqID(100), TimeTick: 100}
	records := map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {{
			SourcePchannel:  "p1",
			Vchannel:        "v1",
			SourceMessageId: rmq.NewRmqID(101).IntoProto(),
			SourceTimetick:  101,
			IdempotencyKey:  "key-1",
			IdempotentResult: message.NewIdempotentInsertResult(
				[]uint32{0},
				&schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}},
			),
		}},
	}
	payload, footer, _, err := marshalPChannelSummaryChunk("p1", 7, 5, checkpoint, records)
	require.NoError(t, err)
	chunkKey := buildPChannelSummaryChunkKey("p1", 7)

	// Simulate a byte-different but semantically identical stored chunk by
	// padding the footer with a proto unknown field, which a decoder preserves
	// and ignores. The per-vchannel payloads and the footer's identity fields are
	// untouched, so the content comparison must accept it.
	stored, _ := repackChunkWithPaddedFooter(t, payload)
	require.NotEqual(t, payload, stored)
	require.NoError(t, chunkManager.Write(ctx, chunkKey, stored))

	require.NoError(t, writePChannelSummaryChunkIfAbsent(ctx, chunkKey, payload, footer, 5))
	// The stored chunk is left as-is: it already holds the same content.
	after, err := chunkManager.Read(ctx, chunkKey)
	require.NoError(t, err)
	require.Equal(t, stored, after)
}

// A cleaner whose assignment term is older than the term stamped in the durable
// meta must abort without deleting chunks or rewriting the meta.
func TestPChannelSummaryCleanerFencedByNewerTerm(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelSummaryChunks(t, ctx, "p1", chunkManager, 0, 3)
	catalogState.storeMeta = testPChannelSummaryStoreMeta(t, ctx, "p1", chunkManager, 3, 0, 0)
	catalogState.storeMeta.Term = 5

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1", Term: 3}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	addTestSummaryPinnedAtGeneration(rs.summaryManager, "v1", 2)
	rs.summaryManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	err := rs.summaryManager.cleanPChannelSummary(ctx, resource.Resource().Logger())
	require.ErrorIs(t, err, ErrPChannelSummaryStoreFenced)
	require.Equal(t, int64(5), catalogState.storeMeta.GetTerm())
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinInUseGeneration())
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinAvailableGeneration())
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 0, true)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 1, true)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 2, true)
	requirePChannelSummaryChunkExists(t, ctx, chunkManager, "p1", 3, true)
}

// An open whose assignment term is older than the durable meta's term is a
// stale split-brain open and must fail instead of recovering from (and later
// persisting over) the current owner's state.
func TestRecoverSummariesFencedByNewerTermMeta(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1", Term: 3}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	_, err := rs.summaryManager.recoverSummariesFromStore(context.Background(), "p1", &pchannelSummaryStoreMeta{PChannel: "p1", Term: 5})
	require.ErrorIs(t, err, ErrPChannelSummaryStoreFenced)
}

func TestPersistPChannelSummaryFencesBeforeSavingVChannelMetas(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCASCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	footer, _, _ := writeTestPChannelSummaryChunkWithTerm(ctx, t, "p1", 0, 5, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
	}, nil)
	catalogState.storeMeta = newPChannelSummaryStoreMetaFromChunk("p1", footer, 0, 0).intoCatalogMeta()

	summary := newEmptyVChannelSummary("p1", "v1", nil)
	record := committedWriteRecordFromSummaryEntry("p1", "v1", &streamingpb.SummaryEntry{
		Key:            "stale-key",
		CommitTimetick: 210,
		MessageId:      rmq.NewRmqID(210).IntoProto(),
	})
	require.NoError(t, summary.applyCommittedWriteRecord(record, true))
	records, metaUpdate := summary.consumePendingCommittedWriteRecords()

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1", Term: 3}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(200),
		TimeTick:  200,
	})
	rs.summaryManager.setSummaries(map[string]*vchannelSummary{"v1": summary})
	rs.SetLogger(resource.Resource().Logger())

	_, _, err := rs.summaryManager.persistPChannelSummary(ctx, resource.Resource().Logger(), map[string][]*streamingpb.CommittedWriteRecord{
		"v1": records,
	}, map[string]*summaryMetaUpdate{
		"v1": metaUpdate,
	}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(220),
		TimeTick:  220,
	})
	require.ErrorIs(t, err, ErrPChannelSummaryStoreFenced)
	require.NotContains(t, catalogState.operations, "vchannel-summary-meta")
	require.Empty(t, catalogState.summaryMetas)
}

func TestRecoverSummariesReadsOnlyManifestPublishedTerm(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCASCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	staleRecords := map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {
			committedWriteRecordFromSummaryEntry("p1", "v1", &streamingpb.SummaryEntry{
				Key:            "stale-key",
				CommitTimetick: 210,
				MessageId:      rmq.NewRmqID(210).IntoProto(),
			}),
		},
	}
	currentRecords := map[string][]*streamingpb.CommittedWriteRecord{
		"v1": {
			committedWriteRecordFromSummaryEntry("p1", "v1", &streamingpb.SummaryEntry{
				Key:            "current-key",
				CommitTimetick: 220,
				MessageId:      rmq.NewRmqID(220).IntoProto(),
			}),
		},
	}
	writeTestPChannelSummaryChunkWithTerm(ctx, t, "p1", 1, 1, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(210),
		TimeTick:  210,
	}, staleRecords)
	writeTestPChannelSummaryChunkWithTerm(ctx, t, "p1", 1, 2, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(220),
		TimeTick:  220,
	}, currentRecords)
	catalogState.storeMeta = &streamingpb.PChannelSummaryMeta{
		Pchannel:                  "p1",
		SourceCheckpointTimetick:  220,
		SourceCheckpointMessageId: rmq.NewRmqID(220).IntoProto(),
		LatestGeneration:          1,
		MinAvailableGeneration:    1,
		MinInUseGeneration:        1,
		CodecVersion:              uint32(pchannelSummaryCodecVersion),
		Term:                      2,
		ChunkManifest: &streamingpb.PChannelSummaryChunkManifest{
			Ranges: []*streamingpb.PChannelSummaryChunkTermRange{
				{Term: 1, StartGeneration: 0, EndGeneration: 0, StartTimetick: 100, EndTimetick: 100, Sealed: true},
				{Term: 2, StartGeneration: 1, EndGeneration: 1, StartTimetick: 220, EndTimetick: 220},
			},
		},
	}

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1", Term: 2}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(230),
		TimeTick:  230,
	})
	rs.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	rs.SetLogger(resource.Resource().Logger())

	recoverTestSummaries(ctx, t, rs, "p1", false)
	summary := rs.summaryManager.summaries()["v1"]
	require.NotNil(t, summary)
	require.Contains(t, summary.entries, "current-key")
	require.NotContains(t, summary.entries, "stale-key")
}

func TestRecoverSummariesRejectsManifestFooterTermMismatch(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCASCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	payload, _, _, err := marshalPChannelSummaryChunk("p1", 0, 1, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
	}, nil)
	require.NoError(t, err)
	require.NoError(t, chunkManager.Write(ctx, buildPChannelSummaryChunkKey("p1", 0, 2), payload))
	catalogState.storeMeta = &streamingpb.PChannelSummaryMeta{
		Pchannel:                  "p1",
		SourceCheckpointTimetick:  100,
		SourceCheckpointMessageId: rmq.NewRmqID(100).IntoProto(),
		LatestGeneration:          0,
		MinAvailableGeneration:    0,
		MinInUseGeneration:        0,
		CodecVersion:              uint32(pchannelSummaryCodecVersion),
		Term:                      2,
		ChunkManifest: &streamingpb.PChannelSummaryChunkManifest{
			Ranges: []*streamingpb.PChannelSummaryChunkTermRange{
				{Term: 2, StartGeneration: 0, EndGeneration: 0, StartTimetick: 100, EndTimetick: 100},
			},
		},
	}

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1", Term: 2}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
	})
	rs.vchannels = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{
		{Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL},
	})
	rs.SetLogger(resource.Resource().Logger())

	err = recoverTestSummariesWithError(ctx, rs, "p1", false)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
	require.Contains(t, err.Error(), "term mismatch")
}

func TestRecoverSummariesSealsPreviousTermByScanningChunks(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelSummaryCASCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	for generation := uint64(0); generation <= 12; generation++ {
		writeTestPChannelSummaryChunkWithTerm(ctx, t, "p1", generation, 4, chunkManager, &utility.WALCheckpoint{
			MessageID: rmq.NewRmqID(int64(100 + generation)),
			TimeTick:  100 + generation,
		}, nil)
	}
	catalogState.storeMeta = &streamingpb.PChannelSummaryMeta{
		Pchannel:                  "p1",
		SourceCheckpointTimetick:  100,
		SourceCheckpointMessageId: rmq.NewRmqID(100).IntoProto(),
		LatestGeneration:          0,
		MinAvailableGeneration:    0,
		MinInUseGeneration:        0,
		CodecVersion:              uint32(pchannelSummaryCodecVersion),
		Term:                      4,
		ChunkManifest: &streamingpb.PChannelSummaryChunkManifest{
			Ranges: []*streamingpb.PChannelSummaryChunkTermRange{
				{Term: 4, StartGeneration: 0, EndGeneration: 0, StartTimetick: 100, EndTimetick: 100},
			},
		},
	}

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1", Term: 5}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(200),
		TimeTick:  200,
	})
	rs.SetLogger(resource.Resource().Logger())

	recovered, err := rs.summaryManager.recoverSummariesFromStore(ctx, "p1", pchannelSummaryStoreMetaFromCatalog(catalogState.storeMeta))
	require.NoError(t, err)
	require.Equal(t, uint64(12), recovered.LatestGeneration)
	require.Equal(t, int64(5), catalogState.storeMeta.GetTerm())
	require.Equal(t, uint64(12), catalogState.storeMeta.GetLatestGeneration())
	require.Equal(t, uint64(112), catalogState.storeMeta.GetSourceCheckpointTimetick())
	require.Len(t, catalogState.storeMeta.GetChunkManifest().GetRanges(), 1)
	sealedRange := catalogState.storeMeta.GetChunkManifest().GetRanges()[0]
	require.Equal(t, int64(4), sealedRange.GetTerm())
	require.Equal(t, uint64(0), sealedRange.GetStartGeneration())
	require.Equal(t, uint64(12), sealedRange.GetEndGeneration())
	require.True(t, sealedRange.GetSealed())
	require.GreaterOrEqual(t, len(catalogState.operations), 2)
}

func TestRecoverSummariesAdoptsMultipleCurrentTermOrphanChunks(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelSummaryCASCatalog(t)
	chunkManager := newTestPChannelSummaryCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	footer, _, _ := writeTestPChannelSummaryChunkWithTerm(ctx, t, "p1", 0, 2, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
	}, nil)
	writeTestPChannelSummaryChunkWithTerm(ctx, t, "p1", 1, 2, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(110),
		TimeTick:  110,
	}, nil)
	writeTestPChannelSummaryChunkWithTerm(ctx, t, "p1", 2, 2, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, nil)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1", Term: 2}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	})
	rs.SetLogger(resource.Resource().Logger())

	recovered, err := rs.summaryManager.recoverSummariesFromStore(ctx, "p1", newPChannelSummaryStoreMetaFromChunk("p1", footer, 0, 0))
	require.NoError(t, err)
	require.Equal(t, uint64(2), recovered.LatestGeneration)
	require.Len(t, recovered.ChunkManifest.GetRanges(), 1)
	require.Equal(t, uint64(0), recovered.ChunkManifest.GetRanges()[0].GetStartGeneration())
	require.Equal(t, uint64(2), recovered.ChunkManifest.GetRanges()[0].GetEndGeneration())
	require.Equal(t, uint64(120), recovered.SourceCheckpoint.TimeTick)
}

func TestPChannelSummaryManifestRejectsTermSwitchGenerationGap(t *testing.T) {
	manifest := &streamingpb.PChannelSummaryChunkManifest{
		Ranges: []*streamingpb.PChannelSummaryChunkTermRange{
			{Term: 1, StartGeneration: 0, EndGeneration: 5, Sealed: true},
		},
	}

	_, err := pchannelSummaryManifestWithChunk(manifest, 3, 7, 700)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
	require.Contains(t, err.Error(), "generation gap")
}
