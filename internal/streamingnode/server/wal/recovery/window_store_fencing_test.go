//go:build test
// +build test

package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// The chunk store's Exist->Write is not atomic, so two split-brain owners can
// both pass the absence check for the same generation. The footer term must
// arbitrate: the stale owner is fenced, the newer owner overwrites, and only a
// same-term payload mismatch remains corruption.
func TestWritePChannelWindowChunkIfAbsentArbitratesByTerm(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelWindowCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	checkpoint := &utility.WALCheckpoint{MessageID: rmq.NewRmqID(100), TimeTick: 100}
	stalePayload, _, _, err := marshalPChannelWindowChunk("p1", 7, 3, checkpoint, nil)
	require.NoError(t, err)
	currentPayload, _, _, err := marshalPChannelWindowChunk("p1", 7, 5, checkpoint, nil)
	require.NoError(t, err)
	chunkKey := buildPChannelWindowChunkKey("p1", 7)

	// A stale owner (term 3) must not overwrite the newer owner's chunk (term 5).
	require.NoError(t, chunkManager.Write(ctx, chunkKey, currentPayload))
	err = writePChannelWindowChunkIfAbsent(ctx, chunkKey, stalePayload, 3)
	require.ErrorIs(t, err, ErrPChannelWindowStoreFenced)
	stored, err := chunkManager.Read(ctx, chunkKey)
	require.NoError(t, err)
	require.Equal(t, currentPayload, stored)

	// The newer owner overwrites a stale owner's leftover chunk.
	require.NoError(t, chunkManager.Write(ctx, chunkKey, stalePayload))
	require.NoError(t, writePChannelWindowChunkIfAbsent(ctx, chunkKey, currentPayload, 5))
	stored, err = chunkManager.Read(ctx, chunkKey)
	require.NoError(t, err)
	require.Equal(t, currentPayload, stored)

	// Same term, different payload: undecidable — corruption, as before.
	conflictPayload, _, _, err := marshalPChannelWindowChunk("p1", 7, 5, &utility.WALCheckpoint{MessageID: rmq.NewRmqID(200), TimeTick: 200}, nil)
	require.NoError(t, err)
	err = writePChannelWindowChunkIfAbsent(ctx, chunkKey, conflictPayload, 5)
	require.ErrorIs(t, err, ErrPChannelWindowStoreCorrupted)
}

// A cleaner whose assignment term is older than the term stamped in the durable
// meta must abort without deleting chunks or rewriting the meta.
func TestPChannelWindowCleanerFencedByNewerTerm(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))
	writeTestPChannelWindowChunks(t, ctx, "p1", chunkManager, 0, 3)
	catalogState.storeMeta = testPChannelWindowStoreMeta(t, ctx, "p1", chunkManager, 3, 0, 0)
	catalogState.storeMeta.Term = 5

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1", Term: 3}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	addTestIdempotencyWindowPinnedAtGeneration(rs.windowManager, "v1", 2)
	rs.windowManager.markActiveViewsInitialized()
	rs.SetLogger(resource.Resource().Logger())

	err := rs.windowManager.cleanPChannelWindow(ctx, resource.Resource().Logger())
	require.ErrorIs(t, err, ErrPChannelWindowStoreFenced)
	require.Equal(t, int64(5), catalogState.storeMeta.GetTerm())
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinInUseGeneration())
	require.Equal(t, uint64(0), catalogState.storeMeta.GetMinAvailableGeneration())
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 0, true)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 1, true)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 2, true)
	requirePChannelWindowChunkExists(t, ctx, chunkManager, "p1", 3, true)
}

// An open whose assignment term is older than the durable meta's term is a
// stale split-brain open and must fail instead of recovering from (and later
// persisting over) the current owner's state.
func TestRecoverWindowsFencedByNewerTermMeta(t *testing.T) {
	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1", Term: 3}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(300),
		TimeTick:  300,
	})
	_, err := rs.windowManager.recoverIdempotencyWindowsFromPChannelWindowStore(context.Background(), "p1", &pchannelWindowStoreMeta{PChannel: "p1", Term: 5})
	require.ErrorIs(t, err, ErrPChannelWindowStoreFenced)
}

func TestPersistPChannelWindowFencesBeforeSavingVChannelMetas(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCASCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	footer, _, _ := writeTestPChannelWindowChunkWithTerm(ctx, t, "p1", 0, 5, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
	}, nil)
	catalogState.storeMeta = newPChannelWindowStoreMetaFromChunk("p1", footer, 0, 0).intoCatalogMeta()

	window := newEmptyVChannelWindow("p1", "v1", nil)
	record := *committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
		Key:            "stale-key",
		CommitTimetick: 210,
		MessageId:      rmq.NewRmqID(210).IntoProto(),
	})
	require.NoError(t, window.applyCommittedWriteRecord(record, true))
	records, metaUpdate := window.consumePendingCommittedWriteRecords()

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1", Term: 3}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(200),
		TimeTick:  200,
	})
	rs.windowManager.setIdempotencyWindows(map[string]*vchannelWindow{"v1": window})
	rs.SetLogger(resource.Resource().Logger())

	_, _, err := rs.windowManager.persistPChannelWindow(ctx, resource.Resource().Logger(), map[string][]committedWriteRecord{
		"v1": records,
	}, map[string]*idempotencyWindowMetaUpdate{
		"v1": metaUpdate,
	}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(220),
		TimeTick:  220,
	})
	require.ErrorIs(t, err, ErrPChannelWindowStoreFenced)
	require.NotContains(t, catalogState.operations, "vchannel-window-meta")
	require.Empty(t, catalogState.windowMetas)
}

func TestRecoverWindowsReadsOnlyManifestPublishedTerm(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCASCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	staleRecords := map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:            "stale-key",
				CommitTimetick: 210,
				MessageId:      rmq.NewRmqID(210).IntoProto(),
			}),
		},
	}
	currentRecords := map[string][]committedWriteRecord{
		"v1": {
			*committedWriteRecordFromWindowEntry("p1", "v1", &streamingpb.WindowEntry{
				Key:            "current-key",
				CommitTimetick: 220,
				MessageId:      rmq.NewRmqID(220).IntoProto(),
			}),
		},
	}
	writeTestPChannelWindowChunkWithTerm(ctx, t, "p1", 1, 1, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(210),
		TimeTick:  210,
	}, staleRecords)
	writeTestPChannelWindowChunkWithTerm(ctx, t, "p1", 1, 2, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(220),
		TimeTick:  220,
	}, currentRecords)
	catalogState.storeMeta = &streamingpb.PChannelWindowMeta{
		Pchannel:                  "p1",
		SourceCheckpointTimetick:  220,
		SourceCheckpointMessageId: rmq.NewRmqID(220).IntoProto(),
		LatestGeneration:          1,
		MinAvailableGeneration:    1,
		MinInUseGeneration:        1,
		CodecVersion:              uint32(pchannelWindowCodecVersion),
		Term:                      2,
		ChunkManifest: &streamingpb.PChannelWindowChunkManifest{
			Ranges: []*streamingpb.PChannelWindowChunkTermRange{
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

	recoverTestIdempotencyWindows(ctx, t, rs, "p1", false)
	window := rs.windowManager.idempotencyWindows()["v1"]
	require.NotNil(t, window)
	require.Contains(t, window.entries, "current-key")
	require.NotContains(t, window.entries, "stale-key")
}

func TestRecoverWindowsRejectsManifestFooterTermMismatch(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCASCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	payload, _, _, err := marshalPChannelWindowChunk("p1", 0, 1, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
	}, nil)
	require.NoError(t, err)
	require.NoError(t, chunkManager.Write(ctx, buildPChannelWindowChunkKey("p1", 0, 2), payload))
	catalogState.storeMeta = &streamingpb.PChannelWindowMeta{
		Pchannel:                  "p1",
		SourceCheckpointTimetick:  100,
		SourceCheckpointMessageId: rmq.NewRmqID(100).IntoProto(),
		LatestGeneration:          0,
		MinAvailableGeneration:    0,
		MinInUseGeneration:        0,
		CodecVersion:              uint32(pchannelWindowCodecVersion),
		Term:                      2,
		ChunkManifest: &streamingpb.PChannelWindowChunkManifest{
			Ranges: []*streamingpb.PChannelWindowChunkTermRange{
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

	err = recoverTestIdempotencyWindowsWithError(ctx, rs, "p1", false)
	require.ErrorIs(t, err, ErrPChannelWindowStoreCorrupted)
	require.Contains(t, err.Error(), "term mismatch")
}

func TestRecoverWindowsSealsPreviousTermByScanningChunks(t *testing.T) {
	ctx := context.Background()
	catalog, catalogState := newTestPChannelWindowCASCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	for generation := uint64(0); generation <= 12; generation++ {
		writeTestPChannelWindowChunkWithTerm(ctx, t, "p1", generation, 4, chunkManager, &utility.WALCheckpoint{
			MessageID: rmq.NewRmqID(int64(100 + generation)),
			TimeTick:  100 + generation,
		}, nil)
	}
	catalogState.storeMeta = &streamingpb.PChannelWindowMeta{
		Pchannel:                  "p1",
		SourceCheckpointTimetick:  100,
		SourceCheckpointMessageId: rmq.NewRmqID(100).IntoProto(),
		LatestGeneration:          0,
		MinAvailableGeneration:    0,
		MinInUseGeneration:        0,
		CodecVersion:              uint32(pchannelWindowCodecVersion),
		Term:                      4,
		ChunkManifest: &streamingpb.PChannelWindowChunkManifest{
			Ranges: []*streamingpb.PChannelWindowChunkTermRange{
				{Term: 4, StartGeneration: 0, EndGeneration: 0, StartTimetick: 100, EndTimetick: 100},
			},
		},
	}

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1", Term: 5}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(200),
		TimeTick:  200,
	})
	rs.SetLogger(resource.Resource().Logger())

	recovered, err := rs.windowManager.recoverIdempotencyWindowsFromPChannelWindowStore(ctx, "p1", pchannelWindowStoreMetaFromCatalog(catalogState.storeMeta))
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

func TestRecoverWindowsAdoptsMultipleCurrentTermOrphanChunks(t *testing.T) {
	ctx := context.Background()
	catalog, _ := newTestPChannelWindowCASCatalog(t)
	chunkManager := newTestPChannelWindowCleanerChunkManager()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(chunkManager))

	footer, _, _ := writeTestPChannelWindowChunkWithTerm(ctx, t, "p1", 0, 2, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  100,
	}, nil)
	writeTestPChannelWindowChunkWithTerm(ctx, t, "p1", 1, 2, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(110),
		TimeTick:  110,
	}, nil)
	writeTestPChannelWindowChunkWithTerm(ctx, t, "p1", 2, 2, chunkManager, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	}, nil)

	rs := newRecoveryStorage(types.PChannelInfo{Name: "p1", Term: 2}, &utility.WALCheckpoint{
		MessageID: rmq.NewRmqID(120),
		TimeTick:  120,
	})
	rs.SetLogger(resource.Resource().Logger())

	recovered, err := rs.windowManager.recoverIdempotencyWindowsFromPChannelWindowStore(ctx, "p1", newPChannelWindowStoreMetaFromChunk("p1", footer, 0, 0))
	require.NoError(t, err)
	require.Equal(t, uint64(2), recovered.LatestGeneration)
	require.Len(t, recovered.ChunkManifest.GetRanges(), 1)
	require.Equal(t, uint64(0), recovered.ChunkManifest.GetRanges()[0].GetStartGeneration())
	require.Equal(t, uint64(2), recovered.ChunkManifest.GetRanges()[0].GetEndGeneration())
	require.Equal(t, uint64(120), recovered.SourceCheckpoint.TimeTick)
}

func TestPChannelWindowManifestRejectsTermSwitchGenerationGap(t *testing.T) {
	manifest := &streamingpb.PChannelWindowChunkManifest{
		Ranges: []*streamingpb.PChannelWindowChunkTermRange{
			{Term: 1, StartGeneration: 0, EndGeneration: 5, Sealed: true},
		},
	}

	_, err := pchannelWindowManifestWithChunk(manifest, 3, 7, 700)
	require.ErrorIs(t, err, ErrPChannelWindowStoreCorrupted)
	require.Contains(t, err.Error(), "generation gap")
}
