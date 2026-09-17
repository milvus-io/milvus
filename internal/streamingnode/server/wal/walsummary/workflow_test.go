package walsummary

import (
	"context"
	"fmt"
	"maps"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func nextTermStore(s *Store) *Store { return NewStore(s.chunkManager, s.PChannel(), s.Term()+1) }

func testRecordRange(sections map[string]*ChunkSections) TimeTickRange {
	start, end := uint64(math.MaxUint64), uint64(0)
	for _, section := range sections {
		for _, record := range section.Inserts {
			start = min(start, record.GetSourceTimetick())
			end = max(end, record.GetSourceTimetick())
		}
		for _, record := range section.Transform {
			start = min(start, record.GetTimeTick())
			end = max(end, record.GetTimeTick())
		}
	}
	if start == math.MaxUint64 {
		return TimeTickRange{End: 1}
	}
	if start > 0 {
		start--
	}
	return TimeTickRange{Start: start, End: end}
}

func stageChunk(t *testing.T, m *Manager, tt uint64) *chunkWriteTask {
	t.Helper()
	m.ObserveMessage(context.Background(), newTestIdempotentInsertMessage(t, "v1", tt, fmt.Sprint(tt), []int64{int64(tt)}, []uint32{0}))
	m.requestSeal()
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pendingSealed[len(m.pendingSealed)-1].task
}

func TestConcurrentCompletionAndFirstPublication(t *testing.T) {
	ctx := context.Background()
	m, store := newTestManagerWithStore(t)
	first, second, third := stageChunk(t, m, 100), stageChunk(t, m, 200), stageChunk(t, m, 300)
	require.NoError(t, third.Execute(ctx))
	require.NoError(t, second.Execute(ctx))
	require.Empty(t, m.Manifest().Chunks)
	require.Nil(t, m.manifestTask, "out-of-order completion cannot publish a hole")
	require.Equal(t, uint64(0), m.LastAcked())
	require.NoError(t, first.Execute(ctx))
	require.Len(t, m.Manifest().Chunks, 3)
	require.Equal(t, uint64(0), m.LastAcked(), "first manifest must be discoverable")
	failure := errors.New("PUT unavailable")
	patch := mockey.Mock((*Store).WriteManifest).Return(failure).Build()
	defer patch.UnPatch()
	requireSummaryError(t, m.manifestTask.Execute(ctx), nodescheduler.ErrDelay)
	require.Equal(t, uint64(0), m.LastAcked())
	patch.UnPatch()
	require.NoError(t, m.manifestTask.Execute(ctx))
	require.Equal(t, uint64(300), m.LastAcked())
	require.False(t, m.HasPendingWork())
	disk, found, err := store.ReadManifest(ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, disk.Chunks, 3, "completions coalesce into one snapshot")
}

func TestPublishedTermConfirmsAndRecoversUnpublishedTail(t *testing.T) {
	ctx := context.Background()
	m, store := newTestManagerWithStore(t)
	require.NoError(t, m.Restore(ctx))
	require.NoError(t, drainSummary(ctx, m)) // discoverable empty term
	require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
	require.Equal(t, uint64(100), m.LastAcked())
	disk, _, err := store.ReadManifest(ctx)
	require.NoError(t, err)
	require.Empty(t, disk.Chunks)
	successor := newTestManager(t, nextTermStore(store), 1<<30)
	puts := mockey.Mock((*Store).WriteManifest).To(func(*Store, context.Context, *streamingpb.PChannelSummaryManifest) error {
		t.Error("Restore performed an inline PUT")
		return nil
	}).Build()
	defer puts.UnPatch()
	require.NoError(t, successor.Restore(ctx))
	puts.UnPatch()
	require.Equal(t, uint64(100), successor.LastAcked())
	require.True(t, successor.HasPendingWork())
	require.NoError(t, drainSummary(ctx, successor))
	require.False(t, successor.HasPendingWork())
	_, found, err := store.ReadManifest(ctx)
	require.NoError(t, err)
	require.False(t, found, "old manifests are released even while their chunks remain")
	records, err := successor.ReadIdempotencyEntries(ctx, "v1", 0, 100)
	require.NoError(t, err)
	require.Len(t, records.Inserts, 1)
}

func TestManifestPublicationRetainsConcurrentUpdates(t *testing.T) {
	ctx := context.Background()
	m, store := newTestManagerWithStore(t)
	require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
	second := stageChunk(t, m, 200)
	entered, release := make(chan struct{}), make(chan struct{})
	calls := 0
	patch := mockey.Mock((*Store).WriteManifest).To(func(s *Store, ctx context.Context, snapshot *streamingpb.PChannelSummaryManifest) error {
		calls++
		if calls == 1 {
			close(entered)
			<-release
		}
		payload, err := marshalManifest(snapshot)
		if err != nil {
			return err
		}
		return s.chunkManager.Write(ctx, s.ManifestKey(), payload)
	}).Build()
	defer patch.UnPatch()
	done := make(chan error, 1)
	go func() { done <- m.manifestTask.Execute(ctx) }()
	<-entered
	require.NoError(t, second.Execute(ctx))
	close(release)
	requireSummaryError(t, <-done, nodescheduler.ErrDelay)
	require.True(t, m.HasPendingWork())
	require.NoError(t, m.manifestTask.Execute(ctx))
	require.Equal(t, 2, calls)
	require.False(t, m.HasPendingWork())
	disk, _, err := store.ReadManifest(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(200), disk.Coverage.EndTimeTick)
}

func TestNodeSchedulerUploadsInParallel(t *testing.T) {
	ctx := context.Background()
	m, store := newTestManagerWithStore(t)
	scheduler := nodescheduler.New(3)
	firstEntered, release := make(chan struct{}), make(chan struct{})
	patch := mockey.Mock((*Store).WriteChunk).To(func(s *Store, ctx context.Context, gen uint64, sections map[string]*ChunkSections, coverage TimeTickRange) (*streamingpb.PChannelSummaryChunkFooter, uint64, error) {
		// Consume mock arguments before blocking: runtime patching cannot tell
		// the compiler that the original callee's stack map now escapes.
		payload, footer, err := marshalChunk(s.PChannel(), gen, s.Term(), sections, coverage)
		if err != nil {
			return nil, 0, err
		}
		if gen == 0 {
			close(firstEntered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			}
		}
		err = s.chunkManager.Write(ctx, s.ChunkKey(gen), payload)
		return footer, uint64(len(payload)), err
	}).Build()
	defer patch.UnPatch()
	defer scheduler.Close()
	m.cfg.Runtime.Scheduler = scheduler
	m.cfg.FlushMaxBytes = 1
	m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 100, "a", []int64{1}, []uint32{0}))
	<-firstEntered
	m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 200, "b", []int64{2}, []uint32{0}))
	require.Eventually(t, func() bool { exists, _ := store.chunkManager.Exist(ctx, store.ChunkKey(1)); return exists }, time.Second, time.Millisecond)
	require.Empty(t, m.Manifest().Chunks)
	close(release)
	require.Eventually(t, func() bool { return !m.HasPendingWork() && m.LastAcked() == 200 }, 3*time.Second, time.Millisecond)
}

func TestRecoveryPrefixBoundaryAndTermFilter(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	for _, gen := range []uint64{98, 99, 100, 102} {
		_, _, err := store.WriteChunk(ctx, gen, writeSections(map[string][]uint64{"v1": {gen + 1}}), TimeTickRange{Start: gen, End: gen + 1})
		require.NoError(t, err)
	}
	foreign := nextTermStore(store)
	_, _, err := foreign.WriteChunk(ctx, 101, writeSections(map[string][]uint64{"v1": {102}}), TimeTickRange{Start: 101, End: 102})
	require.NoError(t, err)
	var prefixes []string
	cm := store.chunkManager.(*storage.LocalChunkManager)
	var original func(*storage.LocalChunkManager, context.Context, string, bool, storage.ChunkObjectWalkFunc) error
	patch := mockey.Mock((*storage.LocalChunkManager).WalkWithPrefix).Origin(&original).To(func(c *storage.LocalChunkManager, ctx context.Context, prefix string, recursive bool, walk storage.ChunkObjectWalkFunc) error {
		prefixes = append(prefixes, prefix)
		return original(c, ctx, prefix, recursive, walk)
	}).Build()
	defer patch.UnPatch()
	entries, err := store.ProbeChunkForward(ctx, 98)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, uint64(100), entries[2].Generation)
	require.Equal(t, []string{buildChunkPrefix(cm, store.PChannel()) + "000000000000000000", buildChunkPrefix(cm, store.PChannel()) + "000000000000000001"}, prefixes)
}

func TestRecoveryEmptyManifestAndGenerationGap(t *testing.T) {
	ctx := context.Background()
	m, store := newTestManagerWithStore(t)
	require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
	require.NoError(t, drainSummary(ctx, m))
	// Generation 1 is missing. Generation 2 must not advance recovery.
	_, _, err := store.WriteChunk(ctx, 2, writeSections(map[string][]uint64{"v1": {300}}), TimeTickRange{Start: 200, End: 300})
	require.NoError(t, err)
	successor := newTestManager(t, nextTermStore(store), 1<<30)
	require.NoError(t, successor.Restore(ctx))
	require.Equal(t, uint64(1), successor.nextGeneration)
	require.Equal(t, uint64(100), successor.LatestCoveredTimeTick())
	require.NoError(t, drainSummary(ctx, successor))
	require.NoError(t, stageChunk(t, successor, 200).Execute(ctx))
	require.NoError(t, drainSummary(ctx, successor))
	// GC may remove the complete retained set; coverage must survive it.
	successor.cfg.RetentionMaxBytes = 1
	require.NoError(t, gcSummary(ctx, successor))
	require.Empty(t, successor.Manifest().Chunks)
	latest := newTestManager(t, nextTermStore(successor.cfg.Store), 1<<30)
	require.NoError(t, latest.Restore(ctx))
	require.Empty(t, latest.Manifest().Chunks)
	require.Equal(t, uint64(2), latest.nextGeneration)
	require.Equal(t, uint64(200), latest.LastAcked())
	latest.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 100, "expired", []int64{1}, []uint32{0}))
	require.Empty(t, latest.pending, "GC history must not be restaged during WAL replay")
}

func TestRecoveryRejectsNewerOwnerAndSameTermKeyReuse(t *testing.T) {
	ctx := context.Background()
	m, store := newTestManagerWithStore(t)
	require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
	require.NoError(t, drainSummary(ctx, m))
	stale := newTestManager(t, NewStore(store.chunkManager, store.PChannel(), 0), 1<<30)
	require.Error(t, stale.Restore(ctx))
	require.Zero(t, stale.LastAcked())
	reopened := newTestManager(t, store, 1<<30)
	require.NoError(t, reopened.Restore(ctx))
	reopened.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 200, "new", []int64{2}, []uint32{0}))
	reopened.requestSeal()
	require.ErrorContains(t, reopened.terminalErr, "fresh assignment term")
	require.Equal(t, uint64(100), reopened.LastAcked())
	exists, err := store.chunkManager.Exist(ctx, store.ChunkKey(1))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestRecoveryDoesNotAdoptUndiscoverableTerm(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	_, _, err := store.WriteChunk(ctx, 0, writeSections(map[string][]uint64{"v1": {100}}), TimeTickRange{Start: 0, End: 100})
	require.NoError(t, err)
	m := newTestManager(t, nextTermStore(store), 1<<30)
	require.NoError(t, m.Restore(ctx))
	require.Empty(t, m.Manifest().Chunks)
	require.Zero(t, m.LastAcked())
	require.NoError(t, drainSummary(ctx, m))
}

func TestGCWaitsForPublicationAndReaders(t *testing.T) {
	ctx := context.Background()
	m, store := newTestManagerWithStore(t)
	require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
	require.NoError(t, drainSummary(ctx, m))
	entered, release := make(chan struct{}), make(chan struct{})
	var original func(*Store, context.Context, uint64, int64, map[string]*streamingpb.VChannelSummaryChunkIndex) (map[string]*ChunkSections, error)
	patch := mockey.Mock((*Store).ReadIdempotencySectionsOfChunk).Origin(&original).To(func(s *Store, ctx context.Context, gen uint64, term int64, index map[string]*streamingpb.VChannelSummaryChunkIndex) (map[string]*ChunkSections, error) {
		copiedIndex := maps.Clone(index)
		close(entered)
		<-release
		return original(s, ctx, gen, term, copiedIndex)
	}).Build()
	defer patch.UnPatch()
	readDone := make(chan error, 1)
	go func() {
		records, err := m.ReadIdempotencyEntries(ctx, "v1", 0, 100)
		if err == nil && len(records.Inserts) != 1 {
			err = errors.New("missing reader records")
		}
		readDone <- err
	}()
	<-entered
	m.cfg.RetentionMaxBytes = 1
	require.NoError(t, m.GCOnce(ctx))
	require.NoError(t, m.manifestTask.Execute(ctx))
	gcDone := make(chan error, 1)
	go func() { gcDone <- m.gcTask.Execute(ctx) }()
	exists, err := store.chunkManager.Exist(ctx, store.ChunkKey(0))
	require.NoError(t, err)
	require.True(t, exists)
	select {
	case <-gcDone:
		t.Fatal("GC bypassed active reader")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-readDone)
	require.NoError(t, <-gcDone)
	exists, err = store.chunkManager.Exist(ctx, store.ChunkKey(0))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestGCDeletionFailureIsRediscoveredAfterRestart(t *testing.T) {
	ctx := context.Background()
	m, store := newTestManagerWithStore(t)
	require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
	require.NoError(t, drainSummary(ctx, m))
	m.cfg.RetentionMaxBytes = 1
	require.NoError(t, m.GCOnce(ctx))
	requireSummaryError(t, m.gcTask.Execute(ctx), nodescheduler.ErrDelay, "unpublished removal cannot authorize deletion")
	require.NoError(t, m.manifestTask.Execute(ctx))
	failure := errors.New("delete unavailable")
	patch := mockey.Mock((*Store).DeleteChunk).Return(failure).Build()
	defer patch.UnPatch()
	requireSummaryError(t, m.gcTask.Execute(ctx), failure)
	patch.UnPatch()
	successor := newTestManager(t, nextTermStore(store), 1<<30)
	require.NoError(t, successor.Restore(ctx))
	require.NoError(t, gcSummary(ctx, successor))
	exists, err := store.chunkManager.Exist(ctx, store.ChunkKey(0))
	require.NoError(t, err)
	require.False(t, exists)
	require.Empty(t, successor.Manifest().Chunks)
}

func TestManifestValidationAndGenerationExhaustion(t *testing.T) {
	for _, bad := range []*streamingpb.PChannelSummaryManifest{
		{Coverage: &streamingpb.SummaryCoverage{}},
		{Chunks: []*streamingpb.PChannelSummaryChunkIndexEntry{{Generation: 1}}},
		{Coverage: &streamingpb.SummaryCoverage{Generation: 2, Term: 1, EndTimeTick: 100}, Chunks: []*streamingpb.PChannelSummaryChunkIndexEntry{{Generation: 1, StartTimetick: 100, EndTimetick: 99}}},
		{Coverage: &streamingpb.SummaryCoverage{Term: 1, EndTimeTick: 100}, TransformFastForwardTimeTick: map[string]uint64{"v1": 101}},
	} {
		requireSummaryError(t, validateManifest(bad), ErrStoreCorrupted)
	}
	m, _ := newTestManagerWithStore(t)
	m.nextGeneration = math.MaxUint64
	stageChunk(t, m, 100)
	require.True(t, m.generationExhausted)
	m.ObserveMessage(context.Background(), newTestIdempotentInsertMessage(t, "v1", 200, "two", []int64{2}, []uint32{0}))
	m.requestSeal()
	requireSummaryError(t, m.terminalErr, ErrStoreCorrupted)
	require.Equal(t, uint64(math.MaxUint64), m.pendingSealed[0].Generation)
}

func TestTailTransientFailureAndWrongIdentity(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	_, _, err := store.WriteChunk(ctx, 0, writeSections(map[string][]uint64{"v1": {100}}), TimeTickRange{Start: 0, End: 100})
	require.NoError(t, err)
	failure := errors.New("read timeout")
	patch := mockey.Mock((*storage.LocalChunkManager).Read).Return(nil, failure).Build()
	defer patch.UnPatch()
	_, err = store.ProbeChunkForward(ctx, 0)
	requireSummaryError(t, err, failure)
	patch.UnPatch()
	payload, _, err := marshalChunk("wrong-channel", 0, store.Term(), writeSections(map[string][]uint64{"v1": {100}}), TimeTickRange{Start: 0, End: 100})
	require.NoError(t, err)
	require.NoError(t, store.chunkManager.Write(ctx, store.ChunkKey(0), payload))
	_, err = store.ProbeChunkForward(ctx, 0)
	requireSummaryError(t, err, ErrStoreCorrupted)
}

func TestSweepProtectsFutureTermAndCurrentTail(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	for _, s := range []*Store{store, nextTermStore(store)} {
		for _, gen := range []uint64{0, 1} {
			_, _, err := s.WriteChunk(ctx, gen, writeSections(map[string][]uint64{"v1": {gen + 1}}), testRecordRange(writeSections(map[string][]uint64{"v1": {gen + 1}})))
			require.NoError(t, err)
		}
	}
	n, finished, err := store.sweepGarbage(ctx, store.Term(), &streamingpb.SummaryCoverage{Generation: 0, Term: store.Term()}, nil, 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.False(t, finished)
	keys, _, err := storage.ListAllChunkWithPrefix(ctx, store.chunkManager, buildChunkPrefix(store.chunkManager, store.PChannel()), false)
	require.NoError(t, err)
	require.Len(t, keys, 3)
	for _, key := range keys {
		require.False(t, strings.HasSuffix(key, "/00000000000000000000_00000000000000000001"))
	}
}

func requireSummaryError(t *testing.T, err, target error, args ...any) {
	t.Helper()
	require.True(t, errors.Is(err, target), "expected %v in %v: %v", target, err, args)
}

func TestGCRetainsWorkArrivingDuringSweep(t *testing.T) {
	ctx := context.Background()
	m, _ := newTestManagerWithStore(t)
	require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
	require.NoError(t, drainSummary(ctx, m))
	require.NoError(t, m.GCOnce(ctx))
	entered, release := make(chan struct{}), make(chan struct{})
	var original func(*Store, context.Context, int64, *streamingpb.SummaryCoverage, map[ChunkRef]struct{}, int) (int, bool, error)
	patch := mockey.Mock((*Store).sweepGarbage).Origin(&original).To(func(s *Store, ctx context.Context, term int64, last *streamingpb.SummaryCoverage, refs map[ChunkRef]struct{}, budget int) (int, bool, error) {
		deleted, finished, err := original(s, ctx, term, last, refs, budget)
		close(entered)
		<-release
		return deleted, finished, err
	}).Build()
	defer patch.UnPatch()
	done := make(chan error, 1)
	go func() { done <- m.gcTask.Execute(ctx) }()
	<-entered
	m.cfg.RetentionMaxBytes = 1
	require.NoError(t, m.GCOnce(ctx))
	close(release)
	requireSummaryError(t, <-done, nodescheduler.ErrDelay)
	patch.UnPatch()
	require.NoError(t, drainSummary(ctx, m))
	require.False(t, m.HasPendingWork())
	exists, err := m.cfg.Store.chunkManager.Exist(ctx, m.cfg.Store.ChunkKey(0))
	require.NoError(t, err)
	require.False(t, exists, "GC requests arriving during sweep must not be lost")
}

func TestObsoleteManifestCleanupRetriesWithoutNewChunk(t *testing.T) {
	ctx := context.Background()
	m, store := newTestManagerWithStore(t)
	require.NoError(t, m.Restore(ctx))
	require.NoError(t, drainSummary(ctx, m))
	successor := newTestManager(t, nextTermStore(store), 1<<30)
	require.NoError(t, successor.Restore(ctx))
	failure := errors.New("manifest delete unavailable")
	patch := mockey.Mock((*Store).DeleteManifestsBelowTerm).Return(failure).Build()
	defer patch.UnPatch()
	requireSummaryError(t, successor.manifestTask.Execute(ctx), nodescheduler.ErrDelay)
	require.True(t, successor.HasPendingWork())
	patch.UnPatch()
	require.NoError(t, drainSummary(ctx, successor))
	require.False(t, successor.HasPendingWork())
	terms, err := store.ListManifestTerms(ctx, math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, []int64{2}, terms)
}

func TestChunkCoverageIncludesMessagesWithoutSummaryRecords(t *testing.T) {
	ctx := context.Background()
	m, store := newTestManagerWithStore(t)
	m.InitLastAcked(50)
	m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 100, "a", []int64{1}, []uint32{0}))
	observeReadBarrier(m, 150)
	require.NoError(t, persistSummary(ctx, m))
	m.ObserveMessage(ctx, newTestIdempotentInsertMessage(t, "v1", 200, "b", []int64{2}, []uint32{0}))
	observeReadBarrier(m, 250)
	require.NoError(t, persistSummary(ctx, m))
	manifest := m.Manifest()
	require.Equal(t, uint64(50), manifest.Coverage.StartTimeTick)
	require.Equal(t, uint64(250), manifest.Coverage.EndTimeTick)
	require.Len(t, manifest.Chunks, 2)
	require.Equal(t, uint64(150), manifest.Chunks[0].EndTimetick)
	require.Equal(t, manifest.Chunks[0].EndTimetick, manifest.Chunks[1].StartTimetick)
	require.Equal(t, uint64(100), manifest.Chunks[0].Vchannels[0].EndTimetick)
	m.cfg.RetentionMaxBytes = 1
	require.NoError(t, gcSummary(ctx, m))
	require.Empty(t, m.Manifest().Chunks)
	require.Equal(t, manifest.Coverage, m.Manifest().Coverage)
	restored := newTestManager(t, nextTermStore(store), 1<<30)
	require.NoError(t, restored.Restore(ctx))
	require.Equal(t, manifest.Coverage, restored.Manifest().Coverage)
	require.Equal(t, uint64(250), restored.LastAcked())
}

func TestRecoveryRejectsUnpublishedTailCoverageGap(t *testing.T) {
	ctx := context.Background()
	m, store := newTestManagerWithStore(t)
	require.NoError(t, stageChunk(t, m, 100).Execute(ctx))
	require.NoError(t, drainSummary(ctx, m))
	_, _, err := store.WriteChunk(ctx, 1, writeSections(map[string][]uint64{"v1": {200}}), TimeTickRange{Start: 150, End: 200})
	require.NoError(t, err)
	restored := newTestManager(t, nextTermStore(store), 1<<30)
	require.ErrorContains(t, restored.Restore(ctx), "not contiguous")
	require.Zero(t, restored.LastAcked(), "a valid object cannot authorize a coverage gap")
}
