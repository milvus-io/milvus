// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// countingAlterCatalog wraps a real catalog and counts AlterSegments calls so a
// batch commit can assert its whole set landed in exactly one catalog transaction.
type countingAlterCatalog struct {
	metastore.DataCoordCatalog
	alterCalls atomic.Int32
}

func (c *countingAlterCatalog) AlterSegments(ctx context.Context, segments []*datapb.SegmentInfo, binlogs ...metastore.BinlogsIncrement) error {
	c.alterCalls.Add(1)
	return c.DataCoordCatalog.AlterSegments(ctx, segments, binlogs...)
}

func addV3Segment(t *testing.T, meta *meta, segmentID int64, basePath string, version int64, state commonpb.SegmentState) {
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		State:          state,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(basePath, version),
	})))
}

// bumpVersionMock mocks the loon transaction to return the next revision of the
// base it was handed, so each segment advances independently and deterministically.
func bumpVersionMock() *mockey.Mocker {
	return mockey.Mock(packed.CommitManifestUpdates).To(
		func(base string, version int64, _ *indexpb.StorageConfig, _ *packed.ManifestUpdates) (string, error) {
			return packed.MarshalManifestPath(base, version+1), nil
		},
	).Build()
}

func commitUpdates(segmentID int64, basePath string, operators ...UpdateOperator) SegmentManifestCommit {
	return SegmentManifestCommit{
		SegmentID:     segmentID,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: ManifestMutation{
			Type:    ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{DeltaLogs: []packed.DeltaLogEntry{{Path: basePath + "/_delta/1", NumEntries: 1}}},
		},
		CatalogMutation: SegmentCatalogMutation{Operators: operators},
	}
}

// TestCommitSegmentManifestsBatchesInSingleCatalogWrite is the core guarantee: N
// segments' manifest pointers advance, but the catalog sees exactly one AlterSegments
// transaction for the whole batch.
func TestCommitSegmentManifestsBatchesInSingleCatalogWrite(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	baseA := "/tmp/milvus/insert_log/1/10/300"
	baseB := "/tmp/milvus/insert_log/1/10/301"
	addV3Segment(t, meta, 300, baseA, 5, commonpb.SegmentState_Flushed)
	addV3Segment(t, meta, 301, baseB, 9, commonpb.SegmentState_Flushed)
	catalog := &countingAlterCatalog{DataCoordCatalog: meta.catalog}
	meta.catalog = catalog

	mock := bumpVersionMock()
	defer mock.UnPatch()

	require.NoError(t, meta.CommitSegmentManifests(context.Background(), []SegmentManifestCommit{
		commitUpdates(300, baseA),
		commitUpdates(301, baseB),
	}))

	require.Equal(t, packed.MarshalManifestPath(baseA, 6), meta.GetSegment(context.Background(), 300).GetManifestPath())
	require.Equal(t, packed.MarshalManifestPath(baseB, 10), meta.GetSegment(context.Background(), 301).GetManifestPath())
	require.EqualValues(t, 1, catalog.alterCalls.Load())
}

func TestCommitSegmentManifestsRejectsNewSegment(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	err = meta.CommitSegmentManifests(context.Background(), []SegmentManifestCommit{{
		SegmentID: 310,
		Mutation:  ManifestMutation{Type: ManifestMutationNoop, ManifestPath: packed.MarshalManifestPath("/tmp/x", 1)},
		CatalogMutation: SegmentCatalogMutation{NewSegment: &datapb.SegmentInfo{
			ID: 310, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
		}},
	}})
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestCommitSegmentManifestsRejectsDuplicateSegmentID(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	base := "/tmp/milvus/insert_log/1/10/311"
	err = meta.CommitSegmentManifests(context.Background(), []SegmentManifestCommit{
		commitUpdates(311, base),
		commitUpdates(311, base),
	})
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

// A dropped segment in the set is a benign skip: the healthy siblings still commit
// atomically and the batch reports success.
func TestCommitSegmentManifestsSkipsDroppedSegment(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	baseHealthy := "/tmp/milvus/insert_log/1/10/320"
	baseDropped := "/tmp/milvus/insert_log/1/10/321"
	addV3Segment(t, meta, 320, baseHealthy, 1, commonpb.SegmentState_Flushed)
	addV3Segment(t, meta, 321, baseDropped, 1, commonpb.SegmentState_Dropped)
	catalog := &countingAlterCatalog{DataCoordCatalog: meta.catalog}
	meta.catalog = catalog

	mock := bumpVersionMock()
	defer mock.UnPatch()

	require.NoError(t, meta.CommitSegmentManifests(context.Background(), []SegmentManifestCommit{
		commitUpdates(320, baseHealthy),
		commitUpdates(321, baseDropped),
	}))

	require.Equal(t, packed.MarshalManifestPath(baseHealthy, 2), meta.GetSegment(context.Background(), 320).GetManifestPath())
	// The dropped segment's pointer must not advance.
	require.Equal(t, packed.MarshalManifestPath(baseDropped, 1), meta.GetSegment(context.Background(), 321).GetManifestPath())
	require.EqualValues(t, 1, catalog.alterCalls.Load())
}

// If every segment in the batch is skipped and there are no extra operators, the
// batch is a no-op that never touches the catalog.
func TestCommitSegmentManifestsAllSkippedIsNoop(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	base := "/tmp/milvus/insert_log/1/10/322"
	addV3Segment(t, meta, 322, base, 1, commonpb.SegmentState_Dropped)
	catalog := &countingAlterCatalog{DataCoordCatalog: meta.catalog}
	meta.catalog = catalog

	mock := bumpVersionMock()
	defer mock.UnPatch()

	require.NoError(t, meta.CommitSegmentManifests(context.Background(), []SegmentManifestCommit{
		commitUpdates(322, base),
	}))
	require.EqualValues(t, 0, catalog.alterCalls.Load())
}

// A stale Noop CAS on ANY member aborts the whole batch before any catalog write:
// neither the stale segment nor its healthy siblings advance. (Structured members
// cannot pin a CAS; ExpectedManifest is the externally-prepared adopter's guard.)
func TestCommitSegmentManifestsAbortsAtomicallyOnStaleNoopCAS(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	baseGood := "/tmp/milvus/insert_log/1/10/330"
	baseStale := "/tmp/milvus/insert_log/1/10/331"
	addV3Segment(t, meta, 330, baseGood, 5, commonpb.SegmentState_Flushed)
	addV3Segment(t, meta, 331, baseStale, 5, commonpb.SegmentState_Flushed)
	catalog := &countingAlterCatalog{DataCoordCatalog: meta.catalog}
	meta.catalog = catalog

	noopAdopt := func(segmentID int64, basePath string, expectedVer, preparedVer int64) SegmentManifestCommit {
		return SegmentManifestCommit{
			SegmentID:        segmentID,
			ExpectedManifest: packed.MarshalManifestPath(basePath, expectedVer),
			Mutation: ManifestMutation{
				Type:         ManifestMutationNoop,
				ManifestPath: packed.MarshalManifestPath(basePath, preparedVer),
			},
		}
	}
	// 331 declares an ExpectedManifest older than its actual pointer -> stale CAS.
	err = meta.CommitSegmentManifests(context.Background(), []SegmentManifestCommit{
		noopAdopt(330, baseGood, 5, 6),
		noopAdopt(331, baseStale, 4, 6),
	})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.ErrorIs(t, err, errSegmentManifestStale)

	require.Equal(t, packed.MarshalManifestPath(baseGood, 5), meta.GetSegment(context.Background(), 330).GetManifestPath())
	require.Equal(t, packed.MarshalManifestPath(baseStale, 5), meta.GetSegment(context.Background(), 331).GetManifestPath())
	require.EqualValues(t, 0, catalog.alterCalls.Load())
}

// A structured batch member must not pin ExpectedManifest; the batch rejects it
// up front, before any lock or manifest I/O.
func TestCommitSegmentManifestsRejectExpectedManifestOnStructuredMutation(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	base := "/tmp/milvus/insert_log/1/10/312"
	addV3Segment(t, meta, 312, base, 1, commonpb.SegmentState_Flushed)

	commit := commitUpdates(312, base)
	commit.ExpectedManifest = packed.MarshalManifestPath(base, 1)
	err = meta.CommitSegmentManifests(context.Background(), []SegmentManifestCommit{commit})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Equal(t, packed.MarshalManifestPath(base, 1), meta.GetSegment(context.Background(), 312).GetManifestPath())
}

// A caller operator that fails during publication aborts the whole batch before the
// single catalog write, leaving every member's pointer untouched.
func TestCommitSegmentManifestsAbortsAtomicallyOnOperatorFailure(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	baseA := "/tmp/milvus/insert_log/1/10/340"
	baseB := "/tmp/milvus/insert_log/1/10/341"
	addV3Segment(t, meta, 340, baseA, 1, commonpb.SegmentState_Flushed)
	addV3Segment(t, meta, 341, baseB, 1, commonpb.SegmentState_Flushed)
	catalog := &countingAlterCatalog{DataCoordCatalog: meta.catalog}
	meta.catalog = catalog

	mock := bumpVersionMock()
	defer mock.UnPatch()

	failing := func(modPack *updateSegmentPack) bool {
		return modPack.fail(merr.WrapErrServiceInternalMsg("operator boom"))
	}
	err = meta.CommitSegmentManifests(context.Background(), []SegmentManifestCommit{
		commitUpdates(340, baseA),
		commitUpdates(341, baseB, failing),
	})
	require.ErrorIs(t, err, merr.ErrServiceInternal)

	require.Equal(t, packed.MarshalManifestPath(baseA, 1), meta.GetSegment(context.Background(), 340).GetManifestPath())
	require.Equal(t, packed.MarshalManifestPath(baseB, 1), meta.GetSegment(context.Background(), 341).GetManifestPath())
	require.EqualValues(t, 0, catalog.alterCalls.Load())
}

// extraOperators ride in the same atomic transaction as the manifest commits.
func TestCommitSegmentManifestsCommitsExtraOperatorsAtomically(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	baseCommit := "/tmp/milvus/insert_log/1/10/350"
	baseExtra := "/tmp/milvus/insert_log/1/10/351"
	addV3Segment(t, meta, 350, baseCommit, 1, commonpb.SegmentState_Flushed)
	addV3Segment(t, meta, 351, baseExtra, 1, commonpb.SegmentState_Flushed)
	catalog := &countingAlterCatalog{DataCoordCatalog: meta.catalog}
	meta.catalog = catalog

	mock := bumpVersionMock()
	defer mock.UnPatch()

	require.NoError(t, meta.CommitSegmentManifests(context.Background(),
		[]SegmentManifestCommit{commitUpdates(350, baseCommit)},
		UpdateIsImporting(351, true),
	))

	require.Equal(t, packed.MarshalManifestPath(baseCommit, 2), meta.GetSegment(context.Background(), 350).GetManifestPath())
	require.True(t, meta.GetSegment(context.Background(), 351).GetIsImporting())
	require.EqualValues(t, 1, catalog.alterCalls.Load())
}

// With no manifest commits, a batch degenerates to committing the extra operators.
func TestCommitSegmentManifestsExtraOperatorsOnly(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	base := "/tmp/milvus/insert_log/1/10/360"
	addV3Segment(t, meta, 360, base, 1, commonpb.SegmentState_Flushed)

	require.NoError(t, meta.CommitSegmentManifests(context.Background(), nil, UpdateIsImporting(360, true)))
	require.True(t, meta.GetSegment(context.Background(), 360).GetIsImporting())
}

// An empty batch with no operators is a silent no-op.
func TestCommitSegmentManifestsEmptyIsNoop(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, meta.CommitSegmentManifests(context.Background(), nil))
}

// A batch member whose pointer is advanced mid-I/O by an out-of-lock writer aborts
// the WHOLE batch: the prepared revision (base+2, past the monotonic guard) was not
// built on the concurrent revision, so publishing any member would break batch
// atomicity or drop that revision. Nothing from the batch reaches the catalog.
func TestCommitSegmentManifestsAbortsWhenPointerAdvancesDuringManifestIO(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	baseMoved := "/tmp/milvus/insert_log/1/10/390"
	baseQuiet := "/tmp/milvus/insert_log/1/10/391"
	addV3Segment(t, meta, 390, baseMoved, 7, commonpb.SegmentState_Flushed)
	addV3Segment(t, meta, 391, baseQuiet, 7, commonpb.SegmentState_Flushed)
	catalog := &countingAlterCatalog{DataCoordCatalog: meta.catalog}
	meta.catalog = catalog

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	mock := mockey.Mock(packed.CommitManifestUpdates).To(
		func(base string, version int64, _ *indexpb.StorageConfig, _ *packed.ManifestUpdates) (string, error) {
			entered <- struct{}{}
			<-release
			return packed.MarshalManifestPath(base, version+2), nil
		},
	).Build()
	defer mock.UnPatch()

	result := make(chan error, 1)
	go func() {
		result <- meta.CommitSegmentManifests(context.Background(), []SegmentManifestCommit{
			commitUpdates(390, baseMoved),
			commitUpdates(391, baseQuiet),
		})
	}()

	// One mock entry proves stage-2 snapshots are taken; inject the out-of-lock
	// ack on 390 while its revision generation is still in flight.
	<-entered
	require.NoError(t, meta.UpdateSegmentsInfo(context.Background(), UpdateManifestVersion(390, 8)))
	close(release)

	err = <-result
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.ErrorIs(t, err, errSegmentManifestStale)

	// The ack's pointer survives; the untouched sibling did not advance either.
	require.Equal(t, packed.MarshalManifestPath(baseMoved, 8), meta.GetSegment(context.Background(), 390).GetManifestPath())
	require.Equal(t, packed.MarshalManifestPath(baseQuiet, 7), meta.GetSegment(context.Background(), 391).GetManifestPath())
	// Exactly one catalog write happened: the injected ack's own AlterSegments.
	// The aborted batch contributed none.
	require.EqualValues(t, 1, catalog.alterCalls.Load())
}

// Extreme contention must not fail the batch: past the escalation threshold the
// acquisition joins each key's FIFO queue and completes once the holder releases,
// replacing the old timed-out ServiceUnavailable + scheduler re-drive loop.
func TestAcquireSegmentManifestLocksEscalatesInsteadOfTimingOut(t *testing.T) {
	locks := lock.NewKeyLock[int64]()
	// Hold one requested key so TryLockMany can never win {5,7}.
	locks.Lock(7)

	restore := segmentManifestLockEscalationThreshold
	segmentManifestLockEscalationThreshold = 20 * time.Millisecond
	defer func() { segmentManifestLockEscalationThreshold = restore }()

	done := make(chan error, 1)
	go func() {
		done <- acquireSegmentManifestLocks(context.Background(), locks, []int64{5, 7})
	}()

	// Well past the threshold the batch must be parked in phase 2, not failed.
	select {
	case err := <-done:
		t.Fatalf("acquisition finished while key 7 was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	locks.Unlock(7)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("escalated acquisition did not complete after the holder released")
	}
	// The batch really holds the whole set now.
	require.False(t, locks.TryLock(5))
	require.False(t, locks.TryLock(7))
	locks.UnlockMany([]int64{5, 7})
}

// The starvation regression this design exists for: a sustained stream of
// single-segment lockers keeps the hot key's mutex in Go's starvation mode,
// where TryLock — and so TryLockMany — fails unconditionally on every retry.
// The batch must still complete in bounded time via escalation.
func TestAcquireSegmentManifestLocksCompletesUnderSustainedContention(t *testing.T) {
	locks := lock.NewKeyLock[int64]()
	restore := segmentManifestLockEscalationThreshold
	segmentManifestLockEscalationThreshold = 20 * time.Millisecond
	defer func() { segmentManifestLockEscalationThreshold = restore }()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				locks.Lock(7)
				// Hold long enough that competing waiters queue past the 1ms
				// starvation-mode trigger.
				time.Sleep(2 * time.Millisecond)
				locks.Unlock(7)
			}
		}()
	}

	done := make(chan error, 1)
	go func() {
		done <- acquireSegmentManifestLocks(context.Background(), locks, []int64{5, 7})
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("batch acquisition starved under sustained single-segment contention")
	}
	locks.UnlockMany([]int64{5, 7})
	close(stop)
	wg.Wait()
}

// Cancellation still short-circuits with ctx.Err() rather than waiting out the
// acquire ceiling.
func TestAcquireSegmentManifestLocksRespectsContextCancel(t *testing.T) {
	locks := lock.NewKeyLock[int64]()
	locks.Lock(7)
	defer locks.Unlock(7)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := acquireSegmentManifestLocks(ctx, locks, []int64{7})
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), 5*time.Second)
}

// Two batches over disjoint segment sets run fully concurrently and both succeed:
// atomic multi-lock acquisition never deadlocks disjoint acquirers.
func TestCommitSegmentManifestsConcurrentDisjointBatches(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	bases := map[int64]string{
		370: "/tmp/milvus/insert_log/1/10/370",
		371: "/tmp/milvus/insert_log/1/10/371",
		372: "/tmp/milvus/insert_log/1/10/372",
		373: "/tmp/milvus/insert_log/1/10/373",
	}
	for id, base := range bases {
		addV3Segment(t, meta, id, base, 1, commonpb.SegmentState_Flushed)
	}
	mock := bumpVersionMock()
	defer mock.UnPatch()

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	run := func(a, b int64) {
		defer wg.Done()
		errs <- meta.CommitSegmentManifests(context.Background(), []SegmentManifestCommit{
			commitUpdates(a, bases[a]),
			commitUpdates(b, bases[b]),
		})
	}
	wg.Add(2)
	go run(370, 371)
	go run(372, 373)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	for id, base := range bases {
		require.Equal(t, packed.MarshalManifestPath(base, 2), meta.GetSegment(context.Background(), id).GetManifestPath())
	}
}

// Two batches contending the same segment are serialized by the atomic per-segment
// lock: both complete without deadlock, and the batch that queued second generates
// its shared-segment revision from the pointer the first batch published — the
// in-lock base is the sole authority for structured mutations, so the shared
// segment advances exactly twice instead of one batch failing a caller-pinned CAS.
func TestCommitSegmentManifestsConcurrentOverlappingBatches(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	shared := "/tmp/milvus/insert_log/1/10/380"
	onlyA := "/tmp/milvus/insert_log/1/10/381"
	onlyB := "/tmp/milvus/insert_log/1/10/382"
	addV3Segment(t, meta, 380, shared, 1, commonpb.SegmentState_Flushed)
	addV3Segment(t, meta, 381, onlyA, 1, commonpb.SegmentState_Flushed)
	addV3Segment(t, meta, 382, onlyB, 1, commonpb.SegmentState_Flushed)
	mock := bumpVersionMock()
	defer mock.UnPatch()

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs <- meta.CommitSegmentManifests(context.Background(), []SegmentManifestCommit{
			commitUpdates(380, shared),
			commitUpdates(381, onlyA),
		})
	}()
	go func() {
		defer wg.Done()
		errs <- meta.CommitSegmentManifests(context.Background(), []SegmentManifestCommit{
			commitUpdates(380, shared),
			commitUpdates(382, onlyB),
		})
	}()
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	// The shared segment advanced exactly twice — once per batch, the second
	// generated from the first's published pointer under the lock.
	require.Equal(t, packed.MarshalManifestPath(shared, 3), meta.GetSegment(context.Background(), 380).GetManifestPath())
	require.Equal(t, packed.MarshalManifestPath(onlyA, 2), meta.GetSegment(context.Background(), 381).GetManifestPath())
	require.Equal(t, packed.MarshalManifestPath(onlyB, 2), meta.GetSegment(context.Background(), 382).GetManifestPath())
}
