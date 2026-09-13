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
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// ManifestMutationType is deliberately a closed set.  Callers supply data;
// they do not supply a callback which could do additional I/O or re-enter
// meta while the segment commit lock is held.
type ManifestMutationType int

// errSegmentManifestStale is an in-process control-flow marker for an exact
// ExpectedManifest conflict. The returned error remains a typed, retriable
// service-unavailable error for callers that do not consume this marker.
var errSegmentManifestStale = errors.New("stale segment manifest")

const (
	// ManifestMutationCommitUpdates creates a new revision from structured
	// packed updates.  It is the normal StorageV3 publication path.
	ManifestMutationCommitUpdates ManifestMutationType = iota + 1
	// ManifestMutationNoop publishes a manifest path that was prepared by an
	// existing producer.  It intentionally performs no object-storage I/O;
	// migration patches use it to move pointer publication into this framework
	// before the producer learns to return a structured delta.
	ManifestMutationNoop
)

// ManifestMutation is the object-storage part of a segment manifest commit.
// NewFiles, when present in Updates, remains owned by the caller and must be
// destroyed after CommitSegmentManifest returns.
type ManifestMutation struct {
	Type    ManifestMutationType
	Updates *packed.ManifestUpdates
	// ManifestPath is the published result of a Noop mutation.
	ManifestPath string
}

// SegmentCatalogMutation contains the segment fields that become visible with
// the manifest pointer. Each addition here is a reviewable catalog contract.
type SegmentCatalogMutation struct {
	TextStats    map[int64]*datapb.TextIndexStats
	JSONKeyStats map[int64]*datapb.JsonKeyStats
	State        *commonpb.SegmentState
	IsImporting  *bool
	// NewSegment supplies the complete initial catalog record when this commit
	// creates a segment. Its ManifestPath must be empty: the ManifestMutation
	// below is the sole publisher of the first manifest pointer.
	NewSegment *datapb.SegmentInfo
	// Operators are existing DataCoord segment mutations applied to the same
	// optimistic-CAS clone as the manifest pointer. They must not perform
	// manifest I/O or advance ManifestPath themselves.
	Operators []SegmentOperator
}

// SegmentManifestCommit describes one segment-scoped StorageV3 commit.
// ExpectedManifest is an optional optimistic CAS condition for Noop mutations,
// whose revision was prepared outside this framework against a base the caller
// knows: when non-empty, publication proceeds only if the current pointer still
// matches it. A structured (CommitUpdates) mutation must leave it empty — its
// revision is generated from the in-lock pointer, so publication is guarded by
// base stability rather than a caller-pinned pointer.
type SegmentManifestCommit struct {
	SegmentID        int64
	ExpectedManifest string
	StorageConfig    *indexpb.StorageConfig
	Mutation         ManifestMutation
	CatalogMutation  SegmentCatalogMutation
}

// CommitSegmentManifest is the only DataCoord primitive that both creates a
// StorageV3 manifest revision and advances SegmentInfo.manifest_path for a
// post-flush concurrent writer. The per-segment manifest lock serializes object
// storage revision creation; final publication is rebased onto the latest
// cache version and persisted with optimistic CAS.
func (m *meta) CommitSegmentManifest(ctx context.Context, commit SegmentManifestCommit) error {
	if commit.SegmentID == 0 {
		return merr.WrapErrServiceInternalMsg("segment manifest commit requires a segment ID")
	}
	if err := validateExpectedManifestUsage(commit); err != nil {
		return err
	}

	locks := m.getSegmentManifestLocks()
	lockStart := time.Now()
	locks.Lock(commit.SegmentID)
	defer locks.Unlock(commit.SegmentID)
	lockWait := time.Since(lockStart)
	holdStart := time.Now()
	defer func() {
		mlog.Debug(ctx, "segment manifest commit completed",
			mlog.Int64("segmentID", commit.SegmentID),
			mlog.Duration("lockWait", lockWait),
			mlog.Duration("lockHold", time.Since(holdStart)))
	}()

	segment := m.segments.GetSegment(commit.SegmentID)
	if segment != nil {
		segment = segment.Clone()
	}
	isNewSegment := segment == nil
	if isNewSegment {
		if commit.CatalogMutation.NewSegment == nil {
			return merr.WrapErrSegmentNotFound(commit.SegmentID)
		}
		if commit.ExpectedManifest != "" {
			return merr.WrapErrServiceInternalMsg("new segment manifest commit cannot set expected manifest, segmentID=%d", commit.SegmentID)
		}
		if commit.CatalogMutation.NewSegment.GetID() != commit.SegmentID {
			return merr.WrapErrServiceInternalMsg("new segment ID %d does not match manifest commit segmentID %d", commit.CatalogMutation.NewSegment.GetID(), commit.SegmentID)
		}
		if commit.CatalogMutation.NewSegment.GetManifestPath() != "" {
			return merr.WrapErrServiceInternalMsg("new segment manifest path must be empty, segmentID=%d", commit.SegmentID)
		}
		segment = NewSegmentInfo(proto.Clone(commit.CatalogMutation.NewSegment).(*datapb.SegmentInfo))
	} else if commit.CatalogMutation.NewSegment != nil {
		return merr.WrapErrServiceInternalMsg("existing segment manifest commit cannot include a new segment, segmentID=%d", commit.SegmentID)
	}
	if segment.GetStorageVersion() != storage.StorageV3 {
		return merr.WrapErrServiceInternalMsg("segment manifest commit requires StorageV3, segmentID=%d", commit.SegmentID)
	}
	if !isSegmentHealthy(segment) {
		return merr.WrapErrSegmentNotFound(commit.SegmentID, "segment dropped or unhealthy during manifest commit")
	}
	if !matchesExpectedManifest(commit.ExpectedManifest, segment.GetManifestPath()) {
		return staleSegmentManifestError(commit.SegmentID, commit.ExpectedManifest, segment.GetManifestPath())
	}

	manifestPath, err := commitManifestMutation(segment.GetManifestPath(), commit)
	if err != nil {
		return err
	}

	if isNewSegment {
		updated := segment.Clone()
		inc, ok, err := applySegmentCatalogMutation(updated, commit.CatalogMutation)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if !inc.IsEmpty() {
			// Insert persists every binlog family from the complete segment;
			// the increment is meaningful only for existing-segment updates.
			mlog.Debug(ctx, "ignoring binlog increment for new manifest segment",
				mlog.Int64("segmentID", commit.SegmentID))
		}
		updated.ManifestPath = manifestPath
		return m.UpdateSegmentsInfo(ctx, nil, updated.SegmentInfo)
	}

	baseManifest := segment.GetManifestPath()
	mutation := func(latest *SegmentInfo) (BinlogIncrement, bool) {
		if latest.GetStorageVersion() != storage.StorageV3 {
			latest.pendingMutationErr = merr.WrapErrServiceInternalMsg(
				"segment manifest commit requires StorageV3, segmentID=%d", commit.SegmentID)
			return BinlogIncrement{}, false
		}
		if !isSegmentHealthy(latest) {
			latest.pendingMutationErr = merr.WrapErrSegmentNotFound(
				commit.SegmentID, "segment dropped or unhealthy during manifest commit")
			return BinlogIncrement{}, false
		}
		if commit.Mutation.Type == ManifestMutationCommitUpdates && latest.GetManifestPath() == manifestPath {
			// A prior etcd batch committed this segment record before a later
			// batch failed. UpdateSegmentsInfo already applied its returned
			// version to the cache, so the retry must not re-apply additive
			// catalog operators such as stats deltas.
			return BinlogIncrement{}, false
		}
		if commit.Mutation.Type == ManifestMutationNoop {
			if !matchesExpectedManifest(commit.ExpectedManifest, latest.GetManifestPath()) {
				latest.pendingMutationErr = staleSegmentManifestError(
					commit.SegmentID, commit.ExpectedManifest, latest.GetManifestPath())
				return BinlogIncrement{}, false
			}
		} else if latest.GetManifestPath() != baseManifest {
			latest.pendingMutationErr = staleSegmentManifestError(
				commit.SegmentID, baseManifest, latest.GetManifestPath())
			return BinlogIncrement{}, false
		}
		if err := validatePreparedManifest(latest.GetManifestPath(), manifestPath); err != nil {
			latest.pendingMutationErr = merr.Wrap(err, "validate manifest before publication")
			return BinlogIncrement{}, false
		}
		inc, ok, err := applySegmentCatalogMutation(latest, commit.CatalogMutation)
		if err != nil {
			latest.pendingMutationErr = err
			return BinlogIncrement{}, false
		}
		if !ok {
			return BinlogIncrement{}, false
		}
		latest.ManifestPath = manifestPath
		return inc, true
	}
	err = m.UpdateSegmentsInfo(ctx, map[int64][]SegmentOperator{
		commit.SegmentID: {mutation},
	})
	if errors.Is(err, errIgnoredSegmentMetaOperation) {
		mlog.Info(ctx, "segment manifest commit ignored stale segment meta operation", mlog.Err(err))
		return nil
	}
	return err
}

// getSegmentManifestLocks also supports focused unit tests that construct a
// lightweight meta directly instead of calling newMeta.
func (m *meta) getSegmentManifestLocks() *lock.KeyLock[int64] {
	if m.segmentManifestLocks == nil {
		m.segmentManifestLocks = lock.NewKeyLock[int64]()
	}
	return m.segmentManifestLocks
}

func applySegmentCatalogMutation(segment *SegmentInfo, mutation SegmentCatalogMutation) (BinlogIncrement, bool, error) {
	var increment BinlogIncrement
	for _, operator := range mutation.Operators {
		inc, changed := operator(segment)
		if segment.pendingMutationErr != nil {
			return BinlogIncrement{}, false, segment.pendingMutationErr
		}
		if segment.pendingMutationSkip {
			return BinlogIncrement{}, false, nil
		}
		if changed {
			increment.Union(inc)
		}
	}
	applySegmentCatalogTypedFields(segment, mutation)
	return increment, true, nil
}

func commitManifestMutation(baseManifest string, commit SegmentManifestCommit) (string, error) {
	switch commit.Mutation.Type {
	case ManifestMutationCommitUpdates:
		if baseManifest == "" {
			return "", merr.WrapErrServiceInternalMsg("cannot update an empty manifest for segmentID=%d", commit.SegmentID)
		}
		if commit.Mutation.Updates == nil {
			return "", merr.WrapErrServiceInternalMsg("manifest updates are nil for segmentID=%d", commit.SegmentID)
		}
		basePath, version, err := packed.UnmarshalManifestPath(baseManifest)
		if err != nil {
			return "", merr.Wrap(err, "parse expected manifest")
		}
		manifestPath, err := packed.CommitManifestUpdates(basePath, version, commit.StorageConfig, commit.Mutation.Updates)
		if err != nil {
			return "", merr.Wrap(err, "commit segment manifest")
		}
		return manifestPath, nil
	case ManifestMutationNoop:
		if commit.Mutation.ManifestPath == "" {
			return "", merr.WrapErrServiceInternalMsg("noop manifest mutation has no manifest path for segmentID=%d", commit.SegmentID)
		}
		if err := validatePreparedManifest(baseManifest, commit.Mutation.ManifestPath); err != nil {
			return "", merr.Wrap(err, "validate noop manifest")
		}
		return commit.Mutation.ManifestPath, nil
	default:
		return "", merr.WrapErrServiceInternalMsg("unsupported segment manifest mutation %d", commit.Mutation.Type)
	}
}

// validatePreparedManifest makes the Noop/compatibility path obey the same
// monotonic pointer rule as a packed mutation. An equal version is an
// idempotent retry; a first publication has no prior base to compare.
func validatePreparedManifest(baseManifest, preparedManifest string) error {
	preparedBase, preparedVersion, err := packed.UnmarshalManifestPath(preparedManifest)
	if err != nil {
		return err
	}
	if baseManifest == "" {
		return nil
	}
	basePath, baseVersion, err := packed.UnmarshalManifestPath(baseManifest)
	if err != nil {
		return err
	}
	if preparedBase != basePath {
		return merr.WrapErrServiceInternalMsg("prepared manifest base %q does not match expected base %q", preparedBase, basePath)
	}
	if preparedVersion < baseVersion {
		// A prepared manifest that regresses the current version was built from a
		// stale base; tag it so stats callers discard the obsolete result rather
		// than retry, matching the exact-ExpectedManifest conflict path.
		return merr.WrapErrServiceUnavailableErr(errSegmentManifestStale, "prepared manifest version %d regresses expected version %d", preparedVersion, baseVersion)
	}
	return nil
}

func staleSegmentManifestError(segmentID int64, expected, current string) error {
	return merr.WrapErrServiceUnavailableErr(errSegmentManifestStale,
		"stale segment manifest, segmentID=%d expected=%q current=%q", segmentID, expected, current)
}

func matchesExpectedManifest(expected, current string) bool {
	return expected == "" || expected == current
}

// validateExpectedManifestUsage enforces the CAS contract described on
// SegmentManifestCommit: only a Noop mutation may pin an ExpectedManifest. A
// structured mutation is generated from the in-lock pointer, so a caller-pinned
// pointer read outside the lock could only spuriously abort a commit the lock
// already serializes correctly; base stability covers the mid-I/O case.
func validateExpectedManifestUsage(commit SegmentManifestCommit) error {
	if commit.Mutation.Type != ManifestMutationNoop && commit.ExpectedManifest != "" {
		return merr.WrapErrServiceInternalMsg(
			"segment manifest commit with a structured mutation must not set ExpectedManifest, segmentID=%d", commit.SegmentID)
	}
	return nil
}

// applySegmentCatalogTypedFields folds the manifest commit's typed catalog fields
// onto a segment clone. It is shared by single and batch publication so both
// make the exact same field-level changes.
func applySegmentCatalogTypedFields(segment *SegmentInfo, mutation SegmentCatalogMutation) {
	if len(mutation.TextStats) > 0 {
		if segment.TextStatsLogs == nil {
			segment.TextStatsLogs = make(map[int64]*datapb.TextIndexStats)
		}
		for fieldID, stats := range mutation.TextStats {
			segment.TextStatsLogs[fieldID] = proto.Clone(stats).(*datapb.TextIndexStats)
		}
	}
	if len(mutation.JSONKeyStats) > 0 {
		if segment.JsonKeyStats == nil {
			segment.JsonKeyStats = make(map[int64]*datapb.JsonKeyStats)
		}
		for fieldID, stats := range mutation.JSONKeyStats {
			segment.JsonKeyStats[fieldID] = proto.Clone(stats).(*datapb.JsonKeyStats)
		}
	}
	if mutation.State != nil {
		segment.State = *mutation.State
	}
	if mutation.IsImporting != nil {
		segment.IsImporting = *mutation.IsImporting
	}
}

// preparedSegmentManifest pairs a commit with the immutable manifest revision
// that stage 2 produced, ready for optimistic-CAS publication in stage 3.
type preparedSegmentManifest struct {
	commit       SegmentManifestCommit
	manifestPath string
	// baseManifest is the pointer the revision was generated from (the stage-2
	// snapshot). Stage 3 re-checks it so a pointer advanced mid-I/O by an
	// out-of-lock writer aborts the batch instead of being silently overwritten:
	// the loon transaction does not merge concurrent revisions into the prepared
	// one.
	baseManifest string
}

const (
	// segmentManifestLockRetryInitial/Max bound the backoff between atomic
	// multi-lock attempts. A failed TryLockMany holds nothing, so retrying cannot
	// convoy other writers; the backoff only avoids hot-spinning while another
	// holder (a single-segment commit or a competing batch) works and releases.
	segmentManifestLockRetryInitial = 200 * time.Microsecond
	segmentManifestLockRetryMax     = 20 * time.Millisecond
)

// segmentManifestLockEscalationThreshold bounds how long one batch acquisition
// polls TryLockMany before escalating to the fair blocking path. TryLockMany
// guarantees system-wide progress (some committer always wins) but not
// per-caller progress: a key whose mutex sits in Go's starvation mode — a
// persistent stream of blocked single-segment Lock waiters — fails TryLock
// unconditionally, so no retry schedule can ever win it. Past the threshold
// the batch stops polling and joins each key's FIFO queue via LockManyOrdered, which
// completes in bounded time; the hold-and-wait convoy that ordered blocking
// acquisition creates is confined to this escalated path.
//
// The threshold is deliberately many multiples of a single commit's lock hold
// time (hundreds of ms to seconds of manifest I/O): the all-or-nothing attempt
// over a large target set routinely loses to one ordinary in-flight commit, so
// a threshold near one hold time would escalate on everyday contention and
// make the convoy common. At 30s phase 1 virtually always wins first unless a
// key sees a near-continuous commit stream — actual starvation — keeping
// escalation (and its Warn log) a genuine starvation signal, while a starved
// batch still completes far sooner than the timeout + scheduler re-drive loop
// this replaced. It is a var only so tests can shorten it; production never
// mutates it.
var segmentManifestLockEscalationThreshold = 30 * time.Second

// CommitSegmentManifests is the batched form of CommitSegmentManifest. It
// creates StorageV3 manifest revisions for several segments and publishes them
// through one logical UpdateSegmentsInfo call while preserving the per-segment
// single-writer invariant. The persist layer may split that logical write into
// bounded atomic backend transactions; UpdateSegmentsInfo applies committed
// prefixes to the cache and retries the uncommitted remainder.
//
// It runs the three stages the caller specified:
//  1. Acquire every target segment's manifest lock in two phases: the atomic
//     all-or-nothing KeyLock.TryLockMany with backoff (holds nothing while waiting,
//     so no hold-and-wait convoy), escalating after a bounded window to ordered
//     blocking acquisition so extreme single-segment contention cannot starve the
//     batch (see acquireSegmentManifestLocks for the deadlock-safety argument).
//  2. Snapshot each target and generate its new manifest revision in parallel,
//     with each revision based on the pointer observed while the manifest locks
//     are held (a Noop member may pin an ExpectedManifest CAS).
//  3. Stage prepared pointers before the caller's extra mutations, then publish
//     the ordered logical write through UpdateSegmentsInfo. This ordering means
//     an etcd committed prefix may expose targets early but cannot retire L0
//     inputs before their deltalogs are visible.
//
// commits must target existing StorageV3 segments; NewSegment is rejected because the
// single AlterSegments batch cannot create a segment, and duplicate segment IDs are
// rejected. A segment dropped/unhealthy when its revision is generated — or between
// generation and publication — is skipped as a benign terminal outcome
// (logged), matching how single-segment callers treat ErrSegmentNotFound. A
// manifest I/O error, stale pointer, prepared-version regression, or failing
// caller operator aborts before persistence. A later backend-batch failure may
// leave a committed prefix; normal partial-result compensation and retry then
// finish the logical write. Extra mutations share that logical retry unit and
// must not independently advance a V3 manifest pointer.
func (m *meta) CommitSegmentManifests(
	ctx context.Context,
	commits []SegmentManifestCommit,
	extraMutationSets ...map[int64][]SegmentOperator,
) error {
	extraMutations := make(map[int64][]SegmentOperator)
	for _, mutations := range extraMutationSets {
		mergeSegmentMutations(extraMutations, mutations)
	}
	idSet := make(map[int64]struct{}, len(commits))
	for i := range commits {
		commit := commits[i]
		if commit.SegmentID == 0 {
			return merr.WrapErrServiceInternalMsg("segment manifest commit requires a segment ID")
		}
		if err := validateExpectedManifestUsage(commit); err != nil {
			return err
		}
		if commit.CatalogMutation.NewSegment != nil {
			return merr.WrapErrServiceInternalMsg("batch segment manifest commit cannot create a new segment, segmentID=%d", commit.SegmentID)
		}
		if _, dup := idSet[commit.SegmentID]; dup {
			return merr.WrapErrServiceInternalMsg("duplicate segment ID %d in batch manifest commit", commit.SegmentID)
		}
		idSet[commit.SegmentID] = struct{}{}
	}

	if len(commits) == 0 {
		return m.UpdateSegmentsInfo(ctx, extraMutations)
	}

	segmentIDs := make([]int64, 0, len(idSet))
	for id := range idSet {
		segmentIDs = append(segmentIDs, id)
	}
	sort.Slice(segmentIDs, func(i, j int) bool { return segmentIDs[i] < segmentIDs[j] })

	locks := m.getSegmentManifestLocks()
	lockStart := time.Now()
	if err := acquireSegmentManifestLocks(ctx, locks, segmentIDs); err != nil {
		return err
	}
	lockWait := time.Since(lockStart)
	holdStart := time.Now()
	defer func() {
		locks.UnlockMany(segmentIDs)
		mlog.Debug(ctx, "batch segment manifest commit completed",
			mlog.Int("segments", len(segmentIDs)),
			mlog.Duration("lockWait", lockWait),
			mlog.Duration("lockHold", time.Since(holdStart)))
	}()

	prepared, err := m.prepareSegmentManifests(ctx, commits)
	if err != nil {
		return err
	}
	if len(prepared) == 0 {
		return m.UpdateSegmentsInfo(ctx, extraMutations)
	}

	mutations := make(map[int64][]SegmentOperator, len(prepared)+len(extraMutations))
	priority := make([]int64, 0, len(prepared))
	for i := range prepared {
		segmentID := prepared[i].commit.SegmentID
		mutations[segmentID] = append(mutations[segmentID], m.publishSegmentManifestOperator(prepared[i]))
		priority = append(priority, segmentID)
	}
	for segmentID, operators := range extraMutations {
		mutations[segmentID] = append(mutations[segmentID], operators...)
	}

	// Target manifest publications are staged before the caller's extra
	// mutations. This matters on etcd, where a logical update can be chunked:
	// a crash after a committed prefix may expose targets early, but never
	// retire L0 inputs before their deltalogs are published.
	return m.updateSegmentsInfo(ctx, mutations, priority, nil)
}

func acquireSegmentManifestLocks(ctx context.Context, locks *lock.KeyLock[int64], segmentIDs []int64) error {
	backoff := segmentManifestLockRetryInitial
	start := time.Now()
	for attempt := 1; ; attempt++ {
		if locks.TryLockMany(segmentIDs) {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		elapsed := time.Since(start)
		if elapsed >= segmentManifestLockEscalationThreshold {
			// Escalation is itself a signal worth watching: it means at least one
			// target segment saw a sustained stream of single-segment commits for
			// the whole polling window.
			mlog.Warn(ctx, "segment manifest lock acquisition escalating to blocking path",
				mlog.Int64s("segmentIDs", segmentIDs),
				mlog.Int("attempts", attempt),
				mlog.Duration("elapsed", elapsed))
			// segmentIDs is already sorted and de-duplicated; LockManyOrdered
			// re-enforces both rather than trusting the caller invariant on the
			// path where getting it wrong would deadlock.
			lock.LockManyOrdered(locks, segmentIDs)
			return nil
		}
		// One line per failed attempt so a task queueing on lock contention is
		// visible under debug; silent in production unless debug logging is on.
		mlog.Debug(ctx, "segment manifest lock acquisition contended; retrying",
			mlog.Int64s("segmentIDs", segmentIDs),
			mlog.Int("attempt", attempt),
			mlog.Duration("elapsed", elapsed),
			mlog.Duration("nextBackoff", backoff))
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if backoff < segmentManifestLockRetryMax {
			backoff *= 2
			if backoff > segmentManifestLockRetryMax {
				backoff = segmentManifestLockRetryMax
			}
		}
	}
}

// prepareSegmentManifests snapshots the target segments once, then generates each
// segment's new manifest revision in parallel. A segment that is gone
// or unhealthy at snapshot time is skipped (nil result); any real generation failure
// aborts the batch. The returned slice holds only the segments that produced a
// revision, in unspecified order.
func (m *meta) prepareSegmentManifests(ctx context.Context, commits []SegmentManifestCommit) ([]preparedSegmentManifest, error) {
	snapshots := make(map[int64]*SegmentInfo, len(commits))
	for i := range commits {
		id := commits[i].SegmentID
		if segment := m.segments.GetSegment(id); segment != nil {
			snapshots[id] = segment.Clone()
		}
	}

	poolSize := paramtable.Get().DataCoordCfg.L0ManifestUpdatePoolSize.GetAsInt()
	if poolSize < 1 {
		poolSize = 1
	}
	if poolSize > len(commits) {
		poolSize = len(commits)
	}
	pool := conc.NewPool[*preparedSegmentManifest](poolSize)
	defer pool.Release()

	futures := make([]*conc.Future[*preparedSegmentManifest], 0, len(commits))
	for i := range commits {
		commit := commits[i]
		snapshot := snapshots[commit.SegmentID]
		futures = append(futures, pool.Submit(func() (*preparedSegmentManifest, error) {
			return prepareSegmentManifest(ctx, commit, snapshot)
		}))
	}
	if err := conc.BlockOnAll(futures...); err != nil {
		return nil, err
	}
	prepared := make([]preparedSegmentManifest, 0, len(futures))
	for _, future := range futures {
		if result := future.Value(); result != nil {
			prepared = append(prepared, *result)
		}
	}
	return prepared, nil
}

// prepareSegmentManifest is the per-segment stage-2 worker: validate the snapshot and
// run the manifest mutation to produce the prepared revision. A dropped/unhealthy
// segment returns (nil, nil) to be skipped; a stale CAS or I/O error returns a real
// error to abort the batch.
func prepareSegmentManifest(ctx context.Context, commit SegmentManifestCommit, snapshot *SegmentInfo) (*preparedSegmentManifest, error) {
	if snapshot == nil || !isSegmentHealthy(snapshot) {
		mlog.Warn(ctx, "segment dropped or unhealthy before batch manifest generation; skipping",
			mlog.Int64("segmentID", commit.SegmentID))
		return nil, nil
	}
	if snapshot.GetStorageVersion() != storage.StorageV3 {
		return nil, merr.WrapErrServiceInternalMsg("segment manifest commit requires StorageV3, segmentID=%d", commit.SegmentID)
	}
	if !matchesExpectedManifest(commit.ExpectedManifest, snapshot.GetManifestPath()) {
		return nil, staleSegmentManifestError(commit.SegmentID, commit.ExpectedManifest, snapshot.GetManifestPath())
	}
	manifestPath, err := commitManifestMutation(snapshot.GetManifestPath(), commit)
	if err != nil {
		return nil, err
	}
	return &preparedSegmentManifest{
		commit:       commit,
		manifestPath: manifestPath,
		baseManifest: snapshot.GetManifestPath(),
	}, nil
}

// publishSegmentManifestOperator produces the stage-3 operator for one prepared
// revision. It rebases onto the latest record, re-checks pointer and monotonic
// guards, applies caller operators and typed fields, then advances the pointer.
// A segment dropped during manifest I/O is skipped without failing the batch.
func (m *meta) publishSegmentManifestOperator(prepared preparedSegmentManifest) SegmentOperator {
	return func(segment *SegmentInfo) (BinlogIncrement, bool) {
		commit := prepared.commit
		if !isSegmentHealthy(segment) {
			mlog.Warn(m.ctx, "segment dropped or unhealthy during batch manifest commit; skipping publication",
				mlog.Int64("segmentID", commit.SegmentID))
			segment.pendingMutationSkip = true
			return BinlogIncrement{}, false
		}
		if segment.GetStorageVersion() != storage.StorageV3 {
			segment.pendingMutationErr = merr.WrapErrServiceInternalMsg(
				"segment manifest commit requires StorageV3, segmentID=%d", commit.SegmentID)
			return BinlogIncrement{}, false
		}
		if commit.Mutation.Type == ManifestMutationCommitUpdates && segment.GetManifestPath() == prepared.manifestPath {
			segment.pendingMutationSkip = true
			return BinlogIncrement{}, false
		}
		if commit.Mutation.Type == ManifestMutationNoop {
			if !matchesExpectedManifest(commit.ExpectedManifest, segment.GetManifestPath()) {
				segment.pendingMutationErr = staleSegmentManifestError(
					commit.SegmentID, commit.ExpectedManifest, segment.GetManifestPath())
				return BinlogIncrement{}, false
			}
		} else if segment.GetManifestPath() != prepared.baseManifest {
			segment.pendingMutationErr = staleSegmentManifestError(
				commit.SegmentID, prepared.baseManifest, segment.GetManifestPath())
			return BinlogIncrement{}, false
		}
		if err := validatePreparedManifest(segment.GetManifestPath(), prepared.manifestPath); err != nil {
			segment.pendingMutationErr = merr.Wrap(err, "validate manifest before publication")
			return BinlogIncrement{}, false
		}

		increment, ok, err := applySegmentCatalogMutation(segment, commit.CatalogMutation)
		if err != nil {
			segment.pendingMutationErr = err
			return BinlogIncrement{}, false
		}
		if !ok {
			return BinlogIncrement{}, false
		}
		segment.ManifestPath = prepared.manifestPath
		return increment, true
	}
}
