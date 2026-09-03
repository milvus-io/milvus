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
	"slices"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
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
	// Operators are existing DataCoord segment mutations applied to a clone
	// under segMu.  They are a migration adapter: callers retain their current
	// metadata contract while the manifest mutation is Noop.  They must not
	// perform manifest I/O or include UpdateManifest.
	Operators []UpdateOperator
	// SegmentIndexes, when non-empty, changes SegmentIndex records in the same
	// catalog transaction that publishes the manifest revision the change
	// refers to, so an index can never be visible in one store while absent
	// from the other. It carries the caller's intent, not a prepared record:
	// each record is re-read and projected under indexMeta's per-buildID lock,
	// so the persisted value cannot be built from a stale read.
	//
	// A normal index task still supplies exactly one upsert. Multiple entries
	// are accepted only for removals, allowing one GC manifest revision to
	// retract several indexes and retire their records atomically.
	SegmentIndexes []SegmentIndexMutation

	// setManifestHasIndex is framework-owned. Structured mutations that add an
	// index entry set the segment's sticky recovery marker in the same catalog
	// transaction as the new manifest pointer.
	setManifestHasIndex bool
}

// SegmentIndexMutationType selects which change a SegmentIndexMutation makes
// to the targeted record.
type SegmentIndexMutationType int

const (
	// SegmentIndexUpsert projects a worker result into memory and retires the
	// etcd task row while the manifest revision publishes its artifact.
	SegmentIndexUpsert SegmentIndexMutationType = iota + 1
	// SegmentIndexRemove deletes the record, retiring an artifact the manifest
	// revision retracts.
	SegmentIndexRemove
)

// SegmentIndexMutation is one SegmentIndex half of a manifest commit: a record
// identified by BuildID, changed atomically with the
// manifest pointer. It is deliberately generic - the framework only stages the
// resulting catalog action and runs the resulting in-memory install; all
// SegmentIndex semantics live in indexMeta.stageSegmentIndexMutation.
type SegmentIndexMutation struct {
	Type    SegmentIndexMutationType
	BuildID int64
	// FinishedTask is the raw worker result an upsert projects the persisted
	// record from. Required for SegmentIndexUpsert, rejected for
	// SegmentIndexRemove.
	FinishedTask *workerpb.IndexTaskInfo
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
// StorageV3 manifest revision and advances SegmentInfo.manifest_path. Lock
// order is segmentManifestLocks[segmentID] -> indexMeta.keyLock -> segMu. No
// caller may enter this protocol while holding segMu. Manifest I/O runs outside
// segMu; the final catalog mutation is rebased onto the latest SegmentInfo and
// catalog + memory publication stays in one segMu critical section. No lock is
// ever acquired inside that section: the fieldIndexLock-guarded index gauge
// update a staged SegmentIndex mutation defers runs only after segMu is
// released (still under keyLock).
func (m *meta) CommitSegmentManifest(ctx context.Context, commit SegmentManifestCommit) error {
	if commit.SegmentID == 0 {
		return merr.WrapErrServiceInternalMsg("segment manifest commit requires a segment ID")
	}
	if err := validateExpectedManifestUsage(commit); err != nil {
		return err
	}
	commit.CatalogMutation.setManifestHasIndex = manifestMutationAddsIndexEntry(commit.Mutation)
	// KeyLock.Lock is synchronous: a caller blocks here only when another
	// transaction for this segment is in flight. There is no asynchronous
	// queue or goroutine. Different segment IDs can perform manifest I/O
	// concurrently; their final full-record publication is serialized by segMu.
	locks := m.getSegmentManifestLocks()
	lockStart := time.Now()
	locks.Lock(commit.SegmentID)
	defer locks.Unlock(commit.SegmentID)
	lockWait := time.Since(lockStart)
	holdStart := time.Now()
	indexBuildIDs, err := validateSegmentIndexMutations(commit)
	if err != nil {
		return err
	}
	defer func() {
		mlog.Debug(ctx, "segment manifest commit completed",
			mlog.Int64("segmentID", commit.SegmentID),
			mlog.Duration("lockWait", lockWait),
			mlog.Duration("lockHold", time.Since(holdStart)))
	}()

	// A SegmentIndex mutation contributes data to the manifest itself. Hold its
	// build lock from the authoritative task projection through object-storage
	// publication and the catalog transaction, so a concurrent reset or version
	// update cannot make the manifest entry disagree with the in-memory record.
	var stagedIndexes []*stagedSegmentIndexMutation
	if len(indexBuildIDs) > 0 {
		lock.LockManyOrdered(m.indexMeta.keyLock, indexBuildIDs)
		defer m.indexMeta.keyLock.UnlockMany(indexBuildIDs)
	}

	// Snapshot the manifest input, then release segMu before object-storage I/O.
	m.segMu.RLock()
	segment := m.segments.GetSegment(commit.SegmentID)
	if segment != nil {
		segment = segment.Clone()
	}
	m.segMu.RUnlock()

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
		// A segment retired (dropped) after the worker finished is gone for
		// publication purposes: the pointer must not advance and the caller must
		// not retry the obsolete result. Report not-found rather than an
		// unclassified internal error so callers that already treat a missing
		// segment as a benign, terminal outcome (stats SetJobInfo discards the
		// result and finishes the task) do not stall re-polling forever.
		return merr.WrapErrSegmentNotFound(commit.SegmentID, "segment dropped or unhealthy during manifest commit")
	}
	if !matchesExpectedManifest(commit.ExpectedManifest, segment.GetManifestPath()) {
		return staleSegmentManifestError(commit.SegmentID, commit.ExpectedManifest, segment.GetManifestPath())
	}

	for i := range commit.CatalogMutation.SegmentIndexes {
		indexMutation := &commit.CatalogMutation.SegmentIndexes[i]
		staged, err := m.indexMeta.stageSegmentIndexMutation(*indexMutation)
		if err != nil {
			if errors.Is(err, errSegmentIndexRecordGone) {
				mlog.Warn(ctx, "index task no longer exists, discarding manifest commit",
					mlog.Int64("buildID", indexMutation.BuildID),
					mlog.Int64("segmentID", commit.SegmentID))
				return nil
			}
			return err
		}
		if staged.record != nil && staged.record.SegmentID != commit.SegmentID {
			return merr.WrapErrServiceInternalMsg(
				"segment index mutation buildID=%d belongs to segment %d, not manifest segment %d",
				indexMutation.BuildID, staged.record.SegmentID, commit.SegmentID)
		}
		if indexMutation.Type == SegmentIndexRemove && staged.record != nil &&
			!commitRetractsIndexIdentity(commit, staged.record.IndexID, indexMutation.BuildID) {
			return merr.WrapErrServiceInternalMsg(
				"segment index removal does not match the manifest retraction, segmentID=%d indexID=%d buildID=%d",
				commit.SegmentID, staged.record.IndexID, indexMutation.BuildID)
		}
		stagedIndexes = append(stagedIndexes, staged)
		if indexMutation.Type == SegmentIndexUpsert {
			for _, manifestIndex := range commit.Mutation.Updates.Indexes {
				if manifestIndex.BuildID != indexMutation.BuildID {
					continue
				}
				if err := validateManifestIndexPublishable(commit.SegmentID, manifestIndex); err != nil {
					return err
				}
				if err := validateManifestIndexTaskProjection(m, segment, manifestIndex, staged.record); err != nil {
					return err
				}
				break
			}
		}
	}

	manifestPath, err := commitManifestMutation(segment.GetManifestPath(), commit)
	if err != nil {
		return err
	}

	// Re-enter segMu only for the final full-record publication. Ordinary
	// segment writers may have changed unrelated fields during manifest I/O, so
	// apply the catalog mutation to the latest clone rather than the I/O input.
	//
	// The section is an inner function so segMu is released before
	// deferredIndexMetric runs. That closure takes indexMeta.fieldIndexLock for
	// the stored-index-size gauge, and every index DDL (CreateIndex, AlterIndex,
	// MarkIndexAsDeleted, RemoveIndex) holds that lock's write side across an
	// etcd round trip; sync.RWMutex is writer-preferring, so acquiring it inside
	// segMu would park this commit — segMu held — behind that round trip and
	// stall every segment reader. No lock is acquired inside segMu.
	var deferredIndexMetrics []func()
	if err := func() error {
		m.segMu.Lock()
		defer m.segMu.Unlock()
		latest := m.segments.GetSegment(commit.SegmentID)
		if isNewSegment {
			if latest != nil {
				return staleSegmentManifestError(commit.SegmentID, "", latest.GetManifestPath())
			}
		} else {
			if latest == nil {
				return merr.WrapErrSegmentNotFound(commit.SegmentID)
			}
			latest = latest.Clone()
			if latest.GetStorageVersion() != storage.StorageV3 {
				return merr.WrapErrServiceInternalMsg("segment manifest commit requires StorageV3, segmentID=%d", commit.SegmentID)
			}
			if !isSegmentHealthy(latest) {
				// Same as the pre-I/O check above: a segment dropped during manifest
				// I/O is treated as not-found so callers discard rather than retry.
				return merr.WrapErrSegmentNotFound(commit.SegmentID, "segment dropped or unhealthy during manifest commit")
			}
			if commit.Mutation.Type == ManifestMutationNoop {
				// A Noop mutation publishes a revision prepared outside this framework;
				// it was not generated from the in-lock base, so publication is guarded
				// by the caller's optional CAS plus the monotonic check below rather
				// than base stability.
				if !matchesExpectedManifest(commit.ExpectedManifest, latest.GetManifestPath()) {
					return staleSegmentManifestError(commit.SegmentID, commit.ExpectedManifest, latest.GetManifestPath())
				}
			} else if latest.GetManifestPath() != segment.GetManifestPath() {
				// A structured mutation was generated from the in-lock snapshot. The
				// manifest lock serializes every framework writer, so a pointer that
				// moved between that snapshot and this publication section can only
				// come from an out-of-lock writer (the DDL/backfill ack path adopting
				// an externally minted version). The loon OVERWRITE transaction built
				// the prepared revision from the snapshot base alone — it does not
				// merge the concurrent revision's contents — so publishing here would
				// silently drop that revision. Fail as stale so the caller discards or
				// re-drives against the fresh base.
				return staleSegmentManifestError(commit.SegmentID, segment.GetManifestPath(), latest.GetManifestPath())
			}
			if err := validatePreparedManifest(latest.GetManifestPath(), manifestPath); err != nil {
				return merr.Wrap(err, "validate manifest before publication")
			}
			segment = latest
		}

		updated, metricMutation, err := m.applySegmentCatalogMutation(segment, commit.CatalogMutation)
		if err != nil {
			// Preserve UpdateSegmentsInfo's contract for stale SaveBinlogPaths
			// requests: the prepared immutable revision remains unpublished and
			// the caller need not retry an operation that is no longer applicable.
			if errors.Is(err, errIgnoredSegmentMetaOperation) {
				mlog.Info(ctx, "segment manifest commit ignored stale segment meta operation", mlog.Err(err))
				return nil
			}
			return err
		}
		updated.ManifestPath = manifestPath
		var action metastore.UpdateAction
		if isNewSegment {
			action = metastore.AddSegment(updated.SegmentInfo)
			metricMutation.addNewSeg(
				updated.GetState(),
				updated.GetLevel(),
				updated.GetIsSorted(),
				updated.GetStorageVersion(),
				segmentMetricFormatLabel(updated),
				updated.GetNumOfRows(),
			)
		} else {
			action = metastore.AlterSegment(updated.SegmentInfo)
		}
		actions := []metastore.UpdateAction{action}

		// The index record and the manifest pointer whose revision publishes or
		// retracts its artifact are staged into one catalog transaction, so an
		// index can never be visible against a revision that does not carry it,
		// nor claimed by a record after the revision dropped it.
		for _, stagedIndex := range stagedIndexes {
			if stagedIndex.action != nil {
				actions = append(actions, *stagedIndex.action)
			}
		}

		if err := m.catalog.Update(ctx, actions...); err != nil {
			return merr.Wrap(err, "publish segment manifest")
		}
		metricMutation.commit()
		// Memory is installed only after the catalog write has succeeded while the
		// same segMu critical section still excludes competing full-record writers.
		m.segments.SetSegment(commit.SegmentID, updated)
		for _, stagedIndex := range stagedIndexes {
			deferredIndexMetrics = append(deferredIndexMetrics, stagedIndex.install())
		}
		return nil
	}(); err != nil {
		return err
	}
	// segMu is released; the segment manifest lock and keyLock(buildID) are
	// still held, so the deferred gauge update stays serialized against
	// MarkIndexAsDeleted (via fieldIndexLock) and ordered before the next
	// writer of the same build, without ever awaiting fieldIndexLock inside
	// segMu.
	for _, deferredIndexMetric := range deferredIndexMetrics {
		deferredIndexMetric()
	}
	return nil
}

func validateSegmentIndexMutations(commit SegmentManifestCommit) ([]int64, error) {
	mutations := commit.CatalogMutation.SegmentIndexes
	buildIDs := make([]int64, 0, len(mutations))
	seen := make(map[int64]struct{}, len(mutations))
	upserts := 0
	for _, mutation := range mutations {
		if mutation.BuildID == 0 {
			return nil, merr.WrapErrServiceInternalMsg("segment index mutation requires a build ID")
		}
		if _, ok := seen[mutation.BuildID]; ok {
			return nil, merr.WrapErrServiceInternalMsg(
				"duplicate segment index mutation buildID=%d, segmentID=%d", mutation.BuildID, commit.SegmentID)
		}
		seen[mutation.BuildID] = struct{}{}
		buildIDs = append(buildIDs, mutation.BuildID)
		switch mutation.Type {
		case SegmentIndexUpsert:
			upserts++
			if !commitPublishesIndexEntry(commit, mutation.BuildID) {
				return nil, merr.WrapErrServiceInternalMsg(
					"segment index upsert requires a matching manifest entry, segmentID=%d buildID=%d",
					commit.SegmentID, mutation.BuildID)
			}
		case SegmentIndexRemove:
			if !commitRetractsIndexEntry(commit, mutation.BuildID) {
				return nil, merr.WrapErrServiceInternalMsg(
					"segment index removal requires a matching manifest retraction, segmentID=%d buildID=%d",
					commit.SegmentID, mutation.BuildID)
			}
		}
	}
	if upserts > 0 && len(mutations) != 1 {
		return nil, merr.WrapErrServiceInternalMsg(
			"segment manifest commit cannot combine an index upsert with other index mutations, segmentID=%d",
			commit.SegmentID)
	}
	sort.Slice(buildIDs, func(i, j int) bool { return buildIDs[i] < buildIDs[j] })
	return buildIDs, nil
}

func commitPublishesIndexEntry(commit SegmentManifestCommit, buildID int64) bool {
	if commit.Mutation.Type != ManifestMutationCommitUpdates || commit.Mutation.Updates == nil {
		return false
	}
	for _, index := range commit.Mutation.Updates.Indexes {
		if index.BuildID == buildID {
			return true
		}
	}
	return false
}

func commitRetractsIndexEntry(commit SegmentManifestCommit, buildID int64) bool {
	if commit.Mutation.Type != ManifestMutationCommitUpdates || commit.Mutation.Updates == nil {
		return false
	}
	for _, drop := range commit.Mutation.Updates.DropIndexes {
		if drop.ExpectedBuildID == buildID {
			return true
		}
	}
	return false
}

func commitRetractsIndexIdentity(commit SegmentManifestCommit, indexID, buildID int64) bool {
	if commit.Mutation.Updates == nil {
		return false
	}
	for _, drop := range commit.Mutation.Updates.DropIndexes {
		if drop.IndexID == indexID && drop.ExpectedBuildID == buildID {
			return true
		}
	}
	return false
}

func manifestMutationAddsIndexEntry(mutation ManifestMutation) bool {
	return mutation.Type == ManifestMutationCommitUpdates &&
		mutation.Updates != nil && len(mutation.Updates.Indexes) > 0
}

// validateManifestIndexTaskProjection checks the task-owned fields without
// rereading the collection's index definition. The caller already built and
// validated that immutable definition snapshot; only the SegmentIndex task can
// race between that build and this commit, and keyLock now holds it stable.
func validateManifestIndexTaskProjection(m *meta, segment *SegmentInfo, entry packed.ManifestIndexInfo, segIdx *model.SegmentIndex) error {
	basePath, _, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return merr.Wrap(err, "parse segment manifest path for index publication")
	}
	indexPrefix := metautil.NewIndexPathBuilder(
		m.chunkManager.RootPath(),
		segIdx.IndexStorePathVersion,
		segIdx.CollectionID,
		segIdx.PartitionID,
		segIdx.SegmentID,
		segIdx.BuildID,
		segIdx.IndexVersion,
	).BuildPrefix()
	expectedPath, err := packed.ManifestIndexRelativePath(basePath, indexPrefix)
	if err != nil {
		return err
	}
	if entry.IndexID != segIdx.IndexID || entry.BuildID != segIdx.BuildID ||
		entry.IndexVersion != segIdx.IndexVersion || entry.NumRows != segIdx.NumRows ||
		entry.SerializedSize != int64(segIdx.IndexSerializedSize) || entry.MemSize != int64(segIdx.IndexMemSize) ||
		entry.CurrentIndexVersion != segIdx.CurrentIndexVersion ||
		entry.CurrentScalarIndexVersion != segIdx.CurrentScalarIndexVersion ||
		entry.IndexStorePathVersion != segIdx.IndexStorePathVersion || entry.Path != expectedPath ||
		!slices.Equal(entry.IndexFileKeys, segIdx.IndexFileKeys) {
		return merr.WrapErrServiceInternalMsg(
			"segment index changed before manifest commit, segmentID=%d buildID=%d", segIdx.SegmentID, segIdx.BuildID)
	}
	return nil
}

// getSegmentManifestLocks also supports focused unit tests that construct a
// lightweight meta directly instead of calling newMeta.
func (m *meta) getSegmentManifestLocks() *lock.KeyLock[int64] {
	m.segMu.Lock()
	defer m.segMu.Unlock()
	if m.segmentManifestLocks == nil {
		m.segmentManifestLocks = lock.NewKeyLock[int64]()
	}
	return m.segmentManifestLocks
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

func (m *meta) applySegmentCatalogMutation(current *SegmentInfo, mutation SegmentCatalogMutation) (*SegmentInfo, *segMetricMutation, error) {
	pack := &updateSegmentPack{
		meta:       m,
		segments:   make(map[int64]*SegmentInfo),
		increments: make(map[int64]metastore.BinlogsIncrement),
		metricMutation: &segMetricMutation{
			stateChange:             make(segmentMetricStateChange),
			deferSegmentLabelChange: true,
		},
	}
	// Always seed the pack from the segment-lock snapshot. Operators then never
	// re-read the shared SegmentsInfo map while catalog I/O is intentionally
	// outside segMu. This also lets creation commits use the same machinery
	// before their segment is visible in meta.
	pack.segments[current.GetID()] = current.Clone()
	for _, operator := range mutation.Operators {
		operator(pack)
		if pack.err != nil {
			return nil, nil, pack.err
		}
	}
	if len(pack.l0ManifestUpdates) > 0 {
		return nil, nil, merr.WrapErrServiceInternalMsg("segment manifest commit catalog mutation must not contain L0 manifest updates")
	}
	segment := pack.Get(current.GetID())
	if segment == nil {
		segment = current.Clone()
	}
	applySegmentCatalogTypedFields(segment, mutation)
	if err := pack.Validate(); err != nil {
		return nil, nil, err
	}
	// Operators prepare metric transitions as part of UpdateSegmentsInfo.
	// Do this after applying the typed fields too, so a state mutation is
	// reflected only once the catalog write succeeds.
	pack.prepareSegmentMetricUpdates()
	return segment, pack.metricMutation, nil
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
	// A Noop publishes a revision this framework did not build, so it cannot
	// prove that revision either carries an upserted artifact or excludes a
	// removed one. Pairing it with either record mutation would reintroduce the
	// cross-store inconsistency the atomic structured commit prevents.
	if commit.Mutation.Type == ManifestMutationNoop && len(commit.CatalogMutation.SegmentIndexes) > 0 {
		return merr.WrapErrServiceInternalMsg(
			"segment manifest commit cannot pair a noop mutation with segment index mutations, segmentID=%d", commit.SegmentID)
	}
	return nil
}

// applySegmentCatalogTypedFields folds the manifest commit's typed catalog fields
// onto a segment clone. It is shared by the single-segment applySegmentCatalogMutation
// and the batch publish operator so both make the exact same field-level changes.
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
	if mutation.setManifestHasIndex {
		// Sticky, set-only: a stale true costs one manifest read, while a
		// false value could hide manifest-resident indexes after a mode flip.
		segment.ManifestHasIndex = true
	}
}

// preparedSegmentManifest pairs a commit with the immutable manifest revision that
// stage 2 produced for it, ready to be published under segMu in stage 3.
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

// CommitSegmentManifests is the batched form of CommitSegmentManifest. It creates a
// StorageV3 manifest revision for several segments and advances their
// SegmentInfo.manifest_path in a SINGLE catalog transaction (one AlterSegments via
// UpdateSegmentsInfo), while preserving the per-segment single-writer invariant that
// protects the manifest pointer from concurrent writers (stats, index, GC, compaction).
//
// It runs the three stages the caller specified:
//  1. Acquire every target segment's manifest lock in two phases: the atomic
//     all-or-nothing KeyLock.TryLockMany with backoff (holds nothing while waiting,
//     so no hold-and-wait convoy), escalating after a bounded window to ordered
//     blocking acquisition so extreme single-segment contention cannot starve the
//     batch (see acquireSegmentManifestLocks for the deadlock-safety argument).
//  2. Generate each segment's new manifest revision in parallel, OUTSIDE segMu — the
//     loon transaction is object-storage I/O — each generated from the segment's
//     current in-lock manifest pointer (a Noop member may pin an ExpectedManifest CAS).
//  3. Publish every prepared pointer plus the caller's extraOperators in one
//     m.UpdateSegmentsInfo call: a single segMu critical section, one catalog write.
//
// Lock order is segmentManifestLocks -> segMu (the manifest locks are all held
// before UpdateSegmentsInfo takes segMu). indexMeta.keyLock never enters this path
// at all, because SegmentIndex mutations are rejected below; the single-segment
// commit, which does take it, orders it BEFORE segMu. No caller may hold segMu.
//
// commits must target existing StorageV3 segments; NewSegment is rejected because the
// single AlterSegments batch cannot create a segment, SegmentIndex is rejected
// because the batch's one catalog transaction carries segment records only, and
// duplicate segment IDs are rejected. A segment dropped/unhealthy when its revision is generated — or between
// generation and publication — is skipped as a benign terminal outcome (logged),
// matching how single-segment callers treat ErrSegmentNotFound; it does not fail the
// batch. Any other failure (manifest I/O error, a stale pointer — Noop CAS conflict or
// mid-I/O base movement, prepared-version regression, a failing caller operator) aborts the whole batch with nothing
// committed, so the caller retries on a fresh base. extraOperators are committed in the
// same transaction and must be pure catalog mutations: they must not advance a V3
// manifest pointer (which would require its own per-segment manifest lock).
func (m *meta) CommitSegmentManifests(ctx context.Context, commits []SegmentManifestCommit, extraOperators ...UpdateOperator) error {
	idSet := make(map[int64]struct{}, len(commits))
	for _, commit := range commits {
		if commit.SegmentID == 0 {
			return merr.WrapErrServiceInternalMsg("segment manifest commit requires a segment ID")
		}
		if err := validateExpectedManifestUsage(commit); err != nil {
			return err
		}
		if commit.CatalogMutation.NewSegment != nil {
			return merr.WrapErrServiceInternalMsg("batch segment manifest commit cannot create a new segment, segmentID=%d", commit.SegmentID)
		}
		if len(commit.CatalogMutation.SegmentIndexes) > 0 {
			// The batch publishes through UpdateSegmentsInfo, which writes only
			// segment records; it cannot stage the SegmentIndex action into the
			// same catalog transaction the way CommitSegmentManifest does.
			// Accepting the field here would advance the manifest pointer while
			// silently dropping the index record change, stranding the artifact
			// or leaving a record claiming a retracted one.
			return merr.WrapErrServiceInternalMsg("batch segment manifest commit cannot mutate a segment index, segmentID=%d", commit.SegmentID)
		}
		if manifestMutationAddsIndexEntry(commit.Mutation) {
			// The same restriction applies to the manifest half by itself. The
			// batch path cannot publish the matching SegmentIndex action, so it
			// must not create an index entry that is invisible until restart.
			return merr.WrapErrServiceInternalMsg("batch segment manifest commit cannot publish a segment index, segmentID=%d", commit.SegmentID)
		}
		if _, dup := idSet[commit.SegmentID]; dup {
			return merr.WrapErrServiceInternalMsg("duplicate segment ID %d in batch manifest commit", commit.SegmentID)
		}
		idSet[commit.SegmentID] = struct{}{}
	}

	if len(commits) == 0 {
		// A manifest-free batch still needs to publish the caller's operators, but
		// those never touch a V3 pointer so they need no manifest lock.
		if len(extraOperators) == 0 {
			return nil
		}
		return m.UpdateSegmentsInfo(ctx, extraOperators...)
	}

	segmentIDs := make([]int64, 0, len(idSet))
	for id := range idSet {
		segmentIDs = append(segmentIDs, id)
	}
	sort.Slice(segmentIDs, func(i, j int) bool { return segmentIDs[i] < segmentIDs[j] })

	// Stage 1: acquire all manifest locks as one atomic operation.
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

	// Stage 2: generate every segment's manifest revision in parallel, off segMu.
	prepared, err := m.prepareSegmentManifests(ctx, commits)
	if err != nil {
		return err
	}
	if len(prepared) == 0 && len(extraOperators) == 0 {
		return nil
	}

	// Stage 3: publish all prepared pointers and the extra operators in one shot.
	operators := make([]UpdateOperator, 0, len(prepared)+len(extraOperators))
	for i := range prepared {
		operators = append(operators, m.publishSegmentManifestOperator(prepared[i]))
	}
	operators = append(operators, extraOperators...)
	return m.UpdateSegmentsInfo(ctx, operators...)
}

// acquireSegmentManifestLocks takes every segment's manifest lock in two phases.
// Phase 1 is the atomic all-or-nothing TryLockMany with bounded backoff: it holds
// nothing while it waits, so it cannot convoy single-segment commits, and it wins
// on the first conflict-free attempt in the common low-contention case. If phase 1
// cannot win the whole set within segmentManifestLockEscalationThreshold (extreme
// contention: some key never leaves starvation-mode handoff, so TryLock on it can
// never succeed), phase 2 acquires the sorted keys with blocking Lock in order.
// Go's starvation mode hands each mutex over FIFO-fairly, so the batch then
// completes in bounded time instead of failing and being re-driven; the escalated
// acquisition is not cancellable mid-way, but each wait is bounded by the queue of
// in-flight commits ahead of it. segmentIDs must be sorted and de-duplicated —
// that order, plus the manifest-lock discipline (single-segment commits never take
// a second manifest lock while holding one; no caller enters this protocol holding
// segMu), is what makes phase 2 deadlock-free (see lock.LockManyOrdered).
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
// segment's new manifest revision in parallel outside segMu. A segment that is gone
// or unhealthy at snapshot time is skipped (nil result); any real generation failure
// aborts the batch. The returned slice holds only the segments that produced a
// revision, in unspecified order.
func (m *meta) prepareSegmentManifests(ctx context.Context, commits []SegmentManifestCommit) ([]preparedSegmentManifest, error) {
	m.segMu.RLock()
	snapshots := make(map[int64]*SegmentInfo, len(commits))
	for i := range commits {
		id := commits[i].SegmentID
		if segment := m.segments.GetSegment(id); segment != nil {
			snapshots[id] = segment.Clone()
		}
	}
	m.segMu.RUnlock()

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

// publishSegmentManifestOperator produces the stage-3 operator that publishes one
// prepared revision inside UpdateSegmentsInfo's segMu section: it rebases onto the
// latest record, re-checks the CAS and monotonic-version guards, applies the commit's
// caller operators and typed fields, then advances the manifest pointer. A segment
// dropped during manifest I/O is skipped without failing the batch.
func (m *meta) publishSegmentManifestOperator(prepared preparedSegmentManifest) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		commit := prepared.commit
		// Peek the latest record without seeding the pack, so a skip leaves nothing
		// to persist. We hold segMu (via UpdateSegmentsInfo) and every manifest lock.
		latest := modPack.meta.segments.GetSegment(commit.SegmentID)
		if latest == nil || !isSegmentHealthy(latest) {
			mlog.Warn(modPack.meta.ctx, "segment dropped or unhealthy during batch manifest commit; skipping publication",
				mlog.Int64("segmentID", commit.SegmentID))
			return true
		}
		if latest.GetStorageVersion() != storage.StorageV3 {
			return modPack.fail(merr.WrapErrServiceInternalMsg("segment manifest commit requires StorageV3, segmentID=%d", commit.SegmentID))
		}
		if commit.Mutation.Type == ManifestMutationNoop {
			// Externally prepared revision: guarded by the caller's optional CAS
			// plus the monotonic check below, not base stability.
			if !matchesExpectedManifest(commit.ExpectedManifest, latest.GetManifestPath()) {
				return modPack.fail(staleSegmentManifestError(commit.SegmentID, commit.ExpectedManifest, latest.GetManifestPath()))
			}
		} else if latest.GetManifestPath() != prepared.baseManifest {
			// Same rule as CommitSegmentManifest: the pointer moved since the stage-2
			// snapshot, so an out-of-lock writer advanced it during manifest I/O and
			// the prepared revision does not contain that revision's contents. Abort
			// the whole batch so the caller retries on the fresh base.
			return modPack.fail(staleSegmentManifestError(commit.SegmentID, prepared.baseManifest, latest.GetManifestPath()))
		}
		if err := validatePreparedManifest(latest.GetManifestPath(), prepared.manifestPath); err != nil {
			return modPack.fail(merr.Wrap(err, "validate manifest before publication"))
		}

		for _, operator := range commit.CatalogMutation.Operators {
			operator(modPack)
			if modPack.err != nil {
				return false
			}
		}
		if len(modPack.l0ManifestUpdates) > 0 {
			return modPack.fail(merr.WrapErrServiceInternalMsg("segment manifest commit catalog mutation must not contain L0 manifest updates, segmentID=%d", commit.SegmentID))
		}
		segment := modPack.Get(commit.SegmentID)
		if segment == nil {
			// Raced to a drop between the peek and Get; skip rather than fail.
			return true
		}
		applySegmentCatalogTypedFields(segment, commit.CatalogMutation)
		segment.ManifestPath = prepared.manifestPath
		return true
	}
}
