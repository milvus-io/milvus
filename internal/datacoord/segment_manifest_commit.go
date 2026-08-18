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
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ManifestMutationType is deliberately a closed set.  Callers supply data;
// they do not supply a callback which could do additional I/O or re-enter
// meta while the segment commit lock is held.
type ManifestMutationType int

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
}

// SegmentManifestCommit describes one segment-scoped StorageV3 commit.
// ExpectedManifest is an optional optimistic CAS condition. When non-empty,
// publication proceeds only if the current pointer matches it. When empty,
// the per-segment transaction lock serializes publication without requiring a
// caller to know the current pointer.
type SegmentManifestCommit struct {
	SegmentID        int64
	ExpectedManifest string
	StorageConfig    *indexpb.StorageConfig
	Mutation         ManifestMutation
	CatalogMutation  SegmentCatalogMutation
}

// CommitSegmentManifest is the only DataCoord primitive that both creates a
// StorageV3 manifest revision and advances SegmentInfo.manifest_path.  Lock
// order is segmentManifestLocks[segmentID] -> segMu -> indexMeta.keyLock. No
// caller may enter this protocol while holding segMu; multi-segment paths must
// acquire segmentManifestLocks in ascending SegmentID order before segMu. This
// function does not acquire an index lock; a caller that needs one must acquire
// it only after entering this protocol.
func (m *meta) CommitSegmentManifest(ctx context.Context, commit SegmentManifestCommit) error {
	if commit.SegmentID == 0 {
		return merr.WrapErrServiceInternalMsg("segment manifest commit requires a segment ID")
	}

	// KeyLock.Lock is synchronous: a caller blocks here only when another
	// transaction for this segment is in flight. There is no asynchronous
	// queue or goroutine. Different segment IDs have independent locks, so
	// their manifest I/O continues concurrently; lockWait records same-segment
	// contention while lockHold below records the serialized transaction time.
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

	// Snapshot the currently published pointer under segMu, then release it
	// before opening the (potentially slow) object-storage transaction.
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
		return merr.WrapErrServiceInternalMsg("segment manifest commit requires a healthy segment, segmentID=%d", commit.SegmentID)
	}
	if !matchesExpectedManifest(commit.ExpectedManifest, segment.GetManifestPath()) {
		return staleSegmentManifestError(commit.SegmentID, commit.ExpectedManifest, segment.GetManifestPath())
	}

	manifestPath, err := commitManifestMutation(segment.GetManifestPath(), commit)
	if err != nil {
		return err
	}

	// segmentManifestLocks owns this segment's publication sequence, including
	// the catalog write below. Use the snapshot taken after acquiring that lock;
	// do not re-enter segMu or re-load the segment after manifest I/O.
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

	if err := m.catalog.Update(ctx, action); err != nil {
		return merr.Wrap(err, "publish segment manifest")
	}
	metricMutation.commit()
	// Memory is installed only after the catalog write has succeeded. This
	// short critical section protects SegmentsInfo's map; it does not include
	// any etcd or object-storage operation.
	m.segMu.Lock()
	m.segments.SetSegment(commit.SegmentID, updated)
	m.segMu.Unlock()
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
		return merr.WrapErrServiceUnavailableMsg("prepared manifest version %d regresses expected version %d", preparedVersion, baseVersion)
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
	return merr.WrapErrServiceUnavailableMsg(
		"stale segment manifest, segmentID=%d expected=%q current=%q", segmentID, expected, current)
}

func matchesExpectedManifest(expected, current string) bool {
	return expected == "" || expected == current
}
