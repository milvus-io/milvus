// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"strconv"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// CommitBackfillResult fetches the Spark-produced BackfillResult JSON from
// object storage, classifies each segment entry (V2 vs V3), and applies the
// updates directly through DataCoord's commit framework — it does NOT go
// through the broadcast/WAL pipeline. V2 segments carry a column-group upsert
// via UpdateSegmentsInfo; V3 segments are committed through
// meta.CommitSegmentManifests: delta entries (ops) generate a new manifest
// revision from the segment's current pointer, legacy entries (a pre-baked
// version) are adopted only when the current manifest still matches the
// result's sourceVersion.
//
// The commit is a per-cluster catalog operation: in a global cluster the
// primary and the standby each run it against their own metadata, so it must
// remain self-contained and idempotent. The per-segment manifest lock
// serializes each cluster's own concurrent writers (stats/index/GC/compaction).
func (s *Server) CommitBackfillResult(ctx context.Context, req *datapb.CommitBackfillResultRequest) (*datapb.CommitBackfillResultResponse, error) {
	log := mlog.With(mlog.String("resultPath", req.GetResultPath()))
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.CommitBackfillResultResponse{Status: merr.Status(err)}, nil
	}

	result, err := s.loadBackfillResult(ctx, req.GetResultPath())
	if err != nil {
		log.Warn(ctx, "CommitBackfillResult failed to load result JSON", mlog.Err(err))
		return &datapb.CommitBackfillResultResponse{Status: merr.Status(err)}, nil
	}
	total := int32(len(result.Segments))

	coll, err := s.broker.DescribeCollectionInternal(ctx, result.CollectionID)
	if err != nil {
		log.Warn(ctx, "CommitBackfillResult failed to describe collection", mlog.Err(err), mlog.FieldCollectionID(result.CollectionID))
		return &datapb.CommitBackfillResultResponse{Status: merr.Status(err)}, nil
	}

	// Schema-version fence (fast-fail pre-check): a result computed against a
	// schema that is no longer live (e.g. the function was dropped/changed
	// while the Spark job was in flight) must not be committed against the
	// current segments. The fence is re-checked before each commit batch below
	// to narrow the window between this read and the catalog writes.
	if err := checkBackfillSchemaVersion(result.CollectionID, result.SchemaVersion, coll); err != nil {
		log.Warn(ctx, "CommitBackfillResult rejected by schema version fence",
			mlog.Err(err),
			mlog.Int32("resultSchemaVersion", result.SchemaVersion),
			mlog.Int32("collectionSchemaVersion", coll.GetSchema().GetVersion()))
		return &datapb.CommitBackfillResultResponse{
			Status:          merr.Status(err),
			TotalSegments:   total,
			FailedSegments:  int32(len(result.Segments)),
			SegmentStatuses: allSegmentsFailed(result, err.Error()),
		}, nil
	}

	// Classification validates each segment's op columns / column-group field
	// IDs against the live schema (see checkBackfillOpColumns), so a producer
	// writing a field name instead of an ID, or targeting the primary key, a
	// system field, or a function output, is rejected per segment instead of
	// committing a phantom column group.
	units, statuses := s.classifyBackfillSegments(ctx, result, coll)

	// Nothing passed pre-validation: surface as top-level failure so the caller
	// knows no commit happened. segment_statuses still carry per-segment
	// diagnostics.
	if len(units) == 0 {
		return &datapb.CommitBackfillResultResponse{
			Status:          merr.Status(merr.WrapErrParameterInvalidMsg("no backfill segments passed pre-validation")),
			TotalSegments:   total,
			SegmentStatuses: statuses,
			FailedSegments:  int32(len(statuses)),
		}, nil
	}

	// Split the units into the two dispatch paths and apply them directly:
	// every V2 column-group upsert in bounded batches of UpdateSegmentsInfo
	// (each batch one catalog transaction holding segMu for at most
	// maxBackfillCommitBatch records), and the V3 manifest commits in bounded
	// batches of CommitSegmentManifests (each batch one all-or-nothing catalog
	// transaction). A failed batch does not cancel later batches; per-segment
	// statuses reflect batch-level outcomes.
	v2Units := make([]backfillCommitUnit, 0)
	v3Units := make([]backfillCommitUnit, 0)
	for _, unit := range units {
		if unit.kind == "v2" {
			v2Units = append(v2Units, unit)
		} else {
			v3Units = append(v3Units, unit)
		}
	}

	var lastErr error
	// V2 dispatch: bounded UpdateSegmentsInfo batches, gated by the schema fence
	// re-check. On mismatch the schema moved after the fast-fail, so the whole
	// result (V2 and V3 alike) is stale and nothing is applied. Batching bounds
	// the segMu hold time of each AlterSegments, matching the old broadcast cap
	// of maxItemsPerBroadcast so a large V2 result cannot block flush/compaction
	// meta writes for the whole commit.
	for start := 0; start < len(v2Units); start += maxBackfillCommitBatch {
		end := start + maxBackfillCommitBatch
		if end > len(v2Units) {
			end = len(v2Units)
		}
		batch := v2Units[start:end]
		if err := s.recheckBackfillSchemaVersion(ctx, result); err != nil {
			lastErr = err
			// Mark every V2 unit not yet committed (this batch and all later
			// batches) as failed, plus all V3 units: on a schema-version mismatch
			// nothing in the result may be applied, and the per-segment contract
			// must account for every segment.
			appendUnitStatuses(&statuses, v2Units[start:], false, err.Error())
			appendUnitStatuses(&statuses, v3Units, false, err.Error())
			committed, failed := countStatuses(statuses)
			log.Warn(ctx, "CommitBackfillResult rejected by schema version fence before V2 commit",
				mlog.Err(err), mlog.Int32("failed", failed))
			respStatus := merr.Success()
			if committed == 0 {
				respStatus = merr.Status(lastErr)
			}
			return &datapb.CommitBackfillResultResponse{
				Status:            respStatus,
				TotalSegments:     total,
				CommittedSegments: committed,
				FailedSegments:    failed,
				SegmentStatuses:   sortStatuses(statuses),
			}, nil
		}
		operators := make([]UpdateOperator, 0, len(batch))
		for _, unit := range batch {
			operators = append(operators, unit.operator)
		}
		if err := s.meta.UpdateSegmentsInfo(ctx, operators...); err != nil {
			log.Error(ctx, "CommitBackfillResult V2 column-group update failed",
				mlog.Err(err), mlog.Int("segments", len(batch)))
			lastErr = err
			appendUnitStatuses(&statuses, batch, false, err.Error())
			continue
		}
		appendUnitStatuses(&statuses, batch, true, "")
	}

	// Segments whose delta was already applied (replay of a committed result)
	// are reported committed without issuing another manifest commit. The
	// remaining V3 units split into per-segment commits (those that also mutate
	// SegmentIndex records, which the batch path rejects) and the batch path.
	v3Applied := make([]backfillCommitUnit, 0)
	v3Single := make([]backfillCommitUnit, 0)
	v3ToCommit := make([]backfillCommitUnit, 0, len(v3Units))
	for _, unit := range v3Units {
		switch {
		case unit.alreadyApplied:
			v3Applied = append(v3Applied, unit)
		case unit.singleCommit:
			v3Single = append(v3Single, unit)
		default:
			v3ToCommit = append(v3ToCommit, unit)
		}
	}
	if len(v3Applied) > 0 {
		appendUnitStatuses(&statuses, v3Applied, true, "")
	}

	// A V3 commit advances the manifest pointer that the DataView snapshot
	// serves; recompute so consumers observe the new manifest versions. The
	// framework does not recompute for manifest commits (stats, L0, bump-schema
	// and the old ack callback all call recomputeDataView themselves), so this
	// path must do it too, once, whenever any V3 segment was committed.
	v3Committed := false
	markV3Committed := func() { v3Committed = true }

	// Per-segment commits (replace of a dropped column): each goes through
	// meta.CommitSegmentManifest so the SegmentIndexRemove lands in the same
	// catalog transaction as the new manifest pointer. Each commit does manifest
	// object-storage I/O; run them with bounded concurrency (errgroup.SetLimit,
	// not one goroutine per segment) so a large replace result neither pays that
	// latency serially inside the caller's request nor parks a goroutine per
	// segment. A failed commit records its per-segment status; the others still
	// run.
	if len(v3Single) > 0 {
		if err := s.recheckBackfillSchemaVersion(ctx, result); err != nil {
			log.Warn(ctx, "CommitBackfillResult rejected by schema version fence before commit",
				mlog.Err(err), mlog.Int("segments", len(v3Single)))
			lastErr = err
			appendUnitStatuses(&statuses, v3Single, false, err.Error())
		} else {
			var mu sync.Mutex
			g, gCtx := errgroup.WithContext(ctx)
			g.SetLimit(backfillClassifyConcurrency)
			for _, unit := range v3Single {
				g.Go(func() error {
					if err := s.meta.CommitSegmentManifest(gCtx, unit.commit); err != nil {
						log.Error(ctx, "CommitBackfillResult V3 manifest commit failed",
							mlog.Err(err), mlog.FieldSegmentID(unit.segmentID))
						mu.Lock()
						lastErr = err
						appendUnitStatuses(&statuses, []backfillCommitUnit{unit}, false, err.Error())
						mu.Unlock()
						return nil
					}
					mu.Lock()
					appendUnitStatuses(&statuses, []backfillCommitUnit{unit}, true, "")
					markV3Committed()
					mu.Unlock()
					return nil
				})
			}
			_ = g.Wait()
		}
	}

	for start := 0; start < len(v3ToCommit); start += maxBackfillCommitBatch {
		end := start + maxBackfillCommitBatch
		if end > len(v3ToCommit) {
			end = len(v3ToCommit)
		}
		batch := v3ToCommit[start:end]
		// Re-check the schema fence immediately before the catalog writes so a
		// drop/alter-function committing between the fast-fail above and this
		// commit is still caught. On mismatch, abort the remaining batches:
		// continuing would apply a result computed against a stale schema.
		if err := s.recheckBackfillSchemaVersion(ctx, result); err != nil {
			log.Warn(ctx, "CommitBackfillResult rejected by schema version fence before batch commit",
				mlog.Err(err), mlog.Int("batchStart", start), mlog.Int("batchEnd", end))
			lastErr = err
			for i := start; i < len(v3ToCommit); i++ {
				appendUnitStatuses(&statuses, v3ToCommit[i:i+1], false, err.Error())
			}
			break
		}

		commits := make([]SegmentManifestCommit, 0, len(batch))
		for _, unit := range batch {
			commits = append(commits, unit.commit)
		}
		if err := s.meta.CommitSegmentManifests(ctx, commits); err != nil {
			log.Error(ctx, "CommitBackfillResult V3 manifest commit batch failed",
				mlog.Err(err), mlog.Int("batchStart", start), mlog.Int("batchEnd", end))
			lastErr = err
			appendUnitStatuses(&statuses, batch, false, err.Error())
			continue
		}
		appendUnitStatuses(&statuses, batch, true, "")
		markV3Committed()
	}

	// The backfill commit is a Flushed->Flushed SegmentMeta mutation that
	// advances manifest pointers; reconcile the DataView snapshot so consumers
	// observe the new versions. V2 column-group upserts do not touch the
	// manifest, so only V3 commits trigger.
	if v3Committed {
		s.meta.recomputeDataView(ctx, result.CollectionID)
	}

	committed, failed := countStatuses(statuses)
	log.Info(ctx, "CommitBackfillResult committed",
		mlog.Int32("total", total),
		mlog.Int32("committed", committed),
		mlog.Int32("failed", failed))

	// Top-level Success unless every commit failed -- partial failures are
	// surfaced through per-segment statuses.
	respStatus := merr.Success()
	if committed == 0 && lastErr != nil {
		respStatus = merr.Status(lastErr)
	}

	return &datapb.CommitBackfillResultResponse{
		Status:            respStatus,
		TotalSegments:     total,
		CommittedSegments: committed,
		FailedSegments:    failed,
		SegmentStatuses:   sortStatuses(statuses),
	}, nil
}

// maxBackfillCommitBatch bounds the number of SegmentManifestCommit entries
// dispatched to a single meta.CommitSegmentManifests call. It keeps one
// failing commit from dragging an arbitrarily large batch, and bounds the
// manifest-lock hold time of the batch's all-or-nothing lock acquisition.
const maxBackfillCommitBatch = 512

// backfillCommitUnit is one validated backfill segment commit after
// pre-validation. It carries either a V2 column-group upsert operator or a V3
// SegmentManifestCommit, dispatched directly by CommitBackfillResult (no
// broadcast).
type backfillCommitUnit struct {
	segmentID int64
	kind      string // "v2" | "v3"
	// set for v2: the column-group upsert operator
	operator UpdateOperator
	// set for v3: the manifest commit (delta CommitUpdates or legacy Noop)
	commit SegmentManifestCommit
	// alreadyApplied is set for a V3 delta replay that the pre-validation
	// recognized as already committed (the target column exists in the
	// manifest with exactly the ops' descriptors). Such a segment is reported
	// committed without issuing another manifest commit, so a retry after a
	// lost response recovers a success instead of failing on the
	// existing-column rejection milvus-storage enforces.
	alreadyApplied bool
	// singleCommit routes the unit through meta.CommitSegmentManifest (the
	// per-segment commit) instead of the batch CommitSegmentManifests. Needed
	// when the commit also mutates SegmentIndex records, which the batch path
	// rejects: a backfill replace of an indexed column retracts the stale
	// SegmentIndex records atomically with the manifest pointer.
	singleCommit bool
}

func appendUnitStatuses(out *[]*datapb.CommitBackfillResultSegmentStatus, batch []backfillCommitUnit, ok bool, reason string) {
	for _, unit := range batch {
		*out = append(*out, &datapb.CommitBackfillResultSegmentStatus{
			SegmentId: unit.segmentID, Ok: ok, Kind: unit.kind, Reason: reason,
		})
	}
}

// maxBackfillResultBytes caps the size of the result JSON read from object
// storage. The JSON is produced by an external system (Spark) and loaded into
// memory in one shot; a hard cap protects DataCoord from OOM on an oversized
// or malicious input. Real-world backfill results for collections in the
// hundreds of thousands of segments comfortably fit within this limit.
const maxBackfillResultBytes int64 = 64 * 1024 * 1024 // 64MiB

// loadBackfillResult reads and decodes the result JSON. The bucket is inferred
// from the configured chunk manager (if it exposes BucketName()) and used to
// reject s3a://<other-bucket>/... paths early.
func (s *Server) loadBackfillResult(ctx context.Context, rawPath string) (*BackfillResult, error) {
	if rawPath == "" {
		return nil, merr.WrapErrParameterMissingMsg("result_path is required")
	}
	bucket := bucketFromChunkManager(s.meta.chunkManager)
	key, err := normalizeObjectKey(rawPath, bucket)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg(err.Error())
	}
	// Pre-check the object size so an untrusted external caller cannot force
	// an unbounded in-memory Read.
	size, err := s.meta.chunkManager.Size(ctx, key)
	if err != nil {
		return nil, err
	}
	if size > maxBackfillResultBytes {
		return nil, merr.WrapErrParameterInvalidMsg(
			"backfill result JSON " + strconv.FormatInt(size, 10) +
				" bytes exceeds limit " + strconv.FormatInt(maxBackfillResultBytes, 10))
	}
	raw, err := s.meta.chunkManager.Read(ctx, key)
	if err != nil {
		return nil, err
	}
	var result BackfillResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("failed to decode backfill result JSON: " + err.Error())
	}
	if !result.Success {
		return nil, merr.WrapErrParameterInvalidMsg("backfill reported success=false; refusing to commit")
	}
	if result.CollectionID == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("backfill result missing collectionId")
	}
	if len(result.Segments) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("backfill result has no segments")
	}
	return &result, nil
}

// classifyBackfillSegments validates each segment entry and constructs the
// commit units, dispatched directly by CommitBackfillResult (no broadcast).
// V2 entries become a column-group upsert operator; V3 delta entries (ops)
// become a ManifestMutationCommitUpdates commit generated from the segment's
// current pointer; V3 legacy entries (a pre-baked version) become a Noop
// commit pinned by ExpectedManifest to the result's sourceVersion. Returns the
// units plus per-segment failure statuses recorded during pre-validation (so
// callers can surface them even when a segment never reached a commit). coll is
// the live collection description: op columns and V2 column-group field IDs
// are validated against its schema (nil schema disables the check).
func (s *Server) classifyBackfillSegments(ctx context.Context, result *BackfillResult, coll *milvuspb.DescribeCollectionResponse) ([]backfillCommitUnit, []*datapb.CommitBackfillResultSegmentStatus) {
	bucket := bucketFromChunkManager(s.meta.chunkManager)
	validFields := backfillOpColumnFieldIDs(coll.GetSchema())

	// Deterministic order so commit units & diagnostic output are stable.
	segIDs := make([]string, 0, len(result.Segments))
	for k := range result.Segments {
		segIDs = append(segIDs, k)
	}
	sort.Strings(segIDs)

	// Per-segment classification is dominated by one manifest read from object
	// storage per V3 delta entry (the replay/conflict pre-check). Run it with
	// bounded concurrency so a large result does not pay the object-storage
	// latency serially, while preserving the deterministic per-segment order in
	// the returned units and statuses. Meta reads (GetSegment) take segMu.RLock;
	// the per-segment body appends nothing to shared slices. errgroup.SetLimit
	// bounds the live goroutine count, so a result with hundreds of thousands of
	// segments does not park one goroutine per segment.
	results := make([]classifySegmentResult, len(segIDs))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(backfillClassifyConcurrency)
	for i, segIDStr := range segIDs {
		g.Go(func() error {
			// errgroup.SetLimit never skips a slot (g.Go blocks instead), so
			// every closure runs. A canceled/timed-out request must not keep
			// reading every remaining manifest: short-circuit segments that
			// have not started yet and record them as failed below.
			res := classifySegmentResult{segIDStr: segIDStr}
			if gCtx.Err() == nil {
				res = s.classifyOneBackfillSegment(gCtx, bucket, result, segIDStr, validFields)
				res.segIDStr = segIDStr
			} else {
				res.err = gCtx.Err()
			}
			results[i] = res
			return nil
		})
	}
	_ = g.Wait()

	units := make([]backfillCommitUnit, 0, len(result.Segments))
	statuses := make([]*datapb.CommitBackfillResultSegmentStatus, 0)
	for _, res := range results {
		if res.err != nil {
			// Context canceled during classification: the request is being
			// torn down, but the per-segment contract still accounts for the
			// segment — surface it as failed rather than silently dropping it
			// and under-reporting the total.
			segID, perr := strconv.ParseInt(res.segIDStr, 10, 64)
			if perr != nil {
				segID = 0
			}
			statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: inferKindFromSegIDStr(result, res.segIDStr),
				Reason: "classification canceled: " + res.err.Error(),
			})
			continue
		}
		if res.unit != nil {
			units = append(units, *res.unit)
		}
		if res.status != nil {
			statuses = append(statuses, res.status)
		}
	}
	return units, statuses
}

// backfillClassifyConcurrency bounds the number of segment classifications
// (each reading the segment's manifest from object storage) running in
// parallel during CommitBackfillResult pre-validation.
const backfillClassifyConcurrency = 32

// classifySegmentResult is the outcome of classifying one backfill segment.
// At most one of unit / status / err is set: a commit unit, a per-segment
// failure status recorded during pre-validation, or a context cancellation
// (segments not yet started when the request was torn down). segIDStr is
// always the source key, retained so a canceled segment can still be reported
// as failed.
type classifySegmentResult struct {
	segIDStr string
	unit     *backfillCommitUnit
	status   *datapb.CommitBackfillResultSegmentStatus
	err      error
}

// inferKindFromSegIDStr returns the v2/v3 kind of a segment entry that was not
// classified (context cancellation) by looking at its declared storage form.
func inferKindFromSegIDStr(result *BackfillResult, segIDStr string) string {
	entry, ok := result.Segments[segIDStr]
	if !ok {
		return ""
	}
	return inferKind(&entry)
}

// classifyOneBackfillSegment validates one segment entry and constructs its
// commit unit (or a pre-validation failure status). Pure: reads only, no shared
// state mutated, so it is safe to run concurrently under bounded concurrency.
// validFields is the schema's backfillable field-ID set; nil disables the
// schema-column check (see checkBackfillOpColumns).
func (s *Server) classifyOneBackfillSegment(ctx context.Context, bucket string, result *BackfillResult, segIDStr string, validFields map[int64]struct{}) classifySegmentResult {
	entry := result.Segments[segIDStr]
	segID, perr := strconv.ParseInt(segIDStr, 10, 64)
	if perr != nil {
		return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
			SegmentId: 0, Ok: false, Kind: "", Reason: "invalid segment id " + segIDStr,
		}}
	}

	segInfo := s.meta.GetSegment(ctx, segID)
	if segInfo == nil {
		return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
			SegmentId: segID, Ok: false, Kind: inferKind(&entry), Reason: "segment not found in meta",
		}}
	}
	if segInfo.GetCollectionID() != result.CollectionID {
		return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
			SegmentId: segID, Ok: false, Kind: inferKind(&entry),
			Reason: "segment does not belong to the result's collection",
		}}
	}
	// Partition-scoped backfills set PartitionID to the target partition;
	// collection-wide / multi-partition backfills leave it at 0 (no check).
	// Spark historically emits -1 for multi-partition results; partition IDs
	// are always positive, so any value <= 0 means "no partition check".
	// Rejecting a mismatching segment otherwise prevents writing metadata
	// against the wrong partition.
	if result.PartitionID > 0 && segInfo.GetPartitionID() != result.PartitionID {
		return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
			SegmentId: segID, Ok: false, Kind: inferKind(&entry),
			Reason: "segment does not belong to the result's partition",
		}}
	}
	if segInfo.GetState() != commonpb.SegmentState_Flushed {
		return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
			SegmentId: segID, Ok: false, Kind: inferKind(&entry),
			Reason: "segment state is not Flushed: " + segInfo.GetState().String(),
		}}
	}

	if entry.IsV2() {
		if segInfo.GetStorageVersion() != storage.StorageV2 {
			return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: "v2",
				Reason: "segment storage version is not V2",
			}}
		}
		if err := checkBackfillV2GroupFields(validFields, &entry); err != nil {
			return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: "v2", Reason: err.Error(),
			}}
		}
		groups, err := buildV2Groups(bucket, &entry)
		if err != nil {
			return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: "v2", Reason: err.Error(),
			}}
		}
		return classifySegmentResult{unit: &backfillCommitUnit{
			segmentID: segID,
			kind:      "v2",
			operator:  UpdateSegmentColumnGroupsOperator(segID, groups),
		}}
	}

	// V3 path.
	if segInfo.GetStorageVersion() != storage.StorageV3 {
		return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
			SegmentId: segID, Ok: false, Kind: "v3",
			Reason: "segment storage version is not V3",
		}}
	}
	if segInfo.GetManifestPath() == "" {
		return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
			SegmentId: segID, Ok: false, Kind: "v3",
			Reason: "segment has no existing manifest path",
		}}
	}

	basePath, currentVer, verErr := packed.UnmarshalManifestPath(segInfo.GetManifestPath())
	if verErr != nil {
		return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
			SegmentId: segID, Ok: false, Kind: "v3",
			Reason: "failed to parse current manifest path: " + verErr.Error(),
		}}
	}

	// Delta mode: apply the manifest operations to the segment's current
	// manifest. No source-version comparison — the new revision is
	// generated from the pointer current under the per-segment manifest
	// lock (rebase semantics), so concurrent stats/index commits are
	// preserved rather than rejected.
	if entry.IsV3Delta() {
		// Schema-column pre-check: a producer writing a field NAME instead of an
		// ID (columns are keyed by ID) would make DropColumns a no-op and land a
		// phantom column group, and a replace on the PK / a system field / a
		// function output must not go through. Reject before the manifest read.
		for i := range entry.Ops {
			if err := checkBackfillOpColumns(validFields, entry.Ops[i].Columns); err != nil {
				return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
					SegmentId: segID, Ok: false, Kind: "v3", Reason: err.Error(),
				}}
			}
		}
		// Already-applied replay detection: an op whose target column already
		// exists in the manifest with exactly the op's descriptors is a
		// re-submission of a committed result and must return success instead of
		// hitting milvus-storage's existing-column rejection or republishing (and
		// for an indexed field, re-retracting the rebuilt index records). The
		// decision is taken against an unlocked manifest read here; when it omits
		// an already-applied op, InspectedManifest pins the pointer it was taken
		// against and the commit re-validates it under the manifest lock, so a
		// concurrent writer cannot race the omission into silently skipping a
		// conflicting operation.
		updates, alreadyApplied, omitted, err := s.deltaOpsToManifestUpdates(ctx, segInfo, basePath, &entry)
		if err != nil {
			return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: "v3", Reason: err.Error(),
			}}
		}
		if alreadyApplied {
			return classifySegmentResult{unit: &backfillCommitUnit{
				segmentID:      segID,
				kind:           "v3",
				alreadyApplied: true,
			}}
		}
		unit := backfillCommitUnit{
			segmentID: segID,
			kind:      "v3",
			commit: SegmentManifestCommit{
				SegmentID:     segID,
				StorageConfig: createStorageConfig(),
				Mutation: ManifestMutation{
					Type:    ManifestMutationCommitUpdates,
					Updates: updates,
				},
			},
		}
		if omitted {
			unit.commit.InspectedManifest = segInfo.GetManifestPath()
		}
		// A replace's DropColumns strips the replaced column's index entries
		// from the manifest inside milvus-storage, so every SegmentIndex
		// record on those indexes must be retracted in the SAME catalog
		// transaction as the new manifest pointer. The single-commit path
		// resolves the index-removal set from the CURRENT index definitions
		// under the manifest lock — an index created after this
		// classification is still caught — and stages the SegmentIndexRemove
		// atomically; the batch path cannot retract records at all. Route
		// every commit that drops columns through it.
		if len(updates.DropColumns) > 0 {
			unit.singleCommit = true
		}
		return classifySegmentResult{unit: &unit}
	}

	// Legacy mode: adopt a pre-baked version. Enforce that the base the
	// version was built from still equals the current manifest; a source
	// that moved (stats/index committed) means the pre-baked revision does
	// not contain those changes and must be rejected. The authoritative
	// check happens under the manifest lock via ExpectedManifest; this
	// pre-check only fast-fails with a precise diagnostic.
	if entry.Version <= 0 {
		return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
			SegmentId: segID, Ok: false, Kind: "v3",
			Reason: "missing or invalid manifest version",
		}}
	}
	if entry.SourceVersion > 0 && entry.SourceVersion != currentVer {
		return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
			SegmentId: segID, Ok: false, Kind: "v3",
			Reason: "source manifest version " + strconv.FormatInt(entry.SourceVersion, 10) +
				" does not match current " + strconv.FormatInt(currentVer, 10) +
				"; re-run the backfill against the current manifest",
		}}
	}
	// Reject stale results (e.g. Spark retry) that would move the manifest
	// pointer backwards. The commit framework short-circuits only on
	// equality, so strict monotonicity here is the guard against silent
	// rollback.
	if entry.Version <= currentVer {
		return classifySegmentResult{status: &datapb.CommitBackfillResultSegmentStatus{
			SegmentId: segID, Ok: false, Kind: "v3",
			Reason: "incoming manifest version " + strconv.FormatInt(entry.Version, 10) +
				" is not greater than current " + strconv.FormatInt(currentVer, 10),
		}}
	}
	// ExpectedManifest pins the base the pre-baked version was built from.
	// Empty when the result carries no sourceVersion (produced before the
	// field existed) — best-effort, preserving the pre-CAS behavior.
	expectedManifest := ""
	if entry.SourceVersion > 0 {
		expectedManifest = packed.MarshalManifestPath(basePath, entry.SourceVersion)
	}
	return classifySegmentResult{unit: &backfillCommitUnit{
		segmentID: segID,
		kind:      "v3",
		commit: SegmentManifestCommit{
			SegmentID:        segID,
			StorageConfig:    createStorageConfig(),
			ExpectedManifest: expectedManifest,
			Mutation: ManifestMutation{
				Type:         ManifestMutationNoop,
				ManifestPath: packed.MarshalManifestPath(basePath, entry.Version),
			},
		},
	}}
}

// deltaOpsToManifestUpdates converts a delta backfill's ops into the manifest
// updates to commit, applying already-applied replay detection first: an op
// whose target column already exists in the segment's manifest with exactly the
// op's file descriptors is treated as already committed and excluded, so a
// re-submission after a lost response returns success instead of failing on
// milvus-storage's existing-column rejection or republishing (and for an
// indexed field, re-retracting the rebuilt index records). For an "add", a
// column present with different descriptors is a conflict and fails. For a
// "replace", only an exact match is a replay; any other state commits (drop +
// re-add is idempotent: the C++ layer applies the drop before add validation),
// which is also the normal first-time replace of a pre-existing column.
// When every op is already applied, alreadyApplied is true and the caller skips
// the segment. omitted reports whether an op was excluded as already applied;
// the caller must then pin the inspected manifest pointer (InspectedManifest)
// so the commit re-validates the omission under the manifest lock instead of
// trusting this unlocked read.
func (s *Server) deltaOpsToManifestUpdates(ctx context.Context, segInfo *SegmentInfo, basePath string, entry *BackfillSegment) (updates *packed.ManifestUpdates, alreadyApplied bool, omitted bool, err error) {
	existing, err := packed.ReadManifestColumnGroups(segInfo.GetManifestPath(), createStorageConfig())
	if err != nil {
		return nil, false, false, merr.Wrap(err, "read segment manifest for backfill delta replay check")
	}
	remaining := &BackfillSegment{}
	allApplied := true
	for i := range entry.Ops {
		op := &entry.Ops[i]
		switch matchOpAgainstManifest(op, existing) {
		case opNotPresent:
			remaining.Ops = append(remaining.Ops, *op)
			allApplied = false
		case opAlreadyApplied:
			// already committed; exclude from this replay
			omitted = true
		case opConflict:
			return nil, false, false, merr.WrapErrParameterInvalidMsg(
				"backfill manifest column %v already exists with different files; re-run the backfill", op.Columns)
		}
	}
	if allApplied {
		return nil, true, false, nil
	}
	updates, err = opsToManifestUpdates(remaining, basePath)
	if err != nil {
		return nil, false, false, err
	}
	return updates, false, omitted, nil
}

// opState classifies an op against the column groups already present in the
// segment's manifest.
type opState int

const (
	// opNotPresent: the column does not exist in the op's shape; the op must be
	// committed. For a replace this also covers a column present under different
	// descriptors — replacing an existing column is its normal first-time
	// behavior.
	opNotPresent opState = iota
	// opAlreadyApplied: the column exists in exactly the op's shape; a replay
	// of a committed result, safe to skip.
	opAlreadyApplied
	// opConflict: the column exists in a different shape and the op cannot
	// overwrite it (an "add" onto an existing column would hit milvus-storage's
	// existing-column rejection).
	opConflict
)

// matchOpAgainstManifest classifies one op against the manifest's current
// column groups. An exact descriptor match is a replay for both op kinds. For
// an "add", a column present under different descriptors is a conflict; for a
// "replace" it is the normal commit path. Descriptor identity follows the same
// path + row-range notion milvus-storage itself uses for fragment identity.
func matchOpAgainstManifest(op *BackfillManifestOp, existing []packed.ColumnGroupEntry) opState {
	opCols := make(map[string]struct{}, len(op.Columns))
	for _, col := range op.Columns {
		opCols[col] = struct{}{}
	}
	for _, g := range existing {
		if !columnSetsIntersect(g.Columns, opCols) {
			continue
		}
		if sameColumnSet(g.Columns, opCols) && sameFileDescriptors(g.Files, op.Files) {
			return opAlreadyApplied
		}
		if op.Type == "replace" {
			return opNotPresent
		}
		return opConflict
	}
	return opNotPresent
}

func columnSetsIntersect(columns []string, target map[string]struct{}) bool {
	for _, col := range columns {
		if _, ok := target[col]; ok {
			return true
		}
	}
	return false
}

func sameColumnSet(columns []string, target map[string]struct{}) bool {
	if len(columns) != len(target) {
		return false
	}
	for _, col := range columns {
		if _, ok := target[col]; !ok {
			return false
		}
	}
	return true
}

// sameFileDescriptors compares the op's file list against the manifest group's
// as a multiset keyed by path + inclusive-start/exclusive-end row range — the
// same fragment identity milvus-storage uses. Order and file properties are
// not part of the identity.
func sameFileDescriptors(manifestFiles []packed.ColumnGroupFileEntry, opFiles []BackfillManifestFile) bool {
	if len(manifestFiles) != len(opFiles) {
		return false
	}
	seen := make(map[string]int, len(manifestFiles))
	for _, f := range manifestFiles {
		seen[f.Path+":"+strconv.FormatInt(f.StartIndex, 10)+":"+strconv.FormatInt(f.EndIndex, 10)]++
	}
	for _, f := range opFiles {
		key := f.Path + ":" + strconv.FormatInt(f.StartIndex, 10) + ":" + strconv.FormatInt(f.EndIndex, 10)
		if seen[key] == 0 {
			return false
		}
		seen[key]--
	}
	return true
}

func inferKind(entry *BackfillSegment) string {
	if entry.IsV2() {
		return "v2"
	}
	return "v3"
}

// backfillOpColumnFieldIDs returns the set of field IDs a backfill result may
// target: user fields that are not the primary key, not a system field (row ID
// / timestamp) and not a function output. A nil schema (a describe response
// without schema) yields nil, which disables the column check — production
// describes always carry the schema.
func backfillOpColumnFieldIDs(schema *schemapb.CollectionSchema) map[int64]struct{} {
	if schema == nil {
		return nil
	}
	fields := make(map[int64]struct{}, len(schema.GetFields()))
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() || field.GetIsFunctionOutput() || common.IsSystemField(field.GetFieldID()) {
			continue
		}
		fields[field.GetFieldID()] = struct{}{}
	}
	return fields
}

// checkBackfillOpColumns validates the field IDs a V3 delta op targets against
// the schema's backfillable set. Columns are keyed by field ID, but nothing in
// the result contract stops a producer from writing a field NAME instead: the
// drop becomes a no-op and a phantom column group lands in the manifest while
// the real field stays untouched, and the segment is reported committed. A
// replace on the primary key, a system field, or a function output is the same
// class of corruption. This is an Input error: the result violates the external
// contract and retrying cannot fix it.
func checkBackfillOpColumns(validFields map[int64]struct{}, columns []string) error {
	if validFields == nil {
		return nil
	}
	for _, col := range columns {
		fid, err := strconv.ParseInt(col, 10, 64)
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("backfill manifest op column %q is not a field ID", col)
		}
		if _, ok := validFields[fid]; !ok {
			return merr.WrapErrParameterInvalidMsg(
				"backfill manifest op column %d is not a backfillable field: must be a user field that is not the primary key, a system field, or a function output", fid)
		}
	}
	return nil
}

// checkBackfillV2GroupFields validates the field IDs a V2 column group carries,
// with the same contract as checkBackfillOpColumns. Field IDs are the only form
// V2 can express (the JSON field is an integer array), but a valid integer that
// is not a live user field would still register a phantom column group.
func checkBackfillV2GroupFields(validFields map[int64]struct{}, entry *BackfillSegment) error {
	if validFields == nil {
		return nil
	}
	for i := range entry.ColumnGroups {
		for _, fid := range entry.ColumnGroups[i].FieldIDs {
			if _, ok := validFields[fid]; !ok {
				return merr.WrapErrParameterInvalidMsg(
					"backfill V2 column group field %d is not a backfillable field: must be a user field that is not the primary key, a system field, or a function output", fid)
			}
		}
	}
	return nil
}

// checkBackfillSchemaVersion enforces the schema-version fence on a backfill
// result. A result computed against a schema that is no longer live (e.g. the
// function was dropped/changed while the Spark job was in flight) must not be
// committed against the current segments. Results that carry no version (0 —
// produced before Spark stamped the schema version it read) are exempt from
// the fence for backward compatibility.
func checkBackfillSchemaVersion(collectionID int64, expectedSchemaVersion int32, coll *milvuspb.DescribeCollectionResponse) error {
	if expectedSchemaVersion == 0 {
		return nil
	}
	currentVersion := int32(0)
	if coll.GetSchema() != nil {
		currentVersion = coll.GetSchema().GetVersion()
	}
	if expectedSchemaVersion != currentVersion {
		return merr.WrapErrCollectionSchemaMisMatch(
			collectionID,
			"backfill result schema version "+strconv.FormatInt(int64(expectedSchemaVersion), 10)+
				" does not match collection's current schema version "+strconv.FormatInt(int64(currentVersion), 10)+
				"; re-run the backfill against the current schema")
	}
	return nil
}

// recheckBackfillSchemaVersion re-reads the collection and re-applies the
// schema fence immediately before a commit batch, so a drop/alter-function
// committing between the handler's fast-fail and the catalog writes is still
// caught. Results without a schema version (0) are exempt. Applies to both the
// V2 and V3 dispatch paths (see CommitBackfillResult).
func (s *Server) recheckBackfillSchemaVersion(ctx context.Context, result *BackfillResult) error {
	if result.SchemaVersion == 0 {
		return nil
	}
	freshColl, err := s.broker.DescribeCollectionInternal(ctx, result.CollectionID)
	if err != nil {
		return err
	}
	return checkBackfillSchemaVersion(result.CollectionID, result.SchemaVersion, freshColl)
}

// allSegmentsFailed builds a per-segment failure status for every segment in
// the result. Used when a collection-level rejection (e.g. the schema-version
// fence) fails the whole result before any broadcast, so callers still see
// which segments were rejected and why.
func allSegmentsFailed(result *BackfillResult, reason string) []*datapb.CommitBackfillResultSegmentStatus {
	statuses := make([]*datapb.CommitBackfillResultSegmentStatus, 0, len(result.Segments))
	for segIDStr, entry := range result.Segments {
		segID, perr := strconv.ParseInt(segIDStr, 10, 64)
		if perr != nil {
			segID = 0
		}
		statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
			SegmentId: segID, Ok: false, Kind: inferKind(&entry), Reason: reason,
		})
	}
	return sortStatuses(statuses)
}

func countStatuses(statuses []*datapb.CommitBackfillResultSegmentStatus) (committed, failed int32) {
	for _, st := range statuses {
		if st.GetOk() {
			committed++
		} else {
			failed++
		}
	}
	return committed, failed
}

func sortStatuses(statuses []*datapb.CommitBackfillResultSegmentStatus) []*datapb.CommitBackfillResultSegmentStatus {
	sort.SliceStable(statuses, func(i, j int) bool {
		return statuses[i].GetSegmentId() < statuses[j].GetSegmentId()
	})
	return statuses
}
