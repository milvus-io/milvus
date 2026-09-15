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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
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

	units, statuses := s.classifyBackfillSegments(ctx, result)
	total := int32(len(result.Segments))

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

	// Split the units into the two dispatch paths and apply them directly:
	// every V2 column-group upsert in one UpdateSegmentsInfo (one catalog
	// transaction), and the V3 manifest commits in bounded batches of
	// CommitSegmentManifests (each batch one all-or-nothing catalog
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
	// V2 dispatch: a single UpdateSegmentsInfo carrying every column-group
	// upsert, gated by the schema fence re-check. On mismatch the schema moved
	// after the fast-fail, so the whole result (V2 and V3 alike) is stale and
	// nothing is applied.
	if len(v2Units) > 0 {
		if err := s.recheckBackfillSchemaVersion(ctx, result); err != nil {
			lastErr = err
			appendUnitStatuses(&statuses, v2Units, false, err.Error())
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
		operators := make([]UpdateOperator, 0, len(v2Units))
		for _, unit := range v2Units {
			operators = append(operators, unit.operator)
		}
		if err := s.meta.UpdateSegmentsInfo(ctx, operators...); err != nil {
			log.Error(ctx, "CommitBackfillResult V2 column-group update failed",
				mlog.Err(err), mlog.Int("segments", len(v2Units)))
			lastErr = err
			appendUnitStatuses(&statuses, v2Units, false, err.Error())
		} else {
			appendUnitStatuses(&statuses, v2Units, true, "")
		}
	}

	for start := 0; start < len(v3Units); start += maxBackfillCommitBatch {
		end := start + maxBackfillCommitBatch
		if end > len(v3Units) {
			end = len(v3Units)
		}
		batch := v3Units[start:end]
		// Re-check the schema fence immediately before the catalog writes so a
		// drop/alter-function committing between the fast-fail above and this
		// commit is still caught. On mismatch, abort the remaining batches:
		// continuing would apply a result computed against a stale schema.
		if err := s.recheckBackfillSchemaVersion(ctx, result); err != nil {
			log.Warn(ctx, "CommitBackfillResult rejected by schema version fence before batch commit",
				mlog.Err(err), mlog.Int("batchStart", start), mlog.Int("batchEnd", end))
			lastErr = err
			for i := start; i < len(v3Units); i++ {
				appendUnitStatuses(&statuses, v3Units[i:i+1], false, err.Error())
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
// callers can surface them even when a segment never reached a commit).
func (s *Server) classifyBackfillSegments(ctx context.Context, result *BackfillResult) ([]backfillCommitUnit, []*datapb.CommitBackfillResultSegmentStatus) {
	bucket := bucketFromChunkManager(s.meta.chunkManager)
	units := make([]backfillCommitUnit, 0, len(result.Segments))
	statuses := make([]*datapb.CommitBackfillResultSegmentStatus, 0)

	// Deterministic order so commit units & diagnostic output are stable.
	segIDs := make([]string, 0, len(result.Segments))
	for k := range result.Segments {
		segIDs = append(segIDs, k)
	}
	sort.Strings(segIDs)

	for _, segIDStr := range segIDs {
		entry := result.Segments[segIDStr]
		segID, perr := strconv.ParseInt(segIDStr, 10, 64)
		if perr != nil {
			statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: 0, Ok: false, Kind: "", Reason: "invalid segment id " + segIDStr,
			})
			continue
		}

		segInfo := s.meta.GetSegment(ctx, segID)
		if segInfo == nil {
			statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: inferKind(&entry), Reason: "segment not found in meta",
			})
			continue
		}
		if segInfo.GetCollectionID() != result.CollectionID {
			statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: inferKind(&entry),
				Reason: "segment does not belong to the result's collection",
			})
			continue
		}
		// Partition-scoped backfills set PartitionID to the target partition;
		// collection-wide / multi-partition backfills leave it at 0 (no check).
		// Spark historically emits -1 for multi-partition results; partition IDs
		// are always positive, so any value <= 0 means "no partition check".
		// Rejecting a mismatching segment otherwise prevents writing metadata
		// against the wrong partition.
		if result.PartitionID > 0 && segInfo.GetPartitionID() != result.PartitionID {
			statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: inferKind(&entry),
				Reason: "segment does not belong to the result's partition",
			})
			continue
		}
		if segInfo.GetState() != commonpb.SegmentState_Flushed {
			statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: inferKind(&entry),
				Reason: "segment state is not Flushed: " + segInfo.GetState().String(),
			})
			continue
		}

		if entry.IsV2() {
			if segInfo.GetStorageVersion() != storage.StorageV2 {
				statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
					SegmentId: segID, Ok: false, Kind: "v2",
					Reason: "segment storage version is not V2",
				})
				continue
			}
			groups, err := buildV2Groups(bucket, &entry)
			if err != nil {
				statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
					SegmentId: segID, Ok: false, Kind: "v2", Reason: err.Error(),
				})
				continue
			}
			units = append(units, backfillCommitUnit{
				segmentID: segID,
				kind:      "v2",
				operator:  UpdateSegmentColumnGroupsOperator(segID, groups),
			})
			continue
		}

		// V3 path.
		if segInfo.GetStorageVersion() != storage.StorageV3 {
			statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: "v3",
				Reason: "segment storage version is not V3",
			})
			continue
		}
		if segInfo.GetManifestPath() == "" {
			statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: "v3",
				Reason: "segment has no existing manifest path",
			})
			continue
		}

		basePath, currentVer, verErr := packed.UnmarshalManifestPath(segInfo.GetManifestPath())
		if verErr != nil {
			statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: "v3",
				Reason: "failed to parse current manifest path: " + verErr.Error(),
			})
			continue
		}

		// Delta mode: apply the manifest operations to the segment's current
		// manifest. No source-version comparison — the new revision is
		// generated from the pointer current under the per-segment manifest
		// lock (rebase semantics), so concurrent stats/index commits are
		// preserved rather than rejected.
		if entry.IsV3Delta() {
			updates, err := opsToManifestUpdates(&entry)
			if err != nil {
				statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
					SegmentId: segID, Ok: false, Kind: "v3", Reason: err.Error(),
				})
				continue
			}
			units = append(units, backfillCommitUnit{
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
			})
			continue
		}

		// Legacy mode: adopt a pre-baked version. Enforce that the base the
		// version was built from still equals the current manifest; a source
		// that moved (stats/index committed) means the pre-baked revision does
		// not contain those changes and must be rejected. The authoritative
		// check happens under the manifest lock via ExpectedManifest; this
		// pre-check only fast-fails with a precise diagnostic.
		if entry.Version <= 0 {
			statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: "v3",
				Reason: "missing or invalid manifest version",
			})
			continue
		}
		if entry.SourceVersion > 0 && entry.SourceVersion != currentVer {
			statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: "v3",
				Reason: "source manifest version " + strconv.FormatInt(entry.SourceVersion, 10) +
					" does not match current " + strconv.FormatInt(currentVer, 10) +
					"; re-run the backfill against the current manifest",
			})
			continue
		}
		// Reject stale results (e.g. Spark retry) that would move the manifest
		// pointer backwards. The commit framework short-circuits only on
		// equality, so strict monotonicity here is the guard against silent
		// rollback.
		if entry.Version <= currentVer {
			statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: "v3",
				Reason: "incoming manifest version " + strconv.FormatInt(entry.Version, 10) +
					" is not greater than current " + strconv.FormatInt(currentVer, 10),
			})
			continue
		}
		// ExpectedManifest pins the base the pre-baked version was built from.
		// Empty when the result carries no sourceVersion (produced before the
		// field existed) — best-effort, preserving the pre-CAS behavior.
		expectedManifest := ""
		if entry.SourceVersion > 0 {
			expectedManifest = packed.MarshalManifestPath(basePath, entry.SourceVersion)
		}
		units = append(units, backfillCommitUnit{
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
		})
	}
	return units, statuses
}

func inferKind(entry *BackfillSegment) string {
	if entry.IsV2() {
		return "v2"
	}
	return "v3"
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
