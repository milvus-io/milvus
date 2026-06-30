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
	"fmt"
	"sort"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// CommitBackfillResult fetches the Spark-produced BackfillResult JSON from
// object storage, classifies each segment entry (V2 vs V3), and dispatches the
// updates through the BatchUpdateManifest broadcast pipeline. V2 segments carry
// a column-group upsert; V3 segments carry a manifest version bump. The
// broadcaster ensures serialization against compaction and other DDL-like
// operations on the same collection.
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

	items, statuses := s.classifyBackfillSegments(ctx, result)
	total := int32(len(result.Segments))

	// Nothing passed pre-validation: surface as top-level failure so the caller
	// knows no broadcast happened. segment_statuses still carry per-segment
	// diagnostics.
	if len(items) == 0 {
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

	// bump_defence: resolve the field IDs this backfill commits so each per-batch
	// BatchUpdateManifest message can DECLARE them via field_backfill (the gate itself
	// is registered by the batch's ack callback, atomic with the durable apply). A
	// validation failure REJECTS the whole commit: applying the mutation without a gate
	// would be a silent fail-open.
	backfillFieldIDs, ferr := resolveBackfillGateFields(coll.GetSchema(), result)
	if ferr != nil {
		log.Warn(ctx, "CommitBackfillResult rejected: invalid bump_defence gate declaration", mlog.Err(ferr))
		return &datapb.CommitBackfillResultResponse{
			Status:          merr.Status(ferr),
			TotalSegments:   total,
			SegmentStatuses: statuses,
			FailedSegments:  int32(len(statuses)),
		}, nil
	}

	// Split items across multiple broadcast messages so a single
	// BatchUpdateManifestMessageBody never exceeds the broker's message size
	// limit (Pulsar defaults to 5MiB). Each batch acquires its own broadcaster
	// because broadcasterWithRK consumes its resource-key guards on the first
	// Broadcast call and would panic on any subsequent call. Failure of one
	// batch does not cancel subsequent batches; per-segment statuses reflect
	// batch-level outcomes.
	// The commit-level gate identity: every batch of this commit carries it so all their
	// registrations dedupe onto ONE bump_defence round (batching is a message-size
	// artifact, not a semantic boundary). The raw result path is stable across RPC
	// retries of the same commit.
	backfillSource := fmt.Sprintf("backfillresult:%s", req.GetResultPath())

	channels := []string{streaming.WAL().ControlChannel()}
	var lastErr error
	for start := 0; start < len(items); start += maxItemsPerBroadcast {
		end := start + maxItemsPerBroadcast
		if end > len(items) {
			end = len(items)
		}
		batch := items[start:end]
		if err := broadcastBackfillBatch(ctx, coll, result.CollectionID, channels, batch, backfillFieldIDs, backfillSource); err != nil {
			log.Error(ctx, "CommitBackfillResult broadcast batch failed",
				mlog.Err(err), mlog.Int("batchStart", start), mlog.Int("batchEnd", end))
			lastErr = err
			appendItemStatuses(&statuses, batch, false, err.Error())
			continue
		}
		appendItemStatuses(&statuses, batch, true, "")
	}

	committed, failed := countStatuses(statuses)
	log.Info(ctx, "CommitBackfillResult broadcast completed",
		mlog.Int32("total", total),
		mlog.Int32("committed", committed),
		mlog.Int32("failed", failed))

	// Top-level Success unless every broadcast failed -- partial failures are
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

// resolveBackfillGateFields resolves and validates the field set the bump_defence gate
// must cover for this commit. The declared NewFieldNames is the SINGLE information
// source (mandatory, every name validated against the schema); the V2 column-group
// payload is used purely as a CONSISTENCY CHECK against it -- the two exist
// independently in external input, and when they contradict neither side can be
// trusted, so any inconsistency rejects the whole commit rather than silently
// preferring one source:
//   - a V2 column group carrying a field unknown to the schema -> reject;
//   - a V2 column group writing a field NOT declared -> reject;
//   - a declared field backed by no mutation (when there are no V3 entries whose
//     opaque manifests could carry it) -> reject.
func resolveBackfillGateFields(schema *schemapb.CollectionSchema, result *BackfillResult) ([]int64, error) {
	byName := make(map[string]int64, len(schema.GetFields()))
	byID := make(map[int64]struct{}, len(schema.GetFields()))
	for _, f := range schema.GetFields() {
		byName[f.GetName()] = f.GetFieldID()
		byID[f.GetFieldID()] = struct{}{}
	}

	// The declaration: mandatory and schema-validated. Committing without it would
	// apply the mutation ungated (silent fail-open).
	if len(result.NewFieldNames) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg(
			"backfill result declares no newFieldNames; committing it without a gate would expose partial results")
	}
	declared := make(map[int64]struct{}, len(result.NewFieldNames))
	for _, name := range result.NewFieldNames {
		fid, ok := byName[name]
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg(
				"backfill result newFieldNames entry %q does not match any collection field", name)
		}
		declared[fid] = struct{}{}
	}

	// The V2 mutation payload: consistency check against the declaration.
	mutated := make(map[int64]struct{})
	hasV3 := false
	for segID, entry := range result.Segments {
		if !entry.IsV2() {
			hasV3 = true
			continue
		}
		for _, cg := range entry.ColumnGroups {
			for _, fid := range cg.FieldIDs {
				if fid < common.StartOfUserFieldID {
					continue // system columns ride along; nothing searchable to gate
				}
				if _, ok := byID[fid]; !ok {
					return nil, merr.WrapErrParameterInvalidMsg(
						"backfill result segment %s carries column-group field %d unknown to the collection schema", segID, fid)
				}
				if _, ok := declared[fid]; !ok {
					return nil, merr.WrapErrParameterInvalidMsg(
						"backfill result segment %s writes field %d which newFieldNames does not declare", segID, fid)
				}
				mutated[fid] = struct{}{}
			}
		}
	}
	// Without V3 entries every declared field must be backed by a mutation; with V3
	// entries the undeclared remainder is presumed to live in the opaque manifests.
	if !hasV3 {
		for fid := range declared {
			if _, ok := mutated[fid]; !ok {
				return nil, merr.WrapErrParameterInvalidMsg(
					"backfill result declares field %d in newFieldNames but no column group writes it", fid)
			}
		}
	}

	out := make([]int64, 0, len(declared))
	for fid := range declared {
		out = append(out, fid)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// maxItemsPerBroadcast caps the number of BatchUpdateManifestItem entries
// packed into a single broadcast message. With item payloads in the
// ~1-2KiB range for V2 column groups this stays well under Pulsar's default
// 5MiB maxMessageSize while keeping broadcast overhead low.
const maxItemsPerBroadcast = 512

// broadcastBackfillBatch acquires a fresh broadcaster bound to the
// collection's shared resource keys and issues exactly one broadcast for the
// given items. broadcasterWithRK nils out its lock guards on the first
// Broadcast call, so each batch needs its own broadcaster.
func broadcastBackfillBatch(
	ctx context.Context,
	coll *milvuspb.DescribeCollectionResponse,
	collectionID int64,
	channels []string,
	items []*messagespb.BatchUpdateManifestItem,
	backfillFieldIDs []int64,
	backfillSource string,
) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(coll.GetDbName()),
		message.NewSharedCollectionNameResourceKey(coll.GetDbName(), coll.GetCollectionName()),
	)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	body := &message.BatchUpdateManifestMessageBody{Items: items}
	if len(backfillFieldIDs) > 0 {
		// Declare the backfilled fields + the commit-level source so this batch's ack
		// callback registers the bump_defence gate and all batches of the same commit
		// dedupe onto one round. A generic manifest update leaves field_backfill
		// nil -> no gate.
		body.FieldBackfill = &messagespb.FieldBackfill{FieldIds: backfillFieldIDs, Source: backfillSource}
	}
	_, err = broadcaster.Broadcast(ctx, message.NewBatchUpdateManifestMessageBuilderV2().
		WithHeader(&message.BatchUpdateManifestMessageHeader{
			CollectionId: collectionID,
		}).
		WithBody(body).
		WithBroadcast(channels).
		MustBuildBroadcast(),
	)
	return err
}

func appendItemStatuses(out *[]*datapb.CommitBackfillResultSegmentStatus, batch []*messagespb.BatchUpdateManifestItem, ok bool, reason string) {
	for _, it := range batch {
		kind := "v3"
		if it.GetV2ColumnGroups() != nil {
			kind = "v2"
		}
		*out = append(*out, &datapb.CommitBackfillResultSegmentStatus{
			SegmentId: it.GetSegmentId(), Ok: ok, Kind: kind, Reason: reason,
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
// broadcast items. Returns the items to broadcast plus per-segment failure
// statuses recorded during pre-validation (so callers can surface them to the
// client even when a segment never reached broadcast).
func (s *Server) classifyBackfillSegments(ctx context.Context, result *BackfillResult) ([]*messagespb.BatchUpdateManifestItem, []*datapb.CommitBackfillResultSegmentStatus) {
	bucket := bucketFromChunkManager(s.meta.chunkManager)
	items := make([]*messagespb.BatchUpdateManifestItem, 0, len(result.Segments))
	statuses := make([]*datapb.CommitBackfillResultSegmentStatus, 0)

	// Deterministic order so broadcast items & diagnostic output are stable.
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
		// collection-wide backfills leave it at 0 (no check). When non-zero,
		// rejecting a mismatching segment prevents writing metadata against
		// the wrong partition.
		if result.PartitionID != 0 && segInfo.GetPartitionID() != result.PartitionID {
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
			items = append(items, &messagespb.BatchUpdateManifestItem{
				SegmentId: segID,
				V2ColumnGroups: &messagespb.BatchUpdateManifestV2ColumnGroups{
					ColumnGroups: groups,
				},
			})
		} else {
			// V3 path
			if segInfo.GetStorageVersion() != storage.StorageV3 {
				statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
					SegmentId: segID, Ok: false, Kind: "v3",
					Reason: "segment storage version is not V3",
				})
				continue
			}
			// UpdateManifestVersion no-ops when ManifestPath is empty. Reject
			// here so the caller never sees a fake committed=true.
			if segInfo.GetManifestPath() == "" {
				statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
					SegmentId: segID, Ok: false, Kind: "v3",
					Reason: "segment has no existing manifest path",
				})
				continue
			}
			if entry.Version <= 0 {
				statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
					SegmentId: segID, Ok: false, Kind: "v3",
					Reason: "missing or invalid manifest version",
				})
				continue
			}
			// Reject stale results (e.g. Spark retry) that would move the
			// manifest pointer backwards. UpdateManifestVersion short-circuits
			// only on equality, so enforcing strict monotonicity here is the
			// correct guard against silent rollback.
			_, currentVer, verErr := packed.UnmarshalManifestPath(segInfo.GetManifestPath())
			if verErr != nil {
				statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
					SegmentId: segID, Ok: false, Kind: "v3",
					Reason: "failed to parse current manifest path: " + verErr.Error(),
				})
				continue
			}
			if entry.Version <= currentVer {
				statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
					SegmentId: segID, Ok: false, Kind: "v3",
					Reason: "incoming manifest version " + strconv.FormatInt(entry.Version, 10) +
						" is not greater than current " + strconv.FormatInt(currentVer, 10),
				})
				continue
			}
			items = append(items, &messagespb.BatchUpdateManifestItem{
				SegmentId:       segID,
				ManifestVersion: entry.Version,
			})
		}
	}
	return items, statuses
}

func inferKind(entry *BackfillSegment) string {
	if entry.IsV2() {
		return "v2"
	}
	return "v3"
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
