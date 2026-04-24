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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// CommitBackfillResult fetches the Spark-produced BackfillResult JSON from
// object storage, classifies each segment entry (V2 vs V3), and dispatches the
// updates through the BatchUpdateManifest broadcast pipeline. V2 segments carry
// a column-group upsert; V3 segments carry a manifest version bump. The
// broadcaster ensures serialization against compaction and other DDL-like
// operations on the same collection.
func (s *Server) CommitBackfillResult(ctx context.Context, req *datapb.CommitBackfillResultRequest) (*datapb.CommitBackfillResultResponse, error) {
	log := log.Ctx(ctx).With(zap.String("resultPath", req.GetResultPath()))
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		return &datapb.CommitBackfillResultResponse{Status: merr.Status(err)}, nil
	}

	result, err := s.loadBackfillResult(ctx, req.GetResultPath())
	if err != nil {
		log.Warn("CommitBackfillResult failed to load result JSON", zap.Error(err))
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
		log.Warn("CommitBackfillResult failed to describe collection", zap.Error(err), zap.Int64("collectionID", result.CollectionID))
		return &datapb.CommitBackfillResultResponse{Status: merr.Status(err)}, nil
	}

	// Split items across multiple broadcast messages so a single
	// BatchUpdateManifestMessageBody never exceeds the broker's message size
	// limit (Pulsar defaults to 5MiB). Each batch acquires its own broadcaster
	// because broadcasterWithRK consumes its resource-key guards on the first
	// Broadcast call and would panic on any subsequent call. Failure of one
	// batch does not cancel subsequent batches; per-segment statuses reflect
	// batch-level outcomes.
	channels := []string{streaming.WAL().ControlChannel()}
	var lastErr error
	for start := 0; start < len(items); start += maxItemsPerBroadcast {
		end := start + maxItemsPerBroadcast
		if end > len(items) {
			end = len(items)
		}
		batch := items[start:end]
		if err := broadcastBackfillBatch(ctx, coll, result.CollectionID, channels, batch); err != nil {
			log.Error("CommitBackfillResult broadcast batch failed",
				zap.Error(err), zap.Int("batchStart", start), zap.Int("batchEnd", end))
			lastErr = err
			appendItemStatuses(&statuses, batch, false, err.Error())
			continue
		}
		appendItemStatuses(&statuses, batch, true, "")
	}

	committed, failed := countStatuses(statuses)
	log.Info("CommitBackfillResult broadcast completed",
		zap.Int32("total", total),
		zap.Int32("committed", committed),
		zap.Int32("failed", failed))

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
) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(coll.GetDbName()),
		message.NewSharedCollectionNameResourceKey(coll.GetDbName(), coll.GetCollectionName()),
	)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	_, err = broadcaster.Broadcast(ctx, message.NewBatchUpdateManifestMessageBuilderV2().
		WithHeader(&message.BatchUpdateManifestMessageHeader{
			CollectionId: collectionID,
		}).
		WithBody(&message.BatchUpdateManifestMessageBody{
			Items: items,
		}).
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
		return nil, merr.WrapErrParameterInvalidMsg("result_path is required")
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
	return
}

func sortStatuses(statuses []*datapb.CommitBackfillResultSegmentStatus) []*datapb.CommitBackfillResultSegmentStatus {
	sort.SliceStable(statuses, func(i, j int) bool {
		return statuses[i].GetSegmentId() < statuses[j].GetSegmentId()
	})
	return statuses
}
