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
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
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

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(coll.GetDbName()),
		message.NewSharedCollectionNameResourceKey(coll.GetDbName(), coll.GetCollectionName()),
	)
	if err != nil {
		log.Warn("CommitBackfillResult failed to start broadcast", zap.Error(err))
		return &datapb.CommitBackfillResultResponse{Status: merr.Status(err)}, nil
	}
	defer broadcaster.Close()

	if _, err := broadcaster.Broadcast(ctx, message.NewBatchUpdateManifestMessageBuilderV2().
		WithHeader(&message.BatchUpdateManifestMessageHeader{
			CollectionId: result.CollectionID,
		}).
		WithBody(&message.BatchUpdateManifestMessageBody{
			Items: items,
		}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast(),
	); err != nil {
		log.Error("CommitBackfillResult broadcast failed", zap.Error(err))
		// Mark every broadcast item as failed; pre-validation failures keep their
		// recorded reason.
		itemIDs := make(map[int64]string, len(items))
		for _, it := range items {
			kind := "v3"
			if it.GetV2ColumnGroups() != nil {
				kind = "v2"
			}
			itemIDs[it.GetSegmentId()] = kind
		}
		for segID, kind := range itemIDs {
			statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
				SegmentId: segID, Ok: false, Kind: kind, Reason: err.Error(),
			})
		}
		return &datapb.CommitBackfillResultResponse{
			Status:          merr.Status(err),
			TotalSegments:   total,
			FailedSegments:  int32(len(statuses)),
			SegmentStatuses: sortStatuses(statuses),
		}, nil
	}

	// Broadcast accepted. All broadcast items are considered committed from the
	// caller's perspective -- the ack callback applies the operators and any
	// failure there surfaces through datacoord's internal retry / critical log.
	for _, it := range items {
		kind := "v3"
		if it.GetV2ColumnGroups() != nil {
			kind = "v2"
		}
		statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
			SegmentId: it.GetSegmentId(), Ok: true, Kind: kind,
		})
	}

	committed, failed := countStatuses(statuses)
	log.Info("CommitBackfillResult broadcast completed",
		zap.Int32("total", total),
		zap.Int32("committed", committed),
		zap.Int32("failed", failed))

	return &datapb.CommitBackfillResultResponse{
		Status:            merr.Success(),
		TotalSegments:     total,
		CommittedSegments: committed,
		FailedSegments:    failed,
		SegmentStatuses:   sortStatuses(statuses),
	}, nil
}

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
			if entry.Version <= 0 {
				statuses = append(statuses, &datapb.CommitBackfillResultSegmentStatus{
					SegmentId: segID, Ok: false, Kind: "v3",
					Reason: "missing or invalid manifest version",
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
