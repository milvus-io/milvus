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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// A flush group is a local physical realization of WAL data. Its source ID
// remains stable through private sorts and retries; no group ID is replicated.
func (s *Server) progressStreamingFlushPublication(ctx context.Context, segmentID int64) {
	g := s.meta.GetSegmentChangeGroup(ctx, 0, segmentID)
	if g == nil || g.Source != model.SegmentChangeSourceStreamingFlush || g.IsTerminal() {
		return
	}
	source := s.meta.GetSegment(ctx, g.StreamingSegmentID)
	if source == nil || !meta.IsFlushState(source.GetState()) {
		return
	}
	if g.State == model.SegmentChangeStateStaged {
		notifyStreamingFlushStats(g.NewSegmentIDs...)
	}
	if g.State == model.SegmentChangeStateStaged && meta.EnableSortCompaction() {
		// Notifications are best effort. Re-issue required sort work on polling
		// after queue saturation or a restart between registration and postFlush.
		candidates := s.meta.GroupSortCandidates(g.CollectionID)
		for _, id := range g.NewSegmentIDs {
			if candidates[id] {
				select {
				case meta.GetStatsTaskChSingleton() <- id:
				default:
				}
			}
		}
	}
	// Only a retry of an already durable registration reaches this path. The
	// first receipt stays independent of readiness; subsequent publication polls
	// can complete ready groups without waiting for two inspector timer ticks.
	inspector := newSegmentChangeInspector(ctx, s.meta, s.allocator)
	defer inspector.Stop()
	if g.State == model.SegmentChangeStateStaged {
		inspector.evaluateStaged(ctx, g)
		g = s.meta.GetSegmentChangeGroup(ctx, g.CollectionID, g.GroupID)
	}
	if g != nil && g.State == model.SegmentChangeStateReady {
		inspector.tryPublish(ctx, g)
	}
}

func (s *Server) streamingFlushResult(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, bool) {
	segmentID := req.GetSegmentID()
	g := s.meta.GetSegmentChangeGroup(ctx, 0, segmentID)
	if g == nil || g.Source != model.SegmentChangeSourceStreamingFlush {
		return nil, false
	}
	if g.CollectionID != req.GetCollectionID() || g.PartitionID != req.GetPartitionID() || g.VChannel != req.GetChannel() {
		return merr.Status(merr.WrapErrDataIntegrityMsg("streaming flush %d retry changed its producer identity", segmentID)), true
	}
	if g.StreamingDataRetired || s.meta.GetCollection(g.CollectionID) == nil {
		return dataview.FlushResultStatus(nil), true
	}
	if !g.IsTerminal() {
		source := s.meta.GetSegment(ctx, g.StreamingSegmentID)
		if source != nil && !meta.IsFlushState(source.GetState()) {
			return nil, false // Replay an interrupted registration from SN meta.
		}
	}
	ref, err := s.dataViewManager.Latest(ctx, g.CollectionID)
	if err != nil {
		return merr.Status(err), true
	}
	through := ref.Version()
	ref.Deref()
	// Read group state after the snapshot. An alive group proves that its
	// growing source still serves through this immutable DataView version.
	g = s.meta.GetSegmentChangeGroup(ctx, 0, segmentID)
	if g == nil {
		return merr.Status(merr.WrapErrServiceUnavailableMsg("streaming flush group %d disappeared", segmentID)), true
	}
	if g.State == model.SegmentChangeStateCommitted {
		if g.StreamingDataVersion == nil {
			return merr.Status(merr.WrapErrDataIntegrityMsg("published streaming group %d has no handoff version", g.GroupID)), true
		}
		if token := req.GetBase().GetProperties()[dataview.FlushHandoffAck]; token != "" {
			if token != dataview.FlushHandoffToken(g.StreamingDataVersion) {
				return merr.Status(merr.WrapErrDataIntegrityMsg("streaming flush %d handoff acknowledgment has the wrong version", segmentID)), true
			}
			if err := s.meta.AcknowledgeStreamingFlush(ctx, g.GroupID, g.StreamingDataVersion); err != nil {
				return merr.Status(err), true
			}
		}
		return dataview.FlushResultStatus(g.StreamingDataVersion), true
	}
	if g.IsTerminal() {
		// Accepted WAL rows can retire only after an explicit collection drop.
		if s.meta.GetCollection(g.CollectionID) == nil {
			return dataview.FlushResultStatus(nil), true
		}
		return merr.Status(merr.WrapErrDataIntegrityMsg("streaming flush %d ended before publication", segmentID)), true
	}
	// Old writers interpret a successful version response as a final seal.
	// Never send them a pending-through proof during a rolling replacement.
	if req.GetBase().GetProperties()[dataview.FlushGroupProtocol] != "1" {
		return merr.Status(merr.WrapErrServiceUnavailableMsg("streaming flush %d is waiting for query publication", segmentID)), true
	}
	return dataview.PendingFlushResultStatus(through), true
}

func (s *Server) stageStreamingFlush(ctx context.Context, req *datapb.SaveBinlogPathsRequest, operators []meta.UpdateOperator) (*commonpb.Status, error) {
	if s.dataViewManager == nil {
		return nil, merr.WrapErrServiceUnavailableMsg("streaming flush publication manager is unavailable")
	}
	source := s.meta.GetSegment(ctx, req.GetSegmentID())
	if source == nil || source.GetCollectionID() != req.GetCollectionID() || source.GetPartitionID() != req.GetPartitionID() || source.GetInsertChannel() != req.GetChannel() {
		return nil, merr.WrapErrDataIntegrityMsg("streaming flush %d does not match its source metadata", req.GetSegmentID())
	}
	group, _, err := s.meta.StageAdmittedChange(ctx, &model.SegmentChangeGroup{
		GroupID: req.GetSegmentID(), SourceJobID: req.GetSegmentID(), StreamingSegmentID: req.GetSegmentID(),
		Source: model.SegmentChangeSourceStreamingFlush, CollectionID: req.GetCollectionID(), PartitionID: req.GetPartitionID(),
		VChannel: req.GetChannel(), NewSegmentIDs: []int64{req.GetSegmentID()},
		GrowingTransformStartAfterTimetick: req.GetGrowingTransformStartAfterTimetick(),
	}, true, operators...)
	if err != nil {
		return nil, err
	}
	if group == nil {
		return dataview.FlushResultStatus(nil), nil
	}
	// Preserve the ordinary post-flush path: it wakes required sort work even
	// when the automatic compaction sweep is configured with a long interval.
	select {
	case s.flushCh <- req.GetSegmentID():
	default:
		// Publication polling retries required preparation; a full notification
		// queue must not delay the durable registration receipt.
	}
	meta.NotifySegmentIndexBuild(req.GetSegmentID())
	notifyStreamingFlushStats(group.NewSegmentIDs...)
	result, handled := s.streamingFlushResult(ctx, req)
	if !handled {
		return nil, merr.WrapErrServiceUnavailableMsg("streaming flush %d registration is not durable yet", req.GetSegmentID())
	}
	return result, nil
}

// A full preparation queue must not delay the durability receipt. Publication
// polling retries these notifications, including after coordinator recovery.
func notifyStreamingFlushStats(ids ...int64) {
	for _, id := range ids {
		select {
		case meta.GetStatsBuildChSingleton() <- id:
		default:
		}
	}
}

func (s *Server) streamingFlushAdmitted(ctx context.Context, segmentID int64) bool {
	g := s.meta.GetSegmentChangeGroup(ctx, 0, segmentID)
	if g != nil && g.Source == model.SegmentChangeSourceStreamingFlush {
		return true
	}
	if !Params.DataCoordCfg.SegmentChangeEnabled.GetAsBool() {
		return false
	}
	// Historical Flushed records without a seal binding may already belong
	// to a sealed DataView. Preserve their legacy handshake; staging them now
	// could make old views query both the sealed record and the SN source.
	source := s.meta.GetSegment(ctx, segmentID)
	return source != nil && source.GetState() != commonpb.SegmentState_Flushed
}
