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

package copysegment

import (
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// registerRestoreSegments resumes registration from the durable Job, before
// dispatching any copy tasks. AddSegment preserves already-registered records.
func (c *Checker) registerRestoreSegments(job CopySegmentJob) error {
	snapshot, err := loadCopySnapshot(c.Ctx, job, c.SnapshotSource)
	if err != nil {
		return err
	}
	sources := make(map[int64]*datapb.SegmentDescription, len(snapshot.Segments))
	for _, segment := range snapshot.Segments {
		sources[segment.GetSegmentId()] = segment
	}
	recipe := job.(*Job)
	for _, mapping := range job.GetIdMappings() {
		source := sources[mapping.GetSourceSegmentId()]
		if source == nil {
			return merr.WrapErrDataIntegrityMsg("restore source segment %d is absent from snapshot", mapping.GetSourceSegmentId())
		}
		channel, ok := recipe.GetChannelMapping()[source.GetChannelName()]
		if !ok {
			return merr.WrapErrDataIntegrityMsg("restore job has no mapping for source channel %s", source.GetChannelName())
		}
		segment := RestoredSegmentInfo(source, job.GetCollectionId(), mapping.GetPartitionId(), mapping.GetTargetSegmentId(), channel, recipe.GetTransformStartAfterTimeticks()[channel])
		if err := c.Meta.AddSegment(c.Ctx, segment); err != nil {
			return err
		}
	}
	return nil
}

func RestoredSegmentInfo(source *datapb.SegmentDescription, collectionID, partitionID, segmentID int64, channel string, start uint64) *meta.SegmentInfo {
	startPos := proto.Clone(source.GetStartPosition()).(*msgpb.MsgPosition)
	dmlPos := proto.Clone(source.GetDmlPosition()).(*msgpb.MsgPosition)
	if startPos != nil {
		startPos.ChannelName = channel
	}
	if dmlPos != nil {
		dmlPos.ChannelName = channel
	}
	return meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: segmentID, CollectionID: collectionID, PartitionID: partitionID,
		InsertChannel: channel, NumOfRows: source.GetNumOfRows(),
		State: commonpb.SegmentState_Importing, IsImporting: true,
		MaxRowNum: Params.DataCoordCfg.SegmentMaxSize.GetAsInt64(),
		Level:     source.GetSegmentLevel(), LastExpireTime: math.MaxUint64,
		StartPosition: startPos, DmlPosition: dmlPos,
		StorageVersion: source.GetStorageVersion(), IsSorted: source.GetIsSorted(),
		CommitTimestamp: source.GetCommitTimestamp(), TransformStartAfterTimetick: start,
	})
}
