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

package utils

import (
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

func SegmentBinlogs2SegmentInfo(collectionID int64, partitionID int64, segmentBinlogs *datapb.SegmentBinlogs) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID:            segmentBinlogs.GetSegmentID(),
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: segmentBinlogs.GetInsertChannel(),
		NumOfRows:     segmentBinlogs.GetNumOfRows(),
		Binlogs:       segmentBinlogs.GetFieldBinlogs(),
		Statslogs:     segmentBinlogs.GetStatslogs(),
		Deltalogs:     segmentBinlogs.GetDeltalogs(),
	}
}

func MergeMetaSegmentIntoSegmentInfo(info *querypb.SegmentInfo, segments ...*meta.Segment) {
	first := segments[0]
	if info.GetSegmentID() == 0 {
		*info = querypb.SegmentInfo{
			SegmentID:    first.GetID(),
			CollectionID: first.GetCollectionID(),
			PartitionID:  first.GetPartitionID(),
			NumRows:      first.GetNumOfRows(),
			DmChannel:    first.GetInsertChannel(),
			NodeIds:      make([]int64, 0),
			SegmentState: commonpb.SegmentState_Sealed,
		}
	}

	for _, segment := range segments {
		info.NodeIds = append(info.NodeIds, segment.Node)
	}
}

// packSegmentLoadInfo packs SegmentLoadInfo for given segment,
// packs with index if withIndex is true, this fetch indexes from IndexCoord
func PackSegmentLoadInfo(resp *datapb.GetSegmentInfoResponse, indexes []*querypb.FieldIndexInfo, readableVersion int64) *querypb.SegmentLoadInfo {
	var (
		deltaPosition *msgpb.MsgPosition
		positionSrc   string
	)

	segment := resp.GetInfos()[0]

	if resp.GetChannelCheckpoint() != nil && resp.ChannelCheckpoint[segment.InsertChannel] != nil {
		deltaPosition = resp.ChannelCheckpoint[segment.InsertChannel]
		positionSrc = "channelCheckpoint"
	} else if segment.GetDmlPosition() != nil {
		deltaPosition = segment.GetDmlPosition()
		positionSrc = "segmentDMLPos"
	} else {
		deltaPosition = segment.GetStartPosition()
		positionSrc = "segmentStartPos"
	}

	posTime := tsoutil.PhysicalTime(deltaPosition.GetTimestamp())
	tsLag := time.Since(posTime)
	if tsLag >= 10*time.Minute {
		log.Warn("delta position is quite stale",
			zap.Int64("collectionID", segment.GetCollectionID()),
			zap.Int64("segmentID", segment.GetID()),
			zap.String("channel", segment.InsertChannel),
			zap.String("positionSource", positionSrc),
			zap.Uint64("posTs", deltaPosition.GetTimestamp()),
			zap.Time("posTime", posTime),
			zap.Duration("tsLag", tsLag))
	}
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:       segment.ID,
		PartitionID:     segment.PartitionID,
		CollectionID:    segment.CollectionID,
		BinlogPaths:     segment.Binlogs,
		NumOfRows:       segment.NumOfRows,
		Statslogs:       segment.Statslogs,
		Deltalogs:       segment.Deltalogs,
		InsertChannel:   segment.InsertChannel,
		IndexInfos:      indexes,
		StartPosition:   segment.GetStartPosition(),
		DeltaPosition:   deltaPosition,
		ReadableVersion: readableVersion,
	}
	loadInfo.SegmentSize = calculateSegmentSize(loadInfo)
	return loadInfo
}

func calculateSegmentSize(segmentLoadInfo *querypb.SegmentLoadInfo) int64 {
	segmentSize := int64(0)

	fieldIndex := make(map[int64]*querypb.FieldIndexInfo)
	for _, index := range segmentLoadInfo.IndexInfos {
		if index.EnableIndex {
			fieldID := index.FieldID
			fieldIndex[fieldID] = index
		}
	}

	for _, fieldBinlog := range segmentLoadInfo.BinlogPaths {
		fieldID := fieldBinlog.FieldID
		if index, ok := fieldIndex[fieldID]; ok {
			segmentSize += index.IndexSize
		} else {
			segmentSize += getFieldSizeFromFieldBinlog(fieldBinlog)
		}
	}

	// Get size of state data
	for _, fieldBinlog := range segmentLoadInfo.Statslogs {
		segmentSize += getFieldSizeFromFieldBinlog(fieldBinlog)
	}

	// Get size of delete data
	for _, fieldBinlog := range segmentLoadInfo.Deltalogs {
		segmentSize += getFieldSizeFromFieldBinlog(fieldBinlog)
	}

	return segmentSize
}

func getFieldSizeFromFieldBinlog(fieldBinlog *datapb.FieldBinlog) int64 {
	fieldSize := int64(0)
	for _, binlog := range fieldBinlog.Binlogs {
		fieldSize += binlog.LogSize
	}

	return fieldSize
}

func MergeDmChannelInfo(infos []*datapb.VchannelInfo) *meta.DmChannel {
	var dmChannel *meta.DmChannel

	for _, info := range infos {
		if dmChannel == nil {
			dmChannel = meta.DmChannelFromVChannel(info)
			continue
		}

		if info.SeekPosition.GetTimestamp() < dmChannel.SeekPosition.GetTimestamp() {
			dmChannel.SeekPosition = info.SeekPosition
		}
		dmChannel.DroppedSegmentIds = append(dmChannel.DroppedSegmentIds, info.DroppedSegmentIds...)
		dmChannel.UnflushedSegmentIds = append(dmChannel.UnflushedSegmentIds, info.UnflushedSegmentIds...)
		dmChannel.FlushedSegmentIds = append(dmChannel.FlushedSegmentIds, info.FlushedSegmentIds...)
	}

	return dmChannel
}
