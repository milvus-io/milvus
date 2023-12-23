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

package importv2

import (
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/samber/lo"
)

func WrapNoTaskError(taskID int64, taskType TaskType) error {
	return merr.WrapErrImportFailed(fmt.Sprintf("cannot find %s with id %d", taskType.String(), taskID))
}

func NewSyncTask(task *ImportTask, segmentID, partitionID int64, vchannel string, insertData *storage.InsertData) *syncmgr.SyncTask {
	metaCache := task.metaCaches[vchannel]
	AddSegment(metaCache, vchannel, segmentID, partitionID, task.GetCollectionID())

	synTask := syncmgr.NewSyncTask().
		WithInsertData(insertData).
		WithCollectionID(task.GetCollectionID()).
		WithPartitionID(partitionID).
		WithChannelName(vchannel).
		WithSegmentID(segmentID).
		//WithStartPosition(startPos).
		//WithTimeRange(tsFrom, tsTo).
		//WithLevel(segmentInfo.Level()).
		//WithCheckpoint(wb.checkpoint).
		//WithSchema(task.GetSchema()).
		//WithBatchSize(batchSize).
		WithMetaCache(metaCache).
		//WithMetaWriter(wb.metaWriter).
		WithFailureCallback(func(err error) {
			// TODO
		})
	return synTask
}

func PickSegment(task Task, fileInfo *datapb.ImportFileRequestInfo, vchannel string, partitionID int64) int64 {
	infos := task.(*ImportTask).GetSegmentsInfo()
	targets := lo.FilterMap(fileInfo.GetSegmentsInfo(), func(info *datapb.ImportSegmentRequestInfo, _ int) (int64, bool) {
		return info.GetSegmentID(), info.GetVchannel() == vchannel && info.GetPartitionID() == partitionID
	})
	infos = lo.Filter(infos, func(info *datapb.ImportSegmentInfo, _ int) bool {
		return lo.Contains(targets, info.GetSegmentID())
	})
	segment := lo.MinBy(infos, func(seg1 *datapb.ImportSegmentInfo, seg2 *datapb.ImportSegmentInfo) bool {
		return seg1.GetImportedRows() < seg2.GetImportedRows()
	})
	return segment.GetSegmentID()
}

func AddSegment(metaCache metacache.MetaCache, vchannel string, segID, partID, collID int64) {
	if _, ok := metaCache.GetSegmentByID(segID); !ok {
		metaCache.AddSegment(&datapb.SegmentInfo{
			ID:            segID,
			State:         commonpb.SegmentState_Importing,
			CollectionID:  collID,
			PartitionID:   partID,
			InsertChannel: vchannel,
		}, func(info *datapb.SegmentInfo) *metacache.BloomFilterSet {
			bfs := metacache.NewBloomFilterSet()
			return bfs
		}, metacache.UpdateImporting(true))
	}
}

func InitMetaCaches(req *datapb.ImportRequest) map[string]metacache.MetaCache {
	metaCaches := make(map[string]metacache.MetaCache)
	channels := make(map[string]struct{})
	for _, fileInfo := range req.GetFilesInfo() {
		for _, info := range fileInfo.GetSegmentsInfo() {
			channels[info.GetVchannel()] = struct{}{}
		}
	}
	for _, channel := range lo.Keys(channels) {
		info := &datapb.ChannelWatchInfo{
			Vchan: &datapb.VchannelInfo{
				CollectionID: req.GetCollectionID(),
				ChannelName:  channel,
			},
			Schema: req.GetSchema(),
		}
		metaCache := metacache.NewMetaCache(info, func(segment *datapb.SegmentInfo) *metacache.BloomFilterSet {
			return metacache.NewBloomFilterSet()
		})
		metaCaches[channel] = metaCache
	}
	return metaCaches
}
