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

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
		WithMetaCache(metaCache)

	// TODO: dyh, fix these
	// WithStartPosition(startPos).
	// WithTimeRange(tsFrom, tsTo).
	// WithLevel(segmentInfo.Level()).
	// WithCheckpoint(wb.checkpoint).
	// WithSchema(task.GetSchema()).
	// WithBatchSize(batchSize).
	// WithMetaWriter(wb.metaWriter).
	// WithFailureCallback()

	return synTask
}

func NewImportSegmentInfo(syncTask *syncmgr.SyncTask, task *ImportTask) (*datapb.ImportSegmentInfo, error) {
	segmentID := syncTask.SegmentID()
	insertBinlogs, statsBinlog, _ := syncTask.Binlogs()
	metaCache := task.metaCaches[syncTask.ChannelName()]
	segment, ok := metaCache.GetSegmentByID(segmentID)
	if !ok {
		return nil, merr.WrapErrSegmentNotFound(segmentID, "import failed")
	}
	return &datapb.ImportSegmentInfo{
		SegmentID:    segmentID,
		ImportedRows: segment.FlushedRows(),
		Binlogs:      lo.Values(insertBinlogs),
		Statslogs:    lo.Values(statsBinlog),
	}, nil
}

func InitHashedData(channels []string, partitions []int64, schema *schemapb.CollectionSchema) (HashedData, error) {
	var err error
	res := make(HashedData, len(channels))
	for i := range channels {
		res[i] = make([]*storage.InsertData, len(partitions))
		for j := range partitions {
			res[i][j], err = storage.NewInsertData(schema)
			if err != nil {
				return nil, err
			}
		}
	}
	return res, nil
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

func HashFunc(pkDataType schemapb.DataType) (func(pk interface{}, shardNum int64) int64, error) {
	switch pkDataType {
	case schemapb.DataType_Int64:
		return func(pk interface{}, shardNum int64) int64 {
			hash, _ := typeutil.Hash32Int64(pk.(int64))
			return int64(hash) % shardNum
		}, nil
	case schemapb.DataType_VarChar:
		return func(pk interface{}, shardNum int64) int64 {
			hash := typeutil.HashString2Uint32(pk.(string)) // TODO: use HashString?
			return int64(hash) % shardNum
		}, nil
	default:
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("unexpected pk type %s", pkDataType.String()))
	}
}
