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
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func WrapTaskNotFoundError(taskID int64) error {
	return merr.WrapErrImportFailed(fmt.Sprintf("cannot find import task with id %d", taskID))
}

func NewSyncTask(ctx context.Context,
	allocator allocator.Interface,
	metaCaches map[string]metacache.MetaCache,
	ts uint64,
	segmentID, partitionID, collectionID int64, vchannel string,
	insertData *storage.InsertData,
	deleteData *storage.DeleteData,
) (syncmgr.Task, error) {
	metaCache := metaCaches[vchannel]
	if _, ok := metaCache.GetSegmentByID(segmentID); !ok {
		metaCache.AddSegment(&datapb.SegmentInfo{
			ID:            segmentID,
			State:         commonpb.SegmentState_Importing,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: vchannel,
		}, func(info *datapb.SegmentInfo) pkoracle.PkStat {
			bfs := pkoracle.NewBloomFilterSet()
			return bfs
		})
	}

	var serializer syncmgr.Serializer
	var err error
	serializer, err = syncmgr.NewStorageSerializer(
		allocator,
		metaCache,
		nil,
	)
	if err != nil {
		return nil, err
	}

	syncPack := &syncmgr.SyncPack{}
	syncPack.WithInsertData([]*storage.InsertData{insertData}).
		WithDeleteData(deleteData).
		WithCollectionID(collectionID).
		WithPartitionID(partitionID).
		WithChannelName(vchannel).
		WithSegmentID(segmentID).
		WithTimeRange(ts, ts).
		WithBatchSize(int64(insertData.GetRowNum()))

	return serializer.EncodeBuffer(ctx, syncPack)
}

func NewImportSegmentInfo(syncTask syncmgr.Task, metaCaches map[string]metacache.MetaCache) (*datapb.ImportSegmentInfo, error) {
	segmentID := syncTask.SegmentID()
	insertBinlogs, statsBinlog, deltaLog := syncTask.(*syncmgr.SyncTask).Binlogs()
	metaCache := metaCaches[syncTask.ChannelName()]
	segment, ok := metaCache.GetSegmentByID(segmentID)
	if !ok {
		return nil, merr.WrapErrSegmentNotFound(segmentID, "import failed")
	}
	var deltaLogs []*datapb.FieldBinlog
	if len(deltaLog.GetBinlogs()) > 0 {
		deltaLogs = []*datapb.FieldBinlog{deltaLog}
	}
	return &datapb.ImportSegmentInfo{
		SegmentID:    segmentID,
		ImportedRows: segment.FlushedRows(),
		Binlogs:      lo.Values(insertBinlogs),
		Statslogs:    lo.Values(statsBinlog),
		Deltalogs:    deltaLogs,
	}, nil
}

func PickSegment(segments []*datapb.ImportRequestSegment, vchannel string, partitionID int64) (int64, error) {
	candidates := lo.Filter(segments, func(info *datapb.ImportRequestSegment, _ int) bool {
		return info.GetVchannel() == vchannel && info.GetPartitionID() == partitionID
	})

	if len(candidates) == 0 {
		return 0, fmt.Errorf("no candidate segments found for channel %s and partition %d",
			vchannel, partitionID)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return candidates[r.Intn(len(candidates))].GetSegmentID(), nil
}

func CheckRowsEqual(schema *schemapb.CollectionSchema, data *storage.InsertData) error {
	if len(data.Data) == 0 {
		return nil
	}
	idToField := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})

	var field int64
	var rows int
	for fieldID, d := range data.Data {
		if idToField[fieldID].GetIsPrimaryKey() && idToField[fieldID].GetAutoID() {
			continue
		}
		field, rows = fieldID, d.RowNum()
		break
	}
	for fieldID, d := range data.Data {
		if idToField[fieldID].GetIsPrimaryKey() && idToField[fieldID].GetAutoID() {
			continue
		}
		if d.RowNum() != rows {
			return merr.WrapErrImportFailed(
				fmt.Sprintf("imported rows are not aligned, field '%s' with '%d' rows, field '%s' with '%d' rows",
					idToField[field].GetName(), rows, idToField[fieldID].GetName(), d.RowNum()))
		}
	}
	return nil
}

func AppendSystemFieldsData(task *ImportTask, data *storage.InsertData) error {
	pkField, err := typeutil.GetPrimaryFieldSchema(task.GetSchema())
	if err != nil {
		return err
	}
	rowNum := GetInsertDataRowCount(data, task.GetSchema())
	ids := make([]int64, rowNum)
	start, _, err := task.allocator.Alloc(uint32(rowNum))
	if err != nil {
		return err
	}
	for i := 0; i < rowNum; i++ {
		ids[i] = start + int64(i)
	}
	if pkField.GetAutoID() {
		switch pkField.GetDataType() {
		case schemapb.DataType_Int64:
			data.Data[pkField.GetFieldID()] = &storage.Int64FieldData{Data: ids}
		case schemapb.DataType_VarChar:
			strIDs := lo.Map(ids, func(id int64, _ int) string {
				return strconv.FormatInt(id, 10)
			})
			data.Data[pkField.GetFieldID()] = &storage.StringFieldData{Data: strIDs}
		}
	}
	if _, ok := data.Data[common.RowIDField]; !ok { // for binlog import, keep original rowID and ts
		data.Data[common.RowIDField] = &storage.Int64FieldData{Data: ids}
	}
	if _, ok := data.Data[common.TimeStampField]; !ok {
		tss := make([]int64, rowNum)
		ts := int64(task.req.GetTs())
		for i := 0; i < rowNum; i++ {
			tss[i] = ts
		}
		data.Data[common.TimeStampField] = &storage.Int64FieldData{Data: tss}
	}
	return nil
}

func GetInsertDataRowCount(data *storage.InsertData, schema *schemapb.CollectionSchema) int {
	fields := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})
	for fieldID, fd := range data.Data {
		if fields[fieldID].GetIsDynamic() {
			continue
		}
		if fd.RowNum() != 0 {
			return fd.RowNum()
		}
	}
	return 0
}

func LogStats(manager TaskManager) {
	logFunc := func(tasks []Task, taskType TaskType) {
		byState := lo.GroupBy(tasks, func(t Task) datapb.ImportTaskStateV2 {
			return t.GetState()
		})
		log.Info("import task stats", zap.String("type", taskType.String()),
			zap.Int("pending", len(byState[datapb.ImportTaskStateV2_Pending])),
			zap.Int("inProgress", len(byState[datapb.ImportTaskStateV2_InProgress])),
			zap.Int("completed", len(byState[datapb.ImportTaskStateV2_Completed])),
			zap.Int("failed", len(byState[datapb.ImportTaskStateV2_Failed])))
	}
	tasks := manager.GetBy(WithType(PreImportTaskType))
	logFunc(tasks, PreImportTaskType)
	tasks = manager.GetBy(WithType(ImportTaskType))
	logFunc(tasks, ImportTaskType)
}

func UnsetAutoID(schema *schemapb.CollectionSchema) {
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() && field.GetAutoID() {
			field.AutoID = false
			return
		}
	}
}

func NewMetaCache(req *datapb.ImportRequest) map[string]metacache.MetaCache {
	metaCaches := make(map[string]metacache.MetaCache)
	schema := typeutil.AppendSystemFields(req.GetSchema())
	for _, channel := range req.GetVchannels() {
		info := &datapb.ChannelWatchInfo{
			Vchan: &datapb.VchannelInfo{
				CollectionID: req.GetCollectionID(),
				ChannelName:  channel,
			},
			Schema: schema,
		}
		metaCache := metacache.NewMetaCache(info, func(segment *datapb.SegmentInfo) pkoracle.PkStat {
			return pkoracle.NewBloomFilterSet()
		})
		metaCaches[channel] = metaCache
	}
	return metaCaches
}
