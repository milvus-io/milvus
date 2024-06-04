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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func WrapNoTaskError(taskID int64, taskType TaskType) error {
	return merr.WrapErrImportFailed(fmt.Sprintf("cannot find %s with id %d", taskType.String(), taskID))
}

func NewSyncTask(ctx context.Context, task *ImportTask, segmentID, partitionID int64, vchannel string, insertData *storage.InsertData) (syncmgr.Task, error) {
	if params.Params.CommonCfg.EnableStorageV2.GetAsBool() {
		return nil, merr.WrapErrImportFailed("storage v2 is not supported") // TODO: dyh, resolve storage v2
	}

	metaCache := task.metaCaches[vchannel]
	if _, ok := metaCache.GetSegmentByID(segmentID); !ok {
		metaCache.AddSegment(&datapb.SegmentInfo{
			ID:            segmentID,
			State:         commonpb.SegmentState_Importing,
			CollectionID:  task.GetCollectionID(),
			PartitionID:   partitionID,
			InsertChannel: vchannel,
		}, func(info *datapb.SegmentInfo) *metacache.BloomFilterSet {
			bfs := metacache.NewBloomFilterSet()
			return bfs
		})
	}

	var serializer syncmgr.Serializer
	var err error
	serializer, err = syncmgr.NewStorageSerializer(
		metaCache,
		nil,
	)
	if err != nil {
		return nil, err
	}

	syncPack := &syncmgr.SyncPack{}
	syncPack.WithInsertData(insertData).
		WithCollectionID(task.GetCollectionID()).
		WithPartitionID(partitionID).
		WithChannelName(vchannel).
		WithSegmentID(segmentID).
		WithTimeRange(task.req.GetTs(), task.req.GetTs()).
		WithBatchSize(int64(insertData.GetRowNum()))

	return serializer.EncodeBuffer(ctx, syncPack)
}

func NewImportSegmentInfo(syncTask syncmgr.Task, task *ImportTask) (*datapb.ImportSegmentInfo, error) {
	segmentID := syncTask.SegmentID()
	insertBinlogs, statsBinlog, _ := syncTask.(*syncmgr.SyncTask).Binlogs()
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

func PickSegment(task *ImportTask, vchannel string, partitionID int64) int64 {
	candidates := lo.Filter(task.req.GetRequestSegments(), func(info *datapb.ImportRequestSegment, _ int) bool {
		return info.GetVchannel() == vchannel && info.GetPartitionID() == partitionID
	})

	return candidates[rand.Intn(len(candidates))].GetSegmentID()
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
	idRange := task.req.GetAutoIDRange()
	pkField, err := typeutil.GetPrimaryFieldSchema(task.GetSchema())
	if err != nil {
		return err
	}
	rowNum := GetInsertDataRowCount(data, task.GetSchema())
	ids := make([]int64, rowNum)
	for i := 0; i < rowNum; i++ {
		ids[i] = idRange.GetBegin() + int64(i)
	}
	idRange.Begin += int64(rowNum)
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
	data.Data[common.RowIDField] = &storage.Int64FieldData{Data: ids}
	tss := make([]int64, rowNum)
	ts := int64(task.req.GetTs())
	for i := 0; i < rowNum; i++ {
		tss[i] = ts
	}
	data.Data[common.TimeStampField] = &storage.Int64FieldData{Data: tss}
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
