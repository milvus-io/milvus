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
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/pkg/common"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
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

func NewSyncTask(task *ImportTask, segmentID, partitionID int64, vchannel string, insertData *storage.InsertData) (syncmgr.Task, error) {
	metaCache := task.metaCaches[vchannel]
	AddSegment(metaCache, vchannel, segmentID, partitionID, task.GetCollectionID())

	var serializer syncmgr.Serializer
	var err error
	if params.Params.CommonCfg.EnableStorageV2.GetAsBool() {
		serializer, err = syncmgr.NewStorageV2Serializer(
			nil, // TODO: dyh, resolve storage v2
			metaCache,
			nil,
		)
	} else {
		serializer, err = syncmgr.NewStorageSerializer(
			metaCache,
			nil,
		)
	}
	if err != nil {
		return nil, err
	}

	syncPack := &syncmgr.SyncPack{}
	syncPack.WithInsertData(insertData).
		WithCollectionID(task.GetCollectionID()).
		WithPartitionID(partitionID).
		WithChannelName(vchannel).
		WithSegmentID(segmentID)

	return serializer.EncodeBuffer(context.Background(), syncPack) // TODO: dyh, resolve context
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
	requestSegments := task.req.GetRequestSegments()
	targets := lo.Filter(lo.Values(requestSegments), func(info *datapb.ImportRequestSegment, _ int) bool {
		return info.GetVchannel() == vchannel && info.GetPartitionID() == partitionID
	})

	importedSegments := lo.KeyBy(task.GetSegmentsInfo(), func(segment *datapb.ImportSegmentInfo) int64 {
		return segment.GetSegmentID()
	})

	minSegment := lo.MinBy(targets, func(seg1 *datapb.ImportRequestSegment, seg2 *datapb.ImportRequestSegment) bool {
		return importedSegments[seg1.GetSegmentID()].GetImportedRows() < importedSegments[seg2.GetSegmentID()].GetImportedRows()
	})
	return minSegment.GetSegmentID()
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

func CheckRowsEqual(schema *schemapb.CollectionSchema, data *storage.InsertData) error {
	if len(data.Data) == 0 {
		return nil
	}
	fields := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})

	var field int64
	var rows int
	for fieldID, d := range data.Data {
		field, rows = fieldID, d.RowNum()
		break
	}
	for fieldID, d := range data.Data {
		if d.RowNum() != rows {
			return merr.WrapErrImportFailed(
				fmt.Sprintf("imported rows are not aligned, field '%s' with '%d' rows, field '%s' with '%d' rows",
					fields[field].GetName(), rows, fields[fieldID].GetName(), d.RowNum()))
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
	rowNum := GetInsertDataRowNum(data, task.GetSchema())
	ids := make([]int64, rowNum)
	for i := 0; i < rowNum; i++ {
		ids[i] = idRange.GetBegin() + int64(i)
	}
	idRange.Begin += int64(rowNum)
	if pkField.GetAutoID() {
		data.Data[pkField.GetFieldID()] = &storage.Int64FieldData{Data: ids}
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

func GetInsertDataRowNum(data *storage.InsertData, schema *schemapb.CollectionSchema) int {
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

func FillDynamicData(data *storage.InsertData, schema *schemapb.CollectionSchema) error { // TODO: dyh move it to numpy reader, only numpy import need it
	if !schema.GetEnableDynamicField() {
		return nil
	}
	dynamicField, err := typeutil.GetDynamicField(schema)
	if err != nil {
		return nil
	}
	rowNum := GetInsertDataRowNum(data, schema)
	dynamicData := data.Data[dynamicField.GetFieldID()]
	if dynamicData.RowNum() >= rowNum {
		return nil
	}
	jsonFD := dynamicData.(*storage.JSONFieldData)
	bs := []byte("{}")
	count := rowNum - dynamicData.RowNum()
	for i := 0; i < count; i++ {
		jsonFD.Data = append(jsonFD.Data, bs)
	}
	data.Data[dynamicField.GetFieldID()] = dynamicData
	return nil
}
