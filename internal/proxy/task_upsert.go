// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package proxy

import (
	"context"
	"strconv"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type upsertTask struct {
	baseTask
	Condition

	upsertMsg *msgstream.UpsertMsg
	req       *milvuspb.UpsertRequest
	baseMsg   msgstream.BaseMsg

	ctx context.Context

	timestamps       []uint64
	rowIDs           []int64
	result           *milvuspb.MutationResult
	idAllocator      *allocator.IDAllocator
	collectionID     UniqueID
	chMgr            channelsMgr
	chTicker         channelsTimeTicker
	vChannels        []vChan
	pChannels        []pChan
	schema           *schemaInfo
	partitionKeyMode bool
	partitionKeys    *schemapb.FieldData
	// automatic generate pk as new pk wehen autoID == true
	// delete task need use the oldIDs
	oldIDs          *schemapb.IDs
	schemaTimestamp uint64
}

// TraceCtx returns upsertTask context
func (it *upsertTask) TraceCtx() context.Context {
	return it.ctx
}

func (it *upsertTask) ID() UniqueID {
	return it.req.Base.MsgID
}

func (it *upsertTask) SetID(uid UniqueID) {
	it.req.Base.MsgID = uid
}

func (it *upsertTask) Name() string {
	return UpsertTaskName
}

func (it *upsertTask) Type() commonpb.MsgType {
	return it.req.Base.MsgType
}

func (it *upsertTask) BeginTs() Timestamp {
	return it.baseMsg.BeginTimestamp
}

func (it *upsertTask) SetTs(ts Timestamp) {
	it.baseMsg.BeginTimestamp = ts
	it.baseMsg.EndTimestamp = ts
}

func (it *upsertTask) EndTs() Timestamp {
	return it.baseMsg.EndTimestamp
}

func (it *upsertTask) getPChanStats() (map[pChan]pChanStatistics, error) {
	ret := make(map[pChan]pChanStatistics)

	channels := it.getChannels()

	beginTs := it.BeginTs()
	endTs := it.EndTs()

	for _, channel := range channels {
		ret[channel] = pChanStatistics{
			minTs: beginTs,
			maxTs: endTs,
		}
	}
	return ret, nil
}

func (it *upsertTask) setChannels() error {
	collID, err := globalMetaCache.GetCollectionID(it.ctx, it.req.GetDbName(), it.req.CollectionName)
	if err != nil {
		return err
	}
	channels, err := it.chMgr.getChannels(collID)
	if err != nil {
		return err
	}
	it.pChannels = channels
	return nil
}

func (it *upsertTask) getChannels() []pChan {
	return it.pChannels
}

func (it *upsertTask) OnEnqueue() error {
	if it.req.Base == nil {
		it.req.Base = commonpbutil.NewMsgBase()
	}
	it.req.Base.MsgType = commonpb.MsgType_Upsert
	it.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (it *upsertTask) insertPreExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Upsert-insertPreExecute")
	defer sp.End()
	collectionName := it.upsertMsg.InsertMsg.CollectionName
	if err := validateCollectionName(collectionName); err != nil {
		log.Ctx(ctx).Error("valid collection name failed", zap.String("collectionName", collectionName), zap.Error(err))
		return err
	}

	// Calculate embedding fields
	if function.HasNonBM25Functions(it.schema.CollectionSchema.Functions, []int64{}) {
		ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Proxy-Upsert-insertPreExecute-call-function-udf")
		defer sp.End()
		exec, err := function.NewFunctionExecutor(it.schema.CollectionSchema)
		if err != nil {
			return err
		}
		sp.AddEvent("Create-function-udf")
		if err := exec.ProcessInsert(ctx, it.upsertMsg.InsertMsg); err != nil {
			return err
		}
		sp.AddEvent("Call-function-udf")
	}
	rowNums := uint32(it.upsertMsg.InsertMsg.NRows())
	// set upsertTask.insertRequest.rowIDs
	tr := timerecord.NewTimeRecorder("applyPK")
	rowIDBegin, rowIDEnd, _ := it.idAllocator.Alloc(rowNums)
	metrics.ProxyApplyPrimaryKeyLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(float64(tr.ElapseSpan().Milliseconds()))

	it.upsertMsg.InsertMsg.RowIDs = make([]UniqueID, rowNums)
	it.rowIDs = make([]UniqueID, rowNums)
	for i := rowIDBegin; i < rowIDEnd; i++ {
		offset := i - rowIDBegin
		it.upsertMsg.InsertMsg.RowIDs[offset] = i
		it.rowIDs[offset] = i
	}
	// set upsertTask.insertRequest.timeStamps
	rowNum := it.upsertMsg.InsertMsg.NRows()
	it.upsertMsg.InsertMsg.Timestamps = make([]uint64, rowNum)
	it.timestamps = make([]uint64, rowNum)
	for index := range it.timestamps {
		it.upsertMsg.InsertMsg.Timestamps[index] = it.BeginTs()
		it.timestamps[index] = it.BeginTs()
	}
	// set result.SuccIndex
	sliceIndex := make([]uint32, rowNums)
	for i := uint32(0); i < rowNums; i++ {
		sliceIndex[i] = i
	}
	it.result.SuccIndex = sliceIndex

	if it.schema.EnableDynamicField {
		err := checkDynamicFieldData(it.schema.CollectionSchema, it.upsertMsg.InsertMsg)
		if err != nil {
			return err
		}
	}

	err := checkAndFlattenStructFieldData(it.schema.CollectionSchema, it.upsertMsg.InsertMsg)
	if err != nil {
		return err
	}

	allFields := typeutil.GetAllFieldSchemas(it.schema.CollectionSchema)

	// use the passed pk as new pk when autoID == false
	// automatic generate pk as new pk wehen autoID == true
	it.result.IDs, it.oldIDs, err = checkUpsertPrimaryFieldData(allFields, it.schema.CollectionSchema, it.upsertMsg.InsertMsg)
	log := log.Ctx(ctx).With(zap.String("collectionName", it.upsertMsg.InsertMsg.CollectionName))
	if err != nil {
		log.Warn("check primary field data and hash primary key failed when upsert",
			zap.Error(err))
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrParameterInvalid)
	}

	// check varchar/text with analyzer was utf-8 format
	err = checkInputUtf8Compatiable(allFields, it.upsertMsg.InsertMsg)
	if err != nil {
		log.Warn("check varchar/text format failed", zap.Error(err))
		return err
	}

	// set field ID to insert field data
	err = fillFieldPropertiesBySchema(it.upsertMsg.InsertMsg.GetFieldsData(), it.schema.CollectionSchema)
	if err != nil {
		log.Warn("insert set fieldID to fieldData failed when upsert",
			zap.Error(err))
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrParameterInvalid)
	}

	if it.partitionKeyMode {
		fieldSchema, _ := typeutil.GetPartitionKeyFieldSchema(it.schema.CollectionSchema)
		it.partitionKeys, err = getPartitionKeyFieldData(fieldSchema, it.upsertMsg.InsertMsg)
		if err != nil {
			log.Warn("get partition keys from insert request failed",
				zap.String("collectionName", collectionName),
				zap.Error(err))
			return err
		}
	} else {
		partitionTag := it.upsertMsg.InsertMsg.PartitionName
		if err = validatePartitionTag(partitionTag, true); err != nil {
			log.Warn("valid partition name failed", zap.String("partition name", partitionTag), zap.Error(err))
			return err
		}
	}

	if err := newValidateUtil(withNANCheck(), withOverflowCheck(), withMaxLenCheck()).
		Validate(it.upsertMsg.InsertMsg.GetFieldsData(), it.schema.schemaHelper, it.upsertMsg.InsertMsg.NRows()); err != nil {
		return err
	}

	log.Debug("Proxy Upsert insertPreExecute done")

	return nil
}

func (it *upsertTask) deletePreExecute(ctx context.Context) error {
	collName := it.upsertMsg.DeleteMsg.CollectionName
	log := log.Ctx(ctx).With(
		zap.String("collectionName", collName))

	if err := validateCollectionName(collName); err != nil {
		log.Info("Invalid collectionName", zap.Error(err))
		return err
	}
	collID, err := globalMetaCache.GetCollectionID(ctx, it.req.GetDbName(), collName)
	if err != nil {
		log.Info("Failed to get collection id", zap.Error(err))
		return err
	}
	it.upsertMsg.DeleteMsg.CollectionID = collID
	it.collectionID = collID

	if it.partitionKeyMode {
		// multi entities with same pk and diff partition keys may be hashed to multi physical partitions
		// if deleteMsg.partitionID = common.InvalidPartition,
		// all segments with this pk under the collection will have the delete record
		it.upsertMsg.DeleteMsg.PartitionID = common.AllPartitionsID
	} else {
		// partition name could be defaultPartitionName or name specified by sdk
		partName := it.upsertMsg.DeleteMsg.PartitionName
		if err := validatePartitionTag(partName, true); err != nil {
			log.Warn("Invalid partition name", zap.String("partitionName", partName), zap.Error(err))
			return err
		}
		partID, err := globalMetaCache.GetPartitionID(ctx, it.req.GetDbName(), collName, partName)
		if err != nil {
			log.Warn("Failed to get partition id", zap.String("collectionName", collName), zap.String("partitionName", partName), zap.Error(err))
			return err
		}
		it.upsertMsg.DeleteMsg.PartitionID = partID
	}

	it.upsertMsg.DeleteMsg.Timestamps = make([]uint64, it.upsertMsg.DeleteMsg.NumRows)
	for index := range it.upsertMsg.DeleteMsg.Timestamps {
		it.upsertMsg.DeleteMsg.Timestamps[index] = it.BeginTs()
	}
	log.Debug("Proxy Upsert deletePreExecute done")
	return nil
}

func (it *upsertTask) PreExecute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Upsert-PreExecute")
	defer sp.End()

	collectionName := it.req.CollectionName
	log := log.Ctx(ctx).With(zap.String("collectionName", collectionName))

	it.result = &milvuspb.MutationResult{
		Status: merr.Success(),
		IDs: &schemapb.IDs{
			IdField: nil,
		},
		Timestamp: it.EndTs(),
	}

	replicateID, err := GetReplicateID(ctx, it.req.GetDbName(), collectionName)
	if err != nil {
		log.Warn("get replicate info failed", zap.String("collectionName", collectionName), zap.Error(err))
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
	}
	if replicateID != "" {
		return merr.WrapErrCollectionReplicateMode("upsert")
	}

	collID, err := globalMetaCache.GetCollectionID(context.Background(), it.req.GetDbName(), collectionName)
	if err != nil {
		log.Warn("fail to get collection id", zap.Error(err))
		return err
	}
	colInfo, err := globalMetaCache.GetCollectionInfo(ctx, it.req.GetDbName(), collectionName, collID)
	if err != nil {
		log.Warn("fail to get collection info", zap.Error(err))
		return err
	}
	if it.schemaTimestamp != 0 {
		if it.schemaTimestamp != colInfo.updateTimestamp {
			err := merr.WrapErrCollectionSchemaMisMatch(collectionName)
			log.Info("collection schema mismatch", zap.String("collectionName", collectionName),
				zap.Uint64("requestSchemaTs", it.schemaTimestamp),
				zap.Uint64("collectionSchemaTs", colInfo.updateTimestamp),
				zap.Error(err))
			return err
		}
	}

	schema, err := globalMetaCache.GetCollectionSchema(ctx, it.req.GetDbName(), collectionName)
	if err != nil {
		log.Warn("Failed to get collection schema",
			zap.String("collectionName", collectionName),
			zap.Error(err))
		return err
	}
	it.schema = schema

	it.partitionKeyMode, err = isPartitionKeyMode(ctx, it.req.GetDbName(), collectionName)
	if err != nil {
		log.Warn("check partition key mode failed",
			zap.String("collectionName", collectionName),
			zap.Error(err))
		return err
	}
	if it.partitionKeyMode {
		if len(it.req.GetPartitionName()) > 0 {
			return errors.New("not support manually specifying the partition names if partition key mode is used")
		}
	} else {
		// set default partition name if not use partition key
		// insert to _default partition
		partitionTag := it.req.GetPartitionName()
		if len(partitionTag) <= 0 {
			pinfo, err := globalMetaCache.GetPartitionInfo(ctx, it.req.GetDbName(), collectionName, "")
			if err != nil {
				log.Warn("get partition info failed", zap.String("collectionName", collectionName), zap.Error(err))
				return err
			}
			it.req.PartitionName = pinfo.name
		}
	}

	it.upsertMsg = &msgstream.UpsertMsg{
		InsertMsg: &msgstream.InsertMsg{
			InsertRequest: &msgpb.InsertRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Insert),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				CollectionName: it.req.CollectionName,
				PartitionName:  it.req.PartitionName,
				FieldsData:     it.req.FieldsData,
				NumRows:        uint64(it.req.NumRows),
				Version:        msgpb.InsertDataVersion_ColumnBased,
				DbName:         it.req.DbName,
			},
		},
		DeleteMsg: &msgstream.DeleteMsg{
			DeleteRequest: &msgpb.DeleteRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Delete),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				DbName:         it.req.DbName,
				CollectionName: it.req.CollectionName,
				NumRows:        int64(it.req.NumRows),
				PartitionName:  it.req.PartitionName,
				CollectionID:   it.collectionID,
			},
		},
	}
	err = it.insertPreExecute(ctx)
	if err != nil {
		log.Warn("Fail to insertPreExecute", zap.Error(err))
		return err
	}

	err = it.deletePreExecute(ctx)
	if err != nil {
		log.Warn("Fail to deletePreExecute", zap.Error(err))
		return err
	}

	it.result.DeleteCnt = it.upsertMsg.DeleteMsg.NumRows
	it.result.InsertCnt = int64(it.upsertMsg.InsertMsg.NumRows)
	if it.result.DeleteCnt != it.result.InsertCnt {
		log.Info("DeleteCnt and InsertCnt are not the same when upsert",
			zap.Int64("DeleteCnt", it.result.DeleteCnt),
			zap.Int64("InsertCnt", it.result.InsertCnt))
	}
	it.result.UpsertCnt = it.result.InsertCnt
	log.Debug("Proxy Upsert PreExecute done")
	return nil
}

func (it *upsertTask) PostExecute(ctx context.Context) error {
	return nil
}
