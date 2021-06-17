// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxynode

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"time"
	"unsafe"

	"github.com/milvus-io/milvus/internal/proto/planpb"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	InsertTaskName                  = "InsertTask"
	CreateCollectionTaskName        = "CreateCollectionTask"
	DropCollectionTaskName          = "DropCollectionTask"
	SearchTaskName                  = "SearchTask"
	RetrieveTaskName                = "RetrieveTask"
	AnnsFieldKey                    = "anns_field"
	TopKKey                         = "topk"
	MetricTypeKey                   = "metric_type"
	SearchParamsKey                 = "params"
	HasCollectionTaskName           = "HasCollectionTask"
	DescribeCollectionTaskName      = "DescribeCollectionTask"
	GetCollectionStatisticsTaskName = "GetCollectionStatisticsTask"
	GetPartitionStatisticsTaskName  = "GetPartitionStatisticsTask"
	ShowCollectionTaskName          = "ShowCollectionTask"
	CreatePartitionTaskName         = "CreatePartitionTask"
	DropPartitionTaskName           = "DropPartitionTask"
	HasPartitionTaskName            = "HasPartitionTask"
	ShowPartitionTaskName           = "ShowPartitionTask"
	CreateIndexTaskName             = "CreateIndexTask"
	DescribeIndexTaskName           = "DescribeIndexTask"
	DropIndexTaskName               = "DropIndexTask"
	GetIndexStateTaskName           = "GetIndexStateTask"
	GetIndexBuildProgressTaskName   = "GetIndexBuildProgressTask"
	FlushTaskName                   = "FlushTask"
	LoadCollectionTaskName          = "LoadCollectionTask"
	ReleaseCollectionTaskName       = "ReleaseCollectionTask"
	LoadPartitionTaskName           = "LoadPartitionTask"
	ReleasePartitionTaskName        = "ReleasePartitionTask"
)

type task interface {
	TraceCtx() context.Context
	ID() UniqueID       // return ReqID
	SetID(uid UniqueID) // set ReqID
	Name() string
	Type() commonpb.MsgType
	BeginTs() Timestamp
	EndTs() Timestamp
	SetTs(ts Timestamp)
	OnEnqueue() error
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
}

type dmlTask interface {
	task
	getChannels() ([]vChan, error)
	getPChanStats() (map[pChan]pChanStatistics, error)
}

type BaseInsertTask = msgstream.InsertMsg

type InsertTask struct {
	BaseInsertTask
	req *milvuspb.InsertRequest
	Condition
	ctx            context.Context
	dataService    types.DataService
	result         *milvuspb.InsertResponse
	rowIDAllocator *allocator.IDAllocator
	segIDAssigner  *SegIDAssigner
	chMgr          channelsMgr
	chTicker       channelsTimeTicker
	vChannels      []vChan
	pChannels      []pChan
}

func (it *InsertTask) TraceCtx() context.Context {
	return it.ctx
}

func (it *InsertTask) ID() UniqueID {
	return it.Base.MsgID
}

func (it *InsertTask) SetID(uid UniqueID) {
	it.Base.MsgID = uid
}

func (it *InsertTask) Name() string {
	return InsertTaskName
}

func (it *InsertTask) Type() commonpb.MsgType {
	return it.Base.MsgType
}

func (it *InsertTask) BeginTs() Timestamp {
	return it.BeginTimestamp
}

func (it *InsertTask) SetTs(ts Timestamp) {
	it.BeginTimestamp = ts
	it.EndTimestamp = ts
}

func (it *InsertTask) EndTs() Timestamp {
	return it.EndTimestamp
}

func (it *InsertTask) getPChanStats() (map[pChan]pChanStatistics, error) {
	ret := make(map[pChan]pChanStatistics)

	channels, err := it.getChannels()
	if err != nil {
		return ret, err
	}

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

func (it *InsertTask) getChannels() ([]pChan, error) {
	collID, err := globalMetaCache.GetCollectionID(it.ctx, it.CollectionName)
	if err != nil {
		return nil, err
	}
	var channels []pChan
	channels, err = it.chMgr.getChannels(collID)
	if err != nil {
		err = it.chMgr.createDMLMsgStream(collID)
		if err != nil {
			return nil, err
		}
		channels, err = it.chMgr.getChannels(collID)
	}
	return channels, err
}

func (it *InsertTask) OnEnqueue() error {
	it.BaseInsertTask.InsertRequest.Base = &commonpb.MsgBase{}
	return nil
}

func (it *InsertTask) transferColumnBasedRequestToRowBasedData() error {
	dTypes := make([]schemapb.DataType, 0, len(it.req.FieldsData))
	datas := make([][]interface{}, 0, len(it.req.FieldsData))
	rowNum := 0

	appendScalarField := func(getDataFunc func() interface{}) error {
		fieldDatas := reflect.ValueOf(getDataFunc())
		if rowNum != 0 && rowNum != fieldDatas.Len() {
			return errors.New("the row num of different column is not equal")
		}
		rowNum = fieldDatas.Len()
		datas = append(datas, make([]interface{}, 0, rowNum))
		idx := len(datas) - 1
		for i := 0; i < rowNum; i++ {
			datas[idx] = append(datas[idx], fieldDatas.Index(i).Interface())
		}

		return nil
	}

	appendFloatVectorField := func(fDatas []float32, dim int64) error {
		l := len(fDatas)
		if int64(l)%dim != 0 {
			return errors.New("invalid vectors")
		}
		r := int64(l) / dim
		if rowNum != 0 && rowNum != int(r) {
			return errors.New("the row num of different column is not equal")
		}
		rowNum = int(r)
		datas = append(datas, make([]interface{}, 0, rowNum))
		idx := len(datas) - 1
		vector := make([]float32, 0, dim)
		for i := 0; i < l; i++ {
			vector = append(vector, fDatas[i])
			if int64(i+1)%dim == 0 {
				datas[idx] = append(datas[idx], vector)
				vector = make([]float32, 0, dim)
			}
		}

		return nil
	}

	appendBinaryVectorField := func(bDatas []byte, dim int64) error {
		l := len(bDatas)
		if dim%8 != 0 {
			return errors.New("invalid dim")
		}
		if (8*int64(l))%dim != 0 {
			return errors.New("invalid vectors")
		}
		r := (8 * int64(l)) / dim
		if rowNum != 0 && rowNum != int(r) {
			return errors.New("the row num of different column is not equal")
		}
		rowNum = int(r)
		datas = append(datas, make([]interface{}, 0, rowNum))
		idx := len(datas) - 1
		vector := make([]byte, 0, dim)
		for i := 0; i < l; i++ {
			vector = append(vector, bDatas[i])
			if (8*int64(i+1))%dim == 0 {
				datas[idx] = append(datas[idx], vector)
				vector = make([]byte, 0, dim)
			}
		}

		return nil
	}

	for _, field := range it.req.FieldsData {
		switch field.Field.(type) {
		case *schemapb.FieldData_Scalars:
			scalarField := field.GetScalars()
			switch scalarField.Data.(type) {
			case *schemapb.ScalarField_BoolData:
				err := appendScalarField(func() interface{} {
					return scalarField.GetBoolData().Data
				})
				if err != nil {
					return err
				}
			case *schemapb.ScalarField_IntData:
				err := appendScalarField(func() interface{} {
					return scalarField.GetIntData().Data
				})
				if err != nil {
					return err
				}
			case *schemapb.ScalarField_LongData:
				err := appendScalarField(func() interface{} {
					return scalarField.GetLongData().Data
				})
				if err != nil {
					return err
				}
			case *schemapb.ScalarField_FloatData:
				err := appendScalarField(func() interface{} {
					return scalarField.GetFloatData().Data
				})
				if err != nil {
					return err
				}
			case *schemapb.ScalarField_DoubleData:
				err := appendScalarField(func() interface{} {
					return scalarField.GetDoubleData().Data
				})
				if err != nil {
					return err
				}
			case *schemapb.ScalarField_BytesData:
				return errors.New("bytes field is not supported now")
			case *schemapb.ScalarField_StringData:
				return errors.New("string field is not supported now")
			case nil:
				continue
			default:
				continue
			}
		case *schemapb.FieldData_Vectors:
			vectorField := field.GetVectors()
			switch vectorField.Data.(type) {
			case *schemapb.VectorField_FloatVector:
				floatVectorFieldData := vectorField.GetFloatVector().Data
				dim := vectorField.GetDim()
				err := appendFloatVectorField(floatVectorFieldData, dim)
				if err != nil {
					return err
				}
			case *schemapb.VectorField_BinaryVector:
				binaryVectorFieldData := vectorField.GetBinaryVector()
				dim := vectorField.GetDim()
				err := appendBinaryVectorField(binaryVectorFieldData, dim)
				if err != nil {
					return err
				}
			case nil:
				continue
			default:
				continue
			}
		case nil:
			continue
		default:
			continue
		}

		dTypes = append(dTypes, field.Type)
	}

	it.RowData = make([]*commonpb.Blob, 0, rowNum)
	l := len(dTypes)
	// TODO(dragondriver): big endian or little endian?
	endian := binary.LittleEndian
	printed := false
	for i := 0; i < rowNum; i++ {
		blob := &commonpb.Blob{
			Value: make([]byte, 0),
		}

		for j := 0; j < l; j++ {
			var buffer bytes.Buffer
			switch dTypes[j] {
			case schemapb.DataType_Bool:
				d := datas[j][i].(bool)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_Int8:
				d := datas[j][i].(int8)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_Int16:
				d := datas[j][i].(int16)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_Int32:
				d := datas[j][i].(int32)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_Int64:
				d := datas[j][i].(int64)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_Float:
				d := datas[j][i].(float32)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_Double:
				d := datas[j][i].(float64)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_FloatVector:
				d := datas[j][i].([]float32)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			case schemapb.DataType_BinaryVector:
				d := datas[j][i].([]byte)
				err := binary.Write(&buffer, endian, d)
				if err != nil {
					log.Warn("ConvertData", zap.Error(err))
				}
				blob.Value = append(blob.Value, buffer.Bytes()...)
			default:
				log.Warn("unsupported data type")
			}
		}
		if !printed {
			log.Debug("ProxyNode, transform", zap.Any("ID", it.ID()), zap.Any("BlobLen", len(blob.Value)), zap.Any("dTypes", dTypes))
			printed = true
		}
		it.RowData = append(it.RowData, blob)
	}

	return nil
}

func (it *InsertTask) PreExecute(ctx context.Context) error {
	it.Base.MsgType = commonpb.MsgType_Insert
	it.Base.SourceID = Params.ProxyID

	collectionName := it.BaseInsertTask.CollectionName
	if err := ValidateCollectionName(collectionName); err != nil {
		return err
	}
	partitionTag := it.BaseInsertTask.PartitionName
	if err := ValidatePartitionTag(partitionTag, true); err != nil {
		return err
	}

	err := it.transferColumnBasedRequestToRowBasedData()
	if err != nil {
		return err
	}

	rowNum := len(it.RowData)
	it.Timestamps = make([]uint64, rowNum)
	for index := range it.Timestamps {
		it.Timestamps[index] = it.BeginTimestamp
	}

	return nil
}

func (it *InsertTask) _assignSegmentID(stream msgstream.MsgStream, pack *msgstream.MsgPack) (*msgstream.MsgPack, error) {
	newPack := &msgstream.MsgPack{
		BeginTs:        pack.BeginTs,
		EndTs:          pack.EndTs,
		StartPositions: pack.StartPositions,
		EndPositions:   pack.EndPositions,
		Msgs:           nil,
	}
	tsMsgs := pack.Msgs
	hashKeys := stream.ComputeProduceChannelIndexes(tsMsgs)
	reqID := it.Base.MsgID
	channelCountMap := make(map[int32]uint32)    //   channelID to count
	channelMaxTSMap := make(map[int32]Timestamp) //  channelID to max Timestamp
	channelNames := stream.GetProduceChannels()
	log.Debug("_assignSemgentID, produceChannels:", zap.Any("Channels", channelNames))

	for i, request := range tsMsgs {
		if request.Type() != commonpb.MsgType_Insert {
			return nil, fmt.Errorf("msg's must be Insert")
		}
		insertRequest, ok := request.(*msgstream.InsertMsg)
		if !ok {
			return nil, fmt.Errorf("msg's must be Insert")
		}

		keys := hashKeys[i]
		timestampLen := len(insertRequest.Timestamps)
		rowIDLen := len(insertRequest.RowIDs)
		rowDataLen := len(insertRequest.RowData)
		keysLen := len(keys)

		if keysLen != timestampLen || keysLen != rowIDLen || keysLen != rowDataLen {
			return nil, fmt.Errorf("the length of hashValue, timestamps, rowIDs, RowData are not equal")
		}

		for idx, channelID := range keys {
			channelCountMap[channelID]++
			if _, ok := channelMaxTSMap[channelID]; !ok {
				channelMaxTSMap[channelID] = typeutil.ZeroTimestamp
			}
			ts := insertRequest.Timestamps[idx]
			if channelMaxTSMap[channelID] < ts {
				channelMaxTSMap[channelID] = ts
			}
		}
	}

	reqSegCountMap := make(map[int32]map[UniqueID]uint32)

	for channelID, count := range channelCountMap {
		ts, ok := channelMaxTSMap[channelID]
		if !ok {
			ts = typeutil.ZeroTimestamp
			log.Debug("Warning: did not get max Timestamp!")
		}
		channelName := channelNames[channelID]
		if channelName == "" {
			return nil, fmt.Errorf("ProxyNode, repack_func, can not found channelName")
		}
		mapInfo, err := it.segIDAssigner.GetSegmentID(it.CollectionID, it.PartitionID, channelName, count, ts)
		if err != nil {
			return nil, err
		}
		reqSegCountMap[channelID] = make(map[UniqueID]uint32)
		reqSegCountMap[channelID] = mapInfo
		log.Debug("ProxyNode", zap.Int64("repackFunc, reqSegCountMap, reqID", reqID), zap.Any("mapinfo", mapInfo))
	}

	reqSegAccumulateCountMap := make(map[int32][]uint32)
	reqSegIDMap := make(map[int32][]UniqueID)
	reqSegAllocateCounter := make(map[int32]uint32)

	for channelID, segInfo := range reqSegCountMap {
		reqSegAllocateCounter[channelID] = 0
		keys := make([]UniqueID, len(segInfo))
		i := 0
		for key := range segInfo {
			keys[i] = key
			i++
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		accumulate := uint32(0)
		for _, key := range keys {
			accumulate += segInfo[key]
			if _, ok := reqSegAccumulateCountMap[channelID]; !ok {
				reqSegAccumulateCountMap[channelID] = make([]uint32, 0)
			}
			reqSegAccumulateCountMap[channelID] = append(
				reqSegAccumulateCountMap[channelID],
				accumulate,
			)
			if _, ok := reqSegIDMap[channelID]; !ok {
				reqSegIDMap[channelID] = make([]UniqueID, 0)
			}
			reqSegIDMap[channelID] = append(
				reqSegIDMap[channelID],
				key,
			)
		}
	}

	var getSegmentID = func(channelID int32) UniqueID {
		reqSegAllocateCounter[channelID]++
		cur := reqSegAllocateCounter[channelID]
		accumulateSlice := reqSegAccumulateCountMap[channelID]
		segIDSlice := reqSegIDMap[channelID]
		for index, count := range accumulateSlice {
			if cur <= count {
				return segIDSlice[index]
			}
		}
		log.Warn("Can't Found SegmentID")
		return 0
	}

	factor := 10
	threshold := Params.PulsarMaxMessageSize / factor
	log.Debug("ProxyNode", zap.Int("threshold of message size: ", threshold))
	// not accurate
	getFixedSizeOfInsertMsg := func(msg *msgstream.InsertMsg) int {
		size := 0

		size += int(unsafe.Sizeof(*msg.Base))
		size += int(unsafe.Sizeof(msg.DbName))
		size += int(unsafe.Sizeof(msg.CollectionName))
		size += int(unsafe.Sizeof(msg.PartitionName))
		size += int(unsafe.Sizeof(msg.DbID))
		size += int(unsafe.Sizeof(msg.CollectionID))
		size += int(unsafe.Sizeof(msg.PartitionID))
		size += int(unsafe.Sizeof(msg.SegmentID))
		size += int(unsafe.Sizeof(msg.ChannelID))
		size += int(unsafe.Sizeof(msg.Timestamps))
		size += int(unsafe.Sizeof(msg.RowIDs))
		return size
	}

	result := make(map[int32]msgstream.TsMsg)
	curMsgSizeMap := make(map[int32]int)

	for i, request := range tsMsgs {
		insertRequest := request.(*msgstream.InsertMsg)
		keys := hashKeys[i]
		collectionName := insertRequest.CollectionName
		collectionID := insertRequest.CollectionID
		partitionID := insertRequest.PartitionID
		partitionName := insertRequest.PartitionName
		proxyID := insertRequest.Base.SourceID
		for index, key := range keys {
			ts := insertRequest.Timestamps[index]
			rowID := insertRequest.RowIDs[index]
			row := insertRequest.RowData[index]
			segmentID := getSegmentID(key)
			_, ok := result[key]
			if !ok {
				sliceRequest := internalpb.InsertRequest{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_Insert,
						MsgID:     reqID,
						Timestamp: ts,
						SourceID:  proxyID,
					},
					CollectionID:   collectionID,
					PartitionID:    partitionID,
					CollectionName: collectionName,
					PartitionName:  partitionName,
					SegmentID:      segmentID,
					// todo rename to ChannelName
					ChannelID: channelNames[key],
				}
				insertMsg := &msgstream.InsertMsg{
					BaseMsg: msgstream.BaseMsg{
						Ctx: request.TraceCtx(),
					},
					InsertRequest: sliceRequest,
				}
				result[key] = insertMsg
				curMsgSizeMap[key] = getFixedSizeOfInsertMsg(insertMsg)
			}
			curMsg := result[key].(*msgstream.InsertMsg)
			curMsgSize := curMsgSizeMap[key]
			curMsg.HashValues = append(curMsg.HashValues, insertRequest.HashValues[index])
			curMsg.Timestamps = append(curMsg.Timestamps, ts)
			curMsg.RowIDs = append(curMsg.RowIDs, rowID)
			curMsg.RowData = append(curMsg.RowData, row)
			curMsgSize += 4 + 8 + int(unsafe.Sizeof(row.Value))
			curMsgSize += len(row.Value)

			if curMsgSize >= threshold {
				newPack.Msgs = append(newPack.Msgs, curMsg)
				delete(result, key)
				curMsgSize = 0
			}

			curMsgSizeMap[key] = curMsgSize
		}
	}
	for _, msg := range result {
		if msg != nil {
			newPack.Msgs = append(newPack.Msgs, msg)
		}
	}

	return newPack, nil
}

func (it *InsertTask) Execute(ctx context.Context) error {
	collectionName := it.BaseInsertTask.CollectionName
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, collectionName)
	log.Debug("ProxyNode Insert", zap.Any("collSchema", collSchema))
	if err != nil {
		return err
	}
	autoID := collSchema.AutoID
	collID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil {
		return err
	}
	it.CollectionID = collID
	var partitionID UniqueID
	if len(it.PartitionName) > 0 {
		partitionID, err = globalMetaCache.GetPartitionID(ctx, collectionName, it.PartitionName)
		if err != nil {
			return err
		}
	} else {
		partitionID, err = globalMetaCache.GetPartitionID(ctx, collectionName, Params.DefaultPartitionName)
		if err != nil {
			return err
		}
	}
	it.PartitionID = partitionID
	var rowIDBegin UniqueID
	var rowIDEnd UniqueID
	rowNums := len(it.BaseInsertTask.RowData)
	rowIDBegin, rowIDEnd, _ = it.rowIDAllocator.Alloc(uint32(rowNums))

	it.BaseInsertTask.RowIDs = make([]UniqueID, rowNums)
	for i := rowIDBegin; i < rowIDEnd; i++ {
		offset := i - rowIDBegin
		it.BaseInsertTask.RowIDs[offset] = i
	}

	if autoID {
		if it.HashValues == nil || len(it.HashValues) == 0 {
			it.HashValues = make([]uint32, 0)
		}
		for _, rowID := range it.RowIDs {
			hashValue, _ := typeutil.Hash32Int64(rowID)
			it.HashValues = append(it.HashValues, hashValue)
		}
	}

	var tsMsg msgstream.TsMsg = &it.BaseInsertTask
	it.BaseMsg.Ctx = ctx
	msgPack := msgstream.MsgPack{
		BeginTs: it.BeginTs(),
		EndTs:   it.EndTs(),
		Msgs:    make([]msgstream.TsMsg, 1),
	}

	it.result = &milvuspb.InsertResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		RowIDBegin: rowIDBegin,
		RowIDEnd:   rowIDEnd,
	}

	msgPack.Msgs[0] = tsMsg

	stream, err := it.chMgr.getDMLStream(collID)
	if err != nil {
		err = it.chMgr.createDMLMsgStream(collID)
		if err != nil {
			it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			it.result.Status.Reason = err.Error()
			return err
		}
		stream, err = it.chMgr.getDMLStream(collID)
		if err != nil {
			it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			it.result.Status.Reason = err.Error()
			return err
		}
	}

	pchans, err := it.chMgr.getChannels(collID)
	if err != nil {
		return err
	}
	for _, pchan := range pchans {
		log.Debug("ProxyNode InsertTask add pchan", zap.Any("pchan", pchan))
		_ = it.chTicker.addPChan(pchan)
	}

	// Assign SegmentID
	var pack *msgstream.MsgPack
	pack, err = it._assignSegmentID(stream, &msgPack)
	if err != nil {
		return err
	}

	err = stream.Produce(pack)
	if err != nil {
		it.result.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		it.result.Status.Reason = err.Error()
		return err
	}

	return nil
}

func (it *InsertTask) PostExecute(ctx context.Context) error {
	return nil
}

type CreateCollectionTask struct {
	Condition
	*milvuspb.CreateCollectionRequest
	ctx               context.Context
	masterService     types.MasterService
	dataServiceClient types.DataService
	result            *commonpb.Status
	schema            *schemapb.CollectionSchema
}

func (cct *CreateCollectionTask) TraceCtx() context.Context {
	return cct.ctx
}

func (cct *CreateCollectionTask) ID() UniqueID {
	return cct.Base.MsgID
}

func (cct *CreateCollectionTask) SetID(uid UniqueID) {
	cct.Base.MsgID = uid
}

func (cct *CreateCollectionTask) Name() string {
	return CreateCollectionTaskName
}

func (cct *CreateCollectionTask) Type() commonpb.MsgType {
	return cct.Base.MsgType
}

func (cct *CreateCollectionTask) BeginTs() Timestamp {
	return cct.Base.Timestamp
}

func (cct *CreateCollectionTask) EndTs() Timestamp {
	return cct.Base.Timestamp
}

func (cct *CreateCollectionTask) SetTs(ts Timestamp) {
	cct.Base.Timestamp = ts
}

func (cct *CreateCollectionTask) OnEnqueue() error {
	cct.Base = &commonpb.MsgBase{}
	return nil
}

func (cct *CreateCollectionTask) PreExecute(ctx context.Context) error {
	cct.Base.MsgType = commonpb.MsgType_CreateCollection
	cct.Base.SourceID = Params.ProxyID

	cct.schema = &schemapb.CollectionSchema{}
	err := proto.Unmarshal(cct.Schema, cct.schema)
	if err != nil {
		return err
	}

	if int64(len(cct.schema.Fields)) > Params.MaxFieldNum {
		return fmt.Errorf("maximum field's number should be limited to %d", Params.MaxFieldNum)
	}

	// validate collection name
	if err := ValidateCollectionName(cct.schema.Name); err != nil {
		return err
	}

	if err := ValidateDuplicatedFieldName(cct.schema.Fields); err != nil {
		return err
	}

	if err := ValidatePrimaryKey(cct.schema); err != nil {
		return err
	}

	// validate field name
	for _, field := range cct.schema.Fields {
		if err := ValidateFieldName(field.Name); err != nil {
			return err
		}
		if field.DataType == schemapb.DataType_FloatVector || field.DataType == schemapb.DataType_BinaryVector {
			exist := false
			var dim int64 = 0
			for _, param := range field.TypeParams {
				if param.Key == "dim" {
					exist = true
					tmp, err := strconv.ParseInt(param.Value, 10, 64)
					if err != nil {
						return err
					}
					dim = tmp
					break
				}
			}
			if !exist {
				return errors.New("dimension is not defined in field type params")
			}
			if field.DataType == schemapb.DataType_FloatVector {
				if err := ValidateDimension(dim, false); err != nil {
					return err
				}
			} else {
				if err := ValidateDimension(dim, true); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (cct *CreateCollectionTask) Execute(ctx context.Context) error {
	var err error
	cct.result, err = cct.masterService.CreateCollection(ctx, cct.CreateCollectionRequest)
	return err
}

func (cct *CreateCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type DropCollectionTask struct {
	Condition
	*milvuspb.DropCollectionRequest
	ctx           context.Context
	masterService types.MasterService
	result        *commonpb.Status
	chMgr         channelsMgr
	chTicker      channelsTimeTicker
}

func (dct *DropCollectionTask) TraceCtx() context.Context {
	return dct.ctx
}

func (dct *DropCollectionTask) ID() UniqueID {
	return dct.Base.MsgID
}

func (dct *DropCollectionTask) SetID(uid UniqueID) {
	dct.Base.MsgID = uid
}

func (dct *DropCollectionTask) Name() string {
	return DropCollectionTaskName
}

func (dct *DropCollectionTask) Type() commonpb.MsgType {
	return dct.Base.MsgType
}

func (dct *DropCollectionTask) BeginTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *DropCollectionTask) EndTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *DropCollectionTask) SetTs(ts Timestamp) {
	dct.Base.Timestamp = ts
}

func (dct *DropCollectionTask) OnEnqueue() error {
	dct.Base = &commonpb.MsgBase{}
	return nil
}

func (dct *DropCollectionTask) PreExecute(ctx context.Context) error {
	dct.Base.MsgType = commonpb.MsgType_DropCollection
	dct.Base.SourceID = Params.ProxyID

	if err := ValidateCollectionName(dct.CollectionName); err != nil {
		return err
	}
	return nil
}

func (dct *DropCollectionTask) Execute(ctx context.Context) error {
	collID, err := globalMetaCache.GetCollectionID(ctx, dct.CollectionName)
	if err != nil {
		return err
	}

	dct.result, err = dct.masterService.DropCollection(ctx, dct.DropCollectionRequest)
	if err != nil {
		return err
	}

	pchans, _ := dct.chMgr.getChannels(collID)
	for _, pchan := range pchans {
		_ = dct.chTicker.removePChan(pchan)
	}

	_ = dct.chMgr.removeDMLStream(collID)

	return nil
}

func (dct *DropCollectionTask) PostExecute(ctx context.Context) error {
	globalMetaCache.RemoveCollection(ctx, dct.CollectionName)
	return nil
}

type SearchTask struct {
	Condition
	*internalpb.SearchRequest
	ctx            context.Context
	queryMsgStream msgstream.MsgStream
	resultBuf      chan []*internalpb.SearchResults
	result         *milvuspb.SearchResults
	query          *milvuspb.SearchRequest
	chMgr          channelsMgr
}

func (st *SearchTask) TraceCtx() context.Context {
	return st.ctx
}

func (st *SearchTask) ID() UniqueID {
	return st.Base.MsgID
}

func (st *SearchTask) SetID(uid UniqueID) {
	st.Base.MsgID = uid
}

func (st *SearchTask) Name() string {
	return SearchTaskName
}

func (st *SearchTask) Type() commonpb.MsgType {
	return st.Base.MsgType
}

func (st *SearchTask) BeginTs() Timestamp {
	return st.Base.Timestamp
}

func (st *SearchTask) EndTs() Timestamp {
	return st.Base.Timestamp
}

func (st *SearchTask) SetTs(ts Timestamp) {
	st.Base.Timestamp = ts
}

func (st *SearchTask) OnEnqueue() error {
	st.Base = &commonpb.MsgBase{}
	return nil
}

func (st *SearchTask) getChannels() ([]pChan, error) {
	collID, err := globalMetaCache.GetCollectionID(st.ctx, st.query.CollectionName)
	if err != nil {
		return nil, err
	}

	return st.chMgr.getChannels(collID)
}

func (st *SearchTask) getVChannels() ([]vChan, error) {
	collID, err := globalMetaCache.GetCollectionID(st.ctx, st.query.CollectionName)
	if err != nil {
		return nil, err
	}

	_, err = st.chMgr.getChannels(collID)
	if err != nil {
		err := st.chMgr.createDMLMsgStream(collID)
		if err != nil {
			return nil, err
		}
	}

	return st.chMgr.getVChannels(collID)
}

func (st *SearchTask) PreExecute(ctx context.Context) error {
	st.Base.MsgType = commonpb.MsgType_Search
	st.Base.SourceID = Params.ProxyID

	collectionName := st.query.CollectionName
	_, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}

	if err := ValidateCollectionName(st.query.CollectionName); err != nil {
		return err
	}

	for _, tag := range st.query.PartitionNames {
		if err := ValidatePartitionTag(tag, false); err != nil {
			return err
		}
	}
	st.Base.MsgType = commonpb.MsgType_Search

	if st.query.GetDslType() == commonpb.DslType_BoolExprV1 {
		schema, err := globalMetaCache.GetCollectionSchema(ctx, collectionName)
		if err != nil { // err is not nil if collection not exists
			return err
		}

		annsField, err := GetAttrByKeyFromRepeatedKV(AnnsFieldKey, st.query.SearchParams)
		if err != nil {
			return errors.New(AnnsFieldKey + " not found in search_params")
		}

		topKStr, err := GetAttrByKeyFromRepeatedKV(TopKKey, st.query.SearchParams)
		if err != nil {
			return errors.New(TopKKey + " not found in search_params")
		}
		topK, err := strconv.Atoi(topKStr)
		if err != nil {
			return errors.New(TopKKey + " " + topKStr + " is not invalid")
		}

		metricType, err := GetAttrByKeyFromRepeatedKV(MetricTypeKey, st.query.SearchParams)
		if err != nil {
			return errors.New(MetricTypeKey + " not found in search_params")
		}

		searchParams, err := GetAttrByKeyFromRepeatedKV(SearchParamsKey, st.query.SearchParams)
		if err != nil {
			return errors.New(SearchParamsKey + " not found in search_params")
		}

		queryInfo := &planpb.QueryInfo{
			Topk:         int64(topK),
			MetricType:   metricType,
			SearchParams: searchParams,
		}

		plan, err := CreateQueryPlan(schema, st.query.Dsl, annsField, queryInfo)
		if err != nil {
			return errors.New("invalid expression: " + st.query.Dsl)
		}

		st.SearchRequest.DslType = commonpb.DslType_BoolExprV1
		st.SearchRequest.SerializedExprPlan, err = proto.Marshal(plan)
		if err != nil {
			return err
		}
	}

	st.SearchRequest.ResultChannelID = Params.SearchResultChannelNames[0]
	st.SearchRequest.DbID = 0 // todo
	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}
	st.SearchRequest.CollectionID = collectionID
	st.SearchRequest.PartitionIDs = make([]UniqueID, 0)

	partitionsMap, err := globalMetaCache.GetPartitions(ctx, collectionName)
	if err != nil {
		return err
	}

	partitionsRecord := make(map[UniqueID]bool)
	for _, partitionName := range st.query.PartitionNames {
		pattern := fmt.Sprintf("^%s$", partitionName)
		re, err := regexp.Compile(pattern)
		if err != nil {
			return errors.New("invalid partition names")
		}
		found := false
		for name, pID := range partitionsMap {
			if re.MatchString(name) {
				if _, exist := partitionsRecord[pID]; !exist {
					st.PartitionIDs = append(st.PartitionIDs, pID)
					partitionsRecord[pID] = true
				}
				found = true
			}
		}
		if !found {
			errMsg := fmt.Sprintf("PartitonName: %s not found", partitionName)
			return errors.New(errMsg)
		}
	}

	st.SearchRequest.Dsl = st.query.Dsl
	st.SearchRequest.PlaceholderGroup = st.query.PlaceholderGroup

	return nil
}

func (st *SearchTask) Execute(ctx context.Context) error {
	var tsMsg msgstream.TsMsg = &msgstream.SearchMsg{
		SearchRequest: *st.SearchRequest,
		BaseMsg: msgstream.BaseMsg{
			Ctx:            ctx,
			HashValues:     []uint32{uint32(Params.ProxyID)},
			BeginTimestamp: st.Base.Timestamp,
			EndTimestamp:   st.Base.Timestamp,
		},
	}
	msgPack := msgstream.MsgPack{
		BeginTs: st.Base.Timestamp,
		EndTs:   st.Base.Timestamp,
		Msgs:    make([]msgstream.TsMsg, 1),
	}
	msgPack.Msgs[0] = tsMsg
	err := st.queryMsgStream.Produce(&msgPack)
	log.Debug("proxynode", zap.Int("length of searchMsg", len(msgPack.Msgs)))
	log.Debug("proxy node sent one searchMsg",
		zap.Any("collectionID", st.CollectionID),
		zap.Any("msgID", tsMsg.ID()),
	)
	if err != nil {
		log.Debug("proxynode", zap.String("send search request failed", err.Error()))
	}
	return err
}

// TODO: add benchmark to compare with serial implementation
func decodeSearchResultsParallel(searchResults []*internalpb.SearchResults, maxParallel int) ([][]*milvuspb.Hits, error) {
	log.Debug("decodeSearchResultsParallel", zap.Any("NumOfGoRoutines", maxParallel))

	hits := make([][]*milvuspb.Hits, 0)
	// necessary to parallel this?
	for _, partialSearchResult := range searchResults {
		if partialSearchResult.Hits == nil || len(partialSearchResult.Hits) <= 0 {
			continue
		}

		nq := len(partialSearchResult.Hits)
		partialHits := make([]*milvuspb.Hits, nq)

		f := func(idx int) error {
			partialHit := &milvuspb.Hits{}

			err := proto.Unmarshal(partialSearchResult.Hits[idx], partialHit)
			if err != nil {
				return err
			}

			partialHits[idx] = partialHit

			return nil
		}

		err := funcutil.ProcessFuncParallel(nq, maxParallel, f, "decodePartialSearchResult")

		if err != nil {
			return nil, err
		}

		hits = append(hits, partialHits)
	}

	return hits, nil
}

func decodeSearchResultsSerial(searchResults []*internalpb.SearchResults) ([][]*milvuspb.Hits, error) {
	return decodeSearchResultsParallel(searchResults, 1)
}

// TODO: add benchmark to compare with serial implementation
func decodeSearchResultsParallelByNq(searchResults []*internalpb.SearchResults) ([][]*milvuspb.Hits, error) {
	if len(searchResults) <= 0 {
		return nil, errors.New("no need to decode empty search results")
	}
	nq := len(searchResults[0].Hits)
	return decodeSearchResultsParallel(searchResults, nq)
}

// TODO: add benchmark to compare with serial implementation
func decodeSearchResultsParallelByCPU(searchResults []*internalpb.SearchResults) ([][]*milvuspb.Hits, error) {
	return decodeSearchResultsParallel(searchResults, runtime.NumCPU())
}

func decodeSearchResults(searchResults []*internalpb.SearchResults) ([][]*milvuspb.Hits, error) {
	t := time.Now()
	defer func() {
		log.Debug("decodeSearchResults", zap.Any("time cost", time.Since(t)))
	}()
	return decodeSearchResultsParallelByCPU(searchResults)
}

func reduceSearchResultsParallel(hits [][]*milvuspb.Hits, nq, availableQueryNodeNum, topk int, metricType string, maxParallel int) *milvuspb.SearchResults {
	log.Debug("reduceSearchResultsParallel", zap.Any("NumOfGoRoutines", maxParallel))

	ret := &milvuspb.SearchResults{
		Status: &commonpb.Status{
			ErrorCode: 0,
		},
		Hits: make([][]byte, nq),
	}

	const minFloat32 = -1 * float32(math.MaxFloat32)

	f := func(idx int) error {
		locs := make([]int, availableQueryNodeNum)
		reducedHits := &milvuspb.Hits{
			IDs:     make([]int64, 0),
			RowData: make([][]byte, 0),
			Scores:  make([]float32, 0),
		}

		for j := 0; j < topk; j++ {
			valid := false
			choice, maxDistance := 0, minFloat32
			for q, loc := range locs { // query num, the number of ways to merge
				if loc >= len(hits[q][idx].IDs) {
					continue
				}
				distance := hits[q][idx].Scores[loc]
				if distance > maxDistance || (math.Abs(float64(distance-maxDistance)) < math.SmallestNonzeroFloat32 && choice != q) {
					choice = q
					maxDistance = distance
					valid = true
				}
			}
			if !valid {
				break
			}
			choiceOffset := locs[choice]
			// check if distance is valid, `invalid` here means very very big,
			// in this process, distance here is the smallest, so the rest of distance are all invalid
			if hits[choice][idx].Scores[choiceOffset] <= minFloat32 {
				break
			}
			reducedHits.IDs = append(reducedHits.IDs, hits[choice][idx].IDs[choiceOffset])
			if hits[choice][idx].RowData != nil && len(hits[choice][idx].RowData) > 0 {
				reducedHits.RowData = append(reducedHits.RowData, hits[choice][idx].RowData[choiceOffset])
			}
			reducedHits.Scores = append(reducedHits.Scores, hits[choice][idx].Scores[choiceOffset])
			locs[choice]++
		}

		if metricType != "IP" {
			for k := range reducedHits.Scores {
				reducedHits.Scores[k] *= -1
			}
		}

		reducedHitsBs, err := proto.Marshal(reducedHits)
		if err != nil {
			return err
		}

		ret.Hits[idx] = reducedHitsBs

		return nil
	}

	err := funcutil.ProcessFuncParallel(nq, maxParallel, f, "reduceSearchResults")
	if err != nil {
		return nil
	}

	return ret
}

func reduceSearchResultsSerial(hits [][]*milvuspb.Hits, nq, availableQueryNodeNum, topk int, metricType string) *milvuspb.SearchResults {
	return reduceSearchResultsParallel(hits, nq, availableQueryNodeNum, topk, metricType, 1)
}

// TODO: add benchmark to compare with serial implementation
func reduceSearchResultsParallelByNq(hits [][]*milvuspb.Hits, nq, availableQueryNodeNum, topk int, metricType string) *milvuspb.SearchResults {
	return reduceSearchResultsParallel(hits, nq, availableQueryNodeNum, topk, metricType, nq)
}

// TODO: add benchmark to compare with serial implementation
func reduceSearchResultsParallelByCPU(hits [][]*milvuspb.Hits, nq, availableQueryNodeNum, topk int, metricType string) *milvuspb.SearchResults {
	return reduceSearchResultsParallel(hits, nq, availableQueryNodeNum, topk, metricType, runtime.NumCPU())
}

func reduceSearchResults(hits [][]*milvuspb.Hits, nq, availableQueryNodeNum, topk int, metricType string) *milvuspb.SearchResults {
	t := time.Now()
	defer func() {
		log.Debug("reduceSearchResults", zap.Any("time cost", time.Since(t)))
	}()
	return reduceSearchResultsParallelByCPU(hits, nq, availableQueryNodeNum, topk, metricType)
}

func printSearchResult(partialSearchResult *internalpb.SearchResults) {
	for i := 0; i < len(partialSearchResult.Hits); i++ {
		testHits := milvuspb.Hits{}
		err := proto.Unmarshal(partialSearchResult.Hits[i], &testHits)
		if err != nil {
			panic(err)
		}
		fmt.Println(testHits.IDs)
		fmt.Println(testHits.Scores)
	}
}

func (st *SearchTask) PostExecute(ctx context.Context) error {
	t0 := time.Now()
	defer func() {
		log.Debug("WaitAndPostExecute", zap.Any("time cost", time.Since(t0)))
	}()
	for {
		select {
		case <-st.TraceCtx().Done():
			log.Debug("ProxyNode", zap.Int64("SearchTask PostExecute Loop exit caused by ctx.Done", st.ID()))
			return fmt.Errorf("SearchTask:wait to finish failed, timeout: %d", st.ID())
		case searchResults := <-st.resultBuf:
			// fmt.Println("searchResults: ", searchResults)
			filterSearchResult := make([]*internalpb.SearchResults, 0)
			var filterReason string
			for _, partialSearchResult := range searchResults {
				if partialSearchResult.Status.ErrorCode == commonpb.ErrorCode_Success {
					filterSearchResult = append(filterSearchResult, partialSearchResult)
					// For debugging, please don't delete.
					// printSearchResult(partialSearchResult)
				} else {
					filterReason += partialSearchResult.Status.Reason + "\n"
				}
			}

			availableQueryNodeNum := len(filterSearchResult)
			log.Debug("ProxyNode Search PostExecute stage1", zap.Any("availableQueryNodeNum", availableQueryNodeNum))
			if availableQueryNodeNum <= 0 {
				log.Debug("ProxyNode Search PostExecute failed", zap.Any("filterReason", filterReason))
				st.result = &milvuspb.SearchResults{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_UnexpectedError,
						Reason:    filterReason,
					},
				}
				return errors.New(filterReason)
			}

			availableQueryNodeNum = 0
			for _, partialSearchResult := range filterSearchResult {
				if partialSearchResult.Hits == nil || len(partialSearchResult.Hits) <= 0 {
					filterReason += "nq is zero\n"
					continue
				}
				availableQueryNodeNum++
			}
			log.Debug("ProxyNode Search PostExecute stage2", zap.Any("availableQueryNodeNum", availableQueryNodeNum))

			if availableQueryNodeNum <= 0 {
				log.Debug("ProxyNode Search PostExecute stage2 failed", zap.Any("filterReason", filterReason))

				st.result = &milvuspb.SearchResults{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_Success,
						Reason:    filterReason,
					},
				}
				return nil
			}

			hits, err := decodeSearchResults(filterSearchResult)
			log.Debug("ProxyNode Search PostExecute decodeSearchResults", zap.Error(err))
			if err != nil {
				return err
			}

			nq := len(hits[0])
			log.Debug("ProxyNode Search PostExecute", zap.Any("nq", nq))
			if nq <= 0 {
				st.result = &milvuspb.SearchResults{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_Success,
						Reason:    filterReason,
					},
				}
				return nil
			}

			topk := 0
			for _, hit := range hits {
				topk = getMax(topk, len(hit[0].IDs))
			}

			st.result = reduceSearchResults(hits, nq, availableQueryNodeNum, topk, searchResults[0].MetricType)
			log.Debug("ProxyNode Search PostExecute Done")
			return nil
		}
	}
}

type RetrieveTask struct {
	Condition
	*internalpb.RetrieveRequest
	ctx            context.Context
	queryMsgStream msgstream.MsgStream
	resultBuf      chan []*internalpb.RetrieveResults
	result         *milvuspb.RetrieveResults
	retrieve       *milvuspb.RetrieveRequest
	chMgr          channelsMgr
}

func (rt *RetrieveTask) TraceCtx() context.Context {
	return rt.ctx
}

func (rt *RetrieveTask) ID() UniqueID {
	return rt.Base.MsgID
}

func (rt *RetrieveTask) SetID(uid UniqueID) {
	rt.Base.MsgID = uid
}

func (rt *RetrieveTask) Name() string {
	return RetrieveTaskName
}

func (rt *RetrieveTask) Type() commonpb.MsgType {
	return rt.Base.MsgType
}

func (rt *RetrieveTask) BeginTs() Timestamp {
	return rt.Base.Timestamp
}

func (rt *RetrieveTask) EndTs() Timestamp {
	return rt.Base.Timestamp
}

func (rt *RetrieveTask) SetTs(ts Timestamp) {
	rt.Base.Timestamp = ts
}

func (rt *RetrieveTask) OnEnqueue() error {
	rt.Base.MsgType = commonpb.MsgType_Retrieve
	return nil
}

func (rt *RetrieveTask) getChannels() ([]pChan, error) {
	collID, err := globalMetaCache.GetCollectionID(rt.ctx, rt.retrieve.CollectionName)
	if err != nil {
		return nil, err
	}

	return rt.chMgr.getChannels(collID)
}

func (rt *RetrieveTask) getVChannels() ([]vChan, error) {
	collID, err := globalMetaCache.GetCollectionID(rt.ctx, rt.retrieve.CollectionName)
	if err != nil {
		return nil, err
	}

	_, err = rt.chMgr.getChannels(collID)
	if err != nil {
		err := rt.chMgr.createDMLMsgStream(collID)
		if err != nil {
			return nil, err
		}
	}

	return rt.chMgr.getVChannels(collID)
}

func (rt *RetrieveTask) PreExecute(ctx context.Context) error {
	rt.Base.MsgType = commonpb.MsgType_Retrieve
	rt.Base.SourceID = Params.ProxyID

	collectionName := rt.retrieve.CollectionName
	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil {
		log.Debug("Failed to get collection id.", zap.Any("collectionName", collectionName),
			zap.Any("requestID", rt.Base.MsgID), zap.Any("requestType", "retrieve"))
		return err
	}
	log.Info("Get collection id by name.", zap.Any("collectionName", collectionName),
		zap.Any("requestID", rt.Base.MsgID), zap.Any("requestType", "retrieve"))

	if err := ValidateCollectionName(rt.retrieve.CollectionName); err != nil {
		log.Debug("Invalid collection name.", zap.Any("collectionName", collectionName),
			zap.Any("requestID", rt.Base.MsgID), zap.Any("requestType", "retrieve"))
		return err
	}
	log.Info("Validate collection name.", zap.Any("collectionName", collectionName),
		zap.Any("requestID", rt.Base.MsgID), zap.Any("requestType", "retrieve"))

	for _, tag := range rt.retrieve.PartitionNames {
		if err := ValidatePartitionTag(tag, false); err != nil {
			log.Debug("Invalid partition name.", zap.Any("partitionName", tag),
				zap.Any("requestID", rt.Base.MsgID), zap.Any("requestType", "retrieve"))
			return err
		}
	}
	log.Info("Validate partition names.",
		zap.Any("requestID", rt.Base.MsgID), zap.Any("requestType", "retrieve"))

	rt.Base.MsgType = commonpb.MsgType_Retrieve
	if rt.retrieve.Ids == nil {
		errMsg := "Retrieve ids is nil"
		return errors.New(errMsg)
	}
	rt.Ids = rt.retrieve.Ids
	if len(rt.retrieve.OutputFields) == 0 {
		schema, err := globalMetaCache.GetCollectionSchema(ctx, collectionName)
		if err != nil {
			return err
		}
		for _, field := range schema.Fields {
			if field.FieldID >= 100 {
				rt.OutputFields = append(rt.OutputFields, field.Name)
			}
		}
	} else {
		rt.OutputFields = rt.retrieve.OutputFields
	}

	rt.ResultChannelID = Params.RetrieveChannelNames[0]
	rt.DbID = 0 // todo(yukun)

	rt.CollectionID = collectionID
	rt.PartitionIDs = make([]UniqueID, 0)

	partitionsMap, err := globalMetaCache.GetPartitions(ctx, collectionName)
	if err != nil {
		log.Debug("Failed to get partitions in collection.", zap.Any("collectionName", collectionName),
			zap.Any("requestID", rt.Base.MsgID), zap.Any("requestType", "retrieve"))
		return err
	}
	log.Info("Get partitions in collection.", zap.Any("collectionName", collectionName),
		zap.Any("requestID", rt.Base.MsgID), zap.Any("requestType", "retrieve"))

	partitionsRecord := make(map[UniqueID]bool)
	for _, partitionName := range rt.retrieve.PartitionNames {
		pattern := fmt.Sprintf("^%s$", partitionName)
		re, err := regexp.Compile(pattern)
		if err != nil {
			log.Debug("Failed to compile partition name regex expression.", zap.Any("partitionName", partitionName),
				zap.Any("requestID", rt.Base.MsgID), zap.Any("requestType", "retrieve"))
			return errors.New("invalid partition names")
		}
		found := false
		for name, pID := range partitionsMap {
			if re.MatchString(name) {
				if _, exist := partitionsRecord[pID]; !exist {
					rt.PartitionIDs = append(rt.PartitionIDs, pID)
					partitionsRecord[pID] = true
				}
				found = true
			}
		}
		if !found {
			// FIXME(wxyu): undefined behavior
			errMsg := fmt.Sprintf("PartitonName: %s not found", partitionName)
			return errors.New(errMsg)
		}
	}

	log.Info("Retrieve PreExecute done.",
		zap.Any("requestID", rt.Base.MsgID), zap.Any("requestType", "retrieve"))
	return nil
}

func (rt *RetrieveTask) Execute(ctx context.Context) error {
	var tsMsg msgstream.TsMsg = &msgstream.RetrieveMsg{
		RetrieveRequest: *rt.RetrieveRequest,
		BaseMsg: msgstream.BaseMsg{
			Ctx:            ctx,
			HashValues:     []uint32{uint32(Params.ProxyID)},
			BeginTimestamp: rt.Base.Timestamp,
			EndTimestamp:   rt.Base.Timestamp,
		},
	}
	msgPack := msgstream.MsgPack{
		BeginTs: rt.Base.Timestamp,
		EndTs:   rt.Base.Timestamp,
		Msgs:    make([]msgstream.TsMsg, 1),
	}
	msgPack.Msgs[0] = tsMsg
	err := rt.queryMsgStream.Produce(&msgPack)
	log.Debug("proxynode", zap.Int("length of retrieveMsg", len(msgPack.Msgs)))
	if err != nil {
		log.Debug("Failed to send retrieve request.",
			zap.Any("requestID", rt.Base.MsgID), zap.Any("requestType", "retrieve"))
	}

	log.Info("Retrieve Execute done.",
		zap.Any("requestID", rt.Base.MsgID), zap.Any("requestType", "retrieve"))
	return err
}

func (rt *RetrieveTask) PostExecute(ctx context.Context) error {
	t0 := time.Now()
	defer func() {
		log.Debug("WaitAndPostExecute", zap.Any("time cost", time.Since(t0)))
	}()
	select {
	case <-rt.TraceCtx().Done():
		log.Debug("proxynode", zap.Int64("Retrieve: wait to finish failed, timeout!, taskID:", rt.ID()))
		return fmt.Errorf("RetrieveTask:wait to finish failed, timeout : %d", rt.ID())
	case retrieveResults := <-rt.resultBuf:
		retrieveResult := make([]*internalpb.RetrieveResults, 0)
		var reason string
		for _, partialRetrieveResult := range retrieveResults {
			if partialRetrieveResult.Status.ErrorCode == commonpb.ErrorCode_Success {
				retrieveResult = append(retrieveResult, partialRetrieveResult)
			} else {
				reason += partialRetrieveResult.Status.Reason + "\n"
			}
		}

		if len(retrieveResult) == 0 {
			rt.result = &milvuspb.RetrieveResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    reason,
				},
			}
			log.Debug("Retrieve failed on all querynodes.",
				zap.Any("requestID", rt.Base.MsgID), zap.Any("requestType", "retrieve"))
			return errors.New(reason)
		}

		availableQueryNodeNum := 0
		rt.result = &milvuspb.RetrieveResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			Ids:        &schemapb.IDs{},
			FieldsData: make([]*schemapb.FieldData, 0),
		}
		for idx, partialRetrieveResult := range retrieveResult {
			log.Debug("Index-" + strconv.Itoa(idx))
			if partialRetrieveResult.Ids == nil {
				reason += "ids is nil\n"
				continue
			} else {
				intIds, intOk := partialRetrieveResult.Ids.IdField.(*schemapb.IDs_IntId)
				strIds, strOk := partialRetrieveResult.Ids.IdField.(*schemapb.IDs_StrId)
				if !intOk && !strOk {
					reason += "ids is empty\n"
					continue
				}

				if !intOk {
					if idsStr, ok := rt.result.Ids.IdField.(*schemapb.IDs_StrId); ok {
						idsStr.StrId.Data = append(idsStr.StrId.Data, strIds.StrId.Data...)
					} else {
						rt.result.Ids.IdField = &schemapb.IDs_StrId{
							StrId: &schemapb.StringArray{
								Data: strIds.StrId.Data,
							},
						}
					}
				} else {
					if idsInt, ok := rt.result.Ids.IdField.(*schemapb.IDs_IntId); ok {
						idsInt.IntId.Data = append(idsInt.IntId.Data, intIds.IntId.Data...)
					} else {
						rt.result.Ids.IdField = &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: intIds.IntId.Data,
							},
						}
					}
				}

				rt.result.FieldsData = append(rt.result.FieldsData, partialRetrieveResult.FieldsData...)
			}
			availableQueryNodeNum++
		}

		if availableQueryNodeNum == 0 {
			log.Info("Not any valid result found.",
				zap.Any("requestID", rt.Base.MsgID), zap.Any("requestType", "retrieve"))
			rt.result = &milvuspb.RetrieveResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    reason,
				},
			}
			return nil
		}

		schema, err := globalMetaCache.GetCollectionSchema(ctx, rt.retrieve.CollectionName)
		if err != nil {
			return err
		}
		for i := 0; i < len(rt.result.FieldsData); i++ {
			for _, field := range schema.Fields {
				if field.Name == rt.OutputFields[i] {
					rt.result.FieldsData[i].FieldName = field.Name
					rt.result.FieldsData[i].Type = field.DataType
				}
			}
		}
	}

	log.Info("Retrieve PostExecute done.",
		zap.Any("requestID", rt.Base.MsgID), zap.Any("requestType", "retrieve"))
	return nil
}

type HasCollectionTask struct {
	Condition
	*milvuspb.HasCollectionRequest
	ctx           context.Context
	masterService types.MasterService
	result        *milvuspb.BoolResponse
}

func (hct *HasCollectionTask) TraceCtx() context.Context {
	return hct.ctx
}

func (hct *HasCollectionTask) ID() UniqueID {
	return hct.Base.MsgID
}

func (hct *HasCollectionTask) SetID(uid UniqueID) {
	hct.Base.MsgID = uid
}

func (hct *HasCollectionTask) Name() string {
	return HasCollectionTaskName
}

func (hct *HasCollectionTask) Type() commonpb.MsgType {
	return hct.Base.MsgType
}

func (hct *HasCollectionTask) BeginTs() Timestamp {
	return hct.Base.Timestamp
}

func (hct *HasCollectionTask) EndTs() Timestamp {
	return hct.Base.Timestamp
}

func (hct *HasCollectionTask) SetTs(ts Timestamp) {
	hct.Base.Timestamp = ts
}

func (hct *HasCollectionTask) OnEnqueue() error {
	hct.Base = &commonpb.MsgBase{}
	return nil
}

func (hct *HasCollectionTask) PreExecute(ctx context.Context) error {
	hct.Base.MsgType = commonpb.MsgType_HasCollection
	hct.Base.SourceID = Params.ProxyID

	if err := ValidateCollectionName(hct.CollectionName); err != nil {
		return err
	}
	return nil
}

func (hct *HasCollectionTask) Execute(ctx context.Context) error {
	var err error
	hct.result, err = hct.masterService.HasCollection(ctx, hct.HasCollectionRequest)
	if hct.result == nil {
		return errors.New("has collection resp is nil")
	}
	if hct.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(hct.result.Status.Reason)
	}
	return err
}

func (hct *HasCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type DescribeCollectionTask struct {
	Condition
	*milvuspb.DescribeCollectionRequest
	ctx           context.Context
	masterService types.MasterService
	result        *milvuspb.DescribeCollectionResponse
}

func (dct *DescribeCollectionTask) TraceCtx() context.Context {
	return dct.ctx
}

func (dct *DescribeCollectionTask) ID() UniqueID {
	return dct.Base.MsgID
}

func (dct *DescribeCollectionTask) SetID(uid UniqueID) {
	dct.Base.MsgID = uid
}

func (dct *DescribeCollectionTask) Name() string {
	return DescribeCollectionTaskName
}

func (dct *DescribeCollectionTask) Type() commonpb.MsgType {
	return dct.Base.MsgType
}

func (dct *DescribeCollectionTask) BeginTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *DescribeCollectionTask) EndTs() Timestamp {
	return dct.Base.Timestamp
}

func (dct *DescribeCollectionTask) SetTs(ts Timestamp) {
	dct.Base.Timestamp = ts
}

func (dct *DescribeCollectionTask) OnEnqueue() error {
	dct.Base = &commonpb.MsgBase{}
	return nil
}

func (dct *DescribeCollectionTask) PreExecute(ctx context.Context) error {
	dct.Base.MsgType = commonpb.MsgType_DescribeCollection
	dct.Base.SourceID = Params.ProxyID

	if err := ValidateCollectionName(dct.CollectionName); err != nil {
		return err
	}
	return nil
}

func (dct *DescribeCollectionTask) Execute(ctx context.Context) error {
	var err error
	dct.result = &milvuspb.DescribeCollectionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Schema: &schemapb.CollectionSchema{
			Name:        "",
			Description: "",
			AutoID:      false,
			Fields:      make([]*schemapb.FieldSchema, 0),
		},
		CollectionID:         0,
		VirtualChannelNames:  nil,
		PhysicalChannelNames: nil,
	}

	result, err := dct.masterService.DescribeCollection(ctx, dct.DescribeCollectionRequest)

	if err != nil {
		return err
	}

	if result.Status.ErrorCode != commonpb.ErrorCode_Success {
		dct.result.Status = result.Status
	} else {
		dct.result.Schema.Name = result.Schema.Name
		dct.result.Schema.Description = result.Schema.Description
		dct.result.Schema.AutoID = result.Schema.AutoID
		dct.result.CollectionID = result.CollectionID
		dct.result.VirtualChannelNames = result.VirtualChannelNames
		dct.result.PhysicalChannelNames = result.PhysicalChannelNames

		for _, field := range result.Schema.Fields {
			if field.FieldID >= 100 { // TODO(dragondriver): use StartOfUserFieldID replacing 100
				dct.result.Schema.Fields = append(dct.result.Schema.Fields, &schemapb.FieldSchema{
					FieldID:      field.FieldID,
					Name:         field.Name,
					IsPrimaryKey: field.IsPrimaryKey,
					Description:  field.Description,
					DataType:     field.DataType,
					TypeParams:   field.TypeParams,
					IndexParams:  field.IndexParams,
				})
			}
		}
	}
	return nil
}

func (dct *DescribeCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type GetCollectionStatisticsTask struct {
	Condition
	*milvuspb.GetCollectionStatisticsRequest
	ctx         context.Context
	dataService types.DataService
	result      *milvuspb.GetCollectionStatisticsResponse
}

func (g *GetCollectionStatisticsTask) TraceCtx() context.Context {
	return g.ctx
}

func (g *GetCollectionStatisticsTask) ID() UniqueID {
	return g.Base.MsgID
}

func (g *GetCollectionStatisticsTask) SetID(uid UniqueID) {
	g.Base.MsgID = uid
}

func (g *GetCollectionStatisticsTask) Name() string {
	return GetCollectionStatisticsTaskName
}

func (g *GetCollectionStatisticsTask) Type() commonpb.MsgType {
	return g.Base.MsgType
}

func (g *GetCollectionStatisticsTask) BeginTs() Timestamp {
	return g.Base.Timestamp
}

func (g *GetCollectionStatisticsTask) EndTs() Timestamp {
	return g.Base.Timestamp
}

func (g *GetCollectionStatisticsTask) SetTs(ts Timestamp) {
	g.Base.Timestamp = ts
}

func (g *GetCollectionStatisticsTask) OnEnqueue() error {
	g.Base = &commonpb.MsgBase{}
	return nil
}

func (g *GetCollectionStatisticsTask) PreExecute(ctx context.Context) error {
	g.Base.MsgType = commonpb.MsgType_GetCollectionStatistics
	g.Base.SourceID = Params.ProxyID
	return nil
}

func (g *GetCollectionStatisticsTask) Execute(ctx context.Context) error {
	collID, err := globalMetaCache.GetCollectionID(ctx, g.CollectionName)
	if err != nil {
		return err
	}
	req := &datapb.GetCollectionStatisticsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_GetCollectionStatistics,
			MsgID:     g.Base.MsgID,
			Timestamp: g.Base.Timestamp,
			SourceID:  g.Base.SourceID,
		},
		CollectionID: collID,
	}

	result, _ := g.dataService.GetCollectionStatistics(ctx, req)
	if result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(result.Status.Reason)
	}
	g.result = &milvuspb.GetCollectionStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Stats: result.Stats,
	}
	return nil
}

func (g *GetCollectionStatisticsTask) PostExecute(ctx context.Context) error {
	return nil
}

type GetPartitionStatisticsTask struct {
	Condition
	*milvuspb.GetPartitionStatisticsRequest
	ctx         context.Context
	dataService types.DataService
	result      *milvuspb.GetPartitionStatisticsResponse
}

func (g *GetPartitionStatisticsTask) TraceCtx() context.Context {
	return g.ctx
}

func (g *GetPartitionStatisticsTask) ID() UniqueID {
	return g.Base.MsgID
}

func (g *GetPartitionStatisticsTask) SetID(uid UniqueID) {
	g.Base.MsgID = uid
}

func (g *GetPartitionStatisticsTask) Name() string {
	return GetPartitionStatisticsTaskName
}

func (g *GetPartitionStatisticsTask) Type() commonpb.MsgType {
	return g.Base.MsgType
}

func (g *GetPartitionStatisticsTask) BeginTs() Timestamp {
	return g.Base.Timestamp
}

func (g *GetPartitionStatisticsTask) EndTs() Timestamp {
	return g.Base.Timestamp
}

func (g *GetPartitionStatisticsTask) SetTs(ts Timestamp) {
	g.Base.Timestamp = ts
}

func (g *GetPartitionStatisticsTask) OnEnqueue() error {
	g.Base = &commonpb.MsgBase{}
	return nil
}

func (g *GetPartitionStatisticsTask) PreExecute(ctx context.Context) error {
	g.Base.MsgType = commonpb.MsgType_GetPartitionStatistics
	g.Base.SourceID = Params.ProxyID
	return nil
}

func (g *GetPartitionStatisticsTask) Execute(ctx context.Context) error {
	collID, err := globalMetaCache.GetCollectionID(ctx, g.CollectionName)
	if err != nil {
		return err
	}
	partitionID, err := globalMetaCache.GetPartitionID(ctx, g.CollectionName, g.PartitionName)
	if err != nil {
		return err
	}
	req := &datapb.GetPartitionStatisticsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_GetPartitionStatistics,
			MsgID:     g.Base.MsgID,
			Timestamp: g.Base.Timestamp,
			SourceID:  g.Base.SourceID,
		},
		CollectionID: collID,
		PartitionID:  partitionID,
	}

	result, _ := g.dataService.GetPartitionStatistics(ctx, req)
	if result == nil {
		return errors.New("get partition statistics resp is nil")
	}
	if result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(result.Status.Reason)
	}
	g.result = &milvuspb.GetPartitionStatisticsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Stats: result.Stats,
	}
	return nil
}

func (g *GetPartitionStatisticsTask) PostExecute(ctx context.Context) error {
	return nil
}

type ShowCollectionsTask struct {
	Condition
	*milvuspb.ShowCollectionsRequest
	ctx           context.Context
	masterService types.MasterService
	queryService  types.QueryService
	result        *milvuspb.ShowCollectionsResponse
}

func (sct *ShowCollectionsTask) TraceCtx() context.Context {
	return sct.ctx
}

func (sct *ShowCollectionsTask) ID() UniqueID {
	return sct.Base.MsgID
}

func (sct *ShowCollectionsTask) SetID(uid UniqueID) {
	sct.Base.MsgID = uid
}

func (sct *ShowCollectionsTask) Name() string {
	return ShowCollectionTaskName
}

func (sct *ShowCollectionsTask) Type() commonpb.MsgType {
	return sct.Base.MsgType
}

func (sct *ShowCollectionsTask) BeginTs() Timestamp {
	return sct.Base.Timestamp
}

func (sct *ShowCollectionsTask) EndTs() Timestamp {
	return sct.Base.Timestamp
}

func (sct *ShowCollectionsTask) SetTs(ts Timestamp) {
	sct.Base.Timestamp = ts
}

func (sct *ShowCollectionsTask) OnEnqueue() error {
	sct.Base = &commonpb.MsgBase{}
	return nil
}

func (sct *ShowCollectionsTask) PreExecute(ctx context.Context) error {
	sct.Base.MsgType = commonpb.MsgType_ShowCollections
	sct.Base.SourceID = Params.ProxyID

	return nil
}

func (sct *ShowCollectionsTask) Execute(ctx context.Context) error {
	var err error

	respFromMaster, err := sct.masterService.ShowCollections(ctx, sct.ShowCollectionsRequest)

	if err != nil {
		return err
	}

	if respFromMaster == nil {
		return errors.New("failed to show collections")
	}

	if respFromMaster.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(respFromMaster.Status.Reason)
	}

	if sct.ShowCollectionsRequest.Type == milvuspb.ShowCollectionsType_InMemory {
		resp, err := sct.queryService.ShowCollections(ctx, &querypb.ShowCollectionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowCollections,
				MsgID:     sct.ShowCollectionsRequest.Base.MsgID,
				Timestamp: sct.ShowCollectionsRequest.Base.Timestamp,
				SourceID:  sct.ShowCollectionsRequest.Base.SourceID,
			},
			//DbID: sct.ShowCollectionsRequest.DbName,
		})

		if err != nil {
			return err
		}

		if resp == nil {
			return errors.New("failed to show collections")
		}

		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Status.Reason)
		}

		sct.result = &milvuspb.ShowCollectionsResponse{
			Status:          resp.Status,
			CollectionNames: make([]string, 0, len(resp.CollectionIDs)),
			CollectionIds:   make([]int64, 0, len(resp.CollectionIDs)),
		}

		idMap := make(map[int64]string)
		for i, name := range respFromMaster.CollectionNames {
			idMap[respFromMaster.CollectionIds[i]] = name
		}

		for _, id := range resp.CollectionIDs {
			sct.result.CollectionIds = append(sct.result.CollectionIds, id)
			sct.result.CollectionNames = append(sct.result.CollectionNames, idMap[id])
		}
	}

	sct.result = respFromMaster

	return nil
}

func (sct *ShowCollectionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type CreatePartitionTask struct {
	Condition
	*milvuspb.CreatePartitionRequest
	ctx           context.Context
	masterService types.MasterService
	result        *commonpb.Status
}

func (cpt *CreatePartitionTask) TraceCtx() context.Context {
	return cpt.ctx
}

func (cpt *CreatePartitionTask) ID() UniqueID {
	return cpt.Base.MsgID
}

func (cpt *CreatePartitionTask) SetID(uid UniqueID) {
	cpt.Base.MsgID = uid
}

func (cpt *CreatePartitionTask) Name() string {
	return CreatePartitionTaskName
}

func (cpt *CreatePartitionTask) Type() commonpb.MsgType {
	return cpt.Base.MsgType
}

func (cpt *CreatePartitionTask) BeginTs() Timestamp {
	return cpt.Base.Timestamp
}

func (cpt *CreatePartitionTask) EndTs() Timestamp {
	return cpt.Base.Timestamp
}

func (cpt *CreatePartitionTask) SetTs(ts Timestamp) {
	cpt.Base.Timestamp = ts
}

func (cpt *CreatePartitionTask) OnEnqueue() error {
	cpt.Base = &commonpb.MsgBase{}
	return nil
}

func (cpt *CreatePartitionTask) PreExecute(ctx context.Context) error {
	cpt.Base.MsgType = commonpb.MsgType_CreatePartition
	cpt.Base.SourceID = Params.ProxyID

	collName, partitionTag := cpt.CollectionName, cpt.PartitionName

	if err := ValidateCollectionName(collName); err != nil {
		return err
	}

	if err := ValidatePartitionTag(partitionTag, true); err != nil {
		return err
	}

	return nil
}

func (cpt *CreatePartitionTask) Execute(ctx context.Context) (err error) {
	cpt.result, err = cpt.masterService.CreatePartition(ctx, cpt.CreatePartitionRequest)
	if cpt.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if cpt.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(cpt.result.Reason)
	}
	return err
}

func (cpt *CreatePartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type DropPartitionTask struct {
	Condition
	*milvuspb.DropPartitionRequest
	ctx           context.Context
	masterService types.MasterService
	result        *commonpb.Status
}

func (dpt *DropPartitionTask) TraceCtx() context.Context {
	return dpt.ctx
}

func (dpt *DropPartitionTask) ID() UniqueID {
	return dpt.Base.MsgID
}

func (dpt *DropPartitionTask) SetID(uid UniqueID) {
	dpt.Base.MsgID = uid
}

func (dpt *DropPartitionTask) Name() string {
	return DropPartitionTaskName
}

func (dpt *DropPartitionTask) Type() commonpb.MsgType {
	return dpt.Base.MsgType
}

func (dpt *DropPartitionTask) BeginTs() Timestamp {
	return dpt.Base.Timestamp
}

func (dpt *DropPartitionTask) EndTs() Timestamp {
	return dpt.Base.Timestamp
}

func (dpt *DropPartitionTask) SetTs(ts Timestamp) {
	dpt.Base.Timestamp = ts
}

func (dpt *DropPartitionTask) OnEnqueue() error {
	dpt.Base = &commonpb.MsgBase{}
	return nil
}

func (dpt *DropPartitionTask) PreExecute(ctx context.Context) error {
	dpt.Base.MsgType = commonpb.MsgType_DropPartition
	dpt.Base.SourceID = Params.ProxyID

	collName, partitionTag := dpt.CollectionName, dpt.PartitionName

	if err := ValidateCollectionName(collName); err != nil {
		return err
	}

	if err := ValidatePartitionTag(partitionTag, true); err != nil {
		return err
	}

	return nil
}

func (dpt *DropPartitionTask) Execute(ctx context.Context) (err error) {
	dpt.result, err = dpt.masterService.DropPartition(ctx, dpt.DropPartitionRequest)
	if dpt.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if dpt.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(dpt.result.Reason)
	}
	return err
}

func (dpt *DropPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type HasPartitionTask struct {
	Condition
	*milvuspb.HasPartitionRequest
	ctx           context.Context
	masterService types.MasterService
	result        *milvuspb.BoolResponse
}

func (hpt *HasPartitionTask) TraceCtx() context.Context {
	return hpt.ctx
}

func (hpt *HasPartitionTask) ID() UniqueID {
	return hpt.Base.MsgID
}

func (hpt *HasPartitionTask) SetID(uid UniqueID) {
	hpt.Base.MsgID = uid
}

func (hpt *HasPartitionTask) Name() string {
	return HasPartitionTaskName
}

func (hpt *HasPartitionTask) Type() commonpb.MsgType {
	return hpt.Base.MsgType
}

func (hpt *HasPartitionTask) BeginTs() Timestamp {
	return hpt.Base.Timestamp
}

func (hpt *HasPartitionTask) EndTs() Timestamp {
	return hpt.Base.Timestamp
}

func (hpt *HasPartitionTask) SetTs(ts Timestamp) {
	hpt.Base.Timestamp = ts
}

func (hpt *HasPartitionTask) OnEnqueue() error {
	hpt.Base = &commonpb.MsgBase{}
	return nil
}

func (hpt *HasPartitionTask) PreExecute(ctx context.Context) error {
	hpt.Base.MsgType = commonpb.MsgType_HasPartition
	hpt.Base.SourceID = Params.ProxyID

	collName, partitionTag := hpt.CollectionName, hpt.PartitionName

	if err := ValidateCollectionName(collName); err != nil {
		return err
	}

	if err := ValidatePartitionTag(partitionTag, true); err != nil {
		return err
	}
	return nil
}

func (hpt *HasPartitionTask) Execute(ctx context.Context) (err error) {
	hpt.result, err = hpt.masterService.HasPartition(ctx, hpt.HasPartitionRequest)
	if hpt.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if hpt.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(hpt.result.Status.Reason)
	}
	return err
}

func (hpt *HasPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type ShowPartitionsTask struct {
	Condition
	*milvuspb.ShowPartitionsRequest
	ctx           context.Context
	masterService types.MasterService
	result        *milvuspb.ShowPartitionsResponse
}

func (spt *ShowPartitionsTask) TraceCtx() context.Context {
	return spt.ctx
}

func (spt *ShowPartitionsTask) ID() UniqueID {
	return spt.Base.MsgID
}

func (spt *ShowPartitionsTask) SetID(uid UniqueID) {
	spt.Base.MsgID = uid
}

func (spt *ShowPartitionsTask) Name() string {
	return ShowPartitionTaskName
}

func (spt *ShowPartitionsTask) Type() commonpb.MsgType {
	return spt.Base.MsgType
}

func (spt *ShowPartitionsTask) BeginTs() Timestamp {
	return spt.Base.Timestamp
}

func (spt *ShowPartitionsTask) EndTs() Timestamp {
	return spt.Base.Timestamp
}

func (spt *ShowPartitionsTask) SetTs(ts Timestamp) {
	spt.Base.Timestamp = ts
}

func (spt *ShowPartitionsTask) OnEnqueue() error {
	spt.Base = &commonpb.MsgBase{}
	return nil
}

func (spt *ShowPartitionsTask) PreExecute(ctx context.Context) error {
	spt.Base.MsgType = commonpb.MsgType_ShowPartitions
	spt.Base.SourceID = Params.ProxyID

	if err := ValidateCollectionName(spt.CollectionName); err != nil {
		return err
	}
	return nil
}

func (spt *ShowPartitionsTask) Execute(ctx context.Context) error {
	var err error
	spt.result, err = spt.masterService.ShowPartitions(ctx, spt.ShowPartitionsRequest)
	if spt.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if spt.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(spt.result.Status.Reason)
	}
	return err
}

func (spt *ShowPartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}

type CreateIndexTask struct {
	Condition
	*milvuspb.CreateIndexRequest
	ctx           context.Context
	masterService types.MasterService
	result        *commonpb.Status
}

func (cit *CreateIndexTask) TraceCtx() context.Context {
	return cit.ctx
}

func (cit *CreateIndexTask) ID() UniqueID {
	return cit.Base.MsgID
}

func (cit *CreateIndexTask) SetID(uid UniqueID) {
	cit.Base.MsgID = uid
}

func (cit *CreateIndexTask) Name() string {
	return CreateIndexTaskName
}

func (cit *CreateIndexTask) Type() commonpb.MsgType {
	return cit.Base.MsgType
}

func (cit *CreateIndexTask) BeginTs() Timestamp {
	return cit.Base.Timestamp
}

func (cit *CreateIndexTask) EndTs() Timestamp {
	return cit.Base.Timestamp
}

func (cit *CreateIndexTask) SetTs(ts Timestamp) {
	cit.Base.Timestamp = ts
}

func (cit *CreateIndexTask) OnEnqueue() error {
	cit.Base = &commonpb.MsgBase{}
	return nil
}

func (cit *CreateIndexTask) PreExecute(ctx context.Context) error {
	cit.Base.MsgType = commonpb.MsgType_CreateIndex
	cit.Base.SourceID = Params.ProxyID

	collName, fieldName := cit.CollectionName, cit.FieldName

	if err := ValidateCollectionName(collName); err != nil {
		return err
	}

	if err := ValidateFieldName(fieldName); err != nil {
		return err
	}

	return nil
}

func (cit *CreateIndexTask) Execute(ctx context.Context) error {
	var err error
	cit.result, err = cit.masterService.CreateIndex(ctx, cit.CreateIndexRequest)
	if cit.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if cit.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(cit.result.Reason)
	}
	return err
}

func (cit *CreateIndexTask) PostExecute(ctx context.Context) error {
	return nil
}

type DescribeIndexTask struct {
	Condition
	*milvuspb.DescribeIndexRequest
	ctx           context.Context
	masterService types.MasterService
	result        *milvuspb.DescribeIndexResponse
}

func (dit *DescribeIndexTask) TraceCtx() context.Context {
	return dit.ctx
}

func (dit *DescribeIndexTask) ID() UniqueID {
	return dit.Base.MsgID
}

func (dit *DescribeIndexTask) SetID(uid UniqueID) {
	dit.Base.MsgID = uid
}

func (dit *DescribeIndexTask) Name() string {
	return DescribeIndexTaskName
}

func (dit *DescribeIndexTask) Type() commonpb.MsgType {
	return dit.Base.MsgType
}

func (dit *DescribeIndexTask) BeginTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *DescribeIndexTask) EndTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *DescribeIndexTask) SetTs(ts Timestamp) {
	dit.Base.Timestamp = ts
}

func (dit *DescribeIndexTask) OnEnqueue() error {
	dit.Base = &commonpb.MsgBase{}
	return nil
}

func (dit *DescribeIndexTask) PreExecute(ctx context.Context) error {
	dit.Base.MsgType = commonpb.MsgType_DescribeIndex
	dit.Base.SourceID = Params.ProxyID

	if err := ValidateCollectionName(dit.CollectionName); err != nil {
		return err
	}

	// only support default index name for now. @2021.02.18
	if dit.IndexName == "" {
		dit.IndexName = Params.DefaultIndexName
	}

	return nil
}

func (dit *DescribeIndexTask) Execute(ctx context.Context) error {
	var err error
	dit.result, err = dit.masterService.DescribeIndex(ctx, dit.DescribeIndexRequest)
	if dit.result == nil {
		return errors.New("get collection statistics resp is nil")
	}
	if dit.result.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(dit.result.Status.Reason)
	}
	return err
}

func (dit *DescribeIndexTask) PostExecute(ctx context.Context) error {
	return nil
}

type DropIndexTask struct {
	Condition
	ctx context.Context
	*milvuspb.DropIndexRequest
	masterService types.MasterService
	result        *commonpb.Status
}

func (dit *DropIndexTask) TraceCtx() context.Context {
	return dit.ctx
}

func (dit *DropIndexTask) ID() UniqueID {
	return dit.Base.MsgID
}

func (dit *DropIndexTask) SetID(uid UniqueID) {
	dit.Base.MsgID = uid
}

func (dit *DropIndexTask) Name() string {
	return DropIndexTaskName
}

func (dit *DropIndexTask) Type() commonpb.MsgType {
	return dit.Base.MsgType
}

func (dit *DropIndexTask) BeginTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *DropIndexTask) EndTs() Timestamp {
	return dit.Base.Timestamp
}

func (dit *DropIndexTask) SetTs(ts Timestamp) {
	dit.Base.Timestamp = ts
}

func (dit *DropIndexTask) OnEnqueue() error {
	dit.Base = &commonpb.MsgBase{}
	return nil
}

func (dit *DropIndexTask) PreExecute(ctx context.Context) error {
	dit.Base.MsgType = commonpb.MsgType_DropIndex
	dit.Base.SourceID = Params.ProxyID

	collName, fieldName := dit.CollectionName, dit.FieldName

	if err := ValidateCollectionName(collName); err != nil {
		return err
	}

	if err := ValidateFieldName(fieldName); err != nil {
		return err
	}

	return nil
}

func (dit *DropIndexTask) Execute(ctx context.Context) error {
	var err error
	dit.result, err = dit.masterService.DropIndex(ctx, dit.DropIndexRequest)
	if dit.result == nil {
		return errors.New("drop index resp is nil")
	}
	if dit.result.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(dit.result.Reason)
	}
	return err
}

func (dit *DropIndexTask) PostExecute(ctx context.Context) error {
	return nil
}

type GetIndexBuildProgressTask struct {
	Condition
	*milvuspb.GetIndexBuildProgressRequest
	ctx           context.Context
	indexService  types.IndexService
	masterService types.MasterService
	dataService   types.DataService
	result        *milvuspb.GetIndexBuildProgressResponse
}

func (gibpt *GetIndexBuildProgressTask) TraceCtx() context.Context {
	return gibpt.ctx
}

func (gibpt *GetIndexBuildProgressTask) ID() UniqueID {
	return gibpt.Base.MsgID
}

func (gibpt *GetIndexBuildProgressTask) SetID(uid UniqueID) {
	gibpt.Base.MsgID = uid
}

func (gibpt *GetIndexBuildProgressTask) Name() string {
	return GetIndexBuildProgressTaskName
}

func (gibpt *GetIndexBuildProgressTask) Type() commonpb.MsgType {
	return gibpt.Base.MsgType
}

func (gibpt *GetIndexBuildProgressTask) BeginTs() Timestamp {
	return gibpt.Base.Timestamp
}

func (gibpt *GetIndexBuildProgressTask) EndTs() Timestamp {
	return gibpt.Base.Timestamp
}

func (gibpt *GetIndexBuildProgressTask) SetTs(ts Timestamp) {
	gibpt.Base.Timestamp = ts
}

func (gibpt *GetIndexBuildProgressTask) OnEnqueue() error {
	gibpt.Base = &commonpb.MsgBase{}
	return nil
}

func (gibpt *GetIndexBuildProgressTask) PreExecute(ctx context.Context) error {
	gibpt.Base.MsgType = commonpb.MsgType_GetIndexBuildProgress
	gibpt.Base.SourceID = Params.ProxyID

	if err := ValidateCollectionName(gibpt.CollectionName); err != nil {
		return err
	}

	return nil
}

func (gibpt *GetIndexBuildProgressTask) Execute(ctx context.Context) error {
	collectionName := gibpt.CollectionName
	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}

	showPartitionRequest := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowPartitions,
			MsgID:     gibpt.Base.MsgID,
			Timestamp: gibpt.Base.Timestamp,
			SourceID:  Params.ProxyID,
		},
		DbName:         gibpt.DbName,
		CollectionName: collectionName,
		CollectionID:   collectionID,
	}
	partitions, err := gibpt.masterService.ShowPartitions(ctx, showPartitionRequest)
	if err != nil {
		return err
	}

	if gibpt.IndexName == "" {
		gibpt.IndexName = Params.DefaultIndexName
	}

	describeIndexReq := milvuspb.DescribeIndexRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeIndex,
			MsgID:     gibpt.Base.MsgID,
			Timestamp: gibpt.Base.Timestamp,
			SourceID:  Params.ProxyID,
		},
		DbName:         gibpt.DbName,
		CollectionName: gibpt.CollectionName,
		//		IndexName:      gibpt.IndexName,
	}

	indexDescriptionResp, err2 := gibpt.masterService.DescribeIndex(ctx, &describeIndexReq)
	if err2 != nil {
		return err2
	}

	matchIndexID := int64(-1)
	foundIndexID := false
	for _, desc := range indexDescriptionResp.IndexDescriptions {
		if desc.IndexName == gibpt.IndexName {
			matchIndexID = desc.IndexID
			foundIndexID = true
			break
		}
	}
	if !foundIndexID {
		return errors.New(fmt.Sprint("Can't found IndexID for indexName", gibpt.IndexName))
	}

	var allSegmentIDs []UniqueID
	for _, partitionID := range partitions.PartitionIDs {
		showSegmentsRequest := &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
				MsgID:     gibpt.Base.MsgID,
				Timestamp: gibpt.Base.Timestamp,
				SourceID:  Params.ProxyID,
			},
			CollectionID: collectionID,
			PartitionID:  partitionID,
		}
		segments, err := gibpt.masterService.ShowSegments(ctx, showSegmentsRequest)
		if err != nil {
			return err
		}
		if segments.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(segments.Status.Reason)
		}
		allSegmentIDs = append(allSegmentIDs, segments.SegmentIDs...)
	}

	getIndexStatesRequest := &indexpb.GetIndexStatesRequest{
		IndexBuildIDs: make([]UniqueID, 0),
	}

	buildIndexMap := make(map[int64]int64)
	for _, segmentID := range allSegmentIDs {
		describeSegmentRequest := &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeSegment,
				MsgID:     gibpt.Base.MsgID,
				Timestamp: gibpt.Base.Timestamp,
				SourceID:  Params.ProxyID,
			},
			CollectionID: collectionID,
			SegmentID:    segmentID,
		}
		segmentDesc, err := gibpt.masterService.DescribeSegment(ctx, describeSegmentRequest)
		if err != nil {
			return err
		}
		if segmentDesc.IndexID == matchIndexID {
			if segmentDesc.EnableIndex {
				getIndexStatesRequest.IndexBuildIDs = append(getIndexStatesRequest.IndexBuildIDs, segmentDesc.BuildID)
				buildIndexMap[segmentID] = segmentDesc.BuildID
			}
		}
	}

	states, err := gibpt.indexService.GetIndexStates(ctx, getIndexStatesRequest)
	if err != nil {
		return err
	}

	if states.Status.ErrorCode != commonpb.ErrorCode_Success {
		gibpt.result = &milvuspb.GetIndexBuildProgressResponse{
			Status: states.Status,
		}
	}

	buildFinishMap := make(map[int64]bool)
	for _, state := range states.States {
		if state.State == commonpb.IndexState_Finished {
			buildFinishMap[state.IndexBuildID] = true
		}
	}

	infoResp, err := gibpt.dataService.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentInfo,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyID,
		},
		SegmentIDs: allSegmentIDs,
	})
	if err != nil {
		return err
	}

	total := int64(0)
	indexed := int64(0)

	for _, info := range infoResp.Infos {
		total += info.NumOfRows
		if buildFinishMap[buildIndexMap[info.ID]] {
			indexed += info.NumOfRows
		}
	}

	gibpt.result = &milvuspb.GetIndexBuildProgressResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		TotalRows:   total,
		IndexedRows: indexed,
	}

	return nil
}

func (gibpt *GetIndexBuildProgressTask) PostExecute(ctx context.Context) error {
	return nil
}

type GetIndexStateTask struct {
	Condition
	*milvuspb.GetIndexStateRequest
	ctx           context.Context
	indexService  types.IndexService
	masterService types.MasterService
	result        *milvuspb.GetIndexStateResponse
}

func (gist *GetIndexStateTask) TraceCtx() context.Context {
	return gist.ctx
}

func (gist *GetIndexStateTask) ID() UniqueID {
	return gist.Base.MsgID
}

func (gist *GetIndexStateTask) SetID(uid UniqueID) {
	gist.Base.MsgID = uid
}

func (gist *GetIndexStateTask) Name() string {
	return GetIndexStateTaskName
}

func (gist *GetIndexStateTask) Type() commonpb.MsgType {
	return gist.Base.MsgType
}

func (gist *GetIndexStateTask) BeginTs() Timestamp {
	return gist.Base.Timestamp
}

func (gist *GetIndexStateTask) EndTs() Timestamp {
	return gist.Base.Timestamp
}

func (gist *GetIndexStateTask) SetTs(ts Timestamp) {
	gist.Base.Timestamp = ts
}

func (gist *GetIndexStateTask) OnEnqueue() error {
	gist.Base = &commonpb.MsgBase{}
	return nil
}

func (gist *GetIndexStateTask) PreExecute(ctx context.Context) error {
	gist.Base.MsgType = commonpb.MsgType_GetIndexState
	gist.Base.SourceID = Params.ProxyID

	if err := ValidateCollectionName(gist.CollectionName); err != nil {
		return err
	}

	return nil
}

func (gist *GetIndexStateTask) Execute(ctx context.Context) error {
	collectionName := gist.CollectionName
	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	if err != nil { // err is not nil if collection not exists
		return err
	}

	showPartitionRequest := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowPartitions,
			MsgID:     gist.Base.MsgID,
			Timestamp: gist.Base.Timestamp,
			SourceID:  Params.ProxyID,
		},
		DbName:         gist.DbName,
		CollectionName: collectionName,
		CollectionID:   collectionID,
	}
	partitions, err := gist.masterService.ShowPartitions(ctx, showPartitionRequest)
	if err != nil {
		return err
	}

	if gist.IndexName == "" {
		gist.IndexName = Params.DefaultIndexName
	}

	describeIndexReq := milvuspb.DescribeIndexRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeIndex,
			MsgID:     gist.Base.MsgID,
			Timestamp: gist.Base.Timestamp,
			SourceID:  Params.ProxyID,
		},
		DbName:         gist.DbName,
		CollectionName: gist.CollectionName,
		IndexName:      gist.IndexName,
	}

	indexDescriptionResp, err2 := gist.masterService.DescribeIndex(ctx, &describeIndexReq)
	if err2 != nil {
		return err2
	}

	matchIndexID := int64(-1)
	foundIndexID := false
	for _, desc := range indexDescriptionResp.IndexDescriptions {
		if desc.IndexName == gist.IndexName {
			matchIndexID = desc.IndexID
			foundIndexID = true
			break
		}
	}
	if !foundIndexID {
		return errors.New(fmt.Sprint("Can't found IndexID for indexName", gist.IndexName))
	}

	var allSegmentIDs []UniqueID
	for _, partitionID := range partitions.PartitionIDs {
		showSegmentsRequest := &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
				MsgID:     gist.Base.MsgID,
				Timestamp: gist.Base.Timestamp,
				SourceID:  Params.ProxyID,
			},
			CollectionID: collectionID,
			PartitionID:  partitionID,
		}
		segments, err := gist.masterService.ShowSegments(ctx, showSegmentsRequest)
		if err != nil {
			return err
		}
		if segments.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(segments.Status.Reason)
		}
		allSegmentIDs = append(allSegmentIDs, segments.SegmentIDs...)
	}

	getIndexStatesRequest := &indexpb.GetIndexStatesRequest{
		IndexBuildIDs: make([]UniqueID, 0),
	}
	enableIndexBitMap := make([]bool, 0)
	indexBuildIDs := make([]UniqueID, 0)

	for _, segmentID := range allSegmentIDs {
		describeSegmentRequest := &milvuspb.DescribeSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeSegment,
				MsgID:     gist.Base.MsgID,
				Timestamp: gist.Base.Timestamp,
				SourceID:  Params.ProxyID,
			},
			CollectionID: collectionID,
			SegmentID:    segmentID,
		}
		segmentDesc, err := gist.masterService.DescribeSegment(ctx, describeSegmentRequest)
		if err != nil {
			return err
		}
		if segmentDesc.IndexID == matchIndexID {
			indexBuildIDs = append(indexBuildIDs, segmentDesc.BuildID)
			if segmentDesc.EnableIndex {
				enableIndexBitMap = append(enableIndexBitMap, true)
			} else {
				enableIndexBitMap = append(enableIndexBitMap, false)
			}
		}
	}

	log.Debug("proxynode", zap.Int("GetIndexState:: len of allSegmentIDs", len(allSegmentIDs)))
	log.Debug("proxynode", zap.Int("GetIndexState:: len of IndexBuildIDs", len(indexBuildIDs)))
	if len(allSegmentIDs) != len(indexBuildIDs) {
		gist.result = &milvuspb.GetIndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			},
			State: commonpb.IndexState_InProgress,
		}
		return err
	}

	for idx, enableIndex := range enableIndexBitMap {
		if enableIndex {
			getIndexStatesRequest.IndexBuildIDs = append(getIndexStatesRequest.IndexBuildIDs, indexBuildIDs[idx])
		}
	}
	states, err := gist.indexService.GetIndexStates(ctx, getIndexStatesRequest)
	if err != nil {
		return err
	}

	if states.Status.ErrorCode != commonpb.ErrorCode_Success {
		gist.result = &milvuspb.GetIndexStateResponse{
			Status: states.Status,
			State:  commonpb.IndexState_Failed,
		}

		return nil
	}

	for _, state := range states.States {
		if state.State != commonpb.IndexState_Finished {
			gist.result = &milvuspb.GetIndexStateResponse{
				Status: states.Status,
				State:  state.State,
			}

			return nil
		}
	}

	gist.result = &milvuspb.GetIndexStateResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		State: commonpb.IndexState_Finished,
	}

	return nil
}

func (gist *GetIndexStateTask) PostExecute(ctx context.Context) error {
	return nil
}

type FlushTask struct {
	Condition
	*milvuspb.FlushRequest
	ctx         context.Context
	dataService types.DataService
	result      *commonpb.Status
}

func (ft *FlushTask) TraceCtx() context.Context {
	return ft.ctx
}

func (ft *FlushTask) ID() UniqueID {
	return ft.Base.MsgID
}

func (ft *FlushTask) SetID(uid UniqueID) {
	ft.Base.MsgID = uid
}

func (ft *FlushTask) Name() string {
	return FlushTaskName
}

func (ft *FlushTask) Type() commonpb.MsgType {
	return ft.Base.MsgType
}

func (ft *FlushTask) BeginTs() Timestamp {
	return ft.Base.Timestamp
}

func (ft *FlushTask) EndTs() Timestamp {
	return ft.Base.Timestamp
}

func (ft *FlushTask) SetTs(ts Timestamp) {
	ft.Base.Timestamp = ts
}

func (ft *FlushTask) OnEnqueue() error {
	ft.Base = &commonpb.MsgBase{}
	return nil
}

func (ft *FlushTask) PreExecute(ctx context.Context) error {
	ft.Base.MsgType = commonpb.MsgType_Flush
	ft.Base.SourceID = Params.ProxyID
	return nil
}

func (ft *FlushTask) Execute(ctx context.Context) error {
	for _, collName := range ft.CollectionNames {
		collID, err := globalMetaCache.GetCollectionID(ctx, collName)
		if err != nil {
			return err
		}
		flushReq := &datapb.FlushRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Flush,
				MsgID:     ft.Base.MsgID,
				Timestamp: ft.Base.Timestamp,
				SourceID:  ft.Base.SourceID,
			},
			DbID:         0,
			CollectionID: collID,
		}
		var status *commonpb.Status
		status, _ = ft.dataService.Flush(ctx, flushReq)
		if status == nil {
			return errors.New("flush resp is nil")
		}
		if status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(status.Reason)
		}
	}
	ft.result = &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return nil
}

func (ft *FlushTask) PostExecute(ctx context.Context) error {
	return nil
}

type LoadCollectionTask struct {
	Condition
	*milvuspb.LoadCollectionRequest
	ctx          context.Context
	queryService types.QueryService
	result       *commonpb.Status
}

func (lct *LoadCollectionTask) TraceCtx() context.Context {
	return lct.ctx
}

func (lct *LoadCollectionTask) ID() UniqueID {
	return lct.Base.MsgID
}

func (lct *LoadCollectionTask) SetID(uid UniqueID) {
	lct.Base.MsgID = uid
}

func (lct *LoadCollectionTask) Name() string {
	return LoadCollectionTaskName
}

func (lct *LoadCollectionTask) Type() commonpb.MsgType {
	return lct.Base.MsgType
}

func (lct *LoadCollectionTask) BeginTs() Timestamp {
	return lct.Base.Timestamp
}

func (lct *LoadCollectionTask) EndTs() Timestamp {
	return lct.Base.Timestamp
}

func (lct *LoadCollectionTask) SetTs(ts Timestamp) {
	lct.Base.Timestamp = ts
}

func (lct *LoadCollectionTask) OnEnqueue() error {
	lct.Base = &commonpb.MsgBase{}
	return nil
}

func (lct *LoadCollectionTask) PreExecute(ctx context.Context) error {
	log.Debug("LoadCollectionTask PreExecute", zap.String("role", Params.RoleName), zap.Int64("msgID", lct.Base.MsgID))
	lct.Base.MsgType = commonpb.MsgType_LoadCollection
	lct.Base.SourceID = Params.ProxyID

	collName := lct.CollectionName

	if err := ValidateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (lct *LoadCollectionTask) Execute(ctx context.Context) (err error) {
	log.Debug("LoadCollectionTask Execute", zap.String("role", Params.RoleName), zap.Int64("msgID", lct.Base.MsgID))
	collID, err := globalMetaCache.GetCollectionID(ctx, lct.CollectionName)
	if err != nil {
		return err
	}
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, lct.CollectionName)
	if err != nil {
		return err
	}

	request := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_LoadCollection,
			MsgID:     lct.Base.MsgID,
			Timestamp: lct.Base.Timestamp,
			SourceID:  lct.Base.SourceID,
		},
		DbID:         0,
		CollectionID: collID,
		Schema:       collSchema,
	}
	log.Debug("send LoadCollectionRequest to query service", zap.String("role", Params.RoleName), zap.Int64("msgID", request.Base.MsgID), zap.Int64("collectionID", request.CollectionID),
		zap.Any("schema", request.Schema))
	lct.result, err = lct.queryService.LoadCollection(ctx, request)
	if err != nil {
		return fmt.Errorf("call query service LoadCollection: %s", err)
	}
	return nil
}

func (lct *LoadCollectionTask) PostExecute(ctx context.Context) error {
	log.Debug("LoadCollectionTask PostExecute", zap.String("role", Params.RoleName), zap.Int64("msgID", lct.Base.MsgID))
	return nil
}

type ReleaseCollectionTask struct {
	Condition
	*milvuspb.ReleaseCollectionRequest
	ctx          context.Context
	queryService types.QueryService
	result       *commonpb.Status
}

func (rct *ReleaseCollectionTask) TraceCtx() context.Context {
	return rct.ctx
}

func (rct *ReleaseCollectionTask) ID() UniqueID {
	return rct.Base.MsgID
}

func (rct *ReleaseCollectionTask) SetID(uid UniqueID) {
	rct.Base.MsgID = uid
}

func (rct *ReleaseCollectionTask) Name() string {
	return ReleaseCollectionTaskName
}

func (rct *ReleaseCollectionTask) Type() commonpb.MsgType {
	return rct.Base.MsgType
}

func (rct *ReleaseCollectionTask) BeginTs() Timestamp {
	return rct.Base.Timestamp
}

func (rct *ReleaseCollectionTask) EndTs() Timestamp {
	return rct.Base.Timestamp
}

func (rct *ReleaseCollectionTask) SetTs(ts Timestamp) {
	rct.Base.Timestamp = ts
}

func (rct *ReleaseCollectionTask) OnEnqueue() error {
	rct.Base = &commonpb.MsgBase{}
	return nil
}

func (rct *ReleaseCollectionTask) PreExecute(ctx context.Context) error {
	rct.Base.MsgType = commonpb.MsgType_ReleaseCollection
	rct.Base.SourceID = Params.ProxyID

	collName := rct.CollectionName

	if err := ValidateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (rct *ReleaseCollectionTask) Execute(ctx context.Context) (err error) {
	collID, err := globalMetaCache.GetCollectionID(ctx, rct.CollectionName)
	if err != nil {
		return err
	}
	request := &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ReleaseCollection,
			MsgID:     rct.Base.MsgID,
			Timestamp: rct.Base.Timestamp,
			SourceID:  rct.Base.SourceID,
		},
		DbID:         0,
		CollectionID: collID,
	}
	rct.result, err = rct.queryService.ReleaseCollection(ctx, request)
	return err
}

func (rct *ReleaseCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

type LoadPartitionTask struct {
	Condition
	*milvuspb.LoadPartitionsRequest
	ctx          context.Context
	queryService types.QueryService
	result       *commonpb.Status
}

func (lpt *LoadPartitionTask) TraceCtx() context.Context {
	return lpt.ctx
}

func (lpt *LoadPartitionTask) ID() UniqueID {
	return lpt.Base.MsgID
}

func (lpt *LoadPartitionTask) SetID(uid UniqueID) {
	lpt.Base.MsgID = uid
}

func (lpt *LoadPartitionTask) Name() string {
	return LoadPartitionTaskName
}

func (lpt *LoadPartitionTask) Type() commonpb.MsgType {
	return lpt.Base.MsgType
}

func (lpt *LoadPartitionTask) BeginTs() Timestamp {
	return lpt.Base.Timestamp
}

func (lpt *LoadPartitionTask) EndTs() Timestamp {
	return lpt.Base.Timestamp
}

func (lpt *LoadPartitionTask) SetTs(ts Timestamp) {
	lpt.Base.Timestamp = ts
}

func (lpt *LoadPartitionTask) OnEnqueue() error {
	lpt.Base = &commonpb.MsgBase{}
	return nil
}

func (lpt *LoadPartitionTask) PreExecute(ctx context.Context) error {
	lpt.Base.MsgType = commonpb.MsgType_LoadPartitions
	lpt.Base.SourceID = Params.ProxyID

	collName := lpt.CollectionName

	if err := ValidateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (lpt *LoadPartitionTask) Execute(ctx context.Context) error {
	var partitionIDs []int64
	collID, err := globalMetaCache.GetCollectionID(ctx, lpt.CollectionName)
	if err != nil {
		return err
	}
	collSchema, err := globalMetaCache.GetCollectionSchema(ctx, lpt.CollectionName)
	if err != nil {
		return err
	}
	for _, partitionName := range lpt.PartitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, lpt.CollectionName, partitionName)
		if err != nil {
			return err
		}
		partitionIDs = append(partitionIDs, partitionID)
	}
	request := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_LoadPartitions,
			MsgID:     lpt.Base.MsgID,
			Timestamp: lpt.Base.Timestamp,
			SourceID:  lpt.Base.SourceID,
		},
		DbID:         0,
		CollectionID: collID,
		PartitionIDs: partitionIDs,
		Schema:       collSchema,
	}
	lpt.result, err = lpt.queryService.LoadPartitions(ctx, request)
	return err
}

func (lpt *LoadPartitionTask) PostExecute(ctx context.Context) error {
	return nil
}

type ReleasePartitionTask struct {
	Condition
	*milvuspb.ReleasePartitionsRequest
	ctx          context.Context
	queryService types.QueryService
	result       *commonpb.Status
}

func (rpt *ReleasePartitionTask) TraceCtx() context.Context {
	return rpt.ctx
}

func (rpt *ReleasePartitionTask) ID() UniqueID {
	return rpt.Base.MsgID
}

func (rpt *ReleasePartitionTask) SetID(uid UniqueID) {
	rpt.Base.MsgID = uid
}

func (rpt *ReleasePartitionTask) Type() commonpb.MsgType {
	return rpt.Base.MsgType
}

func (rpt *ReleasePartitionTask) Name() string {
	return ReleasePartitionTaskName
}

func (rpt *ReleasePartitionTask) BeginTs() Timestamp {
	return rpt.Base.Timestamp
}

func (rpt *ReleasePartitionTask) EndTs() Timestamp {
	return rpt.Base.Timestamp
}

func (rpt *ReleasePartitionTask) SetTs(ts Timestamp) {
	rpt.Base.Timestamp = ts
}

func (rpt *ReleasePartitionTask) OnEnqueue() error {
	rpt.Base = &commonpb.MsgBase{}
	return nil
}

func (rpt *ReleasePartitionTask) PreExecute(ctx context.Context) error {
	rpt.Base.MsgType = commonpb.MsgType_ReleasePartitions
	rpt.Base.SourceID = Params.ProxyID

	collName := rpt.CollectionName

	if err := ValidateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (rpt *ReleasePartitionTask) Execute(ctx context.Context) (err error) {
	var partitionIDs []int64
	collID, err := globalMetaCache.GetCollectionID(ctx, rpt.CollectionName)
	if err != nil {
		return err
	}
	for _, partitionName := range rpt.PartitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, rpt.CollectionName, partitionName)
		if err != nil {
			return err
		}
		partitionIDs = append(partitionIDs, partitionID)
	}
	request := &querypb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ReleasePartitions,
			MsgID:     rpt.Base.MsgID,
			Timestamp: rpt.Base.Timestamp,
			SourceID:  rpt.Base.SourceID,
		},
		DbID:         0,
		CollectionID: collID,
		PartitionIDs: partitionIDs,
	}
	rpt.result, err = rpt.queryService.ReleasePartitions(ctx, request)
	return err
}

func (rpt *ReleasePartitionTask) PostExecute(ctx context.Context) error {
	return nil
}
