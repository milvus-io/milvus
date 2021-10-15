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

package datanode

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	miniokv "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/trace"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

type (
	// InsertData of storage
	InsertData = storage.InsertData

	// Blob of storage
	Blob = storage.Blob
)

type insertBufferNode struct {
	BaseNode
	channelName  string
	insertBuffer sync.Map // SegmentID to BufferData
	replica      Replica
	idAllocator  allocatorInterface

	flushMap         sync.Map
	flushChan        <-chan *flushMsg
	flushingSegCache *Cache

	minIOKV kv.BaseKV

	timeTickStream          msgstream.MsgStream
	segmentStatisticsStream msgstream.MsgStream

	dsSaveBinlog func(fu *segmentFlushUnit) error
}

type segmentCheckPoint struct {
	numRows int64
	pos     internalpb.MsgPosition
}

type segmentFlushUnit struct {
	collID         UniqueID
	segID          UniqueID
	field2Path     map[UniqueID]string
	checkPoint     map[UniqueID]segmentCheckPoint
	startPositions []*datapb.SegmentStartPosition
	flushed        bool
}

// BufferData buffers insert data, monitoring buffer size and limit
// size and limit both indicate numOfRows
type BufferData struct {
	buffer *InsertData
	size   int64
	limit  int64
}

// newBufferData needs an input dimension to calculate the limit of this buffer
//
// `limit` is the segment numOfRows a buffer can buffer at most.
//
// For a float32 vector field:
//  limit = 16 * 2^20 Byte [By default] / (dimension * 4 Byte)
//
// For a binary vector field:
//  limit = 16 * 2^20 Byte [By default]/ (dimension / 8 Byte)
//
// But since the buffer of binary vector fields is larger than the float32 one
//   with the same dimension, newBufferData takes the smaller buffer limit
//   to fit in both types of vector fields
//
// * This need to change for string field support and multi-vector fields support.
func newBufferData(dimension int64) (*BufferData, error) {
	if dimension == 0 {
		return nil, errors.New("Invalid dimension")
	}

	limit := Params.FlushInsertBufferSize / (dimension * 4)

	return &BufferData{&InsertData{Data: make(map[UniqueID]storage.FieldData)}, 0, limit}, nil
}

func (bd *BufferData) effectiveCap() int64 {
	return bd.limit - bd.size
}

func (bd *BufferData) updateSize(no int64) {
	bd.size += no
}

func (ibNode *insertBufferNode) Name() string {
	return "ibNode"
}

func (ibNode *insertBufferNode) Close() {
	if ibNode.timeTickStream != nil {
		ibNode.timeTickStream.Close()
	}

	if ibNode.segmentStatisticsStream != nil {
		ibNode.segmentStatisticsStream.Close()
	}
}

func (ibNode *insertBufferNode) Operate(in []Msg) []Msg {
	// log.Debug("InsertBufferNode Operating")

	if len(in) != 1 {
		log.Error("Invalid operate message input in insertBufferNode", zap.Int("input length", len(in)))
		return []Msg{}
	}

	fgMsg, ok := in[0].(*flowGraphMsg)
	if !ok {
		log.Error("type assertion failed for flowGraphMsg")
		ibNode.Close()
		return []Msg{}
	}

	var spans []opentracing.Span
	for _, msg := range fgMsg.insertMessages {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	// replace pchannel with vchannel
	startPositions := make([]*internalpb.MsgPosition, 0, len(fgMsg.startPositions))
	for idx := range fgMsg.startPositions {
		pos := proto.Clone(fgMsg.startPositions[idx]).(*internalpb.MsgPosition)
		pos.ChannelName = ibNode.channelName
		startPositions = append(startPositions, pos)
	}
	endPositions := make([]*internalpb.MsgPosition, 0, len(fgMsg.endPositions))
	for idx := range fgMsg.endPositions {
		pos := proto.Clone(fgMsg.endPositions[idx]).(*internalpb.MsgPosition)
		pos.ChannelName = ibNode.channelName
		endPositions = append(endPositions, pos)
	}

	// Updating segment statistics in replica
	seg2Upload, err := ibNode.updateSegStatesInReplica(fgMsg.insertMessages, startPositions[0], endPositions[0])
	if err != nil {
		log.Warn("update segment states in Replica wrong", zap.Error(err))
		return []Msg{}
	}

	if len(seg2Upload) > 0 {
		err := ibNode.uploadMemStates2Coord(seg2Upload)
		if err != nil {
			log.Error("upload segment statistics to coord error", zap.Error(err))
		}
	}

	// insert messages -> buffer
	for _, msg := range fgMsg.insertMessages {
		err := ibNode.bufferInsertMsg(msg, endPositions[0])
		if err != nil {
			log.Warn("msg to buffer failed", zap.Error(err))
		}
	}

	// Find and return the smaller input
	min := func(former, latter int) (smaller int) {
		if former <= latter {
			return former
		}
		return latter
	}

	displaySize := min(10, len(seg2Upload))

	// Log the segment statistics in mem
	for k, segID := range seg2Upload[:displaySize] {
		bd, ok := ibNode.insertBuffer.Load(segID)
		if !ok {
			continue
		}

		log.Debug("insert seg buffer status", zap.Int("No.", k),
			zap.Int64("segmentID", segID),
			zap.Int64("buffer size", bd.(*BufferData).size),
			zap.Int64("buffer limit", bd.(*BufferData).limit))
	}

	// Auto Flush
	finishCh := make(chan segmentFlushUnit, len(seg2Upload))
	finishCnt := sync.WaitGroup{}
	for _, segToFlush := range seg2Upload {
		// If full, auto flush
		if bd, ok := ibNode.insertBuffer.Load(segToFlush); ok && bd.(*BufferData).effectiveCap() <= 0 {

			// Move data from insertBuffer to flushBuffer
			ibuffer := bd.(*BufferData)
			ibNode.flushMap.Store(segToFlush, ibuffer.buffer)
			ibNode.insertBuffer.Delete(segToFlush)

			log.Debug(". Insert Buffer full, auto flushing ", zap.Int64("num of rows", ibuffer.size))

			collMeta, err := ibNode.getCollMetabySegID(segToFlush, fgMsg.timeRange.timestampMax)
			if err != nil {
				log.Error("Auto flush failed .. cannot get collection meta ..", zap.Error(err))
				continue
			}

			collID, partitionID, err := ibNode.getCollectionandPartitionIDbySegID(segToFlush)
			if err != nil {
				log.Error("Auto flush failed .. cannot get collection ID or partition ID..", zap.Error(err))
				continue
			}
			finishCnt.Add(1)

			go flushSegment(collMeta, segToFlush, partitionID, collID,
				&ibNode.flushMap, ibNode.minIOKV, finishCh, &finishCnt, ibNode, ibNode.idAllocator)
		}
	}
	finishCnt.Wait()
	close(finishCh)
	for fu := range finishCh {
		if fu.field2Path == nil {
			log.Debug("segment is empty")
			continue
		}
		fu.checkPoint = ibNode.replica.listSegmentsCheckPoints()
		fu.flushed = false
		if err := ibNode.dsSaveBinlog(&fu); err != nil {
			log.Debug("data service save bin log path failed", zap.Error(err))
		}
	}

	// Manual Flush
	select {
	case fmsg := <-ibNode.flushChan:
		currentSegID := fmsg.segmentID
		log.Debug(". Receiving flush message",
			zap.Int64("segmentID", currentSegID),
			zap.Int64("collectionID", fmsg.collectionID),
		)

		bd, ok := ibNode.insertBuffer.Load(currentSegID)

		if !ok || bd.(*BufferData).size <= 0 { // Buffer empty
			log.Debug(".. Buffer empty ...")
			err = ibNode.dsSaveBinlog(&segmentFlushUnit{
				collID:     fmsg.collectionID,
				segID:      currentSegID,
				field2Path: map[UniqueID]string{},
				checkPoint: ibNode.replica.listSegmentsCheckPoints(),
				flushed:    true,
			})
			if err != nil {
				log.Debug("insert buffer node save binlog failed", zap.Error(err))
				break
			}
			ibNode.replica.segmentFlushed(currentSegID)
		} else { // Buffer not empty
			log.Debug(".. Buffer not empty, flushing ..")
			finishCh := make(chan segmentFlushUnit, 1)

			ibNode.flushMap.Store(currentSegID, bd.(*BufferData).buffer)
			clearFn := func() {
				finishCh <- segmentFlushUnit{field2Path: nil}
				log.Debug(".. Clearing flush Buffer ..")
				ibNode.flushMap.Delete(currentSegID)
				close(finishCh)
			}

			collID, partitionID, err := ibNode.getCollectionandPartitionIDbySegID(currentSegID)
			if err != nil {
				log.Error("Flush failed .. cannot get segment ..", zap.Error(err))
				clearFn()
				break
				// TODO add error handling
			}

			collMeta, err := ibNode.getCollMetabySegID(currentSegID, fgMsg.timeRange.timestampMax)
			if err != nil {
				log.Error("Flush failed .. cannot get collection schema ..", zap.Error(err))
				clearFn()
				break
				// TODO add error handling
			}

			flushSegment(collMeta, currentSegID, partitionID, collID,
				&ibNode.flushMap, ibNode.minIOKV, finishCh, nil, ibNode, ibNode.idAllocator)
			fu := <-finishCh
			close(finishCh)
			if fu.field2Path != nil {
				fu.checkPoint = ibNode.replica.listSegmentsCheckPoints()
				fu.flushed = true
				if err := ibNode.dsSaveBinlog(&fu); err != nil {
					log.Debug("Data service save binlog path failed", zap.Error(err))
				} else {
					ibNode.replica.segmentFlushed(fu.segID)
					ibNode.insertBuffer.Delete(fu.segID)
				}
			}
			//always remove from flushing seg cache
			ibNode.flushingSegCache.Remove(fu.segID)
		}

	default:
	}

	if err := ibNode.writeHardTimeTick(fgMsg.timeRange.timestampMax); err != nil {
		log.Error("send hard time tick into pulsar channel failed", zap.Error(err))
	}

	res := flowGraphMsg{
		deleteMessages: fgMsg.deleteMessages,
		timeRange:      fgMsg.timeRange,
		startPositions: fgMsg.startPositions,
		endPositions:   fgMsg.endPositions,
	}

	for _, sp := range spans {
		sp.Finish()
	}

	// send delete msg to DeleteNode
	return []Msg{&res}
}

// updateSegStatesInReplica updates statistics in replica for the segments in insertMsgs.
//  If the segment doesn't exist, a new segment will be created.
//  The segment number of rows will be updated in mem, waiting to be uploaded to DataCoord.
func (ibNode *insertBufferNode) updateSegStatesInReplica(insertMsgs []*msgstream.InsertMsg, startPos, endPos *internalpb.MsgPosition) (seg2Upload []UniqueID, err error) {
	uniqueSeg := make(map[UniqueID]int64)
	for _, msg := range insertMsgs {

		currentSegID := msg.GetSegmentID()
		collID := msg.GetCollectionID()
		partitionID := msg.GetPartitionID()

		if !ibNode.replica.hasSegment(currentSegID, true) {
			err = ibNode.replica.addNewSegment(currentSegID, collID, partitionID, msg.GetShardName(),
				startPos, endPos)
			if err != nil {
				log.Error("add segment wrong",
					zap.Int64("segID", currentSegID),
					zap.Int64("collID", collID),
					zap.Int64("partID", partitionID),
					zap.String("chanName", msg.GetShardName()),
					zap.Error(err))
				return
			}
		}

		segNum := uniqueSeg[currentSegID]
		uniqueSeg[currentSegID] = segNum + int64(len(msg.RowIDs))
	}

	seg2Upload = make([]UniqueID, 0, len(uniqueSeg))
	for id, num := range uniqueSeg {
		seg2Upload = append(seg2Upload, id)
		ibNode.replica.updateStatistics(id, num)
	}

	return
}

/* #nosec G103 */
// bufferInsertMsg put InsertMsg into buffer
// 	1.1 fetch related schema from replica
// 	1.2 Get buffer data and put data into each field buffer
// 	1.3 Put back into buffer
// 	1.4 Update related statistics
func (ibNode *insertBufferNode) bufferInsertMsg(msg *msgstream.InsertMsg, endPos *internalpb.MsgPosition) error {
	if len(msg.RowIDs) != len(msg.Timestamps) || len(msg.RowIDs) != len(msg.RowData) {
		return errors.New("misaligned messages detected")
	}
	currentSegID := msg.GetSegmentID()
	collectionID := msg.GetCollectionID()

	collSchema, err := ibNode.replica.getCollectionSchema(collectionID, msg.EndTs())
	if err != nil {
		log.Error("Get schema wrong:", zap.Error(err))
		return err
	}

	// Get Dimension
	// TODO GOOSE: under assumption that there's only 1 Vector field in one collection schema
	var dimension int
	for _, field := range collSchema.Fields {
		if field.DataType == schemapb.DataType_FloatVector ||
			field.DataType == schemapb.DataType_BinaryVector {

			for _, t := range field.TypeParams {
				if t.Key == "dim" {
					dimension, err = strconv.Atoi(t.Value)
					if err != nil {
						log.Error("strconv wrong on get dim", zap.Error(err))
						return err
					}
					break
				}
			}
			break
		}
	}

	newbd, err := newBufferData(int64(dimension))
	if err != nil {
		return err
	}
	bd, _ := ibNode.insertBuffer.LoadOrStore(currentSegID, newbd)

	buffer := bd.(*BufferData)
	idata := buffer.buffer

	// 1.2 Get Fields
	var fieldIDs []int64
	var fieldTypes []schemapb.DataType
	for _, field := range collSchema.Fields {
		fieldIDs = append(fieldIDs, field.FieldID)
		fieldTypes = append(fieldTypes, field.DataType)
	}

	blobReaders := make([]io.Reader, 0)
	for _, blob := range msg.RowData {
		blobReaders = append(blobReaders, bytes.NewReader(blob.GetValue()))
	}

	for _, field := range collSchema.Fields {
		switch field.DataType {
		case schemapb.DataType_FloatVector:
			var dim int
			for _, t := range field.TypeParams {
				if t.Key == "dim" {
					dim, err = strconv.Atoi(t.Value)
					if err != nil {
						log.Error("strconv wrong on get dim", zap.Error(err))
						break
					}
					break
				}
			}

			if _, ok := idata.Data[field.FieldID]; !ok {
				idata.Data[field.FieldID] = &storage.FloatVectorFieldData{
					NumRows: make([]int64, 0, 1),
					Data:    make([]float32, 0),
					Dim:     dim,
				}
			}

			fieldData := idata.Data[field.FieldID].(*storage.FloatVectorFieldData)
			for _, r := range blobReaders {
				var v []float32 = make([]float32, dim)

				readBinary(r, &v, field.DataType)

				fieldData.Data = append(fieldData.Data, v...)
			}

			fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))

		case schemapb.DataType_BinaryVector:
			var dim int
			for _, t := range field.TypeParams {
				if t.Key == "dim" {
					dim, err = strconv.Atoi(t.Value)
					if err != nil {
						log.Error("strconv wrong on get dim", zap.Error(err))
						return err
					}
					break
				}
			}

			if _, ok := idata.Data[field.FieldID]; !ok {
				idata.Data[field.FieldID] = &storage.BinaryVectorFieldData{
					NumRows: make([]int64, 0, 1),
					Data:    make([]byte, 0),
					Dim:     dim,
				}
			}
			fieldData := idata.Data[field.FieldID].(*storage.BinaryVectorFieldData)

			for _, r := range blobReaders {
				var v []byte = make([]byte, dim/8)
				readBinary(r, &v, field.DataType)

				fieldData.Data = append(fieldData.Data, v...)
			}

			fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))

		case schemapb.DataType_Bool:
			if _, ok := idata.Data[field.FieldID]; !ok {
				idata.Data[field.FieldID] = &storage.BoolFieldData{
					NumRows: make([]int64, 0, 1),
					Data:    make([]bool, 0),
				}
			}

			fieldData := idata.Data[field.FieldID].(*storage.BoolFieldData)
			for _, r := range blobReaders {
				var v bool
				readBinary(r, &v, field.DataType)

				fieldData.Data = append(fieldData.Data, v)
			}

			fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))

		case schemapb.DataType_Int8:
			if _, ok := idata.Data[field.FieldID]; !ok {
				idata.Data[field.FieldID] = &storage.Int8FieldData{
					NumRows: make([]int64, 0, 1),
					Data:    make([]int8, 0),
				}
			}

			fieldData := idata.Data[field.FieldID].(*storage.Int8FieldData)
			for _, r := range blobReaders {
				var v int8
				readBinary(r, &v, field.DataType)

				fieldData.Data = append(fieldData.Data, v)
			}

			fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))

		case schemapb.DataType_Int16:
			if _, ok := idata.Data[field.FieldID]; !ok {
				idata.Data[field.FieldID] = &storage.Int16FieldData{
					NumRows: make([]int64, 0, 1),
					Data:    make([]int16, 0),
				}
			}

			fieldData := idata.Data[field.FieldID].(*storage.Int16FieldData)
			for _, r := range blobReaders {
				var v int16
				readBinary(r, &v, field.DataType)

				fieldData.Data = append(fieldData.Data, v)
			}

			fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))

		case schemapb.DataType_Int32:
			if _, ok := idata.Data[field.FieldID]; !ok {
				idata.Data[field.FieldID] = &storage.Int32FieldData{
					NumRows: make([]int64, 0, 1),
					Data:    make([]int32, 0),
				}
			}

			fieldData := idata.Data[field.FieldID].(*storage.Int32FieldData)
			for _, r := range blobReaders {
				var v int32
				readBinary(r, &v, field.DataType)

				fieldData.Data = append(fieldData.Data, v)
			}

			fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))

		case schemapb.DataType_Int64:
			if _, ok := idata.Data[field.FieldID]; !ok {
				idata.Data[field.FieldID] = &storage.Int64FieldData{
					NumRows: make([]int64, 0, 1),
					Data:    make([]int64, 0),
				}
			}

			fieldData := idata.Data[field.FieldID].(*storage.Int64FieldData)
			switch field.FieldID {
			case 0: // rowIDs
				fieldData.Data = append(fieldData.Data, msg.RowIDs...)
				fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))
			case 1: // Timestamps
				for _, ts := range msg.Timestamps {
					fieldData.Data = append(fieldData.Data, int64(ts))
				}
				fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))
			default:
				for _, r := range blobReaders {
					var v int64
					readBinary(r, &v, field.DataType)

					fieldData.Data = append(fieldData.Data, v)
				}

				fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))
			}

		case schemapb.DataType_Float:
			if _, ok := idata.Data[field.FieldID]; !ok {
				idata.Data[field.FieldID] = &storage.FloatFieldData{
					NumRows: make([]int64, 0, 1),
					Data:    make([]float32, 0),
				}
			}

			fieldData := idata.Data[field.FieldID].(*storage.FloatFieldData)

			for _, r := range blobReaders {
				var v float32
				readBinary(r, &v, field.DataType)

				fieldData.Data = append(fieldData.Data, v)
			}
			fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))

		case schemapb.DataType_Double:
			if _, ok := idata.Data[field.FieldID]; !ok {
				idata.Data[field.FieldID] = &storage.DoubleFieldData{
					NumRows: make([]int64, 0, 1),
					Data:    make([]float64, 0),
				}
			}

			fieldData := idata.Data[field.FieldID].(*storage.DoubleFieldData)

			for _, r := range blobReaders {
				var v float64
				readBinary(r, &v, field.DataType)

				fieldData.Data = append(fieldData.Data, v)
			}
			fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))
		}
	}

	// update buffer size
	buffer.updateSize(int64(len(msg.RowData)))

	// store in buffer
	ibNode.insertBuffer.Store(currentSegID, buffer)

	// store current endPositions as Segment->EndPostion
	ibNode.replica.updateSegmentEndPosition(currentSegID, endPos)

	// update segment pk filter
	ibNode.replica.updateSegmentPKRange(currentSegID, msg.GetRowIDs())
	return nil
}

// readBinary read data in bytes and write it into receiver.
//  The receiver can be any type in int8, int16, int32, int64, float32, float64 and bool
//  readBinary uses LittleEndian ByteOrder.
func readBinary(reader io.Reader, receiver interface{}, dataType schemapb.DataType) {
	err := binary.Read(reader, binary.LittleEndian, receiver)
	if err != nil {
		log.Error("binary.Read failed", zap.Any("data type", dataType), zap.Error(err))
	}
}

func flushSegment(
	collMeta *etcdpb.CollectionMeta,
	segID, partitionID, collID UniqueID,
	insertData *sync.Map,
	kv kv.BaseKV,
	flushUnit chan<- segmentFlushUnit,
	wgFinish *sync.WaitGroup,
	ibNode *insertBufferNode,
	idAllocator allocatorInterface) {

	if wgFinish != nil {
		defer wgFinish.Done()
	}

	clearFn := func(isSuccess bool) {
		if !isSuccess {
			flushUnit <- segmentFlushUnit{field2Path: nil}
		}

		log.Debug(".. Clearing flush Buffer ..")
		insertData.Delete(segID)
	}

	inCodec := storage.NewInsertCodec(collMeta)

	// buffer data to binlogs
	data, ok := insertData.Load(segID)
	if !ok {
		log.Error("Flush failed ... cannot load insertData ..")
		clearFn(false)
		return
	}

	binLogs, statsBinlogs, err := inCodec.Serialize(partitionID, segID, data.(*InsertData))
	if err != nil {
		log.Error("Flush failed ... cannot generate binlog ..", zap.Error(err))
		clearFn(false)
		return
	}

	log.Debug(".. Saving binlogs to MinIO ..", zap.Int("number", len(binLogs)))
	field2Path := make(map[UniqueID]string, len(binLogs))
	kvs := make(map[string]string, len(binLogs))
	paths := make([]string, 0, len(binLogs))
	field2Logidx := make(map[UniqueID]UniqueID, len(binLogs))

	// write insert binlog
	for _, blob := range binLogs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		if err != nil {
			log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
			clearFn(false)
			return
		}
		log.Debug("save binlog", zap.Int64("fieldID", fieldID))

		logidx, err := idAllocator.allocID()
		if err != nil {
			log.Error("Flush failed ... cannot alloc ID ..", zap.Error(err))
			clearFn(false)
			return
		}

		// no error raise if alloc=false
		k, _ := idAllocator.genKey(false, collID, partitionID, segID, fieldID, logidx)

		key := path.Join(Params.InsertBinlogRootPath, k)
		paths = append(paths, key)
		kvs[key] = string(blob.Value[:])
		field2Path[fieldID] = key
		field2Logidx[fieldID] = logidx
	}

	// write stats binlog
	for _, blob := range statsBinlogs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		if err != nil {
			log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
			clearFn(false)
			return
		}

		logidx := field2Logidx[fieldID]

		// no error raise if alloc=false
		k, _ := idAllocator.genKey(false, collID, partitionID, segID, fieldID, logidx)

		key := path.Join(Params.StatsBinlogRootPath, k)
		kvs[key] = string(blob.Value[:])
	}
	log.Debug("save binlog file to MinIO/S3")

	err = kv.MultiSave(kvs)
	if err != nil {
		log.Error("Flush failed ... cannot save to MinIO ..", zap.Error(err))
		_ = kv.MultiRemove(paths)
		clearFn(false)
		return
	}

	ibNode.replica.updateSegmentCheckPoint(segID)
	startPos := ibNode.replica.listNewSegmentsStartPositions()
	flushUnit <- segmentFlushUnit{collID: collID, segID: segID, field2Path: field2Path, startPositions: startPos}
	clearFn(true)
}

// writeHardTimeTick writes timetick once insertBufferNode operates.
func (ibNode *insertBufferNode) writeHardTimeTick(ts Timestamp) error {
	msgPack := msgstream.MsgPack{}
	timeTickMsg := msgstream.DataNodeTtMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: ts,
			EndTimestamp:   ts,
			HashValues:     []uint32{0},
		},
		DataNodeTtMsg: datapb.DataNodeTtMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DataNodeTt,
				MsgID:     0,
				Timestamp: ts,
			},
			ChannelName: ibNode.channelName,
			Timestamp:   ts,
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, &timeTickMsg)
	return ibNode.timeTickStream.Produce(&msgPack)
}

// uploadMemStates2Coord uploads latest changed segments statistics in DataNode memory to DataCoord
//  through a msgStream channel.
//
// Currently, the statistics includes segment ID and its total number of rows in memory.
func (ibNode *insertBufferNode) uploadMemStates2Coord(segIDs []UniqueID) error {
	log.Debug("Updating segments statistics...")
	statsUpdates := make([]*internalpb.SegmentStatisticsUpdates, 0, len(segIDs))
	for _, segID := range segIDs {
		updates, err := ibNode.replica.getSegmentStatisticsUpdates(segID)
		if err != nil {
			log.Error("get segment statistics updates wrong", zap.Int64("segmentID", segID), zap.Error(err))
			continue
		}

		log.Debug("Segment Statistics to Update",
			zap.Int64("Segment ID", updates.GetSegmentID()),
			zap.Int64("NumOfRows", updates.GetNumRows()),
		)

		statsUpdates = append(statsUpdates, updates)
	}

	segStats := internalpb.SegmentStatistics{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentStatistics,
			MsgID:     UniqueID(0),  // GOOSE TODO
			Timestamp: Timestamp(0), // GOOSE TODO
			SourceID:  Params.NodeID,
		},
		SegStats: statsUpdates,
	}

	var msg msgstream.TsMsg = &msgstream.SegmentStatisticsMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0}, // GOOSE TODO
		},
		SegmentStatistics: segStats,
	}

	var msgPack = msgstream.MsgPack{
		Msgs: []msgstream.TsMsg{msg},
	}
	return ibNode.segmentStatisticsStream.Produce(&msgPack)
}

func (ibNode *insertBufferNode) getCollMetabySegID(segmentID UniqueID, ts Timestamp) (meta *etcdpb.CollectionMeta, err error) {
	if !ibNode.replica.hasSegment(segmentID, true) {
		return nil, fmt.Errorf("No such segment %d in the replica", segmentID)
	}

	collID := ibNode.replica.getCollectionID()
	sch, err := ibNode.replica.getCollectionSchema(collID, ts)
	if err != nil {
		return nil, err
	}

	meta = &etcdpb.CollectionMeta{
		ID:     collID,
		Schema: sch,
	}
	return
}

func (ibNode *insertBufferNode) getCollectionandPartitionIDbySegID(segmentID UniqueID) (collID, partitionID UniqueID, err error) {
	return ibNode.replica.getCollectionAndPartitionID(segmentID)
}

func newInsertBufferNode(ctx context.Context, flushCh <-chan *flushMsg, saveBinlog func(*segmentFlushUnit) error,
	flushingSegCache *Cache, config *nodeConfig) (*insertBufferNode, error) {

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(config.maxQueueLength)
	baseNode.SetMaxParallelism(config.maxParallelism)

	// MinIO
	option := &miniokv.Option{
		Address:           Params.MinioAddress,
		AccessKeyID:       Params.MinioAccessKeyID,
		SecretAccessKeyID: Params.MinioSecretAccessKey,
		UseSSL:            Params.MinioUseSSL,
		CreateBucket:      true,
		BucketName:        Params.MinioBucketName,
	}

	minIOKV, err := miniokv.NewMinIOKV(ctx, option)
	if err != nil {
		return nil, err
	}

	//input stream, data node time tick
	wTt, err := config.msFactory.NewMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	wTt.AsProducer([]string{Params.TimeTickChannelName})
	log.Debug("datanode AsProducer", zap.String("TimeTickChannelName", Params.TimeTickChannelName))
	var wTtMsgStream msgstream.MsgStream = wTt
	wTtMsgStream.Start()

	// update statistics channel
	segS, err := config.msFactory.NewMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	segS.AsProducer([]string{Params.SegmentStatisticsChannelName})
	log.Debug("datanode AsProducer", zap.String("SegmentStatisChannelName", Params.SegmentStatisticsChannelName))
	var segStatisticsMsgStream msgstream.MsgStream = segS
	segStatisticsMsgStream.Start()

	return &insertBufferNode{
		BaseNode:     baseNode,
		insertBuffer: sync.Map{},
		minIOKV:      minIOKV,

		timeTickStream:          wTtMsgStream,
		segmentStatisticsStream: segStatisticsMsgStream,

		flushMap:         sync.Map{},
		flushChan:        flushCh,
		dsSaveBinlog:     saveBinlog,
		flushingSegCache: flushingSegCache,

		replica:     config.replica,
		idAllocator: config.allocator,
		channelName: config.vChannelName,
	}, nil
}
