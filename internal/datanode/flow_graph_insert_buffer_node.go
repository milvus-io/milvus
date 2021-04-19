package datanode

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"path"
	"strconv"
	"unsafe"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/opentracing/opentracing-go"
	oplog "github.com/opentracing/opentracing-go/log"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	miniokv "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
)

const (
	CollectionPrefix = "/collection/"
	SegmentPrefix    = "/segment/"
)

type (
	InsertData = storage.InsertData
	Blob       = storage.Blob

	insertBufferNode struct {
		BaseNode
		insertBuffer *insertBuffer
		replica      collectionReplica
		flushMeta    *metaTable

		minIOKV     kv.Base
		minioPrefix string

		idAllocator allocator

		timeTickStream          msgstream.MsgStream
		segmentStatisticsStream msgstream.MsgStream
		completeFlushStream     msgstream.MsgStream
	}

	insertBuffer struct {
		insertData map[UniqueID]*InsertData // SegmentID to InsertData
		maxSize    int32
	}
)

func (ib *insertBuffer) size(segmentID UniqueID) int32 {
	if ib.insertData == nil || len(ib.insertData) <= 0 {
		return 0
	}
	idata, ok := ib.insertData[segmentID]
	if !ok {
		return 0
	}

	var maxSize int32 = 0
	for _, data := range idata.Data {
		fdata, ok := data.(*storage.FloatVectorFieldData)
		if ok && int32(fdata.NumRows) > maxSize {
			maxSize = int32(fdata.NumRows)
		}

		bdata, ok := data.(*storage.BinaryVectorFieldData)
		if ok && int32(bdata.NumRows) > maxSize {
			maxSize = int32(bdata.NumRows)
		}

	}
	return maxSize
}

func (ib *insertBuffer) full(segmentID UniqueID) bool {
	return ib.size(segmentID) >= ib.maxSize
}

func (ibNode *insertBufferNode) Name() string {
	return "ibNode"
}

func (ibNode *insertBufferNode) Operate(in []*Msg) []*Msg {
	// log.Println("=========== insert buffer Node Operating")

	if len(in) != 1 {
		log.Println("Error: Invalid operate message input in insertBuffertNode, input length = ", len(in))
		// TODO: add error handling
	}

	iMsg, ok := (*in[0]).(*insertMsg)
	if !ok {
		log.Println("Error: type assertion failed for insertMsg")
		// TODO: add error handling
	}

	// Updating segment statistics
	uniqueSeg := make(map[UniqueID]bool)
	for _, msg := range iMsg.insertMessages {
		currentSegID := msg.GetSegmentID()
		collID := msg.GetCollectionID()
		partitionID := msg.GetPartitionID()

		if !ibNode.replica.hasSegment(currentSegID) {
			err := ibNode.replica.addSegment(currentSegID, collID, partitionID, iMsg.startPositions)
			if err != nil {
				log.Println("Error: add segment error", err)
			}
		}

		if !ibNode.flushMeta.hasSegmentFlush(currentSegID) {
			err := ibNode.flushMeta.addSegmentFlush(currentSegID)
			if err != nil {
				log.Println("Error: add segment flush meta error", err)
			}
		}

		err := ibNode.replica.updateStatistics(currentSegID, int64(len(msg.RowIDs)))
		if err != nil {
			log.Println("Error: update Segment Row number wrong, ", err)
		}

		if _, ok := uniqueSeg[currentSegID]; !ok {
			uniqueSeg[currentSegID] = true
		}
	}
	segIDs := make([]UniqueID, 0, len(uniqueSeg))
	for id := range uniqueSeg {
		segIDs = append(segIDs, id)
	}
	err := ibNode.updateSegStatistics(segIDs)
	if err != nil {
		log.Println("Error: update segment statistics error, ", err)
	}

	// iMsg is insertMsg
	// 1. iMsg -> buffer
	for _, msg := range iMsg.insertMessages {
		ctx := msg.GetMsgContext()
		var span opentracing.Span
		if ctx != nil {
			span, _ = opentracing.StartSpanFromContext(ctx, fmt.Sprintf("insert buffer node, start time = %d", msg.BeginTs()))
		} else {
			span = opentracing.StartSpan(fmt.Sprintf("insert buffer node, start time = %d", msg.BeginTs()))
		}
		span.SetTag("hash keys", msg.HashKeys())
		span.SetTag("start time", msg.BeginTs())
		span.SetTag("end time", msg.EndTs())
		if len(msg.RowIDs) != len(msg.Timestamps) || len(msg.RowIDs) != len(msg.RowData) {
			log.Println("Error: misaligned messages detected")
			continue
		}
		currentSegID := msg.GetSegmentID()
		collectionID := msg.GetCollectionID()
		span.LogFields(oplog.Int("segment id", int(currentSegID)))

		idata, ok := ibNode.insertBuffer.insertData[currentSegID]
		if !ok {
			idata = &InsertData{
				Data: make(map[UniqueID]storage.FieldData),
			}
		}

		// 1.1 Get CollectionMeta
		collection, err := ibNode.replica.getCollectionByID(collectionID)
		if err != nil {
			// GOOSE TODO add error handler
			log.Println("bbb, Get meta wrong:", err)
			continue
		}

		collSchema := collection.schema
		// 1.2 Get Fields
		var pos int = 0 // Record position of blob
		for _, field := range collSchema.Fields {
			switch field.DataType {
			case schemapb.DataType_VECTOR_FLOAT:
				var dim int
				for _, t := range field.TypeParams {
					if t.Key == "dim" {
						dim, err = strconv.Atoi(t.Value)
						if err != nil {
							log.Println("strconv wrong")
						}
						break
					}
				}
				if dim <= 0 {
					log.Println("invalid dim")
					// TODO: add error handling
				}

				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.FloatVectorFieldData{
						NumRows: 0,
						Data:    make([]float32, 0),
						Dim:     dim,
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.FloatVectorFieldData)

				var offset int
				for _, blob := range msg.RowData {
					offset = 0
					for j := 0; j < dim; j++ {
						var v float32
						buf := bytes.NewBuffer(blob.GetValue()[pos+offset:])
						if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
							log.Println("binary.read float32 err:", err)
						}
						fieldData.Data = append(fieldData.Data, v)
						offset += int(unsafe.Sizeof(*(&v)))
					}
				}
				pos += offset
				fieldData.NumRows += len(msg.RowIDs)

			case schemapb.DataType_VECTOR_BINARY:
				var dim int
				for _, t := range field.TypeParams {
					if t.Key == "dim" {
						dim, err = strconv.Atoi(t.Value)
						if err != nil {
							log.Println("strconv wrong")
						}
						break
					}
				}
				if dim <= 0 {
					log.Println("invalid dim")
					// TODO: add error handling
				}

				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.BinaryVectorFieldData{
						NumRows: 0,
						Data:    make([]byte, 0),
						Dim:     dim,
					}
				}
				fieldData := idata.Data[field.FieldID].(*storage.BinaryVectorFieldData)

				var offset int
				for _, blob := range msg.RowData {
					bv := blob.GetValue()[pos+offset : pos+(dim/8)]
					fieldData.Data = append(fieldData.Data, bv...)
					offset = len(bv)
				}
				pos += offset
				fieldData.NumRows += len(msg.RowData)

			case schemapb.DataType_BOOL:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.BoolFieldData{
						NumRows: 0,
						Data:    make([]bool, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.BoolFieldData)
				var v bool
				for _, blob := range msg.RowData {
					buf := bytes.NewReader(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Println("binary.Read bool failed:", err)
					}
					fieldData.Data = append(fieldData.Data, v)

				}
				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows += len(msg.RowIDs)

			case schemapb.DataType_INT8:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.Int8FieldData{
						NumRows: 0,
						Data:    make([]int8, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.Int8FieldData)
				var v int8
				for _, blob := range msg.RowData {
					buf := bytes.NewReader(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Println("binary.Read int8 failed:", err)
					}
					fieldData.Data = append(fieldData.Data, v)
				}
				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows += len(msg.RowIDs)

			case schemapb.DataType_INT16:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.Int16FieldData{
						NumRows: 0,
						Data:    make([]int16, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.Int16FieldData)
				var v int16
				for _, blob := range msg.RowData {
					buf := bytes.NewReader(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Println("binary.Read int16 failed:", err)
					}
					fieldData.Data = append(fieldData.Data, v)
				}
				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows += len(msg.RowIDs)

			case schemapb.DataType_INT32:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.Int32FieldData{
						NumRows: 0,
						Data:    make([]int32, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.Int32FieldData)
				var v int32
				for _, blob := range msg.RowData {
					buf := bytes.NewReader(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Println("binary.Read int32 failed:", err)
					}
					fieldData.Data = append(fieldData.Data, v)
				}
				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows += len(msg.RowIDs)

			case schemapb.DataType_INT64:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.Int64FieldData{
						NumRows: 0,
						Data:    make([]int64, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.Int64FieldData)
				switch field.FieldID {
				case 0: // rowIDs
					fieldData.Data = append(fieldData.Data, msg.RowIDs...)
					fieldData.NumRows += len(msg.RowIDs)
				case 1: // Timestamps
					for _, ts := range msg.Timestamps {
						fieldData.Data = append(fieldData.Data, int64(ts))
					}
					fieldData.NumRows += len(msg.Timestamps)
				default:
					var v int64
					for _, blob := range msg.RowData {
						buf := bytes.NewBuffer(blob.GetValue()[pos:])
						if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
							log.Println("binary.Read int64 failed:", err)
						}
						fieldData.Data = append(fieldData.Data, v)
					}
					pos += int(unsafe.Sizeof(*(&v)))
					fieldData.NumRows += len(msg.RowIDs)
				}

			case schemapb.DataType_FLOAT:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.FloatFieldData{
						NumRows: 0,
						Data:    make([]float32, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.FloatFieldData)
				var v float32
				for _, blob := range msg.RowData {
					buf := bytes.NewBuffer(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Println("binary.Read float32 failed:", err)
					}
					fieldData.Data = append(fieldData.Data, v)
				}
				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows += len(msg.RowIDs)

			case schemapb.DataType_DOUBLE:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.DoubleFieldData{
						NumRows: 0,
						Data:    make([]float64, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.DoubleFieldData)
				var v float64
				for _, blob := range msg.RowData {
					buf := bytes.NewBuffer(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Println("binary.Read float64 failed:", err)
					}
					fieldData.Data = append(fieldData.Data, v)
				}

				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows += len(msg.RowIDs)
			}
		}

		// 1.3 store in buffer
		ibNode.insertBuffer.insertData[currentSegID] = idata
		span.LogFields(oplog.String("store in buffer", "store in buffer"))

		// 1.4 if full
		//   1.4.1 generate binlogs
		span.LogFields(oplog.String("generate binlogs", "generate binlogs"))
		if ibNode.insertBuffer.full(currentSegID) {
			log.Printf(". Insert Buffer full, auto flushing (%v) rows of data...", ibNode.insertBuffer.size(currentSegID))

			err = ibNode.flushSegment(currentSegID, msg.GetPartitionID(), collection.ID())
			if err != nil {
				log.Printf("flush segment (%v) fail: %v", currentSegID, err)
			}
		}
	}

	if len(iMsg.insertMessages) > 0 {
		log.Println("---insert buffer status---")
		var stopSign int = 0
		for k := range ibNode.insertBuffer.insertData {
			if stopSign >= 10 {
				log.Printf("......")
				break
			}
			log.Printf("seg(%v) buffer size = (%v)", k, ibNode.insertBuffer.size(k))
			stopSign++
		}
	}

	// iMsg is Flush() msg from dataservice
	//   1. insertBuffer(not empty) -> binLogs -> minIO/S3
	for _, msg := range iMsg.flushMessages {
		for _, currentSegID := range msg.segmentIDs {
			log.Printf(". Receiving flush message segID(%v)...", currentSegID)
			if ibNode.insertBuffer.size(currentSegID) > 0 {
				log.Println(".. Buffer not empty, flushing ...")
				seg, err := ibNode.replica.getSegmentByID(currentSegID)
				if err != nil {
					log.Printf("flush segment fail: %v", err)
					continue
				}

				err = ibNode.flushSegment(currentSegID, seg.partitionID, seg.collectionID)
				if err != nil {
					log.Printf("flush segment (%v) fail: %v", currentSegID, err)
					continue
				}
			}
		}
	}

	if err := ibNode.writeHardTimeTick(iMsg.timeRange.timestampMax); err != nil {
		log.Printf("Error: send hard time tick into pulsar channel failed, %s\n", err.Error())
	}

	var res Msg = &gcMsg{
		gcRecord:  iMsg.gcRecord,
		timeRange: iMsg.timeRange,
	}

	return []*Msg{&res}
}

func (ibNode *insertBufferNode) flushSegment(segID UniqueID, partitionID UniqueID, collID UniqueID) error {

	collSch, err := ibNode.getCollectionSchemaByID(collID)
	if err != nil {
		return errors.Errorf("Get collection by ID wrong, %v", err)
	}

	collMeta := &etcdpb.CollectionMeta{
		Schema: collSch,
		ID:     collID,
	}

	inCodec := storage.NewInsertCodec(collMeta)

	// buffer data to binlogs
	binLogs, err := inCodec.Serialize(partitionID,
		segID, ibNode.insertBuffer.insertData[segID])

	if err != nil {
		return errors.Errorf("generate binlog wrong: %v", err)
	}

	// clear buffer
	delete(ibNode.insertBuffer.insertData, segID)
	log.Println(".. Clearing buffer")

	//   1.5.2 binLogs -> minIO/S3
	collIDStr := strconv.FormatInt(collID, 10)
	partitionIDStr := strconv.FormatInt(partitionID, 10)
	segIDStr := strconv.FormatInt(segID, 10)
	keyPrefix := path.Join(ibNode.minioPrefix, collIDStr, partitionIDStr, segIDStr)

	log.Printf(".. Saving (%v) binlogs to MinIO ...", len(binLogs))
	for index, blob := range binLogs {
		uid, err := ibNode.idAllocator.allocID()
		if err != nil {
			return errors.Errorf("Allocate Id failed, %v", err)
		}

		key := path.Join(keyPrefix, blob.Key, strconv.FormatInt(uid, 10))
		err = ibNode.minIOKV.Save(key, string(blob.Value[:]))
		if err != nil {
			return errors.Errorf("Save to MinIO failed, %v", err)
		}

		fieldID, err := strconv.ParseInt(blob.Key, 10, 32)
		if err != nil {
			return errors.Errorf("string to fieldID wrong, %v", err)
		}

		log.Println("... Appending binlog paths ...", index)
		ibNode.flushMeta.AppendSegBinlogPaths(segID, fieldID, []string{key})
	}
	return nil
}

func (ibNode *insertBufferNode) completeFlush(segID UniqueID) error {
	msgPack := msgstream.MsgPack{}
	completeFlushMsg := internalpb2.SegmentFlushCompletedMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kSegmentFlushDone,
			MsgID:     0, // GOOSE TODO
			Timestamp: 0, // GOOSE TODO
			SourceID:  Params.NodeID,
		},
		SegmentID: segID,
	}
	var msg msgstream.TsMsg = &msgstream.FlushCompletedMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		SegmentFlushCompletedMsg: completeFlushMsg,
	}

	msgPack.Msgs = append(msgPack.Msgs, msg)
	return ibNode.timeTickStream.Produce(&msgPack)

}

func (ibNode *insertBufferNode) writeHardTimeTick(ts Timestamp) error {
	msgPack := msgstream.MsgPack{}
	timeTickMsg := msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: ts,
			EndTimestamp:   ts,
			HashValues:     []uint32{0},
		},
		TimeTickMsg: internalpb2.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kTimeTick,
				MsgID:     0,  // GOOSE TODO
				Timestamp: ts, // GOOSE TODO
				SourceID:  Params.NodeID,
			},
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, &timeTickMsg)
	return ibNode.timeTickStream.Produce(&msgPack)
}

func (ibNode *insertBufferNode) updateSegStatistics(segIDs []UniqueID) error {
	log.Println("Updating segments statistics...")
	statsUpdates := make([]*internalpb2.SegmentStatisticsUpdates, 0, len(segIDs))
	for _, segID := range segIDs {
		updates, err := ibNode.replica.getSegmentStatisticsUpdates(segID)
		if err != nil {
			log.Println("Error get segment", segID, "statistics updates", err)
			continue
		}
		statsUpdates = append(statsUpdates, updates)
	}

	segStats := internalpb2.SegmentStatistics{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kSegmentStatistics,
			MsgID:     UniqueID(0),  // GOOSE TODO
			Timestamp: Timestamp(0), // GOOSE TODO
			SourceID:  Params.NodeID,
		},
		SegStats: statsUpdates,
	}

	var msg msgstream.TsMsg = &msgstream.SegmentStatisticsMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		SegmentStatistics: segStats,
	}

	var msgPack = msgstream.MsgPack{
		Msgs: []msgstream.TsMsg{msg},
	}
	return ibNode.segmentStatisticsStream.Produce(&msgPack)
}

func (ibNode *insertBufferNode) getCollectionSchemaByID(collectionID UniqueID) (*schemapb.CollectionSchema, error) {
	ret, err := ibNode.replica.getCollectionByID(collectionID)
	if err != nil {
		return nil, err
	}
	return ret.schema, nil
}

func newInsertBufferNode(ctx context.Context, flushMeta *metaTable,
	replica collectionReplica, alloc allocator) *insertBufferNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	maxSize := Params.FlushInsertBufferSize
	iBuffer := &insertBuffer{
		insertData: make(map[UniqueID]*InsertData),
		maxSize:    maxSize,
	}

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
		panic(err)
	}
	minioPrefix := Params.InsertBinlogRootPath

	//input stream, data node time tick
	wTt := pulsarms.NewPulsarMsgStream(ctx, 1024)
	wTt.SetPulsarClient(Params.PulsarAddress)
	wTt.CreatePulsarProducers([]string{Params.TimeTickChannelName})
	var wTtMsgStream msgstream.MsgStream = wTt
	wTtMsgStream.Start()

	// update statistics channel
	segS := pulsarms.NewPulsarMsgStream(ctx, 1024)
	segS.SetPulsarClient(Params.PulsarAddress)
	segS.CreatePulsarProducers([]string{Params.SegmentStatisticsChannelName})
	var segStatisticsMsgStream msgstream.MsgStream = segS
	segStatisticsMsgStream.Start()

	// segment flush completed channel
	cf := pulsarms.NewPulsarMsgStream(ctx, 1024)
	cf.SetPulsarClient(Params.PulsarAddress)
	cf.CreatePulsarProducers([]string{Params.CompleteFlushChannelName})
	var completeFlushStream msgstream.MsgStream = cf
	completeFlushStream.Start()

	return &insertBufferNode{
		BaseNode:                baseNode,
		insertBuffer:            iBuffer,
		minIOKV:                 minIOKV,
		minioPrefix:             minioPrefix,
		idAllocator:             alloc,
		timeTickStream:          wTtMsgStream,
		segmentStatisticsStream: segStatisticsMsgStream,
		completeFlushStream:     completeFlushStream,
		replica:                 replica,
		flushMeta:               flushMeta,
	}
}
