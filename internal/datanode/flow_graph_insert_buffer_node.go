package datanode

import (
	"bytes"
	"context"
	"encoding/binary"
	"path"
	"strconv"
	"sync"
	"unsafe"

	"go.uber.org/zap"

	"github.com/opentracing/opentracing-go"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	miniokv "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/storage"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
	"github.com/zilliztech/milvus-distributed/internal/util/trace"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

const (
	CollectionPrefix = "/collection/"
	SegmentPrefix    = "/segment/"
)

type (
	InsertData = storage.InsertData
	Blob       = storage.Blob
)
type insertBufferNode struct {
	BaseNode
	insertBuffer *insertBuffer
	replica      Replica
	flushMeta    *binlogMeta
	flushMap     sync.Map

	minIOKV kv.Base

	timeTickStream          msgstream.MsgStream
	segmentStatisticsStream msgstream.MsgStream
	completeFlushStream     msgstream.MsgStream
}

type insertBuffer struct {
	insertData map[UniqueID]*InsertData // SegmentID to InsertData
	maxSize    int32
}

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

func (ibNode *insertBufferNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {

	if len(in) != 1 {
		log.Error("Invalid operate message input in insertBufferNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	iMsg, ok := in[0].(*insertMsg)
	if !ok {
		log.Error("type assertion failed for insertMsg")
		// TODO: add error handling
	}

	if iMsg == nil {
		return []Msg{}
	}

	var spans []opentracing.Span
	for _, msg := range iMsg.insertMessages {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	// Updating segment statistics
	uniqueSeg := make(map[UniqueID]int64)
	for _, msg := range iMsg.insertMessages {

		currentSegID := msg.GetSegmentID()
		collID := msg.GetCollectionID()
		partitionID := msg.GetPartitionID()

		if !ibNode.replica.hasSegment(currentSegID) {
			err := ibNode.replica.addSegment(currentSegID, collID, partitionID, msg.GetChannelID())
			if err != nil {
				log.Error("add segment wrong", zap.Error(err))
			}

			switch {
			case iMsg.startPositions == nil || len(iMsg.startPositions) <= 0:
				log.Error("insert Msg StartPosition empty")
			default:
				segment, err := ibNode.replica.getSegmentByID(currentSegID)
				if err != nil {
					log.Error("get segment wrong", zap.Error(err))
				}
				var startPosition *internalpb.MsgPosition = nil
				for _, pos := range iMsg.startPositions {
					if pos.ChannelName == segment.channelName {
						startPosition = pos
						break
					}
				}
				if startPosition == nil {
					log.Error("get position wrong", zap.Error(err))
				} else {
					ibNode.replica.setStartPosition(currentSegID, startPosition)
				}
			}
		}

		segNum := uniqueSeg[currentSegID]
		uniqueSeg[currentSegID] = segNum + int64(len(msg.RowIDs))
	}

	segIDs := make([]UniqueID, 0, len(uniqueSeg))
	for id, num := range uniqueSeg {
		segIDs = append(segIDs, id)

		err := ibNode.replica.updateStatistics(id, num)
		if err != nil {
			log.Error("update Segment Row number wrong", zap.Error(err))
		}
	}

	if len(segIDs) > 0 {
		err := ibNode.updateSegStatistics(segIDs)
		if err != nil {
			log.Error("update segment statistics error", zap.Error(err))
		}
	}

	// iMsg is insertMsg
	// 1. iMsg -> buffer
	for _, msg := range iMsg.insertMessages {
		if len(msg.RowIDs) != len(msg.Timestamps) || len(msg.RowIDs) != len(msg.RowData) {
			log.Error("misaligned messages detected")
			continue
		}
		currentSegID := msg.GetSegmentID()
		collectionID := msg.GetCollectionID()

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
			log.Error("Get meta wrong:", zap.Error(err))
			continue
		}

		collSchema := collection.schema
		// 1.2 Get Fields
		var pos int = 0 // Record position of blob
		for _, field := range collSchema.Fields {
			switch field.DataType {
			case schemapb.DataType_FloatVector:
				var dim int
				for _, t := range field.TypeParams {
					if t.Key == "dim" {
						dim, err = strconv.Atoi(t.Value)
						if err != nil {
							log.Error("strconv wrong")
						}
						break
					}
				}
				if dim <= 0 {
					log.Error("invalid dim")
					continue
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
							log.Error("binary.read float32 wrong", zap.Error(err))
						}
						fieldData.Data = append(fieldData.Data, v)
						offset += int(unsafe.Sizeof(*(&v)))
					}
				}
				pos += offset
				fieldData.NumRows += len(msg.RowIDs)

			case schemapb.DataType_BinaryVector:
				var dim int
				for _, t := range field.TypeParams {
					if t.Key == "dim" {
						dim, err = strconv.Atoi(t.Value)
						if err != nil {
							log.Error("strconv wrong")
						}
						break
					}
				}
				if dim <= 0 {
					log.Error("invalid dim")
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
					bv := blob.GetValue()[pos : pos+(dim/8)]
					fieldData.Data = append(fieldData.Data, bv...)
					offset = len(bv)
				}
				pos += offset
				fieldData.NumRows += len(msg.RowData)

			case schemapb.DataType_Bool:
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
						log.Error("binary.Read bool wrong", zap.Error(err))
					}
					fieldData.Data = append(fieldData.Data, v)

				}
				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows += len(msg.RowIDs)

			case schemapb.DataType_Int8:
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
						log.Error("binary.Read int8 wrong", zap.Error(err))
					}
					fieldData.Data = append(fieldData.Data, v)
				}
				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows += len(msg.RowIDs)

			case schemapb.DataType_Int16:
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
						log.Error("binary.Read int16 wrong", zap.Error(err))
					}
					fieldData.Data = append(fieldData.Data, v)
				}
				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows += len(msg.RowIDs)

			case schemapb.DataType_Int32:
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
						log.Error("binary.Read int32 wrong", zap.Error(err))
					}
					fieldData.Data = append(fieldData.Data, v)
				}
				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows += len(msg.RowIDs)

			case schemapb.DataType_Int64:
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
							log.Error("binary.Read int64 wrong", zap.Error(err))
						}
						fieldData.Data = append(fieldData.Data, v)
					}
					pos += int(unsafe.Sizeof(*(&v)))
					fieldData.NumRows += len(msg.RowIDs)
				}

			case schemapb.DataType_Float:
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
						log.Error("binary.Read float32 wrong", zap.Error(err))
					}
					fieldData.Data = append(fieldData.Data, v)
				}
				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows += len(msg.RowIDs)

			case schemapb.DataType_Double:
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
						log.Error("binary.Read float64 wrong", zap.Error(err))
					}
					fieldData.Data = append(fieldData.Data, v)
				}

				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows += len(msg.RowIDs)
			}
		}

		// 1.3 store in buffer
		ibNode.insertBuffer.insertData[currentSegID] = idata

		switch {
		case iMsg.endPositions == nil || len(iMsg.endPositions) <= 0:
			log.Error("insert Msg EndPosition empty")
		default:
			segment, err := ibNode.replica.getSegmentByID(currentSegID)
			if err != nil {
				log.Error("get segment wrong", zap.Error(err))
			}
			var endPosition *internalpb.MsgPosition = nil
			for _, pos := range iMsg.endPositions {
				if pos.ChannelName == segment.channelName {
					endPosition = pos
				}
			}
			if endPosition == nil {
				log.Error("get position wrong", zap.Error(err))
			}
			ibNode.replica.setEndPosition(currentSegID, endPosition)
		}

		// 1.4 if full, auto flush
		if ibNode.insertBuffer.full(currentSegID) {
			log.Debug(". Insert Buffer full, auto flushing ",
				zap.Int32("num of rows", ibNode.insertBuffer.size(currentSegID)))

			collSch, err := ibNode.getCollectionSchemaByID(collection.GetID())
			if err != nil {
				log.Error("Auto flush failed .. cannot get collection schema ..", zap.Error(err))
				continue
			}
			collMeta := &etcdpb.CollectionMeta{
				Schema: collSch,
				ID:     collection.GetID(),
			}

			ibNode.flushMap.Store(currentSegID, ibNode.insertBuffer.insertData[currentSegID])
			delete(ibNode.insertBuffer.insertData, currentSegID)

			finishCh := make(chan bool)
			go flushSegmentTxn(collMeta, currentSegID, msg.GetPartitionID(), collection.GetID(),
				&ibNode.flushMap, ibNode.flushMeta, ibNode.minIOKV,
				finishCh)

			go func(finishCh <-chan bool) {
				if finished := <-finishCh; !finished {
					log.Debug(".. Auto Flush failed ..")
					return
				}
				log.Debug(".. Auto Flush completed ..")
			}(finishCh)
		}
	}

	if len(iMsg.insertMessages) > 0 {
		log.Debug("---insert buffer status---")
		var stopSign int = 0
		for k := range ibNode.insertBuffer.insertData {
			if stopSign >= 10 {
				log.Debug("......")
				break
			}
			log.Debug("seg buffer status", zap.Int64("segmentID", k), zap.Int32("buffer size", ibNode.insertBuffer.size(k)))
			stopSign++
		}
	}

	// iMsg is Flush() msg from dataservice
	//   1. insertBuffer(not empty) -> binLogs -> minIO/S3
	for _, msg := range iMsg.flushMessages {
		for _, currentSegID := range msg.segmentIDs {
			log.Debug(". Receiving flush message", zap.Int64("segmentID", currentSegID))

			finishCh := make(chan bool)
			go ibNode.completeFlush(currentSegID, finishCh)

			if ibNode.insertBuffer.size(currentSegID) <= 0 {
				log.Debug(".. Buffer empty ...")
				finishCh <- true
				continue
			}

			log.Debug(".. Buffer not empty, flushing ..")
			ibNode.flushMap.Store(currentSegID, ibNode.insertBuffer.insertData[currentSegID])
			delete(ibNode.insertBuffer.insertData, currentSegID)
			clearFn := func() {
				finishCh <- false
				log.Debug(".. Clearing flush Buffer ..")
				ibNode.flushMap.Delete(currentSegID)
			}

			seg, err := ibNode.replica.getSegmentByID(currentSegID)
			if err != nil {
				log.Error("Flush failed .. cannot get segment ..", zap.Error(err))
				clearFn()
				continue
			}

			collSch, err := ibNode.getCollectionSchemaByID(seg.collectionID)
			if err != nil {
				log.Error("Flush failed .. cannot get collection schema ..", zap.Error(err))
				clearFn()
				continue
			}

			collMeta := &etcdpb.CollectionMeta{
				Schema: collSch,
				ID:     seg.collectionID,
			}

			go flushSegmentTxn(collMeta, currentSegID, seg.partitionID, seg.collectionID,
				&ibNode.flushMap, ibNode.flushMeta, ibNode.minIOKV, finishCh)
		}
	}

	if err := ibNode.writeHardTimeTick(iMsg.timeRange.timestampMax); err != nil {
		log.Error("send hard time tick into pulsar channel failed", zap.Error(err))
	}

	var res Msg = &gcMsg{
		gcRecord:  iMsg.gcRecord,
		timeRange: iMsg.timeRange,
	}
	for _, sp := range spans {
		sp.Finish()
	}

	return []Msg{res}
}

func flushSegmentTxn(collMeta *etcdpb.CollectionMeta, segID UniqueID, partitionID UniqueID, collID UniqueID,
	insertData *sync.Map, meta *binlogMeta, kv kv.Base, finishCh chan<- bool) {

	clearFn := func(isSuccess bool) {
		finishCh <- isSuccess
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

	binLogs, err := inCodec.Serialize(partitionID, segID, data.(*InsertData))
	if err != nil {
		log.Error("Flush failed ... cannot generate binlog ..", zap.Error(err))
		clearFn(false)
		return
	}

	log.Debug(".. Saving binlogs to MinIO ..", zap.Int("number", len(binLogs)))
	field2Path := make(map[UniqueID]string, len(binLogs))
	kvs := make(map[string]string, len(binLogs))
	paths := make([]string, 0, len(binLogs))
	for _, blob := range binLogs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		if err != nil {
			log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
			clearFn(false)
			return
		}

		k, err := meta.genKey(true, collID, partitionID, segID, fieldID)
		if err != nil {
			log.Error("Flush failed ... cannot alloc ID ..", zap.Error(err))
			clearFn(false)
			return
		}

		key := path.Join(Params.InsertBinlogRootPath, k)
		paths = append(paths, key)
		kvs[key] = string(blob.Value[:])
		field2Path[fieldID] = key
	}

	err = kv.MultiSave(kvs)
	if err != nil {
		log.Error("Flush failed ... cannot save to MinIO ..", zap.Error(err))
		_ = kv.MultiRemove(paths)
		clearFn(false)
		return
	}

	log.Debug(".. Saving binlog paths to etcd ..", zap.Int("number", len(binLogs)))
	err = meta.SaveSegmentBinlogMetaTxn(segID, field2Path)
	if err != nil {
		log.Error("Flush failed ... cannot save binlog paths ..", zap.Error(err))
		_ = kv.MultiRemove(paths)
		clearFn(false)
		return
	}

	clearFn(true)
}

func (ibNode *insertBufferNode) completeFlush(segID UniqueID, finishCh <-chan bool) {
	if finished := <-finishCh; !finished {
		return
	}

	log.Debug(".. Segment flush completed ..")
	ibNode.replica.setIsFlushed(segID)
	ibNode.updateSegStatistics([]UniqueID{segID})

	msgPack := msgstream.MsgPack{}
	completeFlushMsg := internalpb.SegmentFlushCompletedMsg{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentFlushDone,
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
	err := ibNode.completeFlushStream.Produce(&msgPack)
	if err != nil {
		log.Error(".. Produce complete flush msg failed ..", zap.Error(err))
	}
}

func (ibNode *insertBufferNode) writeHardTimeTick(ts Timestamp) error {
	msgPack := msgstream.MsgPack{}
	timeTickMsg := msgstream.TimeTickMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: ts,
			EndTimestamp:   ts,
			HashValues:     []uint32{0},
		},
		TimeTickMsg: internalpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_TimeTick,
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
	log.Debug("Updating segments statistics...")
	statsUpdates := make([]*internalpb.SegmentStatisticsUpdates, 0, len(segIDs))
	for _, segID := range segIDs {
		updates, err := ibNode.replica.getSegmentStatisticsUpdates(segID)
		if err != nil {
			log.Error("get segment statistics updates wrong", zap.Int64("segmentID", segID), zap.Error(err))
			continue
		}
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

func (ibNode *insertBufferNode) getCollectionSchemaByID(collectionID UniqueID) (*schemapb.CollectionSchema, error) {
	ret, err := ibNode.replica.getCollectionByID(collectionID)
	if err != nil {
		return nil, err
	}
	return ret.schema, nil
}

func newInsertBufferNode(ctx context.Context, flushMeta *binlogMeta,
	replica Replica, factory msgstream.Factory) *insertBufferNode {
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

	//input stream, data node time tick
	wTt, _ := factory.NewMsgStream(ctx)
	wTt.AsProducer([]string{Params.TimeTickChannelName})
	log.Debug("datanode AsProducer: " + Params.TimeTickChannelName)
	var wTtMsgStream msgstream.MsgStream = wTt
	wTtMsgStream.Start()

	// update statistics channel
	segS, _ := factory.NewMsgStream(ctx)
	segS.AsProducer([]string{Params.SegmentStatisticsChannelName})
	log.Debug("datanode AsProducer: " + Params.SegmentStatisticsChannelName)
	var segStatisticsMsgStream msgstream.MsgStream = segS
	segStatisticsMsgStream.Start()

	// segment flush completed channel
	cf, _ := factory.NewMsgStream(ctx)
	cf.AsProducer([]string{Params.CompleteFlushChannelName})
	log.Debug("datanode AsProducer: " + Params.CompleteFlushChannelName)
	var completeFlushStream msgstream.MsgStream = cf
	completeFlushStream.Start()

	return &insertBufferNode{
		BaseNode:                baseNode,
		insertBuffer:            iBuffer,
		minIOKV:                 minIOKV,
		timeTickStream:          wTtMsgStream,
		segmentStatisticsStream: segStatisticsMsgStream,
		completeFlushStream:     completeFlushStream,
		replica:                 replica,
		flushMeta:               flushMeta,
		flushMap:                sync.Map{},
	}
}
