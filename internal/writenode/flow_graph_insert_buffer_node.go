package writenode

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"
	"path"
	"strconv"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	miniokv "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"go.etcd.io/etcd/clientv3"
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
		kvClient                      *etcdkv.EtcdKV
		insertBuffer                  *insertBuffer
		minIOKV                       kv.Base
		minioPrifex                   string
		idAllocator                   *allocator.IDAllocator
		outCh                         chan *insertFlushSyncMsg
		pulsarWriteNodeTimeTickStream *msgstream.PulsarMsgStream
	}

	insertBuffer struct {
		insertData map[UniqueID]*InsertData // SegmentID to InsertData
		maxSize    int
	}
)

func (ib *insertBuffer) size(segmentID UniqueID) int {
	if ib.insertData == nil || len(ib.insertData) <= 0 {
		return 0
	}
	idata, ok := ib.insertData[segmentID]
	if !ok {
		return 0
	}

	maxSize := 0
	for _, data := range idata.Data {
		fdata, ok := data.(*storage.FloatVectorFieldData)
		if ok && fdata.NumRows > maxSize {
			maxSize = fdata.NumRows
		}

		bdata, ok := data.(*storage.BinaryVectorFieldData)
		if ok && bdata.NumRows > maxSize {
			maxSize = bdata.NumRows
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

	// iMsg is insertMsg
	// 1. iMsg -> buffer
	for _, msg := range iMsg.insertMessages {
		if len(msg.RowIDs) != len(msg.Timestamps) || len(msg.RowIDs) != len(msg.RowData) {
			log.Println("Error: misaligned messages detected")
			continue
		}
		currentSegID := msg.GetSegmentID()

		idata, ok := ibNode.insertBuffer.insertData[currentSegID]
		if !ok {
			idata = &InsertData{
				Data: make(map[UniqueID]storage.FieldData),
			}
		}

		// 1.1 Get CollectionMeta from etcd
		segMeta, collMeta, err := ibNode.getMeta(currentSegID)
		if err != nil {
			// GOOSE TODO add error handler
			log.Println("Get meta wrong:", err)
			continue
		}

		// 1.2 Get Fields
		var pos int = 0 // Record position of blob
		for _, field := range collMeta.Schema.Fields {
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

				for _, blob := range msg.RowData {
					for j := 0; j < dim; j++ {
						var v float32
						buf := bytes.NewBuffer(blob.GetValue()[pos:])
						if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
							log.Println("binary.read float32 err:", err)
						}
						fieldData.Data = append(fieldData.Data, v)
						pos += int(unsafe.Sizeof(*(&v)))
					}
				}

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

				for _, blob := range msg.RowData {
					bv := blob.GetValue()[pos : pos+(dim/8)]
					fieldData.Data = append(fieldData.Data, bv...)
					pos += len(bv)
				}

				fieldData.NumRows += len(msg.RowData)
			case schemapb.DataType_BOOL:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.BoolFieldData{
						NumRows: 0,
						Data:    make([]bool, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.BoolFieldData)
				for _, blob := range msg.RowData {
					var v bool
					buf := bytes.NewReader(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Println("binary.Read bool failed:", err)
					}
					fieldData.Data = append(fieldData.Data, v)
					pos += int(unsafe.Sizeof(*(&v)))
				}

				fieldData.NumRows += len(msg.RowIDs)
			case schemapb.DataType_INT8:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.Int8FieldData{
						NumRows: 0,
						Data:    make([]int8, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.Int8FieldData)
				for _, blob := range msg.RowData {
					var v int8
					buf := bytes.NewReader(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Println("binary.Read int8 failed:", err)
					}
					fieldData.Data = append(fieldData.Data, v)
					pos += int(unsafe.Sizeof(*(&v)))
				}
				fieldData.NumRows += len(msg.RowIDs)

			case schemapb.DataType_INT16:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.Int16FieldData{
						NumRows: 0,
						Data:    make([]int16, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.Int16FieldData)
				for _, blob := range msg.RowData {
					var v int16
					buf := bytes.NewReader(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Println("binary.Read int16 failed:", err)
					}
					fieldData.Data = append(fieldData.Data, v)
					pos += int(unsafe.Sizeof(*(&v)))
				}

				fieldData.NumRows += len(msg.RowIDs)

			case schemapb.DataType_INT32:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.Int32FieldData{
						NumRows: 0,
						Data:    make([]int32, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.Int32FieldData)
				for _, blob := range msg.RowData {
					var v int32
					buf := bytes.NewReader(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Println("binary.Read int32 failed:", err)
					}
					fieldData.Data = append(fieldData.Data, v)
					pos += int(unsafe.Sizeof(*(&v)))
				}
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
				case 0:
					fieldData.Data = append(fieldData.Data, msg.RowIDs...)
					fieldData.NumRows += len(msg.RowIDs)
				case 1:
					// Timestamps
					for _, ts := range msg.Timestamps {
						fieldData.Data = append(fieldData.Data, int64(ts))
					}
					fieldData.NumRows += len(msg.Timestamps)
				default:

					for _, blob := range msg.RowData {
						var v int64
						buf := bytes.NewBuffer(blob.GetValue()[pos:])
						if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
							log.Println("binary.Read int64 failed:", err)
						}
						fieldData.Data = append(fieldData.Data, v)
						pos += int(unsafe.Sizeof(*(&v)))
					}

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
				for _, blob := range msg.RowData {
					var v float32
					buf := bytes.NewBuffer(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Println("binary.Read float32 failed:", err)
					}
					fieldData.Data = append(fieldData.Data, v)
					pos += int(unsafe.Sizeof(*(&v)))
				}

				fieldData.NumRows += len(msg.RowIDs)

			case schemapb.DataType_DOUBLE:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.DoubleFieldData{
						NumRows: 0,
						Data:    make([]float64, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.DoubleFieldData)
				for _, blob := range msg.RowData {
					var v float64
					buf := bytes.NewBuffer(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Println("binary.Read float64 failed:", err)
					}
					fieldData.Data = append(fieldData.Data, v)
					pos += int(unsafe.Sizeof(*(&v)))
				}

				fieldData.NumRows += len(msg.RowIDs)
			}
		}

		// 1.3 store in buffer
		ibNode.insertBuffer.insertData[currentSegID] = idata

		// 1.4 if full
		//   1.4.1 generate binlogs
		if ibNode.insertBuffer.full(currentSegID) {
			log.Printf(". Insert Buffer full, auto flushing (%v) rows of data...", ibNode.insertBuffer.size(currentSegID))
			// partitionTag -> partitionID
			partitionTag := msg.GetPartitionTag()
			partitionID, err := typeutil.Hash32String(partitionTag)
			if err != nil {
				log.Println("partitionTag to partitionID wrong")
				// TODO GOOSE add error handler
			}

			inCodec := storage.NewInsertCodec(collMeta)

			// buffer data to binlogs
			binLogs, err := inCodec.Serialize(partitionID,
				currentSegID, ibNode.insertBuffer.insertData[currentSegID])

			if err != nil {
				log.Println("generate binlog wrong: ", err)
			}

			// clear buffer
			delete(ibNode.insertBuffer.insertData, currentSegID)
			log.Println(".. Clearing buffer")

			//   1.5.2 binLogs -> minIO/S3
			collIDStr := strconv.FormatInt(segMeta.GetCollectionID(), 10)
			partitionIDStr := strconv.FormatInt(partitionID, 10)
			segIDStr := strconv.FormatInt(currentSegID, 10)
			keyPrefix := path.Join(ibNode.minioPrifex, collIDStr, partitionIDStr, segIDStr)

			log.Printf(".. Saving (%v) binlogs to MinIO ...", len(binLogs))
			for index, blob := range binLogs {
				uid, err := ibNode.idAllocator.AllocOne()
				if err != nil {
					log.Println("Allocate Id failed")
					// GOOSE TODO error handler
				}

				key := path.Join(keyPrefix, blob.Key, strconv.FormatInt(uid, 10))
				err = ibNode.minIOKV.Save(key, string(blob.Value[:]))
				if err != nil {
					log.Println("Save to MinIO failed")
					// GOOSE TODO error handler
				}

				fieldID, err := strconv.ParseInt(blob.Key, 10, 32)
				if err != nil {
					log.Println("string to fieldID wrong")
					// GOOSE TODO error handler
				}

				inBinlogMsg := &insertFlushSyncMsg{
					flushCompleted: false,
					insertBinlogPathMsg: insertBinlogPathMsg{
						ts:      iMsg.timeRange.timestampMax,
						segID:   currentSegID,
						fieldID: fieldID,
						paths:   []string{key},
					},
				}

				log.Println("... Appending binlog paths ...", index)
				ibNode.outCh <- inBinlogMsg
			}
		}
	}

	if len(iMsg.insertMessages) > 0 {
		log.Println("---insert buffer status---")
		var stopSign int = 0
		for k := range ibNode.insertBuffer.insertData {
			if stopSign >= 10 {
				break
			}
			log.Printf("seg(%v) buffer size = (%v)", k, ibNode.insertBuffer.size(k))
			stopSign++
		}
	}

	// iMsg is Flush() msg from master
	//   1. insertBuffer(not empty) -> binLogs -> minIO/S3
	for _, msg := range iMsg.flushMessages {
		currentSegID := msg.GetSegmentID()
		flushTs := msg.GetTimestamp()

		log.Printf(". Receiving flush message segID(%v)...", currentSegID)

		if ibNode.insertBuffer.size(currentSegID) > 0 {
			log.Println(".. Buffer not empty, flushing ...")
			segMeta, collMeta, err := ibNode.getMeta(currentSegID)
			if err != nil {
				// GOOSE TODO add error handler
				log.Println("Get meta wrong: ", err)
			}
			inCodec := storage.NewInsertCodec(collMeta)

			// partitionTag -> partitionID
			partitionTag := segMeta.GetPartitionTag()
			partitionID, err := typeutil.Hash32String(partitionTag)
			if err != nil {
				// GOOSE TODO add error handler
				log.Println("partitionTag to partitionID Wrong: ", err)
			}

			// buffer data to binlogs
			binLogs, err := inCodec.Serialize(partitionID,
				currentSegID, ibNode.insertBuffer.insertData[currentSegID])
			if err != nil {
				log.Println("generate binlog wrong: ", err)
			}

			// clear buffer
			delete(ibNode.insertBuffer.insertData, currentSegID)

			//   binLogs -> minIO/S3
			collIDStr := strconv.FormatInt(segMeta.GetCollectionID(), 10)
			partitionIDStr := strconv.FormatInt(partitionID, 10)
			segIDStr := strconv.FormatInt(currentSegID, 10)
			keyPrefix := path.Join(ibNode.minioPrifex, collIDStr, partitionIDStr, segIDStr)

			for _, blob := range binLogs {
				uid, err := ibNode.idAllocator.AllocOne()
				if err != nil {
					log.Println("Allocate Id failed")
					// GOOSE TODO error handler
				}

				key := path.Join(keyPrefix, blob.Key, strconv.FormatInt(uid, 10))
				err = ibNode.minIOKV.Save(key, string(blob.Value[:]))
				if err != nil {
					log.Println("Save to MinIO failed")
					// GOOSE TODO error handler
				}

				fieldID, err := strconv.ParseInt(blob.Key, 10, 32)
				if err != nil {
					log.Println("string to fieldID wrong")
					// GOOSE TODO error handler
				}

				// Append binlogs
				inBinlogMsg := &insertFlushSyncMsg{
					flushCompleted: false,
					insertBinlogPathMsg: insertBinlogPathMsg{
						ts:      flushTs,
						segID:   currentSegID,
						fieldID: fieldID,
						paths:   []string{key},
					},
				}
				ibNode.outCh <- inBinlogMsg
			}
		}

		// Flushed
		log.Println(".. Flush finished ...")
		inBinlogMsg := &insertFlushSyncMsg{
			flushCompleted: true,
			insertBinlogPathMsg: insertBinlogPathMsg{
				ts:    flushTs,
				segID: currentSegID,
			},
		}

		ibNode.outCh <- inBinlogMsg
	}

	if err := ibNode.writeHardTimeTick(iMsg.timeRange.timestampMax); err != nil {
		log.Printf("Error: send hard time tick into pulsar channel failed, %s\n", err.Error())
	}

	return nil
}

func (ibNode *insertBufferNode) getMeta(segID UniqueID) (*etcdpb.SegmentMeta, *etcdpb.CollectionMeta, error) {

	segMeta := &etcdpb.SegmentMeta{}

	key := path.Join(SegmentPrefix, strconv.FormatInt(segID, 10))
	value, err := ibNode.kvClient.Load(key)
	if err != nil {
		return nil, nil, err
	}
	err = proto.UnmarshalText(value, segMeta)
	if err != nil {
		return nil, nil, err
	}

	collMeta := &etcdpb.CollectionMeta{}
	key = path.Join(CollectionPrefix, strconv.FormatInt(segMeta.GetCollectionID(), 10))
	value, err = ibNode.kvClient.Load(key)
	if err != nil {
		return nil, nil, err
	}
	err = proto.UnmarshalText(value, collMeta)
	if err != nil {
		return nil, nil, err
	}
	return segMeta, collMeta, nil
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
			MsgType:   internalpb.MsgType_kTimeTick,
			PeerID:    Params.WriteNodeID,
			Timestamp: ts,
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, &timeTickMsg)
	return ibNode.pulsarWriteNodeTimeTickStream.Produce(&msgPack)
}

func newInsertBufferNode(ctx context.Context, outCh chan *insertFlushSyncMsg) *insertBufferNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	maxSize := Params.FlushInsertBufSize
	iBuffer := &insertBuffer{
		insertData: make(map[UniqueID]*InsertData),
		maxSize:    maxSize,
	}

	// EtcdKV
	ETCDAddr := Params.EtcdAddress
	MetaRootPath := Params.MetaRootPath
	log.Println("metaRootPath: ", MetaRootPath)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{ETCDAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	kvClient := etcdkv.NewEtcdKV(cli, MetaRootPath)

	// MinIO
	minioendPoint := Params.MinioAddress
	miniioAccessKeyID := Params.MinioAccessKeyID
	miniioSecretAccessKey := Params.MinioSecretAccessKey
	minioUseSSL := Params.MinioUseSSL
	minioBucketName := Params.MinioBucketName

	minioClient, err := minio.New(minioendPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(miniioAccessKeyID, miniioSecretAccessKey, ""),
		Secure: minioUseSSL,
	})
	if err != nil {
		panic(err)
	}
	minIOKV, err := miniokv.NewMinIOKV(ctx, minioClient, minioBucketName)
	if err != nil {
		panic(err)
	}
	minioPrefix := Params.InsertLogRootPath

	idAllocator, err := allocator.NewIDAllocator(ctx, Params.MasterAddress)
	if err != nil {
		panic(err)
	}
	err = idAllocator.Start()
	if err != nil {
		panic(err)
	}

	wTt := msgstream.NewPulsarMsgStream(ctx, 1024) //input stream, write node time tick
	wTt.SetPulsarClient(Params.PulsarAddress)
	wTt.CreatePulsarProducers([]string{Params.WriteNodeTimeTickChannelName})

	return &insertBufferNode{
		BaseNode:                      baseNode,
		kvClient:                      kvClient,
		insertBuffer:                  iBuffer,
		minIOKV:                       minIOKV,
		minioPrifex:                   minioPrefix,
		idAllocator:                   idAllocator,
		outCh:                         outCh,
		pulsarWriteNodeTimeTickStream: wTt,
	}
}
