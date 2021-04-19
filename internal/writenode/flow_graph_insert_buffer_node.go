package writenode

import (
	"context"
	"encoding/binary"
	"log"
	"math"
	"path"
	"strconv"
	"time"

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
		fdata, ok := data.(storage.FloatVectorFieldData)
		if ok && len(fdata.Data) > maxSize {
			maxSize = len(fdata.Data)
		}

		bdata, ok := data.(storage.BinaryVectorFieldData)
		if ok && len(bdata.Data) > maxSize {
			maxSize = len(bdata.Data)
		}

	}
	return maxSize
}

func (ib *insertBuffer) full(segmentID UniqueID) bool {
	// GOOSE TODO
	return ib.size(segmentID) >= ib.maxSize
}

func (ibNode *insertBufferNode) Name() string {
	return "ibNode"
}

func (ibNode *insertBufferNode) Operate(in []*Msg) []*Msg {
	log.Println("=========== insert buffer Node Operating")

	if len(in) != 1 {
		log.Println("Error: Invalid operate message input in insertBuffertNode, input length = ", len(in))
		// TODO: add error handling
	}

	iMsg, ok := (*in[0]).(*insertMsg)
	if !ok {
		log.Println("Error: type assertion failed for insertMsg")
		// TODO: add error handling
	}
	for _, task := range iMsg.insertMessages {
		if len(task.RowIDs) != len(task.Timestamps) || len(task.RowIDs) != len(task.RowData) {
			log.Println("Error: misaligned messages detected")
			continue
		}

		// iMsg is insertMsg
		// 1. iMsg -> buffer
		for _, msg := range iMsg.insertMessages {
			currentSegID := msg.GetSegmentID()

			idata, ok := ibNode.insertBuffer.insertData[currentSegID]
			if !ok {
				idata = &InsertData{
					Data: make(map[UniqueID]storage.FieldData),
				}
			}

			// Timestamps
			_, ok = idata.Data[1].(*storage.Int64FieldData)
			if !ok {
				idata.Data[1] = &storage.Int64FieldData{
					Data:    []int64{},
					NumRows: 0,
				}
			}
			tsData := idata.Data[1].(*storage.Int64FieldData)
			for _, ts := range msg.Timestamps {
				tsData.Data = append(tsData.Data, int64(ts))
			}
			tsData.NumRows += len(msg.Timestamps)

			// 1.1 Get CollectionMeta from etcd
			segMeta := etcdpb.SegmentMeta{}

			key := path.Join(SegmentPrefix, strconv.FormatInt(currentSegID, 10))
			value, _ := ibNode.kvClient.Load(key)
			err := proto.UnmarshalText(value, &segMeta)
			if err != nil {
				log.Println("Load segMeta error")
				// TODO: add error handling
			}

			collMeta := etcdpb.CollectionMeta{}
			key = path.Join(CollectionPrefix, strconv.FormatInt(segMeta.GetCollectionID(), 10))
			value, _ = ibNode.kvClient.Load(key)
			err = proto.UnmarshalText(value, &collMeta)
			if err != nil {
				log.Println("Load collMeta error")
				// TODO: add error handling
			}

			// 1.2 Get Fields
			var pos = 0 // Record position of blob
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
							v := binary.LittleEndian.Uint32(blob.GetValue()[pos*4:])
							fieldData.Data = append(fieldData.Data, math.Float32frombits(v))
							pos++
						}
					}
					fieldData.NumRows += len(msg.RowIDs)
					log.Println("Float vector data:",
						idata.Data[field.FieldID].(*storage.FloatVectorFieldData).Data,
						"NumRows:",
						idata.Data[field.FieldID].(*storage.FloatVectorFieldData).NumRows,
						"Dim:",
						idata.Data[field.FieldID].(*storage.FloatVectorFieldData).Dim)

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
						for d := 0; d < dim/8; d++ {
							v := binary.LittleEndian.Uint32(blob.GetValue()[pos*4:])
							fieldData.Data = append(fieldData.Data, byte(v))
							pos++
						}
					}

					fieldData.NumRows += len(msg.RowData)
					log.Println(
						"Binary vector data:",
						idata.Data[field.FieldID].(*storage.BinaryVectorFieldData).Data,
						"NumRows:",
						idata.Data[field.FieldID].(*storage.BinaryVectorFieldData).NumRows,
						"Dim:",
						idata.Data[field.FieldID].(*storage.BinaryVectorFieldData).Dim)
				case schemapb.DataType_BOOL:
					if _, ok := idata.Data[field.FieldID]; !ok {
						idata.Data[field.FieldID] = &storage.BoolFieldData{
							NumRows: 0,
							Data:    make([]bool, 0),
						}
					}

					fieldData := idata.Data[field.FieldID].(*storage.BoolFieldData)
					for _, blob := range msg.RowData {
						boolInt := binary.LittleEndian.Uint32(blob.GetValue()[pos*4:])
						if boolInt == 1 {
							fieldData.Data = append(fieldData.Data, true)
						} else {
							fieldData.Data = append(fieldData.Data, false)
						}
						pos++
					}

					fieldData.NumRows += len(msg.RowIDs)
					log.Println("Bool data:",
						idata.Data[field.FieldID].(*storage.BoolFieldData).Data)
				case schemapb.DataType_INT8:
					if _, ok := idata.Data[field.FieldID]; !ok {
						idata.Data[field.FieldID] = &storage.Int8FieldData{
							NumRows: 0,
							Data:    make([]int8, 0),
						}
					}

					fieldData := idata.Data[field.FieldID].(*storage.Int8FieldData)
					for _, blob := range msg.RowData {
						v := binary.LittleEndian.Uint32(blob.GetValue()[pos*4:])
						fieldData.Data = append(fieldData.Data, int8(v))
						pos++
					}
					fieldData.NumRows += len(msg.RowIDs)
					log.Println("Int8 data:",
						idata.Data[field.FieldID].(*storage.Int8FieldData).Data)

				case schemapb.DataType_INT16:
					if _, ok := idata.Data[field.FieldID]; !ok {
						idata.Data[field.FieldID] = &storage.Int16FieldData{
							NumRows: 0,
							Data:    make([]int16, 0),
						}
					}

					fieldData := idata.Data[field.FieldID].(*storage.Int16FieldData)
					for _, blob := range msg.RowData {
						v := binary.LittleEndian.Uint32(blob.GetValue()[pos*4:])
						fieldData.Data = append(fieldData.Data, int16(v))
						pos++
					}

					fieldData.NumRows += len(msg.RowIDs)
					log.Println("Int16 data:",
						idata.Data[field.FieldID].(*storage.Int16FieldData).Data)
				case schemapb.DataType_INT32:
					if _, ok := idata.Data[field.FieldID]; !ok {
						idata.Data[field.FieldID] = &storage.Int32FieldData{
							NumRows: 0,
							Data:    make([]int32, 0),
						}
					}

					fieldData := idata.Data[field.FieldID].(*storage.Int32FieldData)
					for _, blob := range msg.RowData {
						v := binary.LittleEndian.Uint32(blob.GetValue()[pos*4:])
						fieldData.Data = append(fieldData.Data, int32(v))
						pos++
					}
					fieldData.NumRows += len(msg.RowIDs)
					log.Println("Int32 data:",
						idata.Data[field.FieldID].(*storage.Int32FieldData).Data)

				case schemapb.DataType_INT64:
					if _, ok := idata.Data[field.FieldID]; !ok {
						idata.Data[field.FieldID] = &storage.Int64FieldData{
							NumRows: 0,
							Data:    make([]int64, 0),
						}
					}

					fieldData := idata.Data[field.FieldID].(*storage.Int64FieldData)
					for _, blob := range msg.RowData {
						v := binary.LittleEndian.Uint32(blob.GetValue()[pos*4:])
						fieldData.Data = append(fieldData.Data, int64(v))
						pos++
					}

					fieldData.NumRows += len(msg.RowIDs)
					log.Println("Int64 data:",
						idata.Data[field.FieldID].(*storage.Int64FieldData).Data)

				case schemapb.DataType_FLOAT:
					if _, ok := idata.Data[field.FieldID]; !ok {
						idata.Data[field.FieldID] = &storage.FloatFieldData{
							NumRows: 0,
							Data:    make([]float32, 0),
						}
					}

					fieldData := idata.Data[field.FieldID].(*storage.FloatFieldData)
					for _, blob := range msg.RowData {
						v := binary.LittleEndian.Uint32(blob.GetValue()[pos*4:])
						fieldData.Data = append(fieldData.Data, math.Float32frombits(v))
						pos++
					}

					fieldData.NumRows += len(msg.RowIDs)
					log.Println("Float32 data:",
						idata.Data[field.FieldID].(*storage.FloatFieldData).Data)

				case schemapb.DataType_DOUBLE:
					if _, ok := idata.Data[field.FieldID]; !ok {
						idata.Data[field.FieldID] = &storage.DoubleFieldData{
							NumRows: 0,
							Data:    make([]float64, 0),
						}
					}

					fieldData := idata.Data[field.FieldID].(*storage.DoubleFieldData)
					for _, blob := range msg.RowData {
						v := binary.LittleEndian.Uint64(blob.GetValue()[pos*4:])
						fieldData.Data = append(fieldData.Data, math.Float64frombits(v))
						pos++
					}

					fieldData.NumRows += len(msg.RowIDs)
					log.Println("Float64 data:",
						idata.Data[field.FieldID].(*storage.DoubleFieldData).Data)
				}
			}

			// 1.3 store in buffer
			ibNode.insertBuffer.insertData[currentSegID] = idata
			// 1.4 Send hardTimeTick msg, GOOSE TODO

			// 1.5 if full
			//   1.5.1 generate binlogs
			if ibNode.insertBuffer.full(currentSegID) {
				// partitionTag -> partitionID
				partitionTag := msg.GetPartitionTag()
				partitionID, err := typeutil.Hash32String(partitionTag)
				if err != nil {
					log.Println("partitionTag to partitionID Wrong")
				}

				inCodec := storage.NewInsertCodec(&collMeta)

				// buffer data to binlogs
				binLogs, err := inCodec.Serialize(partitionID,
					currentSegID, ibNode.insertBuffer.insertData[currentSegID])
				for _, v := range binLogs {
					log.Println("key ", v.Key, "- value ", v.Value)
				}
				if err != nil {
					log.Println("generate binlog wrong")
				}

				// clear buffer
				log.Println("=========", binLogs)
				delete(ibNode.insertBuffer.insertData, currentSegID)

				//   1.5.2 binLogs -> minIO/S3
				collIDStr := strconv.FormatInt(segMeta.GetCollectionID(), 10)
				partitionIDStr := strconv.FormatInt(partitionID, 10)
				segIDStr := strconv.FormatInt(currentSegID, 10)
				keyPrefix := path.Join(ibNode.minioPrifex, collIDStr, partitionIDStr, segIDStr)

				for _, blob := range binLogs {
					uid, err := ibNode.idAllocator.AllocOne()
					if err != nil {
						log.Println("Allocate Id failed")
						// GOOSE TODO error handle
					}

					key := path.Join(keyPrefix, blob.Key, strconv.FormatInt(uid, 10))
					err = ibNode.minIOKV.Save(key, string(blob.Value[:]))
					if err != nil {
						log.Println("Save to MinIO failed")
						// GOOSE TODO error handle
					}
				}
			}
		}

		// iMsg is Flush() msg from master
		//   1. insertBuffer(not empty) -> binLogs -> minIO/S3
		// Return

	}

	if err := ibNode.writeHardTimeTick(iMsg.timeRange.timestampMax); err != nil {
		log.Printf("Error: send hard time tick into pulsar channel failed, %s\n", err.Error())
	}

	return nil
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
