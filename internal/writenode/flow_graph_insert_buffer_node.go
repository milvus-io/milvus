package writenode

import (
	"encoding/binary"
	"log"
	"math"
	"path"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
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
		kvClient     *etcdkv.EtcdKV
		insertBuffer *insertBuffer
	}

	insertBuffer struct {
		insertData map[UniqueID]*InsertData // SegmentID to InsertData
		maxSize    int                      // GOOSE TODO set from write_node.yaml
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
		log.Println("Invalid operate message input in insertBuffertNode, input length = ", len(in))
		// TODO: add error handling
	}

	iMsg, ok := (*in[0]).(*insertMsg)
	if !ok {
		log.Println("type assertion failed for insertMsg")
		// TODO: add error handling
	}
	for _, task := range iMsg.insertMessages {
		if len(task.RowIDs) != len(task.Timestamps) || len(task.RowIDs) != len(task.RowData) {
			log.Println("Error, misaligned messages detected")
			continue
		}

		// iMsg is insertMsg
		// 1. iMsg -> binLogs -> buffer
		for _, msg := range iMsg.insertMessages {
			currentSegID := msg.GetSegmentID()

			idata, ok := ibNode.insertBuffer.insertData[currentSegID]
			if !ok {
				idata = &InsertData{
					Data: make(map[UniqueID]storage.FieldData),
				}
			}

			idata.Data[1] = msg.BeginTimestamp

			// 1.1 Get CollectionMeta from etcd
			//   GOOSE TODO get meta from metaTable
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

					data := make([]float32, 0)
					for _, blob := range msg.RowData {
						for j := pos; j < dim; j++ {
							v := binary.LittleEndian.Uint32(blob.GetValue()[j*4:])
							data = append(data, math.Float32frombits(v))
							pos++
						}
					}
					idata.Data[field.FieldID] = storage.FloatVectorFieldData{
						NumRows: len(msg.RowIDs),
						Data:    data,
						Dim:     dim,
					}

					log.Println("aaaaaaaa", idata)
				case schemapb.DataType_VECTOR_BINARY:
					// GOOSE TODO
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

					data := make([]byte, 0)
					for _, blob := range msg.RowData {
						for d := 0; d < dim/4; d++ {
							v := binary.LittleEndian.Uint32(blob.GetValue()[pos*4:])
							data = append(data, byte(v))
							pos++
						}
					}
					idata.Data[field.FieldID] = storage.BinaryVectorFieldData{
						NumRows: len(data) * 8 / dim,
						Data:    data,
						Dim:     dim,
					}
					log.Println("aaaaaaaa", idata)
				case schemapb.DataType_BOOL:
					data := make([]bool, 0)
					for _, blob := range msg.RowData {
						boolInt := binary.LittleEndian.Uint32(blob.GetValue()[pos*4:])
						if boolInt == 1 {
							data = append(data, true)
						} else {
							data = append(data, false)
						}
						pos++
					}
					idata.Data[field.FieldID] = data
					log.Println("aaaaaaaa", idata)
				case schemapb.DataType_INT8:
					data := make([]int8, 0)
					for _, blob := range msg.RowData {
						v := binary.LittleEndian.Uint32(blob.GetValue()[pos*4:])
						data = append(data, int8(v))
						pos++
					}
					idata.Data[field.FieldID] = data
					log.Println("aaaaaaaa", idata)
				case schemapb.DataType_INT16:
					data := make([]int16, 0)
					for _, blob := range msg.RowData {
						v := binary.LittleEndian.Uint32(blob.GetValue()[pos*4:])
						data = append(data, int16(v))
						pos++
					}
					idata.Data[field.FieldID] = data
					log.Println("aaaaaaaa", idata)
				case schemapb.DataType_INT32:
					data := make([]int32, 0)
					for _, blob := range msg.RowData {
						v := binary.LittleEndian.Uint32(blob.GetValue()[pos*4:])
						data = append(data, int32(v))
						pos++
					}
					idata.Data[field.FieldID] = data
					log.Println("aaaaaaaa", idata)
				case schemapb.DataType_INT64:
					data := make([]int64, 0)
					for _, blob := range msg.RowData {
						v := binary.LittleEndian.Uint32(blob.GetValue()[pos*4:])
						data = append(data, int64(v))
						pos++
					}
					idata.Data[field.FieldID] = data
					log.Println("aaaaaaaa", idata)
				case schemapb.DataType_FLOAT:
					data := make([]float32, 0)
					for _, blob := range msg.RowData {
						v := binary.LittleEndian.Uint32(blob.GetValue()[pos*4:])
						data = append(data, math.Float32frombits(v))
						pos++
					}
					idata.Data[field.FieldID] = data
					log.Println("aaaaaaaa", idata)
				case schemapb.DataType_DOUBLE:
					// GOOSE TODO pos
					data := make([]float64, 0)
					for _, blob := range msg.RowData {
						v := binary.LittleEndian.Uint64(blob.GetValue()[pos*4:])
						data = append(data, math.Float64frombits(v))
						pos++
					}
					idata.Data[field.FieldID] = data
					log.Println("aaaaaaaa", idata)
				}
			}

			// 1.3 store in buffer
			ibNode.insertBuffer.insertData[currentSegID] = idata
			// 1.4 Send hardTimeTick msg

			// 1.5 if full
			//   1.5.1 generate binlogs
			// GOOSE TODO partitionTag -> partitionID
			//   1.5.2 binLogs -> minIO/S3
			if ibNode.insertBuffer.full(currentSegID) {
				continue
			}
		}

		// iMsg is Flush() msg from master
		//   1. insertBuffer(not empty) -> binLogs -> minIO/S3
		// Return

	}
	return nil
}

func newInsertBufferNode() *insertBufferNode {

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	// GOOSE TODO maxSize read from yaml
	maxSize := 10
	iBuffer := &insertBuffer{
		insertData: make(map[UniqueID]*InsertData),
		maxSize:    maxSize,
	}

	// EtcdKV
	ETCDAddr := Params.EtcdAddress
	MetaRootPath := Params.MetaRootPath
	log.Println("metaRootPath: ", MetaRootPath)
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{ETCDAddr},
		DialTimeout: 5 * time.Second,
	})
	kvClient := etcdkv.NewEtcdKV(cli, MetaRootPath)

	return &insertBufferNode{
		BaseNode:     baseNode,
		kvClient:     kvClient,
		insertBuffer: iBuffer,
	}
}
