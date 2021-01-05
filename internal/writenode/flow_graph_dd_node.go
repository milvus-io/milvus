package writenode

import (
	"context"
	"errors"
	"log"
	"path"
	"sort"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	miniokv "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
)

type ddNode struct {
	BaseNode
	ddMsg     *ddMsg
	ddRecords *ddRecords
	ddBuffer  *ddBuffer
	outCh     chan *ddlFlushSyncMsg // for flush sync

	idAllocator *allocator.IDAllocator
	kv          kv.Base
}

type ddData struct {
	ddRequestString []string
	timestamps      []Timestamp
	eventTypes      []storage.EventTypeCode
}

type ddBuffer struct {
	ddData  map[UniqueID]*ddData // collection ID
	maxSize int
}

type ddRecords struct {
	collectionRecords map[UniqueID]interface{}
	partitionRecords  map[UniqueID]interface{}
}

func (d *ddBuffer) size() int {
	if d.ddData == nil || len(d.ddData) <= 0 {
		return 0
	}

	size := 0
	for _, data := range d.ddData {
		size += len(data.ddRequestString)
	}
	return size
}

func (d *ddBuffer) full() bool {
	return d.size() >= d.maxSize
}

func (ddNode *ddNode) Name() string {
	return "ddNode"
}

func (ddNode *ddNode) Operate(in []*Msg) []*Msg {
	//fmt.Println("Do filterDdNode operation")

	if len(in) != 1 {
		log.Println("Invalid operate message input in ddNode, input length = ", len(in))
		// TODO: add error handling
	}

	msMsg, ok := (*in[0]).(*MsgStreamMsg)
	if !ok {
		log.Println("type assertion failed for MsgStreamMsg")
		// TODO: add error handling
	}

	var ddMsg = ddMsg{
		collectionRecords: make(map[string][]metaOperateRecord),
		partitionRecords:  make(map[string][]metaOperateRecord),
		timeRange: TimeRange{
			timestampMin: msMsg.TimestampMin(),
			timestampMax: msMsg.TimestampMax(),
		},
		flushMessages: make([]*msgstream.FlushMsg, 0),
	}
	ddNode.ddMsg = &ddMsg

	// sort tsMessages
	tsMessages := msMsg.TsMessages()
	sort.Slice(tsMessages,
		func(i, j int) bool {
			return tsMessages[i].BeginTs() < tsMessages[j].BeginTs()
		})

	var flush bool = false
	var flushSegID UniqueID
	// do dd tasks
	for _, msg := range tsMessages {
		switch msg.Type() {
		case internalPb.MsgType_kCreateCollection:
			ddNode.createCollection(msg.(*msgstream.CreateCollectionMsg))
		case internalPb.MsgType_kDropCollection:
			ddNode.dropCollection(msg.(*msgstream.DropCollectionMsg))
		case internalPb.MsgType_kCreatePartition:
			ddNode.createPartition(msg.(*msgstream.CreatePartitionMsg))
		case internalPb.MsgType_kDropPartition:
			ddNode.dropPartition(msg.(*msgstream.DropPartitionMsg))
		case internalPb.MsgType_kFlush:
			fMsg := msg.(*msgstream.FlushMsg)
			flush = true
			flushSegID = fMsg.SegmentID
			ddMsg.flushMessages = append(ddMsg.flushMessages, fMsg)
		default:
			log.Println("Non supporting message type:", msg.Type())
		}
	}

	// generate binlog
	if ddNode.ddBuffer.full() || flush {
		log.Println(". dd buffer full or receive Flush msg ...")
		ddCodec := &storage.DataDefinitionCodec{}
		for collectionID, data := range ddNode.ddBuffer.ddData {
			// buffer data to binlog
			binLogs, err := ddCodec.Serialize(data.timestamps, data.ddRequestString, data.eventTypes)
			if err != nil {
				log.Println(err)
				continue
			}
			if len(binLogs) != 2 {
				log.Println("illegal binLogs")
				continue
			}

			// binLogs -> minIO/S3
			if len(data.ddRequestString) != len(data.timestamps) ||
				len(data.timestamps) != len(data.eventTypes) {
				log.Println("illegal ddBuffer, failed to save binlog")
				continue
			} else {
				log.Println(".. dd buffer flushing ...")
				// Blob key example:
				// ${tenant}/data_definition_log/${collection_id}/ts/${log_idx}
				// ${tenant}/data_definition_log/${collection_id}/ddl/${log_idx}
				keyCommon := path.Join(Params.DdLogRootPath, strconv.FormatInt(collectionID, 10))

				// save ts binlog
				timestampLogIdx, err := ddNode.idAllocator.AllocOne()
				if err != nil {
					log.Println(err)
				}
				timestampKey := path.Join(keyCommon, binLogs[0].GetKey(), strconv.FormatInt(timestampLogIdx, 10))
				err = ddNode.kv.Save(timestampKey, string(binLogs[0].GetValue()))
				if err != nil {
					log.Println(err)
				}
				log.Println("save ts binlog, key = ", timestampKey)

				// save dd binlog
				ddLogIdx, err := ddNode.idAllocator.AllocOne()
				if err != nil {
					log.Println(err)
				}
				ddKey := path.Join(keyCommon, binLogs[1].GetKey(), strconv.FormatInt(ddLogIdx, 10))
				err = ddNode.kv.Save(ddKey, string(binLogs[1].GetValue()))
				if err != nil {
					log.Println(err)
				}
				log.Println("save dd binlog, key = ", ddKey)

				ddlFlushMsg := &ddlFlushSyncMsg{
					flushCompleted: false,
					ddlBinlogPathMsg: ddlBinlogPathMsg{
						collID: collectionID,
						paths:  []string{timestampKey, ddKey},
					},
				}

				ddNode.outCh <- ddlFlushMsg
			}

		}
		// clear buffer
		ddNode.ddBuffer.ddData = make(map[UniqueID]*ddData)
	}

	if flush {

		log.Println(".. manual flush completed ...")
		ddlFlushMsg := &ddlFlushSyncMsg{
			flushCompleted: true,
			ddlBinlogPathMsg: ddlBinlogPathMsg{
				segID: flushSegID,
			},
		}

		ddNode.outCh <- ddlFlushMsg

	}

	var res Msg = ddNode.ddMsg
	return []*Msg{&res}
}

func (ddNode *ddNode) createCollection(msg *msgstream.CreateCollectionMsg) {
	collectionID := msg.CollectionID

	// add collection
	if _, ok := ddNode.ddRecords.collectionRecords[collectionID]; ok {
		err := errors.New("collection " + strconv.FormatInt(collectionID, 10) + " is already exists")
		log.Println(err)
		return
	}
	ddNode.ddRecords.collectionRecords[collectionID] = nil

	// TODO: add default partition?

	var schema schemapb.CollectionSchema
	err := proto.Unmarshal((*msg.Schema).Value, &schema)
	if err != nil {
		log.Println(err)
		return
	}
	collectionName := schema.Name
	ddNode.ddMsg.collectionRecords[collectionName] = append(ddNode.ddMsg.collectionRecords[collectionName],
		metaOperateRecord{
			createOrDrop: true,
			timestamp:    msg.Timestamp,
		})

	_, ok := ddNode.ddBuffer.ddData[collectionID]
	if !ok {
		ddNode.ddBuffer.ddData[collectionID] = &ddData{
			ddRequestString: make([]string, 0),
			timestamps:      make([]Timestamp, 0),
			eventTypes:      make([]storage.EventTypeCode, 0),
		}
	}

	ddNode.ddBuffer.ddData[collectionID].ddRequestString = append(ddNode.ddBuffer.ddData[collectionID].ddRequestString, msg.CreateCollectionRequest.String())
	ddNode.ddBuffer.ddData[collectionID].timestamps = append(ddNode.ddBuffer.ddData[collectionID].timestamps, msg.Timestamp)
	ddNode.ddBuffer.ddData[collectionID].eventTypes = append(ddNode.ddBuffer.ddData[collectionID].eventTypes, storage.CreateCollectionEventType)
}

func (ddNode *ddNode) dropCollection(msg *msgstream.DropCollectionMsg) {
	collectionID := msg.CollectionID

	// remove collection
	if _, ok := ddNode.ddRecords.collectionRecords[collectionID]; !ok {
		err := errors.New("cannot found collection " + strconv.FormatInt(collectionID, 10))
		log.Println(err)
		return
	}
	delete(ddNode.ddRecords.collectionRecords, collectionID)

	collectionName := msg.CollectionName.CollectionName
	ddNode.ddMsg.collectionRecords[collectionName] = append(ddNode.ddMsg.collectionRecords[collectionName],
		metaOperateRecord{
			createOrDrop: false,
			timestamp:    msg.Timestamp,
		})

	_, ok := ddNode.ddBuffer.ddData[collectionID]
	if !ok {
		ddNode.ddBuffer.ddData[collectionID] = &ddData{
			ddRequestString: make([]string, 0),
			timestamps:      make([]Timestamp, 0),
			eventTypes:      make([]storage.EventTypeCode, 0),
		}
	}

	ddNode.ddBuffer.ddData[collectionID].ddRequestString = append(ddNode.ddBuffer.ddData[collectionID].ddRequestString, msg.DropCollectionRequest.String())
	ddNode.ddBuffer.ddData[collectionID].timestamps = append(ddNode.ddBuffer.ddData[collectionID].timestamps, msg.Timestamp)
	ddNode.ddBuffer.ddData[collectionID].eventTypes = append(ddNode.ddBuffer.ddData[collectionID].eventTypes, storage.DropCollectionEventType)
}

func (ddNode *ddNode) createPartition(msg *msgstream.CreatePartitionMsg) {
	partitionID := msg.PartitionID
	collectionID := msg.CollectionID

	// add partition
	if _, ok := ddNode.ddRecords.partitionRecords[partitionID]; ok {
		err := errors.New("partition " + strconv.FormatInt(partitionID, 10) + " is already exists")
		log.Println(err)
		return
	}
	ddNode.ddRecords.partitionRecords[partitionID] = nil

	partitionTag := msg.PartitionName.Tag
	ddNode.ddMsg.partitionRecords[partitionTag] = append(ddNode.ddMsg.partitionRecords[partitionTag],
		metaOperateRecord{
			createOrDrop: true,
			timestamp:    msg.Timestamp,
		})

	_, ok := ddNode.ddBuffer.ddData[collectionID]
	if !ok {
		ddNode.ddBuffer.ddData[collectionID] = &ddData{
			ddRequestString: make([]string, 0),
			timestamps:      make([]Timestamp, 0),
			eventTypes:      make([]storage.EventTypeCode, 0),
		}
	}

	ddNode.ddBuffer.ddData[collectionID].ddRequestString = append(ddNode.ddBuffer.ddData[collectionID].ddRequestString, msg.CreatePartitionRequest.String())
	ddNode.ddBuffer.ddData[collectionID].timestamps = append(ddNode.ddBuffer.ddData[collectionID].timestamps, msg.Timestamp)
	ddNode.ddBuffer.ddData[collectionID].eventTypes = append(ddNode.ddBuffer.ddData[collectionID].eventTypes, storage.CreatePartitionEventType)
}

func (ddNode *ddNode) dropPartition(msg *msgstream.DropPartitionMsg) {
	partitionID := msg.PartitionID
	collectionID := msg.CollectionID

	// remove partition
	if _, ok := ddNode.ddRecords.partitionRecords[partitionID]; !ok {
		err := errors.New("cannot found partition " + strconv.FormatInt(partitionID, 10))
		log.Println(err)
		return
	}
	delete(ddNode.ddRecords.partitionRecords, partitionID)

	partitionTag := msg.PartitionName.Tag
	ddNode.ddMsg.partitionRecords[partitionTag] = append(ddNode.ddMsg.partitionRecords[partitionTag],
		metaOperateRecord{
			createOrDrop: false,
			timestamp:    msg.Timestamp,
		})

	_, ok := ddNode.ddBuffer.ddData[collectionID]
	if !ok {
		ddNode.ddBuffer.ddData[collectionID] = &ddData{
			ddRequestString: make([]string, 0),
			timestamps:      make([]Timestamp, 0),
			eventTypes:      make([]storage.EventTypeCode, 0),
		}
	}

	ddNode.ddBuffer.ddData[collectionID].ddRequestString = append(ddNode.ddBuffer.ddData[collectionID].ddRequestString, msg.DropPartitionRequest.String())
	ddNode.ddBuffer.ddData[collectionID].timestamps = append(ddNode.ddBuffer.ddData[collectionID].timestamps, msg.Timestamp)
	ddNode.ddBuffer.ddData[collectionID].eventTypes = append(ddNode.ddBuffer.ddData[collectionID].eventTypes, storage.DropPartitionEventType)
}

func newDDNode(ctx context.Context, outCh chan *ddlFlushSyncMsg) *ddNode {
	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	ddRecords := &ddRecords{
		collectionRecords: make(map[UniqueID]interface{}),
		partitionRecords:  make(map[UniqueID]interface{}),
	}

	bucketName := Params.MinioBucketName
	option := &miniokv.Option{
		Address:           Params.MinioAddress,
		AccessKeyID:       Params.MinioAccessKeyID,
		SecretAccessKeyID: Params.MinioSecretAccessKey,
		UseSSL:            Params.MinioUseSSL,
		BucketName:        bucketName,
		CreateBucket:      true,
	}
	minioKV, err := miniokv.NewMinIOKV(ctx, option)
	if err != nil {
		panic(err)
	}

	idAllocator, err := allocator.NewIDAllocator(ctx, Params.MasterAddress)
	if err != nil {
		panic(err)
	}
	err = idAllocator.Start()
	if err != nil {
		panic(err)
	}

	return &ddNode{
		BaseNode:  baseNode,
		ddRecords: ddRecords,
		ddBuffer: &ddBuffer{
			ddData:  make(map[UniqueID]*ddData),
			maxSize: Params.FlushDdBufSize,
		},
		outCh: outCh,

		idAllocator: idAllocator,
		kv:          minioKV,
	}
}
