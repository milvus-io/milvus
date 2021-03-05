package datanode

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/kv"
	miniokv "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
)

type ddNode struct {
	BaseNode
	ddMsg     *ddMsg
	ddRecords *ddRecords
	ddBuffer  *ddBuffer
	inFlushCh chan *flushMsg

	idAllocator allocator
	kv          kv.Base
	replica     Replica
	flushMeta   *metaTable
}

type ddData struct {
	ddRequestString []string
	timestamps      []Timestamp
	eventTypes      []storage.EventTypeCode
}

type ddBuffer struct {
	ddData  map[UniqueID]*ddData // collection ID
	maxSize int32
}

type ddRecords struct {
	collectionRecords map[UniqueID]interface{}
	partitionRecords  map[UniqueID]interface{}
}

func (d *ddBuffer) size() int32 {
	if d.ddData == nil || len(d.ddData) <= 0 {
		return 0
	}

	var size int32 = 0
	for _, data := range d.ddData {
		size += int32(len(data.ddRequestString))
	}
	return size
}

func (d *ddBuffer) full() bool {
	return d.size() >= d.maxSize
}

func (ddNode *ddNode) Name() string {
	return "ddNode"
}

func (ddNode *ddNode) Operate(ctx context.Context, in []Msg) ([]Msg, context.Context) {

	if len(in) != 1 {
		log.Error("Invalid operate message input in ddNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	msMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Error("type assertion failed for MsgStreamMsg")
		// TODO: add error handling
	}

	ddNode.ddMsg = &ddMsg{
		collectionRecords: make(map[UniqueID][]*metaOperateRecord),
		partitionRecords:  make(map[UniqueID][]*metaOperateRecord),
		timeRange: TimeRange{
			timestampMin: msMsg.TimestampMin(),
			timestampMax: msMsg.TimestampMax(),
		},
		flushMessages: make([]*flushMsg, 0),
		gcRecord: &gcRecord{
			collections: make([]UniqueID, 0),
		},
	}

	// sort tsMessages
	tsMessages := msMsg.TsMessages()
	sort.Slice(tsMessages,
		func(i, j int) bool {
			return tsMessages[i].BeginTs() < tsMessages[j].BeginTs()
		})

	// do dd tasks
	for _, msg := range tsMessages {
		switch msg.Type() {
		case commonpb.MsgType_kCreateCollection:
			ddNode.createCollection(msg.(*msgstream.CreateCollectionMsg))
		case commonpb.MsgType_kDropCollection:
			ddNode.dropCollection(msg.(*msgstream.DropCollectionMsg))
		case commonpb.MsgType_kCreatePartition:
			ddNode.createPartition(msg.(*msgstream.CreatePartitionMsg))
		case commonpb.MsgType_kDropPartition:
			ddNode.dropPartition(msg.(*msgstream.DropPartitionMsg))
		default:
			log.Error("Not supporting message type", zap.Any("Type", msg.Type()))
		}
	}

	select {
	case fmsg := <-ddNode.inFlushCh:
		log.Debug(". receive flush message, flushing ...")
		localSegs := make([]UniqueID, 0)
		for _, segID := range fmsg.segmentIDs {
			if ddNode.replica.hasSegment(segID) {
				localSegs = append(localSegs, segID)
			}
		}
		if len(localSegs) > 0 {
			ddNode.flush()
			fmsg.segmentIDs = localSegs
			ddNode.ddMsg.flushMessages = append(ddNode.ddMsg.flushMessages, fmsg)
		}

	default:
	}

	// generate binlog
	if ddNode.ddBuffer.full() {
		log.Debug(". dd buffer full, auto flushing ...")
		ddNode.flush()
	}

	var res Msg = ddNode.ddMsg
	return []Msg{res}, ctx
}

/*
flush() will do the following:
    generate binlogs for all buffer data in ddNode,
    store the generated binlogs to minIO/S3,
    store the keys(paths to minIO/s3) of the binlogs to etcd.

The keys of the binlogs are generated as below:
	${tenant}/data_definition_log/${collection_id}/ts/${log_idx}
	${tenant}/data_definition_log/${collection_id}/ddl/${log_idx}

*/
func (ddNode *ddNode) flush() {
	// generate binlog
	ddCodec := &storage.DataDefinitionCodec{}
	for collectionID, data := range ddNode.ddBuffer.ddData {
		// buffer data to binlog
		binLogs, err := ddCodec.Serialize(data.timestamps, data.ddRequestString, data.eventTypes)
		if err != nil {
			log.Error("Codec Serialize wrong", zap.Error(err))
			continue
		}
		if len(binLogs) != 2 {
			log.Error("illegal binLogs")
			continue
		}

		// binLogs -> minIO/S3
		if len(data.ddRequestString) != len(data.timestamps) ||
			len(data.timestamps) != len(data.eventTypes) {
			log.Error("illegal ddBuffer, failed to save binlog")
			continue
		} else {
			log.Debug(".. dd buffer flushing ...")
			keyCommon := path.Join(Params.DdBinlogRootPath, strconv.FormatInt(collectionID, 10))

			// save ts binlog
			timestampLogIdx, err := ddNode.idAllocator.allocID()
			if err != nil {
				log.Error("Id allocate wrong", zap.Error(err))
			}
			timestampKey := path.Join(keyCommon, binLogs[0].GetKey(), strconv.FormatInt(timestampLogIdx, 10))
			err = ddNode.kv.Save(timestampKey, string(binLogs[0].GetValue()))
			if err != nil {
				log.Error("Save to minIO/S3 Wrong", zap.Error(err))
			}
			log.Debug("save ts binlog", zap.String("key", timestampKey))

			// save dd binlog
			ddLogIdx, err := ddNode.idAllocator.allocID()
			if err != nil {
				log.Error("Id allocate wrong", zap.Error(err))
			}
			ddKey := path.Join(keyCommon, binLogs[1].GetKey(), strconv.FormatInt(ddLogIdx, 10))
			err = ddNode.kv.Save(ddKey, string(binLogs[1].GetValue()))
			if err != nil {
				log.Error("Save to minIO/S3 Wrong", zap.Error(err))
			}
			log.Debug("save dd binlog", zap.String("key", ddKey))

			ddNode.flushMeta.AppendDDLBinlogPaths(collectionID, []string{timestampKey, ddKey})
		}

	}
	// clear buffer
	ddNode.ddBuffer.ddData = make(map[UniqueID]*ddData)
}

func (ddNode *ddNode) createCollection(msg *msgstream.CreateCollectionMsg) {
	collectionID := msg.CollectionID

	// add collection
	if _, ok := ddNode.ddRecords.collectionRecords[collectionID]; ok {
		err := fmt.Errorf("collection %d is already exists", collectionID)
		log.Error("String conversion wrong", zap.Error(err))
		return
	}
	ddNode.ddRecords.collectionRecords[collectionID] = nil

	// TODO: add default partition?

	var schema schemapb.CollectionSchema
	err := proto.Unmarshal(msg.Schema, &schema)
	if err != nil {
		log.Error("proto unmarshal wrong", zap.Error(err))
		return
	}

	// add collection
	err = ddNode.replica.addCollection(collectionID, &schema)
	if err != nil {
		log.Error("replica add collection wrong", zap.Error(err))
		return
	}

	ddNode.ddMsg.collectionRecords[collectionID] = append(ddNode.ddMsg.collectionRecords[collectionID],
		&metaOperateRecord{
			createOrDrop: true,
			timestamp:    msg.Base.Timestamp,
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
	ddNode.ddBuffer.ddData[collectionID].timestamps = append(ddNode.ddBuffer.ddData[collectionID].timestamps, msg.Base.Timestamp)
	ddNode.ddBuffer.ddData[collectionID].eventTypes = append(ddNode.ddBuffer.ddData[collectionID].eventTypes, storage.CreateCollectionEventType)
}

/*
dropCollection will drop collection in ddRecords but won't drop collection in replica
*/
func (ddNode *ddNode) dropCollection(msg *msgstream.DropCollectionMsg) {
	collectionID := msg.CollectionID

	// remove collection
	if _, ok := ddNode.ddRecords.collectionRecords[collectionID]; !ok {
		log.Error("Cannot find collection", zap.Int64("collection ID", collectionID))
		return
	}
	delete(ddNode.ddRecords.collectionRecords, collectionID)

	ddNode.ddMsg.collectionRecords[collectionID] = append(ddNode.ddMsg.collectionRecords[collectionID],
		&metaOperateRecord{
			createOrDrop: false,
			timestamp:    msg.Base.Timestamp,
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
	ddNode.ddBuffer.ddData[collectionID].timestamps = append(ddNode.ddBuffer.ddData[collectionID].timestamps, msg.Base.Timestamp)
	ddNode.ddBuffer.ddData[collectionID].eventTypes = append(ddNode.ddBuffer.ddData[collectionID].eventTypes, storage.DropCollectionEventType)

	ddNode.ddMsg.gcRecord.collections = append(ddNode.ddMsg.gcRecord.collections, collectionID)
}

func (ddNode *ddNode) createPartition(msg *msgstream.CreatePartitionMsg) {
	partitionID := msg.PartitionID
	collectionID := msg.CollectionID

	// add partition
	if _, ok := ddNode.ddRecords.partitionRecords[partitionID]; ok {
		log.Error("partition is already exists", zap.Int64("partition ID", partitionID))
		return
	}
	ddNode.ddRecords.partitionRecords[partitionID] = nil

	ddNode.ddMsg.partitionRecords[partitionID] = append(ddNode.ddMsg.partitionRecords[partitionID],
		&metaOperateRecord{
			createOrDrop: true,
			timestamp:    msg.Base.Timestamp,
		})

	_, ok := ddNode.ddBuffer.ddData[collectionID]
	if !ok {
		ddNode.ddBuffer.ddData[collectionID] = &ddData{
			ddRequestString: make([]string, 0),
			timestamps:      make([]Timestamp, 0),
			eventTypes:      make([]storage.EventTypeCode, 0),
		}
	}

	ddNode.ddBuffer.ddData[collectionID].ddRequestString =
		append(ddNode.ddBuffer.ddData[collectionID].ddRequestString, msg.CreatePartitionRequest.String())

	ddNode.ddBuffer.ddData[collectionID].timestamps =
		append(ddNode.ddBuffer.ddData[collectionID].timestamps, msg.Base.Timestamp)

	ddNode.ddBuffer.ddData[collectionID].eventTypes =
		append(ddNode.ddBuffer.ddData[collectionID].eventTypes, storage.CreatePartitionEventType)
}

func (ddNode *ddNode) dropPartition(msg *msgstream.DropPartitionMsg) {
	partitionID := msg.PartitionID
	collectionID := msg.CollectionID

	// remove partition
	if _, ok := ddNode.ddRecords.partitionRecords[partitionID]; !ok {
		log.Error("cannot found partition", zap.Int64("partition ID", partitionID))
		return
	}
	delete(ddNode.ddRecords.partitionRecords, partitionID)

	// partitionName := msg.PartitionName
	// ddNode.ddMsg.partitionRecords[partitionName] = append(ddNode.ddMsg.partitionRecords[partitionName],
	ddNode.ddMsg.partitionRecords[partitionID] = append(ddNode.ddMsg.partitionRecords[partitionID],
		&metaOperateRecord{
			createOrDrop: false,
			timestamp:    msg.Base.Timestamp,
		})

	_, ok := ddNode.ddBuffer.ddData[collectionID]
	if !ok {
		ddNode.ddBuffer.ddData[collectionID] = &ddData{
			ddRequestString: make([]string, 0),
			timestamps:      make([]Timestamp, 0),
			eventTypes:      make([]storage.EventTypeCode, 0),
		}
	}

	ddNode.ddBuffer.ddData[collectionID].ddRequestString =
		append(ddNode.ddBuffer.ddData[collectionID].ddRequestString, msg.DropPartitionRequest.String())

	ddNode.ddBuffer.ddData[collectionID].timestamps =
		append(ddNode.ddBuffer.ddData[collectionID].timestamps, msg.Base.Timestamp)

	ddNode.ddBuffer.ddData[collectionID].eventTypes =
		append(ddNode.ddBuffer.ddData[collectionID].eventTypes, storage.DropPartitionEventType)
}

func newDDNode(ctx context.Context, flushMeta *metaTable,
	inFlushCh chan *flushMsg, replica Replica, alloc allocator) *ddNode {
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

	return &ddNode{
		BaseNode:  baseNode,
		ddRecords: ddRecords,
		ddBuffer: &ddBuffer{
			ddData:  make(map[UniqueID]*ddData),
			maxSize: Params.FlushDdBufferSize,
		},
		inFlushCh: inFlushCh,

		idAllocator: alloc,
		kv:          minioKV,
		replica:     replica,
		flushMeta:   flushMeta,
	}
}
