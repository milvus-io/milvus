package datanode

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/opentracing/opentracing-go"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	miniokv "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
	"github.com/zilliztech/milvus-distributed/internal/util/trace"
)

type ddNode struct {
	BaseNode
	ddMsg     *ddMsg
	ddRecords *ddRecords
	ddBuffer  *ddBuffer
	flushMap  *sync.Map
	inFlushCh chan *flushMsg

	kv         kv.Base
	replica    Replica
	binlogMeta *binlogMeta
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

func (ddNode *ddNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {

	if len(in) != 1 {
		log.Error("Invalid operate message input in ddNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	msMsg, ok := in[0].(*MsgStreamMsg)
	if !ok {
		log.Error("type assertion failed for MsgStreamMsg")
		// TODO: add error handling
	}

	if msMsg == nil {
		return []Msg{}
	}
	var spans []opentracing.Span
	for _, msg := range msMsg.TsMessages() {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
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
		case commonpb.MsgType_CreateCollection:
			ddNode.createCollection(msg.(*msgstream.CreateCollectionMsg))
		case commonpb.MsgType_DropCollection:
			ddNode.dropCollection(msg.(*msgstream.DropCollectionMsg))
		case commonpb.MsgType_CreatePartition:
			ddNode.createPartition(msg.(*msgstream.CreatePartitionMsg))
		case commonpb.MsgType_DropPartition:
			ddNode.dropPartition(msg.(*msgstream.DropPartitionMsg))
		default:
			log.Error("Not supporting message type", zap.Any("Type", msg.Type()))
		}
	}

	// generate binlog
	if ddNode.ddBuffer.full() {
		for k, v := range ddNode.ddBuffer.ddData {
			ddNode.flushMap.Store(k, v)
		}
		ddNode.ddBuffer.ddData = make(map[UniqueID]*ddData)
		log.Debug(". dd buffer full, auto flushing ...")
		go flushTxn(ddNode.flushMap, ddNode.kv, ddNode.binlogMeta)
	}

	select {
	case fmsg := <-ddNode.inFlushCh:
		log.Debug(". receive flush message ...")
		localSegs := make([]UniqueID, 0, len(fmsg.segmentIDs))
		for _, segID := range fmsg.segmentIDs {
			if ddNode.replica.hasSegment(segID) {
				localSegs = append(localSegs, segID)
			}
		}

		if len(localSegs) <= 0 {
			log.Debug(".. Segment not exist in this datanode, skip flushing ...")
			break
		}

		log.Debug(".. Segments exist, notifying insertbuffer ...")
		fmsg.segmentIDs = localSegs
		ddNode.ddMsg.flushMessages = append(ddNode.ddMsg.flushMessages, fmsg)

		if ddNode.ddBuffer.size() > 0 {
			log.Debug(".. ddl buffer not empty, flushing ...")
			for k, v := range ddNode.ddBuffer.ddData {
				ddNode.flushMap.Store(k, v)
			}
			ddNode.ddBuffer.ddData = make(map[UniqueID]*ddData)

			go flushTxn(ddNode.flushMap, ddNode.kv, ddNode.binlogMeta)

		}

	default:
	}

	for _, span := range spans {
		span.Finish()
	}

	var res Msg = ddNode.ddMsg
	return []Msg{res}
}

/*
flushTxn() will do the following:
    generate binlogs for all buffer data in ddNode,
    store the generated binlogs to minIO/S3,
    store the keys(paths to minIO/s3) of the binlogs to etcd.

The keys of the binlogs are generated as below:
	${tenant}/data_definition_log/${collection_id}/ts/${log_idx}
	${tenant}/data_definition_log/${collection_id}/ddl/${log_idx}

*/
func flushTxn(ddlData *sync.Map,
	kv kv.Base,
	meta *binlogMeta) {
	// generate binlog
	ddCodec := &storage.DataDefinitionCodec{}
	ddlData.Range(func(cID, d interface{}) bool {

		data := d.(*ddData)
		collID := cID.(int64)
		log.Debug(".. ddl flushing ...", zap.Int64("collectionID", collID), zap.Int("length", len(data.ddRequestString)))
		binLogs, err := ddCodec.Serialize(data.timestamps, data.ddRequestString, data.eventTypes)
		if err != nil || len(binLogs) != 2 {
			log.Error("Codec Serialize wrong", zap.Error(err))
			return false
		}

		if len(data.ddRequestString) != len(data.timestamps) ||
			len(data.timestamps) != len(data.eventTypes) {
			log.Error("illegal ddBuffer, failed to save binlog")
			return false
		}

		kvs := make(map[string]string, 2)
		tsIdx, err := meta.genKey(true)
		if err != nil {
			log.Error("Id allocate wrong", zap.Error(err))
			return false
		}
		tsKey := path.Join(Params.DdlBinlogRootPath, strconv.FormatInt(collID, 10), binLogs[0].GetKey(), tsIdx)
		kvs[tsKey] = string(binLogs[0].GetValue())

		ddlIdx, err := meta.genKey(true)
		if err != nil {
			log.Error("Id allocate wrong", zap.Error(err))
			return false
		}
		ddlKey := path.Join(Params.DdlBinlogRootPath, strconv.FormatInt(collID, 10), binLogs[1].GetKey(), ddlIdx)
		kvs[ddlKey] = string(binLogs[1].GetValue())

		// save ddl/ts binlog to minIO/s3
		log.Debug(".. Saving ddl binlog to minIO/s3 ...")
		err = kv.MultiSave(kvs)
		if err != nil {
			log.Error("Save to minIO/S3 Wrong", zap.Error(err))
			_ = kv.MultiRemove([]string{tsKey, ddlKey})
			return false
		}

		log.Debug(".. Saving ddl binlog meta ...")
		err = meta.SaveDDLBinlogMetaTxn(collID, tsKey, ddlKey)
		if err != nil {
			log.Error("Save binlog meta to etcd Wrong", zap.Error(err))
			_ = kv.MultiRemove([]string{tsKey, ddlKey})
			return false
		}

		log.Debug(".. Clearing ddl flush buffer ...")
		ddlData.Delete(collID)
		return true

	})
	log.Debug(".. DDL flushing completed ...")
}

func (ddNode *ddNode) createCollection(msg *msgstream.CreateCollectionMsg) {
	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)
	defer sp.Finish()

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
	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)
	defer sp.Finish()

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
	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)
	defer sp.Finish()

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
	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	msg.SetTraceCtx(ctx)
	defer sp.Finish()
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

func newDDNode(ctx context.Context, binlogMeta *binlogMeta,
	inFlushCh chan *flushMsg, replica Replica) *ddNode {
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

		// idAllocator: alloc,
		kv:         minioKV,
		replica:    replica,
		binlogMeta: binlogMeta,
		flushMap:   &sync.Map{},
	}
}
