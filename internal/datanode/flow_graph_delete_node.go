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
	"context"
	"encoding/binary"
	"path"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	miniokv "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/opentracing/opentracing-go"
)

type (
	// DeleteData record deleted IDs and Timestamps
	DeleteData = storage.DeleteData
)

// DeleteNode is to process delete msg, flush delete info into storage.
type deleteNode struct {
	BaseNode
	channelName string
	delBuf      sync.Map // map[segmentID]*DelDataBuf
	replica     Replica
	idAllocator allocatorInterface
	flushCh     <-chan *flushMsg
	minIOKV     kv.BaseKV
}

// DelDataBuf buffers insert data, monitoring buffer size and limit
// size and limit both indicate numOfRows
type DelDataBuf struct {
	delData *DeleteData
	size    int64
}

func (ddb *DelDataBuf) updateSize(size int64) {
	ddb.size += size
}

func newDelDataBuf() *DelDataBuf {
	return &DelDataBuf{
		delData: &DeleteData{
			Data: make(map[string]int64),
		},
		size: 0,
	}
}

func (dn *deleteNode) Name() string {
	return "deleteNode"
}

func (dn *deleteNode) Close() {
	log.Info("Flowgraph Delete Node closing")
}

func (dn *deleteNode) bufferDeleteMsg(msg *msgstream.DeleteMsg) error {
	log.Debug("bufferDeleteMsg", zap.Any("primary keys", msg.PrimaryKeys))

	segIDToPkMap := make(map[UniqueID][]int64)
	segIDToTsMap := make(map[UniqueID][]int64)

	m := dn.filterSegmentByPK(msg.PartitionID, msg.PrimaryKeys)
	for i, pk := range msg.PrimaryKeys {
		segIDs, ok := m[pk]
		if !ok {
			log.Warn("primary key not exist in all segments", zap.Int64("primary key", pk))
			continue
		}
		for _, segID := range segIDs {
			segIDToPkMap[segID] = append(segIDToPkMap[segID], pk)
			segIDToTsMap[segID] = append(segIDToTsMap[segID], int64(msg.Timestamps[i]))
		}
	}

	for segID, pks := range segIDToPkMap {
		rows := len(pks)
		tss, ok := segIDToTsMap[segID]
		if !ok || rows != len(tss) {
			log.Error("primary keys and timestamp's element num mis-match")
		}

		newBuf := newDelDataBuf()
		delDataBuf, _ := dn.delBuf.LoadOrStore(segID, newBuf)
		delData := delDataBuf.(*DelDataBuf).delData

		for i := 0; i < rows; i++ {
			delData.Data[strconv.FormatInt(pks[i], 10)] = tss[i]
			log.Debug("delete", zap.Int64("primary key", pks[i]), zap.Int64("ts", tss[i]))
		}

		// store
		delDataBuf.(*DelDataBuf).updateSize(int64(rows))
		dn.delBuf.Store(segID, delDataBuf)
	}

	return nil
}

func (dn *deleteNode) showDelBuf() {
	segments := dn.replica.filterSegments(dn.channelName, 0)
	for _, seg := range segments {
		segID := seg.segmentID
		if v, ok := dn.delBuf.Load(segID); ok {
			delDataBuf, _ := v.(*DelDataBuf)
			log.Debug("del data buffer status", zap.Int64("segID", segID), zap.Int64("size", delDataBuf.size))
			for pk, ts := range delDataBuf.delData.Data {
				log.Debug("del data", zap.String("pk", pk), zap.Int64("ts", ts))
			}
		} else {
			log.Error("segment not exist", zap.Int64("segID", segID))
		}
	}
}

func (dn *deleteNode) Operate(in []Msg) []Msg {
	//log.Debug("deleteNode Operating")

	if len(in) != 1 {
		log.Error("Invalid operate message input in deleteNode", zap.Int("input length", len(in)))
		return nil
	}

	fgMsg, ok := in[0].(*flowGraphMsg)
	if !ok {
		log.Error("type assertion failed for flowGraphMsg")
		return nil
	}

	var spans []opentracing.Span
	for _, msg := range fgMsg.deleteMessages {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	for _, msg := range fgMsg.deleteMessages {
		if err := dn.bufferDeleteMsg(msg); err != nil {
			log.Error("buffer delete msg failed", zap.Error(err))
		}
	}

	// show all data in dn.delBuf
	if len(fgMsg.deleteMessages) != 0 {
		dn.showDelBuf()
	}

	// handle manual flush
	select {
	case fmsg := <-dn.flushCh:
		log.Debug("DeleteNode receives flush message", zap.Int64("collID", fmsg.collectionID))
		dn.flushDelData(fmsg.collectionID, fgMsg.timeRange)

		// clean dn.delBuf
		dn.delBuf = sync.Map{}
	default:
	}

	for _, sp := range spans {
		sp.Finish()
	}
	return nil
}

// filterSegmentByPK returns the bloom filter check result.
// If the key may exists in the segment, returns it in map.
// If the key not exists in the segment, the segment is filter out.
func (dn *deleteNode) filterSegmentByPK(partID UniqueID, pks []int64) map[int64][]int64 {
	result := make(map[int64][]int64)
	buf := make([]byte, 8)
	segments := dn.replica.filterSegments(dn.channelName, partID)
	for _, pk := range pks {
		for _, segment := range segments {
			binary.BigEndian.PutUint64(buf, uint64(pk))
			exist := segment.pkFilter.Test(buf)
			if exist {
				result[pk] = append(result[pk], segment.segmentID)
			}
		}
	}
	return result
}

func (dn *deleteNode) flushDelData(collID UniqueID, timeRange TimeRange) {
	schema, err := dn.replica.getCollectionSchema(collID, timeRange.timestampMax)
	if err != nil {
		log.Error("failed to get collection schema", zap.Error(err))
		return
	}

	delCodec := storage.NewDeleteCodec(&etcdpb.CollectionMeta{
		ID:     collID,
		Schema: schema,
	})

	kvs := make(map[string]string)
	// buffer data to binlogs
	dn.delBuf.Range(func(k, v interface{}) bool {
		segID := k.(int64)
		delDataBuf := v.(*DelDataBuf)
		collID, partID, err := dn.replica.getCollectionAndPartitionID(segID)
		if err != nil {
			log.Error("failed to get collection ID and partition ID", zap.Error(err))
			return false
		}

		blob, err := delCodec.Serialize(partID, segID, delDataBuf.delData)
		if err != nil {
			log.Error("failed to serialize delete data", zap.Error(err))
			return false
		}

		// write insert binlog
		logID, err := dn.idAllocator.allocID()
		if err != nil {
			log.Error("failed to alloc ID", zap.Error(err))
			return false
		}

		blobKey, _ := dn.idAllocator.genKey(false, collID, partID, segID, logID)
		blobPath := path.Join(Params.DeleteBinlogRootPath, blobKey)
		kvs[blobPath] = string(blob.Value[:])
		log.Debug("delete blob path", zap.String("path", blobPath))

		return true
	})

	if len(kvs) > 0 {
		err = dn.minIOKV.MultiSave(kvs)
		if err != nil {
			log.Error("failed to save minIO ..", zap.Error(err))
		}
		log.Debug("save delete blobs to minIO successfully")
	}
}

func newDeleteNode(ctx context.Context, flushCh <-chan *flushMsg, config *nodeConfig) (*deleteNode, error) {
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

	return &deleteNode{
		BaseNode: baseNode,
		delBuf:   sync.Map{},
		flushCh:  flushCh,
		minIOKV:  minIOKV,

		replica:     config.replica,
		idAllocator: config.allocator,
		channelName: config.vChannelName,
	}, nil
}
