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
	"fmt"
	"math"
	"reflect"
	"sync"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/trace"
)

type (
	// DeleteData record deleted IDs and Timestamps
	DeleteData = storage.DeleteData
)

// DeleteNode is to process delete msg, flush delete info into storage.
type deleteNode struct {
	BaseNode

	ctx          context.Context
	channelName  string
	delBuf       sync.Map // map[segmentID]*DelDataBuf
	replica      Replica
	idAllocator  allocatorInterface
	flushManager flushManager

	clearSignal chan<- string
}

// DelDataBuf buffers insert data, monitoring buffer size and limit
// size and limit both indicate numOfRows
type DelDataBuf struct {
	datapb.Binlog
	delData *DeleteData
}

func (ddb *DelDataBuf) updateSize(size int64) {
	ddb.EntriesNum += size
}

func (ddb *DelDataBuf) updateTimeRange(tr TimeRange) {
	if tr.timestampMin < ddb.TimestampFrom {
		ddb.TimestampFrom = tr.timestampMin
	}
	if tr.timestampMax > ddb.TimestampTo {
		ddb.TimestampTo = tr.timestampMax
	}
}

func (ddb *DelDataBuf) updateFromBuf(buf *DelDataBuf) {
	ddb.updateSize(buf.EntriesNum)

	tr := TimeRange{timestampMax: buf.TimestampTo, timestampMin: buf.TimestampFrom}
	ddb.updateTimeRange(tr)

	ddb.delData.Pks = append(ddb.delData.Pks, buf.delData.Pks...)
	ddb.delData.Tss = append(ddb.delData.Tss, buf.delData.Tss...)
}

func newDelDataBuf() *DelDataBuf {
	return &DelDataBuf{
		delData: &DeleteData{},
		Binlog: datapb.Binlog{
			EntriesNum:    0,
			TimestampFrom: math.MaxUint64,
			TimestampTo:   0,
		},
	}
}

func (dn *deleteNode) Name() string {
	return "deleteNode-" + dn.channelName
}

func (dn *deleteNode) Close() {
	log.Info("Flowgraph Delete Node closing")
}

func (dn *deleteNode) bufferDeleteMsg(msg *msgstream.DeleteMsg, tr TimeRange) error {
	log.Debug("bufferDeleteMsg", zap.Any("primary keys", msg.PrimaryKeys), zap.String("vChannelName", dn.channelName))

	// Update delBuf for merged segments
	compactedTo2From := dn.replica.listCompactedSegmentIDs()
	for compactedTo, compactedFrom := range compactedTo2From {
		compactToDelBuff := newDelDataBuf()
		for _, segID := range compactedFrom {
			value, loaded := dn.delBuf.LoadAndDelete(segID)
			if loaded {
				compactToDelBuff.updateFromBuf(value.(*DelDataBuf))
			}
		}
		dn.delBuf.Store(compactedTo, compactToDelBuff)
		dn.replica.removeSegments(compactedFrom...)
		log.Debug("update delBuf for merged segments",
			zap.Int64("compactedTo segmentID", compactedTo),
			zap.Int64s("compactedFrom segmentIDs", compactedFrom),
		)
	}

	primaryKeys := storage.ParseIDs2PrimaryKeys(msg.PrimaryKeys)
	segIDToPks, segIDToTss := dn.filterSegmentByPK(msg.PartitionID, primaryKeys, msg.Timestamps)

	for segID, pks := range segIDToPks {
		rows := len(pks)
		tss, ok := segIDToTss[segID]
		if !ok || rows != len(tss) {
			return fmt.Errorf("primary keys and timestamp's element num mis-match, segmentID = %d", segID)
		}

		var delDataBuf *DelDataBuf
		value, ok := dn.delBuf.Load(segID)
		if ok {
			delDataBuf = value.(*DelDataBuf)
		} else {
			delDataBuf = newDelDataBuf()
		}
		delData := delDataBuf.delData

		for i := 0; i < rows; i++ {
			delData.Pks = append(delData.Pks, pks[i])
			delData.Tss = append(delData.Tss, tss[i])
			log.Debug("delete",
				zap.Any("primary key", pks[i]),
				zap.Uint64("ts", tss[i]),
				zap.Int64("segmentID", segID),
				zap.String("vChannelName", dn.channelName))
		}

		// store
		delDataBuf.updateSize(int64(rows))
		metrics.DataNodeConsumeMsgRowsCount.WithLabelValues(metrics.DeleteLabel, fmt.Sprint(Params.DataNodeCfg.GetNodeID())).Add(float64(rows))
		delDataBuf.updateTimeRange(tr)
		dn.delBuf.Store(segID, delDataBuf)
	}

	return nil
}

func (dn *deleteNode) showDelBuf() {
	segments := dn.replica.filterSegments(dn.channelName, common.InvalidPartitionID)
	for _, seg := range segments {
		segID := seg.segmentID
		if v, ok := dn.delBuf.Load(segID); ok {
			delDataBuf, _ := v.(*DelDataBuf)
			log.Debug("delta buffer status",
				zap.Int64("segment ID", segID),
				zap.Int64("size", delDataBuf.GetEntriesNum()),
				zap.String("vChannel", dn.channelName))
			// TODO control the printed length
			length := len(delDataBuf.delData.Pks)
			for i := 0; i < length; i++ {
				log.Debug("del data",
					zap.Any("pk", delDataBuf.delData.Pks[i]),
					zap.Uint64("ts", delDataBuf.delData.Tss[i]),
					zap.Int64("segment ID", segID),
					zap.String("vChannel", dn.channelName),
				)
			}
		} else {
			log.Debug("segment not exist",
				zap.Int64("segment ID", segID),
				zap.String("vChannel", dn.channelName))
		}
	}
}

// Operate implementing flowgraph.Node, performs delete data process
func (dn *deleteNode) Operate(in []Msg) []Msg {
	//log.Debug("deleteNode Operating")

	if len(in) != 1 {
		log.Error("Invalid operate message input in deleteNode", zap.Int("input length", len(in)))
		return nil
	}

	fgMsg, ok := in[0].(*flowGraphMsg)
	if !ok {
		if in[0] == nil {
			log.Debug("type assertion failed for flowGraphMsg because it's nil")
		} else {
			log.Warn("type assertion failed for flowGraphMsg", zap.String("name", reflect.TypeOf(in[0]).Name()))
		}
		return []Msg{}
	}

	var spans []opentracing.Span
	for _, msg := range fgMsg.deleteMessages {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	for i, msg := range fgMsg.deleteMessages {
		traceID, _, _ := trace.InfoFromSpan(spans[i])
		log.Info("Buffer delete request in DataNode", zap.String("traceID", traceID))

		err := dn.bufferDeleteMsg(msg, fgMsg.timeRange)
		if err != nil {
			// error occurs only when deleteMsg is misaligned, should not happen
			err = fmt.Errorf("buffer delete msg failed, err = %s", err)
			log.Error(err.Error())
			panic(err)
		}
	}

	// show all data in dn.delBuf
	if len(fgMsg.deleteMessages) != 0 {
		dn.showDelBuf()
	}

	// handle flush
	if len(fgMsg.segmentsToFlush) > 0 {
		log.Debug("DeleteNode receives flush message",
			zap.Int64s("segIDs", fgMsg.segmentsToFlush),
			zap.String("vChannelName", dn.channelName))
		for _, segmentToFlush := range fgMsg.segmentsToFlush {
			buf, ok := dn.delBuf.Load(segmentToFlush)
			if !ok {
				// no related delta data to flush, send empty buf to complete flush life-cycle
				dn.flushManager.flushDelData(nil, segmentToFlush, fgMsg.endPositions[0])
			} else {
				err := retry.Do(dn.ctx, func() error {
					return dn.flushManager.flushDelData(buf.(*DelDataBuf), segmentToFlush, fgMsg.endPositions[0])
				}, flowGraphRetryOpt)
				if err != nil {
					err = fmt.Errorf("failed to flush delete data, err = %s", err)
					log.Error(err.Error())
					panic(err)
				}
				// remove delete buf
				dn.delBuf.Delete(segmentToFlush)
			}
		}
	}

	// drop collection signal, delete node shall notify flush manager all data are cleared and send signal to DataSyncService cleaner
	if fgMsg.dropCollection {
		dn.flushManager.notifyAllFlushed()
		log.Debug("DeleteNode notifies BackgroundGC to release vchannel", zap.String("vChannelName", dn.channelName))
		dn.clearSignal <- dn.channelName
	}

	for _, sp := range spans {
		sp.Finish()
	}
	return nil
}

// filterSegmentByPK returns the bloom filter check result.
// If the key may exists in the segment, returns it in map.
// If the key not exists in the segment, the segment is filter out.
func (dn *deleteNode) filterSegmentByPK(partID UniqueID, pks []primaryKey, tss []Timestamp) (map[UniqueID][]primaryKey, map[UniqueID][]uint64) {
	segID2Pks := make(map[UniqueID][]primaryKey)
	segID2Tss := make(map[UniqueID][]uint64)
	buf := make([]byte, 8)
	segments := dn.replica.filterSegments(dn.channelName, partID)
	for index, pk := range pks {
		for _, segment := range segments {
			segmentID := segment.segmentID
			exist := false
			switch pk.Type() {
			case schemapb.DataType_Int64:
				int64Pk := pk.(*int64PrimaryKey)
				common.Endian.PutUint64(buf, uint64(int64Pk.Value))
				exist = segment.pkFilter.Test(buf)
			case schemapb.DataType_VarChar:
				varCharPk := pk.(*varCharPrimaryKey)
				exist = segment.pkFilter.TestString(varCharPk.Value)
			default:
				//TODO::
			}
			if exist {
				segID2Pks[segmentID] = append(segID2Pks[segmentID], pk)
				segID2Tss[segmentID] = append(segID2Tss[segmentID], tss[index])
			}
		}
	}

	return segID2Pks, segID2Tss
}

func newDeleteNode(ctx context.Context, fm flushManager, sig chan<- string, config *nodeConfig) (*deleteNode, error) {
	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(config.maxQueueLength)
	baseNode.SetMaxParallelism(config.maxParallelism)

	return &deleteNode{
		ctx:      ctx,
		BaseNode: baseNode,
		delBuf:   sync.Map{},

		replica:      config.replica,
		idAllocator:  config.allocator,
		channelName:  config.vChannelName,
		flushManager: fm,
		clearSignal:  sig,
	}, nil
}
