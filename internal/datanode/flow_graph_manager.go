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
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type flowgraphManager struct {
	flowgraphs *typeutil.ConcurrentMap[string, *dataSyncService]

	closeCh   chan struct{}
	closeOnce sync.Once
}

func newFlowgraphManager() *flowgraphManager {
	return &flowgraphManager{
		flowgraphs: typeutil.NewConcurrentMap[string, *dataSyncService](),
		closeCh:    make(chan struct{}),
	}
}

func (fm *flowgraphManager) start() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-fm.closeCh:
			return
		case <-ticker.C:
			fm.execute(hardware.GetMemoryCount())
		}
	}
}

func (fm *flowgraphManager) close() {
	fm.dropAll()
	fm.closeOnce.Do(func() {
		close(fm.closeCh)
	})
}

func (fm *flowgraphManager) execute(totalMemory uint64) {
	if !Params.DataNodeCfg.MemoryForceSyncEnable.GetAsBool() {
		return
	}

	var total int64
	channels := make([]struct {
		channel    string
		bufferSize int64
	}, 0)
	fm.flowgraphs.Range(func(key string, value *dataSyncService) bool {
		size := value.channel.getTotalMemorySize()
		channels = append(channels, struct {
			channel    string
			bufferSize int64
		}{key, size})
		total += size
		return true
	})
	if len(channels) == 0 {
		return
	}

	memoryWatermark := float64(totalMemory) * Params.DataNodeCfg.MemoryWatermark.GetAsFloat()
	if float64(total) < memoryWatermark {
		log.RatedDebug(5, "skip force sync because memory level is not high enough",
			zap.Float64("current_total_memory_usage", float64(total)),
			zap.Float64("current_memory_watermark", memoryWatermark),
			zap.Any("channel_memory_usages", channels))
		return
	}

	sort.Slice(channels, func(i, j int) bool {
		return channels[i].bufferSize > channels[j].bufferSize
	})
	if fg, ok := fm.flowgraphs.Get(channels[0].channel); ok { // sync the first channel with the largest memory usage
		fg.channel.forceToSync()
		log.Info("notify flowgraph to sync",
			zap.String("channel", channels[0].channel), zap.Int64("bufferSize", channels[0].bufferSize))
	}
}

func (fm *flowgraphManager) addAndStart(dn *DataNode, vchan *datapb.VchannelInfo, schema *schemapb.CollectionSchema, tickler *tickler) error {
	log := log.With(zap.String("channel", vchan.GetChannelName()))
	if fm.flowgraphs.Contain(vchan.GetChannelName()) {
		log.Warn("try to add an existed DataSyncService")
		return nil
	}

	channel := newChannel(vchan.GetChannelName(), vchan.GetCollectionID(), schema, dn.rootCoord, dn.chunkManager)

	dataSyncService, err := newDataSyncService(dn.ctx, make(chan flushMsg, 100), make(chan resendTTMsg, 100), channel,
		dn.allocator, dn.dispClient, dn.factory, vchan, dn.clearSignal, dn.dataCoord, dn.segmentCache, dn.chunkManager, dn.compactionExecutor, tickler, dn.GetSession().ServerID, dn.timeTickSender)
	if err != nil {
		log.Warn("fail to create new datasyncservice", zap.Error(err))
		return err
	}
	dataSyncService.start()
	fm.flowgraphs.Insert(vchan.GetChannelName(), dataSyncService)

	metrics.DataNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
	return nil
}

func (fm *flowgraphManager) release(vchanName string) {
	if fg, loaded := fm.flowgraphs.GetAndRemove(vchanName); loaded {
		fg.close()
		metrics.DataNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()
	}
	rateCol.removeFlowGraphChannel(vchanName)
}

func (fm *flowgraphManager) getFlushCh(segID UniqueID) (chan<- flushMsg, error) {
	var flushCh chan flushMsg

	fm.flowgraphs.Range(func(key string, fg *dataSyncService) bool {
		if fg.channel.hasSegment(segID, true) {
			flushCh = fg.flushCh
			return false
		}
		return true
	})

	if flushCh != nil {
		return flushCh, nil
	}

	return nil, merr.WrapErrSegmentNotFound(segID, "failed to get flush channel has this segment")
}

func (fm *flowgraphManager) getChannel(segID UniqueID) (Channel, error) {
	var (
		rep    Channel
		exists = false
	)
	fm.flowgraphs.Range(func(key string, fg *dataSyncService) bool {
		if fg.channel.hasSegment(segID, true) {
			exists = true
			rep = fg.channel
			return false
		}
		return true
	})

	if exists {
		return rep, nil
	}

	return nil, fmt.Errorf("cannot find segment %d in all flowgraphs", segID)
}

// resendTT loops through flow graphs, looks for segments that are not flushed,
// and sends them to that flow graph's `resendTTCh` channel so stats of
// these segments will be resent.
func (fm *flowgraphManager) resendTT() []UniqueID {
	var unFlushedSegments []UniqueID
	fm.flowgraphs.Range(func(key string, fg *dataSyncService) bool {
		segIDs := fg.channel.listNotFlushedSegmentIDs()
		if len(segIDs) > 0 {
			log.Info("un-flushed segments found, stats will be resend",
				zap.Int64s("segment IDs", segIDs))
			unFlushedSegments = append(unFlushedSegments, segIDs...)
			fg.resendTTCh <- resendTTMsg{
				segmentIDs: segIDs,
			}
		}
		return true
	})
	return unFlushedSegments
}

func (fm *flowgraphManager) getFlowgraphService(vchan string) (*dataSyncService, bool) {
	return fm.flowgraphs.Get(vchan)
}

func (fm *flowgraphManager) exist(vchan string) bool {
	_, exist := fm.getFlowgraphService(vchan)
	return exist
}

// getFlowGraphNum returns number of flow graphs.
func (fm *flowgraphManager) getFlowGraphNum() int {
	return fm.flowgraphs.Len()
}

func (fm *flowgraphManager) dropAll() {
	log.Info("start drop all flowgraph resources in DataNode")
	fm.flowgraphs.Range(func(key string, value *dataSyncService) bool {
		value.close()
		fm.flowgraphs.GetAndRemove(key)

		log.Info("successfully dropped flowgraph", zap.String("vChannelName", key))
		return true
	})
}
