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
)

type flowgraphManager struct {
	flowgraphs sync.Map // vChannelName -> dataSyncService

	closeCh   chan struct{}
	closeOnce sync.Once
}

func newFlowgraphManager() *flowgraphManager {
	return &flowgraphManager{
		closeCh: make(chan struct{}),
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

func (fm *flowgraphManager) stop() {
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
	fm.flowgraphs.Range(func(key, value interface{}) bool {
		size := value.(*dataSyncService).channel.getTotalMemorySize()
		channels = append(channels, struct {
			channel    string
			bufferSize int64
		}{key.(string), size})
		total += size
		return true
	})
	if len(channels) == 0 {
		return
	}

	if float64(total) < float64(totalMemory)*Params.DataNodeCfg.MemoryWatermark.GetAsFloat() {
		return
	}

	sort.Slice(channels, func(i, j int) bool {
		return channels[i].bufferSize > channels[j].bufferSize
	})
	if fg, ok := fm.flowgraphs.Load(channels[0].channel); ok { // sync the first channel with the largest memory usage
		fg.(*dataSyncService).channel.forceToSync()
		log.Info("notify flowgraph to sync",
			zap.String("channel", channels[0].channel), zap.Int64("bufferSize", channels[0].bufferSize))
	}
}

func (fm *flowgraphManager) addAndStart(dn *DataNode, vchan *datapb.VchannelInfo, schema *schemapb.CollectionSchema, tickler *tickler) error {
	log := log.With(zap.String("channel", vchan.GetChannelName()))
	if _, ok := fm.flowgraphs.Load(vchan.GetChannelName()); ok {
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
	fm.flowgraphs.Store(vchan.GetChannelName(), dataSyncService)

	metrics.DataNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
	return nil
}

func (fm *flowgraphManager) release(vchanName string) {
	if fg, loaded := fm.flowgraphs.LoadAndDelete(vchanName); loaded {
		fg.(*dataSyncService).close()
		metrics.DataNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()
	}
	rateCol.removeFlowGraphChannel(vchanName)
}

func (fm *flowgraphManager) getFlushCh(segID UniqueID) (chan<- flushMsg, error) {
	var flushCh chan flushMsg

	fm.flowgraphs.Range(func(key, value interface{}) bool {
		fg := value.(*dataSyncService)
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
	fm.flowgraphs.Range(func(key, value interface{}) bool {
		fg := value.(*dataSyncService)
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
	fm.flowgraphs.Range(func(key, value interface{}) bool {
		fg := value.(*dataSyncService)
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
	fg, ok := fm.flowgraphs.Load(vchan)
	if ok {
		return fg.(*dataSyncService), ok
	}

	return nil, ok
}

func (fm *flowgraphManager) exist(vchan string) bool {
	_, exist := fm.getFlowgraphService(vchan)
	return exist
}

// getFlowGraphNum returns number of flow graphs.
func (fm *flowgraphManager) getFlowGraphNum() int {
	length := 0
	fm.flowgraphs.Range(func(_, _ interface{}) bool {
		length++
		return true
	})
	return length
}

func (fm *flowgraphManager) dropAll() {
	log.Info("start drop all flowgraph resources in DataNode")
	fm.flowgraphs.Range(func(key, value interface{}) bool {
		value.(*dataSyncService).close()
		fm.flowgraphs.Delete(key.(string))

		log.Info("successfully dropped flowgraph", zap.String("vChannelName", key.(string)))
		return true
	})
}
