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
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/paramtable"

	"go.uber.org/zap"
)

type flowgraphManager struct {
	flowgraphs sync.Map // vChannelName -> dataSyncService
}

func newFlowgraphManager() *flowgraphManager {
	return &flowgraphManager{}
}

func (fm *flowgraphManager) addAndStart(dn *DataNode, vchan *datapb.VchannelInfo, schema *schemapb.CollectionSchema, tickler *tickler) error {
	if _, ok := fm.flowgraphs.Load(vchan.GetChannelName()); ok {
		log.Warn("try to add an existed DataSyncService", zap.String("vChannelName", vchan.GetChannelName()))
		return nil
	}

	channel := newChannel(vchan.GetChannelName(), vchan.GetCollectionID(), schema, dn.rootCoord, dn.chunkManager)

	var alloc allocatorInterface = newAllocator(dn.rootCoord)

	dataSyncService, err := newDataSyncService(dn.ctx, make(chan flushMsg, 100), make(chan resendTTMsg, 100), channel,
		alloc, dn.dispClient, dn.factory, vchan, dn.clearSignal, dn.dataCoord, dn.segmentCache, dn.chunkManager, dn.compactionExecutor, tickler, dn.GetSession().ServerID)
	if err != nil {
		log.Warn("new data sync service fail", zap.String("vChannelName", vchan.GetChannelName()), zap.Error(err))
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

	return nil, fmt.Errorf("cannot find segment %d in all flowgraphs", segID)
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
