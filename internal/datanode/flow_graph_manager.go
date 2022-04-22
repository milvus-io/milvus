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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"

	"go.uber.org/zap"
)

type flowgraphManager struct {
	flowgraphs sync.Map // vChannelName -> dataSyncService
}

func newFlowgraphManager() *flowgraphManager {
	return &flowgraphManager{}
}

func (fm *flowgraphManager) addAndStart(dn *DataNode, vchan *datapb.VchannelInfo) error {
	log.Info("received Vchannel Info",
		zap.String("vChannelName", vchan.GetChannelName()),
		zap.Int("Unflushed Segment Number", len(vchan.GetUnflushedSegments())),
		zap.Int("Flushed Segment Number", len(vchan.GetFlushedSegments())),
	)

	if _, ok := fm.flowgraphs.Load(vchan.GetChannelName()); ok {
		log.Warn("try to add an existed DataSyncService", zap.String("vChannelName", vchan.GetChannelName()))
		return nil
	}

	replica, err := newReplica(dn.ctx, dn.rootCoord, dn.chunkManager, vchan.GetCollectionID())
	if err != nil {
		log.Warn("new replica failed", zap.String("vChannelName", vchan.GetChannelName()), zap.Error(err))
		return err
	}

	var alloc allocatorInterface = newAllocator(dn.rootCoord)

	dataSyncService, err := newDataSyncService(dn.ctx, make(chan flushMsg, 100), replica, alloc, dn.factory, vchan, dn.clearSignal, dn.dataCoord, dn.segmentCache, dn.chunkManager, dn.compactionExecutor)
	if err != nil {
		log.Warn("new data sync service fail", zap.String("vChannelName", vchan.GetChannelName()), zap.Error(err))
		return err
	}
	log.Info("successfully created dataSyncService", zap.String("vChannelName", vchan.GetChannelName()))

	dataSyncService.start()
	log.Info("successfully started dataSyncService", zap.String("vChannelName", vchan.GetChannelName()))

	fm.flowgraphs.Store(vchan.GetChannelName(), dataSyncService)

	metrics.DataNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.GetNodeID())).Inc()
	return nil
}

func (fm *flowgraphManager) release(vchanName string) {
	log.Info("release flowgraph resources begin", zap.String("vChannelName", vchanName))

	if fg, loaded := fm.flowgraphs.LoadAndDelete(vchanName); loaded {
		fg.(*dataSyncService).close()
		metrics.DataNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.GetNodeID())).Dec()
	}
	log.Info("release flowgraph resources end", zap.String("Vchannel", vchanName))
}

func (fm *flowgraphManager) getFlushCh(segID UniqueID) (chan<- flushMsg, error) {
	var (
		flushCh chan flushMsg
		loaded  = false
	)

	fm.flowgraphs.Range(func(key, value interface{}) bool {
		fg := value.(*dataSyncService)
		if fg.replica.hasSegment(segID, true) {
			loaded = true
			flushCh = fg.flushCh
			return false
		}
		return true
	})

	if loaded {
		return flushCh, nil
	}

	return nil, fmt.Errorf("cannot find segment %d in all flowgraphs", segID)
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

func (fm *flowgraphManager) dropAll() {
	log.Info("start drop all flowgraph resources in DataNode")
	fm.flowgraphs.Range(func(key, value interface{}) bool {
		value.(*dataSyncService).close()
		fm.flowgraphs.Delete(key.(string))

		log.Info("successfully dropped flowgraph", zap.String("vChannelName", key.(string)))
		return true
	})
}
