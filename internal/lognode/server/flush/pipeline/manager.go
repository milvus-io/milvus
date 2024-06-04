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

package pipeline

import (
	"context"
	"fmt"
	"io"

	"github.com/milvus-io/milvus/internal/lognode/server/flush/syncmgr"
	"github.com/milvus-io/milvus/internal/lognode/server/flush/writebuffer"
	"github.com/milvus-io/milvus/internal/lognode/server/timetick/timestamp"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Manager interface {
	AddAndStart(vchan *datapb.VchannelInfo, schema *schemapb.CollectionSchema) error
	Remove(channel string)
	Clear()

	Get(channel string) (*Pipeline, bool)
	Has(channel string) bool
	Count() int
	Foreach(func(ds *Pipeline))

	Close()
}

var _ Manager = (*pipelineManagerImpl)(nil)

type pipelineManagerImpl struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	pipelines  *typeutil.ConcurrentMap[string, *Pipeline]

	syncMgr            syncmgr.SyncManager
	writeBufferManager writebuffer.BufferManager
	chunkManager       storage.ChunkManager
	allocator          timestamp.Allocator

	closer io.Closer

	dispClient msgdispatcher.Client
}

func NewPipelineManager(ctx context.Context, factory dependency.Factory, allocator timestamp.Allocator) (Manager, error) {
	if err := initGlobalRateCollector(); err != nil {
		return nil, err
	}
	ctx, cancelFunc := context.WithCancel(ctx)

	pm := &pipelineManagerImpl{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		allocator:  allocator,
		pipelines:  typeutil.NewConcurrentMap[string, *Pipeline](),
	}
	pm.init(factory, allocator)
	return pm, nil
}

func (pm *pipelineManagerImpl) init(factory dependency.Factory, allocator timestamp.Allocator) error {
	pm.dispClient = msgdispatcher.NewClient(factory, typeutil.DataNodeRole, serverID)
	log.Info("DataNode server init dispatcher client done")

	var err error
	pm.chunkManager, err = factory.NewPersistentStorageChunkManager(pm.ctx)
	if err != nil {
		return err
	}

	pm.syncMgr, err = syncmgr.NewSyncManager(pm.chunkManager, pm.allocator)
	if err != nil {
		log.Error("failed to create sync manager", zap.Error(err))
		return err
	}

	pm.writeBufferManager = writebuffer.NewManager(pm.syncMgr)

	log.Info("DataNode server init succeeded",
		zap.String("MsgChannelSubName", paramtable.Get().CommonCfg.DataNodeSubName.GetValue()))
	return nil
}

func (pm *pipelineManagerImpl) AddAndStart(vchan *datapb.VchannelInfo, schema *schemapb.CollectionSchema) error {
	log := log.With(zap.String("channel", vchan.GetChannelName()))
	if pm.pipelines.Contain(vchan.GetChannelName()) {
		log.Warn("try to add an existed DataSyncService")
		return nil
	}

	dataSyncService, err := newDataSyncService(pm.ctx, pm.server, &datapb.ChannelWatchInfo{
		Schema: schema,
		Vchan:  vchan,
	})
	if err != nil {
		log.Warn("fail to create new DataSyncService", zap.Error(err))
		return err
	}
	dataSyncService.start()
	pm.pipelines.Insert(vchan.GetChannelName(), dataSyncService)

	metrics.DataNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
	return nil
}

func (pm *pipelineManagerImpl) Remove(channel string) {
	if fg, loaded := pm.pipelines.Get(channel); loaded {
		fg.close()
		pm.pipelines.Remove(channel)

		metrics.DataNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()
		RateCol.removeFlowGraphChannel(channel)
	}
}

func (pm *pipelineManagerImpl) Clear() {
	log.Info("start drop all Pipeline resource")
	pm.pipelines.Range(func(key string, value *Pipeline) bool {
		value.GracefullyClose()
		pm.pipelines.GetAndRemove(key)

		log.Info("successfully dropped Pipeline", zap.String("vChannelName", key))
		return true
	})
}

func (pm *pipelineManagerImpl) Get(channel string) (*Pipeline, bool) {
	return pm.pipelines.Get(channel)
}

func (pm *pipelineManagerImpl) Has(channel string) bool {
	_, exist := pm.pipelines.Get(channel)
	return exist
}

// Count returns number of pipelines.
func (pm *pipelineManagerImpl) Count() int {
	return pm.pipelines.Len()
}

func (pm *pipelineManagerImpl) Foreach(fn func(ds *Pipeline)) {
	pm.pipelines.Range(func(key string, value *Pipeline) bool {
		fn(value)
		return true
	})
}

func (pm *pipelineManagerImpl) Close() {
	pm.cancelFunc()
}
