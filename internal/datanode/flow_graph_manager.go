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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type FlowgraphManager interface {
	AddFlowgraph(ds *dataSyncService)
	AddandStartWithEtcdTickler(dn *DataNode, vchan *datapb.VchannelInfo, schema *schemapb.CollectionSchema, tickler *etcdTickler) error
	RemoveFlowgraph(channel string)
	ClearFlowgraphs()

	GetFlowgraphService(channel string) (*dataSyncService, bool)
	HasFlowgraph(channel string) bool
	HasFlowgraphWithOpID(channel string, opID UniqueID) bool
	GetFlowgraphCount() int
	GetCollectionIDs() []int64

	Close()
}

var _ FlowgraphManager = (*fgManagerImpl)(nil)

type fgManagerImpl struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	flowgraphs *typeutil.ConcurrentMap[string, *dataSyncService]
}

func newFlowgraphManager() *fgManagerImpl {
	ctx, cancelFunc := context.WithCancel(context.TODO())
	return &fgManagerImpl{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		flowgraphs: typeutil.NewConcurrentMap[string, *dataSyncService](),
	}
}

func (fm *fgManagerImpl) AddFlowgraph(ds *dataSyncService) {
	fm.flowgraphs.Insert(ds.vchannelName, ds)
	metrics.DataNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
}

func (fm *fgManagerImpl) AddandStartWithEtcdTickler(dn *DataNode, vchan *datapb.VchannelInfo, schema *schemapb.CollectionSchema, tickler *etcdTickler) error {
	log := log.With(zap.String("channel", vchan.GetChannelName()))
	if fm.flowgraphs.Contain(vchan.GetChannelName()) {
		log.Warn("try to add an existed DataSyncService")
		return nil
	}

	dataSyncService, err := newServiceWithEtcdTickler(fm.ctx, dn, &datapb.ChannelWatchInfo{
		Schema: schema,
		Vchan:  vchan,
	}, tickler)
	if err != nil {
		log.Warn("fail to create new DataSyncService", zap.Error(err))
		return err
	}
	dataSyncService.start()
	fm.flowgraphs.Insert(vchan.GetChannelName(), dataSyncService)

	metrics.DataNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
	return nil
}

func (fm *fgManagerImpl) RemoveFlowgraph(channel string) {
	if fg, loaded := fm.flowgraphs.Get(channel); loaded {
		fg.close()
		fm.flowgraphs.Remove(channel)

		metrics.DataNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()
		rateCol.removeFlowGraphChannel(channel)
	}
}

func (fm *fgManagerImpl) ClearFlowgraphs() {
	log.Info("start drop all flowgraph resources in DataNode")
	fm.flowgraphs.Range(func(key string, value *dataSyncService) bool {
		value.GracefullyClose()
		fm.flowgraphs.GetAndRemove(key)

		log.Info("successfully dropped flowgraph", zap.String("vChannelName", key))
		return true
	})
}

func (fm *fgManagerImpl) GetFlowgraphService(channel string) (*dataSyncService, bool) {
	return fm.flowgraphs.Get(channel)
}

func (fm *fgManagerImpl) HasFlowgraph(channel string) bool {
	_, exist := fm.flowgraphs.Get(channel)
	return exist
}

func (fm *fgManagerImpl) HasFlowgraphWithOpID(channel string, opID UniqueID) bool {
	ds, exist := fm.flowgraphs.Get(channel)
	return exist && ds.opID == opID
}

// GetFlowgraphCount returns number of flow graphs.
func (fm *fgManagerImpl) GetFlowgraphCount() int {
	return fm.flowgraphs.Len()
}

func (fm *fgManagerImpl) GetCollectionIDs() []int64 {
	collectionSet := typeutil.UniqueSet{}
	fm.flowgraphs.Range(func(key string, value *dataSyncService) bool {
		collectionSet.Insert(value.metacache.Collection())
		return true
	})

	return collectionSet.Collect()
}

func (fm *fgManagerImpl) Close() {
	fm.cancelFunc()
}
