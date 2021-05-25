// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datanode

import (
	"context"
	"time"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/retry"
	"go.etcd.io/etcd/clientv3"

	"go.uber.org/zap"
)

type dataSyncService struct {
	ctx          context.Context
	fg           *flowgraph.TimeTickedFlowGraph
	flushChan    <-chan *flushMsg
	replica      Replica
	idAllocator  allocatorInterface
	msFactory    msgstream.Factory
	collectionID UniqueID
}

func newDataSyncService(ctx context.Context,
	flushChan <-chan *flushMsg,
	replica Replica,
	alloc allocatorInterface,
	factory msgstream.Factory,
	vchanPair *datapb.VchannelPair) *dataSyncService {

	service := &dataSyncService{
		ctx:          ctx,
		fg:           nil,
		flushChan:    flushChan,
		replica:      replica,
		idAllocator:  alloc,
		msFactory:    factory,
		collectionID: vchanPair.GetCollectionID(),
	}

	service.initNodes(vchanPair)
	return service
}

// func (dsService *dataSyncService) init() {
// if len(Params.InsertChannelNames) == 0 {
//     log.Error("InsertChannels not readly, init datasync service failed")
//     return
// }

//     dsService.initNodes()
// }

func (dsService *dataSyncService) start() {
	log.Debug("Data Sync Service Start Successfully")
	if dsService.fg != nil {
		dsService.fg.Start()
	} else {
		log.Debug("Data Sync Service flowgraph nil")
	}
}

func (dsService *dataSyncService) close() {
	if dsService.fg != nil {
		dsService.fg.Close()
	}
}

func (dsService *dataSyncService) initNodes(vchanPair *datapb.VchannelPair) {
	// TODO: add delete pipeline support
	var kvClient *clientv3.Client
	var err error
	connectEtcdFn := func() error {
		kvClient, err = clientv3.New(clientv3.Config{Endpoints: []string{Params.EtcdAddress}})
		if err != nil {
			return err
		}
		return nil
	}
	err = retry.Retry(100000, time.Millisecond*200, connectEtcdFn)
	if err != nil {
		panic(err)
	}

	etcdKV := etcdkv.NewEtcdKV(kvClient, Params.MetaRootPath)
	// New binlogMeta
	mt, _ := NewBinlogMeta(etcdKV, dsService.idAllocator)

	dsService.fg = flowgraph.NewTimeTickedFlowGraph(dsService.ctx)

	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024,
	}

	err = dsService.msFactory.SetParams(m)
	if err != nil {
		panic(err)
	}

	var dmStreamNode Node = newDmInputNode(dsService.ctx, dsService.msFactory, vchanPair.GetDmlVchannelName(), vchanPair.GetDmlPosition())
	var ddStreamNode Node = newDDInputNode(dsService.ctx, dsService.msFactory, vchanPair.GetDdlVchannelName(), vchanPair.GetDdlPosition())

	var filterDmNode Node = newFilteredDmNode()
	var ddNode Node = newDDNode(dsService.ctx, mt, dsService.flushChan, dsService.replica, dsService.idAllocator)
	var insertBufferNode Node = newInsertBufferNode(dsService.ctx, mt, dsService.replica, dsService.msFactory, dsService.idAllocator)
	var gcNode Node = newGCNode(dsService.replica)

	dsService.fg.AddNode(dmStreamNode)
	dsService.fg.AddNode(ddStreamNode)

	dsService.fg.AddNode(filterDmNode)
	dsService.fg.AddNode(ddNode)

	dsService.fg.AddNode(insertBufferNode)
	dsService.fg.AddNode(gcNode)

	// dmStreamNode
	err = dsService.fg.SetEdges(dmStreamNode.Name(),
		[]string{},
		[]string{filterDmNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", dmStreamNode.Name()), zap.Error(err))
		panic("set edges faild in the node")
	}

	// ddStreamNode
	err = dsService.fg.SetEdges(ddStreamNode.Name(),
		[]string{},
		[]string{ddNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", ddStreamNode.Name()), zap.Error(err))
		panic("set edges faild in the node")
	}

	// filterDmNode
	err = dsService.fg.SetEdges(filterDmNode.Name(),
		[]string{dmStreamNode.Name(), ddNode.Name()},
		[]string{insertBufferNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", filterDmNode.Name()), zap.Error(err))
		panic("set edges faild in the node")
	}

	// ddNode
	err = dsService.fg.SetEdges(ddNode.Name(),
		[]string{ddStreamNode.Name()},
		[]string{filterDmNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", ddNode.Name()), zap.Error(err))
		panic("set edges faild in the node")
	}

	// insertBufferNode
	err = dsService.fg.SetEdges(insertBufferNode.Name(),
		[]string{filterDmNode.Name()},
		[]string{gcNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", insertBufferNode.Name()), zap.Error(err))
		panic("set edges faild in the node")
	}

	// gcNode
	err = dsService.fg.SetEdges(gcNode.Name(),
		[]string{insertBufferNode.Name()},
		[]string{})
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", gcNode.Name()), zap.Error(err))
		panic("set edges faild in the node")
	}
}
