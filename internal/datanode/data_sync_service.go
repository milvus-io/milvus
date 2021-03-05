package datanode

import (
	"context"
	"time"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"
	"go.etcd.io/etcd/clientv3"

	"go.uber.org/zap"
)

type dataSyncService struct {
	ctx         context.Context
	fg          *flowgraph.TimeTickedFlowGraph
	flushChan   chan *flushMsg
	replica     Replica
	idAllocator allocatorInterface
	msFactory   msgstream.Factory
}

func newDataSyncService(ctx context.Context, flushChan chan *flushMsg,
	replica Replica, alloc allocatorInterface, factory msgstream.Factory) *dataSyncService {
	service := &dataSyncService{
		ctx:         ctx,
		fg:          nil,
		flushChan:   flushChan,
		replica:     replica,
		idAllocator: alloc,
		msFactory:   factory,
	}
	return service
}

func (dsService *dataSyncService) init() {
	if len(Params.InsertChannelNames) == 0 {
		log.Error("InsertChannels not readly, init datasync service failed")
		return
	}

	dsService.initNodes()
}

func (dsService *dataSyncService) start() {
	log.Debug("Data Sync Service Start Successfully")
	dsService.fg.Start()
}

func (dsService *dataSyncService) close() {
	if dsService.fg != nil {
		dsService.fg.Close()
	}
}

func (dsService *dataSyncService) initNodes() {
	// TODO: add delete pipeline support
	// New metaTable
	var mt *metaTable
	connectEtcdFn := func() error {
		etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{Params.EtcdAddress}})
		if err != nil {
			return err
		}

		etcdKV := etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)
		mt, err = NewMetaTable(etcdKV)
		if err != nil {
			return err
		}
		return nil
	}
	err := retry.Retry(200, time.Millisecond*200, connectEtcdFn)
	if err != nil {
		panic(err)
	}

	dsService.fg = flowgraph.NewTimeTickedFlowGraph(dsService.ctx)

	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024}
	err = dsService.msFactory.SetParams(m)
	if err != nil {
		panic(err)
	}

	var dmStreamNode Node = newDmInputNode(dsService.ctx, dsService.msFactory)
	var ddStreamNode Node = newDDInputNode(dsService.ctx, dsService.msFactory)

	var filterDmNode Node = newFilteredDmNode()
	var ddNode Node = newDDNode(dsService.ctx, mt, dsService.flushChan, dsService.replica, dsService.idAllocator)
	var insertBufferNode Node = newInsertBufferNode(dsService.ctx, mt, dsService.replica, dsService.idAllocator, dsService.msFactory)
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
