package proxynode

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/types"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type ProxyNode struct {
	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

	initParams *internalpb.InitParams
	ip         string
	port       int

	stateCode atomic.Value

	masterService types.MasterService
	indexService  types.IndexService
	dataService   types.DataService
	proxyService  types.ProxyService
	queryService  types.QueryService

	sched *TaskScheduler
	tick  *timeTick

	idAllocator  *allocator.IDAllocator
	tsoAllocator *allocator.TimestampAllocator
	segAssigner  *SegIDAssigner

	manipulationMsgStream msgstream.MsgStream
	queryMsgStream        msgstream.MsgStream
	msFactory             msgstream.Factory

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

func NewProxyNode(ctx context.Context, factory msgstream.Factory) (*ProxyNode, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	node := &ProxyNode{
		ctx:       ctx1,
		cancel:    cancel,
		msFactory: factory,
	}
	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	return node, nil

}

func (node *ProxyNode) Init() error {
	// todo wait for proxyservice state changed to Healthy
	ctx := context.Background()

	err := funcutil.WaitForComponentHealthy(ctx, node.proxyService, "ProxyService", 100, time.Millisecond*200)
	if err != nil {
		return err
	}
	log.Debug("service was ready ...")

	request := &proxypb.RegisterNodeRequest{
		Address: &commonpb.Address{
			Ip:   Params.IP,
			Port: int64(Params.NetworkPort),
		},
	}

	response, err := node.proxyService.RegisterNode(ctx, request)
	if err != nil {
		return err
	}
	if response.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(response.Status.Reason)
	}

	err = Params.LoadConfigFromInitParams(response.InitParams)
	if err != nil {
		return err
	}

	// wait for dataservice state changed to Healthy
	if node.dataService != nil {
		err := funcutil.WaitForComponentHealthy(ctx, node.dataService, "DataService", 100, time.Millisecond*200)
		if err != nil {
			return err
		}
	}

	// wait for queryService state changed to Healthy
	if node.queryService != nil {
		err := funcutil.WaitForComponentHealthy(ctx, node.queryService, "QueryService", 100, time.Millisecond*200)
		if err != nil {
			return err
		}
	}

	// wait for indexservice state changed to Healthy
	if node.indexService != nil {
		err := funcutil.WaitForComponentHealthy(ctx, node.indexService, "IndexService", 100, time.Millisecond*200)
		if err != nil {
			return err
		}
	}

	if node.queryService != nil {
		resp, err := node.queryService.CreateQueryChannel(ctx)
		if err != nil {
			return err
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Status.Reason)
		}

		Params.SearchChannelNames = []string{resp.RequestChannel}
		Params.SearchResultChannelNames = []string{resp.ResultChannel}
	}

	// todo
	//Params.InsertChannelNames, err = node.dataService.GetInsertChannels()
	//if err != nil {
	//	return err
	//}

	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"ReceiveBufSize": Params.MsgStreamSearchBufSize,
		"PulsarBufSize":  1024}
	err = node.msFactory.SetParams(m)
	if err != nil {
		return err
	}

	node.queryMsgStream, _ = node.msFactory.NewMsgStream(node.ctx)
	node.queryMsgStream.AsProducer(Params.SearchChannelNames)
	// FIXME(wxyu): use log.Debug instead
	log.Debug("proxynode", zap.Strings("proxynode AsProducer:", Params.SearchChannelNames))
	log.Debug("create query message stream ...")

	masterAddr := Params.MasterAddress
	idAllocator, err := allocator.NewIDAllocator(node.ctx, masterAddr)

	if err != nil {
		return err
	}
	node.idAllocator = idAllocator
	node.idAllocator.PeerID = Params.ProxyID

	tsoAllocator, err := allocator.NewTimestampAllocator(node.ctx, masterAddr)
	if err != nil {
		return err
	}
	node.tsoAllocator = tsoAllocator
	node.tsoAllocator.PeerID = Params.ProxyID

	segAssigner, err := NewSegIDAssigner(node.ctx, node.dataService, node.lastTick)
	if err != nil {
		panic(err)
	}
	node.segAssigner = segAssigner
	node.segAssigner.PeerID = Params.ProxyID

	node.manipulationMsgStream, _ = node.msFactory.NewMsgStream(node.ctx)
	node.manipulationMsgStream.AsProducer(Params.InsertChannelNames)
	log.Debug("proxynode", zap.Strings("proxynode AsProducer", Params.InsertChannelNames))
	repackFunc := func(tsMsgs []msgstream.TsMsg, hashKeys [][]int32) (map[int32]*msgstream.MsgPack, error) {
		return insertRepackFunc(tsMsgs, hashKeys, node.segAssigner, true)
	}
	node.manipulationMsgStream.SetRepackFunc(repackFunc)
	log.Debug("create manipulation message stream ...")

	node.sched, err = NewTaskScheduler(node.ctx, node.idAllocator, node.tsoAllocator, node.msFactory)
	if err != nil {
		return err
	}

	node.tick = newTimeTick(node.ctx, node.tsoAllocator, time.Millisecond*200, node.sched.TaskDoneTest, node.msFactory)

	return nil
}

func (node *ProxyNode) Start() error {
	err := InitMetaCache(node.masterService)
	if err != nil {
		return err
	}
	log.Debug("init global meta cache ...")

	initGlobalInsertChannelsMap(node)
	log.Debug("init global insert channels map ...")

	node.manipulationMsgStream.Start()
	log.Debug("start manipulation message stream ...")

	node.queryMsgStream.Start()
	log.Debug("start query message stream ...")

	node.sched.Start()
	log.Debug("start scheduler ...")

	node.idAllocator.Start()
	log.Debug("start id allocator ...")

	node.tsoAllocator.Start()
	log.Debug("start tso allocator ...")

	node.segAssigner.Start()
	log.Debug("start seg assigner ...")

	node.tick.Start()
	log.Debug("start time tick ...")

	// Start callbacks
	for _, cb := range node.startCallbacks {
		cb()
	}

	node.UpdateStateCode(internalpb.StateCode_Healthy)
	log.Debug("proxy node is healthy ...")

	return nil
}

func (node *ProxyNode) Stop() error {
	node.cancel()

	globalInsertChannelsMap.closeAllMsgStream()
	node.tsoAllocator.Close()
	node.idAllocator.Close()
	node.segAssigner.Close()
	node.sched.Close()
	node.manipulationMsgStream.Close()
	node.queryMsgStream.Close()
	node.tick.Close()

	node.wg.Wait()

	for _, cb := range node.closeCallbacks {
		cb()
	}

	return nil
}

// AddStartCallback adds a callback in the startServer phase.
func (node *ProxyNode) AddStartCallback(callbacks ...func()) {
	node.startCallbacks = append(node.startCallbacks, callbacks...)
}

func (node *ProxyNode) lastTick() Timestamp {
	return node.tick.LastTick()
}

// AddCloseCallback adds a callback in the Close phase.
func (node *ProxyNode) AddCloseCallback(callbacks ...func()) {
	node.closeCallbacks = append(node.closeCallbacks, callbacks...)
}

func (node *ProxyNode) SetMasterClient(cli types.MasterService) {
	node.masterService = cli
}

func (node *ProxyNode) SetIndexServiceClient(cli types.IndexService) {
	node.indexService = cli
}

func (node *ProxyNode) SetDataServiceClient(cli types.DataService) {
	node.dataService = cli
}

func (node *ProxyNode) SetProxyServiceClient(cli types.ProxyService) {
	node.proxyService = cli
}

func (node *ProxyNode) SetQueryServiceClient(cli types.QueryService) {
	node.queryService = cli
}
