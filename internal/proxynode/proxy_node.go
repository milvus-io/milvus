package proxynode

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"

	"errors"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type NodeImpl struct {
	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

	initParams *internalpb2.InitParams
	ip         string
	port       int

	stateCode atomic.Value

	masterClient       MasterClient
	indexServiceClient IndexServiceClient
	dataServiceClient  DataServiceClient
	proxyServiceClient ProxyServiceClient
	queryServiceClient QueryServiceClient

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

func NewProxyNodeImpl(ctx context.Context, factory msgstream.Factory) (*NodeImpl, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	node := &NodeImpl{
		ctx:       ctx1,
		cancel:    cancel,
		msFactory: factory,
	}
	node.UpdateStateCode(internalpb2.StateCode_ABNORMAL)
	return node, nil

}

type Component interface {
	GetComponentStates(ctx context.Context) (*internalpb2.ComponentStates, error)
}

func (node *NodeImpl) waitForServiceReady(ctx context.Context, service Component, serviceName string) error {

	checkFunc := func() error {
		resp, err := service.GetComponentStates(ctx)
		if err != nil {
			return err
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			return errors.New(resp.Status.Reason)
		}
		if resp.State.StateCode != internalpb2.StateCode_HEALTHY {
			return errors.New("")
		}
		return nil
	}
	// wait for 10 seconds
	err := retry.Retry(200, time.Millisecond*200, checkFunc)
	if err != nil {
		errMsg := fmt.Sprintf("ProxyNode wait for %s ready failed", serviceName)
		return errors.New(errMsg)
	}
	return nil
}

func (node *NodeImpl) Init() error {
	// todo wait for proxyservice state changed to Healthy
	ctx := context.Background()

	err := node.waitForServiceReady(ctx, node.proxyServiceClient, "ProxyService")
	if err != nil {
		return err
	}
	log.Println("service was ready ...")

	request := &proxypb.RegisterNodeRequest{
		Address: &commonpb.Address{
			Ip:   Params.IP,
			Port: int64(Params.NetworkPort),
		},
	}

	response, err := node.proxyServiceClient.RegisterNode(ctx, request)
	if err != nil {
		return err
	}
	if response.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return errors.New(response.Status.Reason)
	}

	err = Params.LoadConfigFromInitParams(response.InitParams)
	if err != nil {
		return err
	}

	// wait for dataservice state changed to Healthy
	if node.dataServiceClient != nil {
		err = node.waitForServiceReady(ctx, node.dataServiceClient, "DataService")
		if err != nil {
			return err
		}
	}

	// wait for queryservice state changed to Healthy
	if node.queryServiceClient != nil {
		err = node.waitForServiceReady(ctx, node.queryServiceClient, "QueryService")
		if err != nil {
			return err
		}
	}

	// wait for indexservice state changed to Healthy
	if node.indexServiceClient != nil {
		err = node.waitForServiceReady(ctx, node.indexServiceClient, "IndexService")
		if err != nil {
			return err
		}
	}

	if node.queryServiceClient != nil {
		resp, err := node.queryServiceClient.CreateQueryChannel(ctx)
		if err != nil {
			return err
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			return errors.New(resp.Status.Reason)
		}

		Params.SearchChannelNames = []string{resp.RequestChannel}
		Params.SearchResultChannelNames = []string{resp.ResultChannel}
	}

	// todo
	//Params.InsertChannelNames, err = node.dataServiceClient.GetInsertChannels()
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
	log.Println("create query message stream ...")

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

	segAssigner, err := NewSegIDAssigner(node.ctx, node.dataServiceClient, node.lastTick)
	if err != nil {
		panic(err)
	}
	node.segAssigner = segAssigner
	node.segAssigner.PeerID = Params.ProxyID

	node.manipulationMsgStream, _ = node.msFactory.NewMsgStream(node.ctx)
	node.manipulationMsgStream.AsProducer(Params.InsertChannelNames)
	repackFuncImpl := func(tsMsgs []msgstream.TsMsg, hashKeys [][]int32) (map[int32]*msgstream.MsgPack, error) {
		return insertRepackFunc(tsMsgs, hashKeys, node.segAssigner, true)
	}
	node.manipulationMsgStream.SetRepackFunc(repackFuncImpl)
	log.Println("create manipulation message stream ...")

	node.sched, err = NewTaskScheduler(node.ctx, node.idAllocator, node.tsoAllocator, node.msFactory)
	if err != nil {
		return err
	}

	node.tick = newTimeTick(node.ctx, node.tsoAllocator, time.Millisecond*200, node.sched.TaskDoneTest, node.msFactory)

	return nil
}

func (node *NodeImpl) Start() error {
	err := InitMetaCache(node.masterClient)
	if err != nil {
		return err
	}
	log.Println("init global meta cache ...")

	initGlobalInsertChannelsMap(node)
	log.Println("init global insert channels map ...")

	node.manipulationMsgStream.Start()
	log.Println("start manipulation message stream ...")

	node.queryMsgStream.Start()
	log.Println("start query message stream ...")

	node.sched.Start()
	log.Println("start scheduler ...")

	node.idAllocator.Start()
	log.Println("start id allocator ...")

	node.tsoAllocator.Start()
	log.Println("start tso allocator ...")

	node.segAssigner.Start()
	log.Println("start seg assigner ...")

	node.tick.Start()
	log.Println("start time tick ...")

	// Start callbacks
	for _, cb := range node.startCallbacks {
		cb()
	}

	node.UpdateStateCode(internalpb2.StateCode_HEALTHY)
	log.Println("proxy node is healthy ...")

	return nil
}

func (node *NodeImpl) Stop() error {
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
func (node *NodeImpl) AddStartCallback(callbacks ...func()) {
	node.startCallbacks = append(node.startCallbacks, callbacks...)
}

func (node *NodeImpl) lastTick() Timestamp {
	return node.tick.LastTick()
}

// AddCloseCallback adds a callback in the Close phase.
func (node *NodeImpl) AddCloseCallback(callbacks ...func()) {
	node.closeCallbacks = append(node.closeCallbacks, callbacks...)
}

func (node *NodeImpl) SetMasterClient(cli MasterClient) {
	node.masterClient = cli
}

func (node *NodeImpl) SetIndexServiceClient(cli IndexServiceClient) {
	node.indexServiceClient = cli
}

func (node *NodeImpl) SetDataServiceClient(cli DataServiceClient) {
	node.dataServiceClient = cli
}

func (node *NodeImpl) SetProxyServiceClient(cli ProxyServiceClient) {
	node.proxyServiceClient = cli
}

func (node *NodeImpl) SetQueryServiceClient(cli QueryServiceClient) {
	node.queryServiceClient = cli
}
