package master

import (
	"context"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

// Server is the pd server.

type (
	UniqueID  = typeutil.UniqueID
	Timestamp = typeutil.Timestamp
)

type Master struct {
	// Server state.
	isServing int64

	// Server start timestamp
	startTimestamp int64

	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	//grpc server
	grpcServer *grpc.Server
	grpcErr    chan error

	kvBase               *kv.EtcdKV
	scheduler            *ddRequestScheduler
	metaTable            *metaTable
	timesSyncMsgProducer *timeSyncMsgProducer

	// tso ticker
	tsoTicker *time.Ticker

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()

	segmentMgr       *SegmentManager
	segmentStatusMsg ms.MsgStream

	//id allocator
	idAllocator *GlobalIDAllocator
	//tso allocator
	tsoAllocator *GlobalTSOAllocator
}

func newKVBase(kvRoot string, etcdAddr []string) *kv.EtcdKV {
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   etcdAddr,
		DialTimeout: 5 * time.Second,
	})
	kvBase := kv.NewEtcdKV(cli, kvRoot)
	return kvBase
}

func Init() {
	rand.Seed(time.Now().UnixNano())
	Params.Init()
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(ctx context.Context) (*Master, error) {
	//Init(etcdAddr, kvRootPath)
	etcdAddress := Params.EtcdAddress
	metaRootPath := Params.EtcdRootPath
	kvRootPath := Params.EtcdRootPath
	pulsarAddr := Params.PulsarAddress

	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
	if err != nil {
		return nil, err
	}
	etcdkv := kv.NewEtcdKV(etcdClient, metaRootPath)
	metakv, err := NewMetaTable(etcdkv)
	if err != nil {
		return nil, err
	}

	//timeSyncMsgProducer
	tsMsgProducer, err := NewTimeSyncMsgProducer(ctx)
	if err != nil {
		return nil, err
	}
	pulsarProxyStream := ms.NewPulsarMsgStream(ctx, 1024) //output stream
	pulsarProxyStream.SetPulsarClient(pulsarAddr)
	pulsarProxyStream.CreatePulsarConsumers(Params.ProxyTimeTickChannelNames, Params.MsgChannelSubName, ms.NewUnmarshalDispatcher(), 1024)
	pulsarProxyStream.Start()
	var proxyStream ms.MsgStream = pulsarProxyStream
	proxyTimeTickBarrier := newSoftTimeTickBarrier(ctx, &proxyStream, Params.ProxyIDList, Params.SoftTimeTickBarrierInterval)
	tsMsgProducer.SetProxyTtBarrier(proxyTimeTickBarrier)

	pulsarWriteStream := ms.NewPulsarMsgStream(ctx, 1024) //output stream
	pulsarWriteStream.SetPulsarClient(pulsarAddr)
	pulsarWriteStream.CreatePulsarConsumers(Params.WriteNodeTimeTickChannelNames, Params.MsgChannelSubName, ms.NewUnmarshalDispatcher(), 1024)
	pulsarWriteStream.Start()
	var writeStream ms.MsgStream = pulsarWriteStream
	writeTimeTickBarrier := newHardTimeTickBarrier(ctx, &writeStream, Params.WriteNodeIDList)
	tsMsgProducer.SetWriteNodeTtBarrier(writeTimeTickBarrier)

	pulsarDMStream := ms.NewPulsarMsgStream(ctx, 1024) //input stream
	pulsarDMStream.SetPulsarClient(pulsarAddr)
	pulsarDMStream.CreatePulsarProducers(Params.InsertChannelNames)
	tsMsgProducer.SetDMSyncStream(pulsarDMStream)

	pulsarK2SStream := ms.NewPulsarMsgStream(ctx, 1024) //input stream
	pulsarK2SStream.SetPulsarClient(pulsarAddr)
	pulsarK2SStream.CreatePulsarProducers(Params.K2SChannelNames)
	tsMsgProducer.SetK2sSyncStream(pulsarK2SStream)

	// stats msg stream
	statsMs := ms.NewPulsarMsgStream(ctx, 1024)
	statsMs.SetPulsarClient(pulsarAddr)
	statsMs.CreatePulsarConsumers([]string{Params.QueryNodeStatsChannelName}, "SegmentStats", ms.NewUnmarshalDispatcher(), 1024)
	statsMs.Start()

	m := &Master{
		ctx:                  ctx,
		startTimestamp:       time.Now().Unix(),
		kvBase:               newKVBase(kvRootPath, []string{etcdAddress}),
		metaTable:            metakv,
		timesSyncMsgProducer: tsMsgProducer,
		grpcErr:              make(chan error),
		segmentStatusMsg:     statsMs,
	}

	//init idAllocator
	m.idAllocator = NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase([]string{etcdAddress}, kvRootPath, "gid"))
	if err := m.idAllocator.Initialize(); err != nil {
		return nil, err
	}

	//init tsoAllocator
	m.tsoAllocator = NewGlobalTSOAllocator("timestamp", tsoutil.NewTSOKVBase([]string{etcdAddress}, kvRootPath, "tso"))
	if err := m.tsoAllocator.Initialize(); err != nil {
		return nil, err
	}

	m.scheduler = NewDDRequestScheduler(func() (UniqueID, error) { return m.idAllocator.AllocOne() })
	m.segmentMgr = NewSegmentManager(metakv,
		func() (UniqueID, error) { return m.idAllocator.AllocOne() },
		func() (Timestamp, error) { return m.tsoAllocator.AllocOne() },
	)

	m.grpcServer = grpc.NewServer()
	masterpb.RegisterMasterServer(m.grpcServer, m)
	return m, nil
}

// AddStartCallback adds a callback in the startServer phase.
func (s *Master) AddStartCallback(callbacks ...func()) {
	s.startCallbacks = append(s.startCallbacks, callbacks...)
}

// AddCloseCallback adds a callback in the Close phase.
func (s *Master) AddCloseCallback(callbacks ...func()) {
	s.closeCallbacks = append(s.closeCallbacks, callbacks...)
}

// Close closes the server.
func (s *Master) Close() {
	if !atomic.CompareAndSwapInt64(&s.isServing, 1, 0) {
		// server is already closed
		return
	}

	log.Print("closing server")

	s.stopServerLoop()

	if s.kvBase != nil {
		s.kvBase.Close()
	}

	// Run callbacks
	for _, cb := range s.closeCallbacks {
		cb()
	}

	log.Print("close server")
}

// IsClosed checks whether server is closed or not.
func (s *Master) IsClosed() bool {
	return atomic.LoadInt64(&s.isServing) == 0
}

func (s *Master) IsServing() bool {
	return !s.IsClosed()
}

// Run runs the pd server.
func (s *Master) Run(grpcPort int64) error {
	if err := s.startServerLoop(s.ctx, grpcPort); err != nil {
		return err
	}
	atomic.StoreInt64(&s.isServing, 1)

	// Run callbacks
	for _, cb := range s.startCallbacks {
		cb()
	}

	return nil
}

// Context returns the context of server.
func (s *Master) Context() context.Context {
	return s.ctx
}

// LoopContext returns the loop context of server.
func (s *Master) LoopContext() context.Context {
	return s.serverLoopCtx
}

func (s *Master) startServerLoop(ctx context.Context, grpcPort int64) error {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(ctx)
	//go s.Se

	s.serverLoopWg.Add(1)
	if err := s.timesSyncMsgProducer.Start(); err != nil {
		return err
	}

	s.serverLoopWg.Add(1)
	go s.grpcLoop(grpcPort)

	if err := <-s.grpcErr; err != nil {
		return err
	}

	s.serverLoopWg.Add(1)
	go s.tasksExecutionLoop()

	s.serverLoopWg.Add(1)
	go s.segmentStatisticsLoop()

	s.serverLoopWg.Add(1)
	go s.tsLoop()

	return nil
}

func (s *Master) stopServerLoop() {
	s.timesSyncMsgProducer.Close()
	s.serverLoopWg.Done()

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
		log.Printf("server is closed, exit grpc server")
	}
	s.serverLoopCancel()
	s.serverLoopWg.Wait()
}

// StartTimestamp returns the start timestamp of this server
func (s *Master) StartTimestamp() int64 {
	return s.startTimestamp
}

func (s *Master) checkGrpcReady(ctx context.Context, targetCh chan error) {
	select {
	case <-time.After(100 * time.Millisecond):
		targetCh <- nil
	case <-ctx.Done():
		return
	}
}

func (s *Master) grpcLoop(grpcPort int64) {
	defer s.serverLoopWg.Done()

	defaultGRPCPort := ":"
	defaultGRPCPort += strconv.FormatInt(grpcPort, 10)
	lis, err := net.Listen("tcp", defaultGRPCPort)
	if err != nil {
		log.Printf("failed to listen: %v", err)
		s.grpcErr <- err
		return
	}
	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	go s.checkGrpcReady(ctx, s.grpcErr)
	if err := s.grpcServer.Serve(lis); err != nil {
		s.grpcErr <- err
	}
}

func (s *Master) tsLoop() {
	defer s.serverLoopWg.Done()
	s.tsoTicker = time.NewTicker(UpdateTimestampStep)
	defer s.tsoTicker.Stop()
	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	for {
		select {
		case <-s.tsoTicker.C:
			if err := s.tsoAllocator.UpdateTSO(); err != nil {
				log.Println("failed to update timestamp", err)
				return
			}
			if err := s.idAllocator.UpdateID(); err != nil {
				log.Println("failed to update id", err)
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Println("tsLoop is closed")
			return
		}
	}
}

func (s *Master) tasksExecutionLoop() {
	defer s.serverLoopWg.Done()
	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()

	for {
		select {
		case task := <-s.scheduler.reqQueue:
			timeStamp, err := (task).Ts()
			if err != nil {
				log.Println(err)
			} else {
				if timeStamp < s.scheduler.scheduleTimeStamp {
					task.Notify(errors.Errorf("input timestamp = %d, schduler timestamp = %d", timeStamp, s.scheduler.scheduleTimeStamp))
				} else {
					s.scheduler.scheduleTimeStamp = timeStamp
					err = task.Execute()
					task.Notify(err)
				}
			}
		case <-ctx.Done():
			log.Print("server is closed, exit task execution loop")
			return
		}
	}
}

func (s *Master) segmentStatisticsLoop() {
	defer s.serverLoopWg.Done()
	defer s.segmentStatusMsg.Close()
	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()

	for {
		select {
		case msg := <-s.segmentStatusMsg.Chan():
			err := s.segmentMgr.HandleQueryNodeMsgPack(msg)
			if err != nil {
				log.Println(err)
			}
		case <-ctx.Done():
			log.Print("server is closed, exit segment statistics loop")
			return
		}
	}
}
