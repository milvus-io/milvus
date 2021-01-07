package proxy

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type Proxy struct {
	proxyLoopCtx    context.Context
	proxyLoopCancel func()
	proxyLoopWg     sync.WaitGroup

	grpcServer   *grpc.Server
	masterConn   *grpc.ClientConn
	masterClient masterpb.MasterClient
	sched        *TaskScheduler
	tick         *timeTick

	idAllocator  *allocator.IDAllocator
	tsoAllocator *allocator.TimestampAllocator
	segAssigner  *allocator.SegIDAssigner

	manipulationMsgStream *msgstream.PulsarMsgStream
	queryMsgStream        *msgstream.PulsarMsgStream

	tracer opentracing.Tracer
	closer io.Closer

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

func Init() {
	Params.Init()
}

func CreateProxy(ctx context.Context) (*Proxy, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	var err error
	p := &Proxy{
		proxyLoopCtx:    ctx1,
		proxyLoopCancel: cancel,
	}

	cfg := &config.Configuration{
		ServiceName: "tracing",
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans: true,
		},
	}
	p.tracer, p.closer, err = cfg.NewTracer(config.Logger(jaeger.StdLogger))
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	opentracing.SetGlobalTracer(p.tracer)

	pulsarAddress := Params.PulsarAddress()

	p.queryMsgStream = msgstream.NewPulsarMsgStream(p.proxyLoopCtx, Params.MsgStreamSearchBufSize())
	p.queryMsgStream.SetPulsarClient(pulsarAddress)
	p.queryMsgStream.CreatePulsarProducers(Params.SearchChannelNames())

	masterAddr := Params.MasterAddress()
	idAllocator, err := allocator.NewIDAllocator(p.proxyLoopCtx, masterAddr)

	if err != nil {
		return nil, err
	}
	p.idAllocator = idAllocator
	p.idAllocator.PeerID = Params.ProxyID()

	tsoAllocator, err := allocator.NewTimestampAllocator(p.proxyLoopCtx, masterAddr)
	if err != nil {
		return nil, err
	}
	p.tsoAllocator = tsoAllocator
	p.tsoAllocator.PeerID = Params.ProxyID()

	segAssigner, err := allocator.NewSegIDAssigner(p.proxyLoopCtx, masterAddr, p.lastTick)
	if err != nil {
		panic(err)
	}
	p.segAssigner = segAssigner
	p.segAssigner.PeerID = Params.ProxyID()

	p.manipulationMsgStream = msgstream.NewPulsarMsgStream(p.proxyLoopCtx, Params.MsgStreamInsertBufSize())
	p.manipulationMsgStream.SetPulsarClient(pulsarAddress)
	p.manipulationMsgStream.CreatePulsarProducers(Params.InsertChannelNames())
	repackFuncImpl := func(tsMsgs []msgstream.TsMsg, hashKeys [][]int32) (map[int32]*msgstream.MsgPack, error) {
		return insertRepackFunc(tsMsgs, hashKeys, p.segAssigner, false)
	}
	p.manipulationMsgStream.SetRepackFunc(repackFuncImpl)

	p.sched, err = NewTaskScheduler(p.proxyLoopCtx, p.idAllocator, p.tsoAllocator)
	if err != nil {
		return nil, err
	}

	p.tick = newTimeTick(p.proxyLoopCtx, p.tsoAllocator, time.Millisecond*200, p.sched.TaskDoneTest)

	return p, nil
}

// AddStartCallback adds a callback in the startServer phase.
func (p *Proxy) AddStartCallback(callbacks ...func()) {
	p.startCallbacks = append(p.startCallbacks, callbacks...)
}

func (p *Proxy) lastTick() Timestamp {
	return p.tick.LastTick()
}

func (p *Proxy) startProxy() error {
	err := p.connectMaster()
	if err != nil {
		return err
	}
	initGlobalMetaCache(p.proxyLoopCtx, p)
	p.manipulationMsgStream.Start()
	p.queryMsgStream.Start()
	p.sched.Start()
	p.idAllocator.Start()
	p.tsoAllocator.Start()
	p.segAssigner.Start()
	p.tick.Start()

	// Start callbacks
	for _, cb := range p.startCallbacks {
		cb()
	}

	p.proxyLoopWg.Add(1)
	go p.grpcLoop()

	return nil
}

// AddCloseCallback adds a callback in the Close phase.
func (p *Proxy) AddCloseCallback(callbacks ...func()) {
	p.closeCallbacks = append(p.closeCallbacks, callbacks...)
}

func (p *Proxy) grpcLoop() {
	defer p.proxyLoopWg.Done()

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(Params.NetworkPort()))
	if err != nil {
		log.Fatalf("Proxy grpc server fatal error=%v", err)
	}

	p.grpcServer = grpc.NewServer()
	servicepb.RegisterMilvusServiceServer(p.grpcServer, p)
	if err = p.grpcServer.Serve(lis); err != nil {
		log.Fatalf("Proxy grpc server fatal error=%v", err)
	}
}

func (p *Proxy) connectMaster() error {
	masterAddr := Params.MasterAddress()
	log.Printf("Proxy connected to master, master_addr=%s", masterAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, masterAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("Proxy connect to master failed, error= %v", err)
		return err
	}
	log.Printf("Proxy connected to master, master_addr=%s", masterAddr)
	p.masterConn = conn
	p.masterClient = masterpb.NewMasterClient(conn)
	return nil
}

func (p *Proxy) Start() error {
	return p.startProxy()
}

func (p *Proxy) stopProxyLoop() {
	p.proxyLoopCancel()

	if p.grpcServer != nil {
		p.grpcServer.GracefulStop()
	}
	p.tsoAllocator.Close()

	p.idAllocator.Close()

	p.segAssigner.Close()

	p.sched.Close()

	p.manipulationMsgStream.Close()

	p.queryMsgStream.Close()

	p.tick.Close()

	p.proxyLoopWg.Wait()

}

// Close closes the server.
func (p *Proxy) Close() {
	p.stopProxyLoop()

	if p.closer != nil {
		p.closer.Close()
	}

	for _, cb := range p.closeCallbacks {
		cb()
	}
	log.Print("proxy closed.")
}
