package proxy

import (
	"context"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/servicepb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type Proxy struct {
	ctx             context.Context
	proxyLoopCtx    context.Context
	proxyLoopCancel func()
	proxyLoopWg     sync.WaitGroup

	grpcServer            *grpc.Server
	masterConn            *grpc.ClientConn
	masterClient          masterpb.MasterClient
	taskSch               *TaskScheduler
	manipulationMsgStream *msgstream.PulsarMsgStream
	queryMsgStream        *msgstream.PulsarMsgStream
	queryResultMsgStream  *msgstream.PulsarMsgStream

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

func CreateProxy(ctx context.Context) (*Proxy, error) {
	rand.Seed(time.Now().UnixNano())
	m := &Proxy{
		ctx: ctx,
	}
	return m, nil
}

// AddStartCallback adds a callback in the startServer phase.
func (s *Proxy) AddStartCallback(callbacks ...func()) {
	s.startCallbacks = append(s.startCallbacks, callbacks...)
}

func (s *Proxy) startProxy(ctx context.Context) error {

	// Run callbacks
	for _, cb := range s.startCallbacks {
		cb()
	}
	return nil
}

// AddCloseCallback adds a callback in the Close phase.
func (s *Proxy) AddCloseCallback(callbacks ...func()) {
	s.closeCallbacks = append(s.closeCallbacks, callbacks...)
}

func (p *Proxy) grpcLoop() {
	defer p.proxyLoopWg.Done()

	// TODO: use address in config instead
	lis, err := net.Listen("tcp", "5053")
	if err != nil {
		log.Fatalf("Proxy grpc server fatal error=%v", err)
	}

	p.grpcServer = grpc.NewServer()
	servicepb.RegisterMilvusServiceServer(p.grpcServer, p)
	if err = p.grpcServer.Serve(lis); err != nil {
		log.Fatalf("Proxy grpc server fatal error=%v", err)
	}
}

func (p *Proxy) pulsarMsgStreamLoop() {
	defer p.proxyLoopWg.Done()
	p.manipulationMsgStream = &msgstream.PulsarMsgStream{}
	p.queryMsgStream = &msgstream.PulsarMsgStream{}
	// TODO: config, RepackFunc
	p.manipulationMsgStream.Start()
	p.queryMsgStream.Start()

	ctx, cancel := context.WithCancel(p.proxyLoopCtx)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			log.Print("proxy is closed...")
			return
		}
	}
}

func (p *Proxy) scheduleLoop() {
	defer p.proxyLoopWg.Done()

	p.taskSch = &TaskScheduler{}
	p.taskSch.Start(p.ctx)
	defer p.taskSch.Close()

	ctx, cancel := context.WithCancel(p.proxyLoopCtx)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			log.Print("proxy is closed...")
			return
		}
	}
}

func (p *Proxy) connectMaster() error {
	log.Printf("Connected to master, master_addr=%s", "127.0.0.1:5053")
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	conn, err := grpc.DialContext(ctx, "127.0.0.1:5053", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("Connect to master failed, error= %v", err)
		return err
	}
	log.Printf("Connected to master, master_addr=%s", "127.0.0.1:5053")
	p.masterConn = conn
	p.masterClient = masterpb.NewMasterClient(conn)
	return nil
}

func (p *Proxy) receiveResultLoop() {
	queryResultBuf := make(map[UniqueID][]*internalpb.SearchResult)

	for {
		msgPack := p.queryResultMsgStream.Consume()
		if msgPack == nil {
			continue
		}
		tsMsg := msgPack.Msgs[0]
		searchResultMsg, _ := (*tsMsg).(*msgstream.SearchResultMsg)
		reqId := UniqueID(searchResultMsg.GetReqId())
		_, ok := queryResultBuf[reqId]
		if !ok {
			queryResultBuf[reqId] = make([]*internalpb.SearchResult, 0)
		}
		queryResultBuf[reqId] = append(queryResultBuf[reqId], &searchResultMsg.SearchResult)
		if len(queryResultBuf[reqId]) == 4 {
			// TODO: use the number of query node instead
			t := p.taskSch.getTaskByReqId(reqId)
			qt := (*t).(*QueryTask)
			qt.resultBuf <- queryResultBuf[reqId]
			delete(queryResultBuf, reqId)
		}
	}
}

func (p *Proxy) queryResultLoop() {
	defer p.proxyLoopWg.Done()
	p.queryResultMsgStream = &msgstream.PulsarMsgStream{}
	// TODO: config
	p.queryResultMsgStream.Start()

	go p.receiveResultLoop()

	ctx, cancel := context.WithCancel(p.proxyLoopCtx)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			log.Print("proxy is closed...")
			return
		}
	}
}



func (p *Proxy) startProxyLoop(ctx context.Context) {
	p.proxyLoopCtx, p.proxyLoopCancel = context.WithCancel(ctx)
	p.proxyLoopWg.Add(4)

	p.connectMaster()

	go p.grpcLoop()
	go p.pulsarMsgStreamLoop()
	go p.queryResultLoop()
	go p.scheduleLoop()
}

func (p *Proxy) Run() error {
	if err := p.startProxy(p.ctx); err != nil {
		return err
	}
	p.startProxyLoop(p.ctx)
	return nil
}

func (p *Proxy) stopProxyLoop() {
	if p.grpcServer != nil{
		p.grpcServer.GracefulStop()
	}
	p.proxyLoopCancel()
	p.proxyLoopWg.Wait()
}

// Close closes the server.
func (p *Proxy) Close() {
	p.stopProxyLoop()
	for _, cb := range p.closeCallbacks {
		cb()
	}
	log.Print("proxy closed.")
}
