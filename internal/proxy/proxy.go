package proxy

import (
	"context"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
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
	taskSch      *TaskScheduler
	tick         *timeTick

	idAllocator  *allocator.IdAllocator
	tsoAllocator *allocator.TimestampAllocator

	manipulationMsgStream *msgstream.PulsarMsgStream
	queryMsgStream        *msgstream.PulsarMsgStream
	queryResultMsgStream  *msgstream.PulsarMsgStream

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

func CreateProxy(ctx context.Context) (*Proxy, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	p := &Proxy{
		proxyLoopCtx:    ctx1,
		proxyLoopCancel: cancel,
	}
	bufSize := int64(1000)
	p.manipulationMsgStream = msgstream.NewPulsarMsgStream(p.proxyLoopCtx, bufSize)
	p.queryMsgStream = msgstream.NewPulsarMsgStream(p.proxyLoopCtx, bufSize)
	p.queryResultMsgStream = msgstream.NewPulsarMsgStream(p.proxyLoopCtx, bufSize)

	idAllocator, err := allocator.NewIdAllocator(p.proxyLoopCtx)

	if err != nil {
		return nil, err
	}
	p.idAllocator = idAllocator

	tsoAllocator, err := allocator.NewTimestampAllocator(p.proxyLoopCtx)
	if err != nil {
		return nil, err
	}
	p.tsoAllocator = tsoAllocator

	p.taskSch, err = NewTaskScheduler(p.proxyLoopCtx, p.idAllocator, p.tsoAllocator)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// AddStartCallback adds a callback in the startServer phase.
func (p *Proxy) AddStartCallback(callbacks ...func()) {
	p.startCallbacks = append(p.startCallbacks, callbacks...)
}

func (p *Proxy) startProxy() error {
	err := p.connectMaster()
	if err != nil {
		return err
	}
	p.manipulationMsgStream.Start()
	p.queryMsgStream.Start()
	p.queryResultMsgStream.Start()
	p.taskSch.Start()
	p.idAllocator.Start()
	p.tsoAllocator.Start()

	// Run callbacks
	for _, cb := range p.startCallbacks {
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

func (p *Proxy) queryResultLoop() {
	defer p.proxyLoopWg.Done()

	queryResultBuf := make(map[UniqueID][]*internalpb.SearchResult)

	for {
		msgPack := p.queryResultMsgStream.Consume()
		if msgPack == nil {
			continue
		}
		tsMsg := msgPack.Msgs[0]
		searchResultMsg, _ := (*tsMsg).(*msgstream.SearchResultMsg)
		reqId := searchResultMsg.GetReqId()
		_, ok := queryResultBuf[reqId]
		if !ok {
			queryResultBuf[reqId] = make([]*internalpb.SearchResult, 0)
		}
		queryResultBuf[reqId] = append(queryResultBuf[reqId], &searchResultMsg.SearchResult)
		if len(queryResultBuf[reqId]) == 4 {
			// TODO: use the number of query node instead
			t := p.taskSch.getTaskByReqId(reqId)
			qt := t.(*QueryTask)
			qt.resultBuf <- queryResultBuf[reqId]
			delete(queryResultBuf, reqId)
		}
	}
}

func (p *Proxy) startProxyLoop() {
	p.proxyLoopWg.Add(2)
	go p.grpcLoop()
	go p.queryResultLoop()

}

func (p *Proxy) Run() error {
	if err := p.startProxy(); err != nil {
		return err
	}
	p.startProxyLoop()
	return nil
}

func (p *Proxy) stopProxyLoop() {
	p.proxyLoopCancel()

	if p.grpcServer != nil {
		p.grpcServer.GracefulStop()
	}
	p.tsoAllocator.Close()
	p.idAllocator.Close()
	p.taskSch.Close()
	p.manipulationMsgStream.Close()
	p.queryMsgStream.Close()
	p.queryResultMsgStream.Close()

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
