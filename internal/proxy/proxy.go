package proxy

import (
	"context"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

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

	idAllocator  *allocator.IDAllocator
	tsoAllocator *allocator.TimestampAllocator

	manipulationMsgStream *msgstream.PulsarMsgStream
	queryMsgStream        *msgstream.PulsarMsgStream
	queryResultMsgStream  *msgstream.PulsarMsgStream

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

func Init() {
	Params.InitParamTable()
}

func CreateProxy(ctx context.Context) (*Proxy, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	p := &Proxy{
		proxyLoopCtx:    ctx1,
		proxyLoopCancel: cancel,
	}

	// TODO: use config instead
	pulsarAddress := "pulsar://localhost:6650"
	bufSize := int64(1000)
	manipulationChannels := []string{"manipulation"}
	queryChannels := []string{"query"}
	queryResultChannels := []string{"QueryResult"}
	queryResultSubName := "QueryResultSubject"
	unmarshal := msgstream.NewUnmarshalDispatcher()

	p.manipulationMsgStream = msgstream.NewPulsarMsgStream(p.proxyLoopCtx, bufSize)
	p.manipulationMsgStream.SetPulsarCient(pulsarAddress)
	p.manipulationMsgStream.CreatePulsarProducers(manipulationChannels)

	p.queryMsgStream = msgstream.NewPulsarMsgStream(p.proxyLoopCtx, bufSize)
	p.queryMsgStream.SetPulsarCient(pulsarAddress)
	p.queryMsgStream.CreatePulsarProducers(queryChannels)

	p.queryResultMsgStream = msgstream.NewPulsarMsgStream(p.proxyLoopCtx, bufSize)
	p.queryResultMsgStream.SetPulsarCient(pulsarAddress)
	p.queryResultMsgStream.CreatePulsarConsumers(queryResultChannels,
		queryResultSubName,
		unmarshal,
		bufSize)

	masterAddr, err := Params.MasterAddress()
	if err != nil {
		panic(err)
	}
	idAllocator, err := allocator.NewIDAllocator(p.proxyLoopCtx, masterAddr)

	if err != nil {
		return nil, err
	}
	p.idAllocator = idAllocator

	tsoAllocator, err := allocator.NewTimestampAllocator(p.proxyLoopCtx, masterAddr)
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
func (p *Proxy) AddCloseCallback(callbacks ...func()) {
	p.closeCallbacks = append(p.closeCallbacks, callbacks...)
}

func (p *Proxy) grpcLoop() {
	defer p.proxyLoopWg.Done()

	// TODO: use address in config instead
	lis, err := net.Listen("tcp", ":5053")
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
	masterAddr, err := Params.MasterAddress()
	if err != nil {
		panic(err)
	}
	log.Printf("Proxy connected to master, master_addr=%s", masterAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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

func (p *Proxy) queryResultLoop() {
	defer p.proxyLoopWg.Done()
	defer p.proxyLoopCancel()

	queryResultBuf := make(map[UniqueID][]*internalpb.SearchResult)

	for {
		select {
		case msgPack, ok := <-p.queryResultMsgStream.Chan():
			if !ok {
				log.Print("buf chan closed")
				return
			}
			if msgPack == nil {
				continue
			}
			tsMsg := msgPack.Msgs[0]
			searchResultMsg, _ := (*tsMsg).(*msgstream.SearchResultMsg)
			reqID := searchResultMsg.GetReqID()
			_, ok = queryResultBuf[reqID]
			if !ok {
				queryResultBuf[reqID] = make([]*internalpb.SearchResult, 0)
			}
			queryResultBuf[reqID] = append(queryResultBuf[reqID], &searchResultMsg.SearchResult)
			if len(queryResultBuf[reqID]) == 4 {
				// TODO: use the number of query node instead
				t := p.taskSch.getTaskByReqID(reqID)
				if t != nil {
					qt := t.(*QueryTask)
					log.Printf("address of query task: %p", qt)
					qt.resultBuf <- queryResultBuf[reqID]
					delete(queryResultBuf, reqID)
				} else {
					log.Printf("task with reqID %v is nil", reqID)
				}
			}
		case <-p.proxyLoopCtx.Done():
			log.Print("proxy server is closed ...")
			return
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
