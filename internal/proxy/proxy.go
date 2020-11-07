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

	servicepb.UnimplementedMilvusServiceServer
	grpcServer            *grpc.Server
	masterConn            *grpc.ClientConn
	masterClient          masterpb.MasterClient
	taskSch               *TaskScheduler
	manipulationMsgStream *msgstream.PulsarMsgStream
	queryMsgStream        *msgstream.PulsarMsgStream
	queryResultMsgStream  *msgstream.PulsarMsgStream
}

func CreateProxy(ctx context.Context) (*Proxy, error) {
	rand.Seed(time.Now().UnixNano())
	m := &Proxy{
		ctx: ctx,
	}
	return m, nil
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
	p.startProxyLoop(p.ctx)

	p.proxyLoopWg.Wait()
	return nil
}
