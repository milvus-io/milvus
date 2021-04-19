package mockmaster

import (
	"context"
	"fmt"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/kv/mockkv"
	"github.com/zilliztech/milvus-distributed/internal/master/id"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/master/tso"
)

const (
	MOCK_GRPC_PORT=":0"
)

var GrpcServerAddr net.Addr

// Server is the pd server.
type Master struct {
	isServing int64

	ctx              context.Context
	serverLoopCtx    context.Context
	serverLoopCancel func()
	serverLoopWg     sync.WaitGroup

	//grpc server
	grpcServer *grpc.Server

	// for tso.
	tsoAllocator tso.Allocator

	kvBase    kv.KVBase

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()

	grpcAddr net.Addr
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(ctx context.Context) (*Master, error) {
	rand.Seed(time.Now().UnixNano())
	id.InitGlobalIdAllocator("idTimestamp", mockkv.NewEtcdKV())

	m := &Master{
		ctx:            ctx,
		kvBase:         mockkv.NewEtcdKV(),
		tsoAllocator: tso.NewGlobalTSOAllocator("timestamp", mockkv.NewEtcdKV()),
	}

	m.grpcServer = grpc.NewServer()
	masterpb.RegisterMasterServer(m.grpcServer, m)
	return m, nil
}

// AddStartCallback adds a callback in the startServer phase.
func (s *Master) AddStartCallback(callbacks ...func()) {
	s.startCallbacks = append(s.startCallbacks, callbacks...)
}

// for unittest, get the grpc server addr
func (s *Master) GetGRPCAddr() net.Addr{
	return s.grpcAddr
}

func (s *Master) startServer(ctx context.Context) error {

	// Run callbacks
	for _, cb := range s.startCallbacks {
		cb()
	}

	// Server has started.
	atomic.StoreInt64(&s.isServing, 1)
	return nil
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

// Run runs the pd server.
func (s *Master) Run() error {

	if err := s.startServer(s.ctx); err != nil {
		return err
	}

	s.startServerLoop(s.ctx)

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

func (s *Master) startServerLoop(ctx context.Context) {
	s.serverLoopCtx, s.serverLoopCancel = context.WithCancel(ctx)
	s.serverLoopWg.Add(3)
	go s.grpcLoop()
	go s.pulsarLoop()
	go s.segmentStatisticsLoop()
}

func (s *Master) stopServerLoop() {
	if s.grpcServer != nil{
		s.grpcServer.GracefulStop()
	}
	s.serverLoopCancel()
	s.serverLoopWg.Wait()
}


func (s *Master) grpcLoop() {
	defer s.serverLoopWg.Done()
	lis, err := net.Listen("tcp", MOCK_GRPC_PORT)
	if err != nil {
		log.Printf("failed to listen: %v", err)
		return
	}

	s.grpcAddr = lis.Addr()

	fmt.Printf("Start MockMaster grpc server , addr:%v\n", s.grpcAddr)

	if err := s.grpcServer.Serve(lis); err != nil {
		panic("grpcServer Startup Failed!")
	}
}

// todo use messagestream
func (s *Master) pulsarLoop() {
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			log.Print("server is closed, exit pulsar loop")
			return
		}
	}
}

func (s *Master) tasksExecutionLoop() {
	defer s.serverLoopWg.Done()
	ctx, _ := context.WithCancel(s.serverLoopCtx)

	for {
		select {
		case <-ctx.Done():
			log.Print("server is closed, exit task execution loop")
			return
		}
	}
}

func (s *Master) segmentStatisticsLoop() {
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			log.Print("server is closed, exit segmentStatistics loop")
			return
		}
	}
}
