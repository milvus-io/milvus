package mockmaster

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/kv/mockkv"
	"github.com/zilliztech/milvus-distributed/internal/master/id"
	"github.com/zilliztech/milvus-distributed/internal/master/tso"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/util/kvutil"
	gparams "github.com/zilliztech/milvus-distributed/internal/util/paramtableutil"
)

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

	kvBase kvutil.Base

	// error chans
	grpcErr chan error

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()

	grpcAddr net.Addr
}

func Init() {
	rand.Seed(time.Now().UnixNano())
	id.Init()
	tso.Init()
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(ctx context.Context) (*Master, error) {
	Init()
	m := &Master{
		ctx:     ctx,
		kvBase:  mockkv.NewEtcdKV(),
		grpcErr: make(chan error),
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
func (s *Master) GetGRPCAddr() net.Addr {
	return s.grpcAddr
}

func (s *Master) startServer(ctx context.Context) error {

	// Run callbacks
	for _, cb := range s.startCallbacks {
		cb()
	}
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
func (s *Master) IsServing() bool {
	return !s.IsClosed()
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
	// Server has started.

	if err := <-s.grpcErr; err != nil {
		s.Close()
		return err
	}

	atomic.StoreInt64(&s.isServing, 1)
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
	s.serverLoopWg.Add(1)
	go s.grpcLoop()
	s.serverLoopWg.Add(1)
	go s.pulsarLoop()
	s.serverLoopWg.Add(1)
	go s.segmentStatisticsLoop()
}

func (s *Master) stopServerLoop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.serverLoopCancel()
	s.serverLoopWg.Wait()
}

func (s *Master) checkReady(ctx context.Context, targetCh chan error) {
	select {
	case <-time.After(100 * time.Millisecond):
		targetCh <- nil
	case <-ctx.Done():
		return
	}
}

func (s *Master) grpcLoop() {
	defer s.serverLoopWg.Done()
	masterAddr, err := gparams.GParams.Load("master.address")
	if err != nil {
		panic(err)
	}
	masterPort, err := gparams.GParams.Load("master.port")
	if err != nil {
		panic(err)
	}
	masterAddr = masterAddr + ":" + masterPort
	lis, err := net.Listen("tcp", masterAddr)
	if err != nil {
		log.Printf("failed to listen: %v", err)
		s.grpcErr <- err
		return
	}
	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()
	go s.checkReady(ctx, s.grpcErr)
	s.grpcAddr = lis.Addr()
	fmt.Printf("Start MockMaster grpc server , addr:%v\n", s.grpcAddr)
	if err := s.grpcServer.Serve(lis); err != nil {
		fmt.Println(err)
		s.grpcErr <- err
	}
}

// todo use messagestream
func (s *Master) pulsarLoop() {
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()

	<-ctx.Done()
	log.Print("server is closed, exit pulsar loop")
}

func (s *Master) tasksExecutionLoop() {
	defer s.serverLoopWg.Done()
	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()

	<-ctx.Done()
	log.Print("server is closed, exit task execution loop")
}

func (s *Master) segmentStatisticsLoop() {
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()

	<-ctx.Done()
	log.Print("server is closed, exit segmentStatistics loop")
}
