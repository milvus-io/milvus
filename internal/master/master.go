package master

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/master/controller"
	"github.com/zilliztech/milvus-distributed/internal/master/id"
	"github.com/zilliztech/milvus-distributed/internal/master/informer"
	"github.com/zilliztech/milvus-distributed/internal/master/tso"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	gparams "github.com/zilliztech/milvus-distributed/internal/util/paramtableutil"
)

// Server is the pd server.
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

	// pulsar client
	pc *informer.PulsarClient

	// chans
	ssChan chan internalpb.SegmentStats

	grpcErr chan error

	kvBase    *kv.EtcdKV
	scheduler *ddRequestScheduler
	mt        *metaTable
	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
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
	id.Init()
	tso.Init()
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(ctx context.Context, kvRootPath string, metaRootPath, tsoRootPath string, etcdAddr []string) (*Master, error) {
	Init()

	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: etcdAddr})
	if err != nil {
		return nil, err
	}
	etcdkv := kv.NewEtcdKV(etcdClient, metaRootPath)
	metakv, err := NewMetaTable(etcdkv)
	if err != nil {
		return nil, err
	}

	m := &Master{
		ctx:            ctx,
		startTimestamp: time.Now().Unix(),
		kvBase:         newKVBase(kvRootPath, etcdAddr),
		scheduler:      NewDDRequestScheduler(),
		mt:             metakv,
		ssChan:         make(chan internalpb.SegmentStats, 10),
		grpcErr:        make(chan error),
		pc:             informer.NewPulsarClient(),
	}
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
	go s.grpcLoop(grpcPort)

	if err := <-s.grpcErr; err != nil {
		return err
	}

	s.serverLoopWg.Add(1)
	go s.tasksExecutionLoop()

	s.serverLoopWg.Add(1)
	go s.segmentStatisticsLoop()
	return nil
}

func (s *Master) stopServerLoop() {
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

// todo use messagestream
func (s *Master) pulsarLoop() {
	defer s.serverLoopWg.Done()

	ctx, cancel := context.WithCancel(s.serverLoopCtx)

	pulsarTopic, err := gparams.GParams.Load("master.pulsartopic")
	if err != nil {
		panic(err)
	}

	consumer, err := s.pc.Client.Subscribe(pulsar.ConsumerOptions{
		Topic:            pulsarTopic,
		SubscriptionName: "my-sub",
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
		return
	}
	defer func() {
		if err := consumer.Unsubscribe(); err != nil {
			log.Fatal(err)
		}
		cancel()
	}()

	consumerChan := consumer.Chan()

	for {
		select {
		case msg := <-consumerChan:
			var m internalpb.SegmentStats
			proto.Unmarshal(msg.Payload(), &m)
			fmt.Printf("Received message msgId: %#v -- content: '%d'\n",
				msg.ID(), m.SegmentID)
			s.ssChan <- m
			consumer.Ack(msg)
		case <-ctx.Done():
			log.Print("server is closed, exit pulsar loop")
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

	ctx, cancel := context.WithCancel(s.serverLoopCtx)
	defer cancel()

	for {
		select {
		case ss := <-s.ssChan:
			controller.ComputeCloseTime(ss, s.kvBase)
		case <-ctx.Done():
			log.Print("server is closed, exit segment statistics loop")
			return
		}
	}
}
