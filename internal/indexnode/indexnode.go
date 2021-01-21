package indexnode

import (
	"context"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	miniokv "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type IndexNode struct {
	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup

	grpcServer *grpc.Server
	sched      *TaskScheduler

	idAllocator *allocator.IDAllocator

	kv kv.Base

	metaTable *metaTable
	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()

	indexNodeID int64
	//serviceClient indexservice.Interface // method factory
}

func (i *IndexNode) Init() error {
	panic("implement me")
}

func (i *IndexNode) Start() error {
	panic("implement me")
}

func (i *IndexNode) Stop() error {
	panic("implement me")
}

func (i *IndexNode) GetComponentStates() (*internalpb2.ComponentStates, error) {
	panic("implement me")
}

func (i *IndexNode) GetTimeTickChannel() (string, error) {
	panic("implement me")
}

func (i *IndexNode) GetStatisticsChannel() (string, error) {
	panic("implement me")
}

func (i *IndexNode) BuildIndex(req *indexpb.BuildIndexCmd) (*commonpb.Status, error) {

	log.Println("Create index with indexID=", req.IndexID)
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}, nil
}

func CreateIndexNode(ctx context.Context) (*IndexNode, error) {
	return &IndexNode{}, nil
}

func NewIndexNode(ctx context.Context, nodeID int64) *IndexNode {
	ctx1, cancel := context.WithCancel(ctx)
	in := &IndexNode{
		loopCtx:    ctx1,
		loopCancel: cancel,

		indexNodeID: nodeID,
	}

	return in
}

type Builder struct {
	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup

	grpcServer *grpc.Server
	sched      *TaskScheduler

	idAllocator *allocator.IDAllocator

	kv kv.Base

	metaTable *metaTable
	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

func (b *Builder) RegisterNode(ctx context.Context, request *indexpb.RegisterNodeRequest) (*indexpb.RegisterNodeResponse, error) {
	panic("implement me")
}

func (b *Builder) NotifyBuildIndex(ctx context.Context, notification *indexpb.BuildIndexNotification) (*commonpb.Status, error) {
	panic("implement me")
}

func Init() {
	rand.Seed(time.Now().UnixNano())
	Params.Init()
}

func CreateBuilder(ctx context.Context) (*Builder, error) {
	ctx1, cancel := context.WithCancel(ctx)
	b := &Builder{
		loopCtx:    ctx1,
		loopCancel: cancel,
	}

	connectEtcdFn := func() error {
		etcdAddress := Params.EtcdAddress
		etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
		if err != nil {
			return err
		}
		etcdKV := etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)
		metakv, err := NewMetaTable(etcdKV)
		if err != nil {
			return err
		}
		b.metaTable = metakv
		return nil
	}
	err := Retry(10, time.Millisecond*200, connectEtcdFn)
	if err != nil {
		return nil, err
	}

	idAllocator, err := allocator.NewIDAllocator(b.loopCtx, Params.MasterAddress)
	b.idAllocator = idAllocator

	connectMinIOFn := func() error {
		option := &miniokv.Option{
			Address:           Params.MinIOAddress,
			AccessKeyID:       Params.MinIOAccessKeyID,
			SecretAccessKeyID: Params.MinIOSecretAccessKey,
			UseSSL:            Params.MinIOUseSSL,
			BucketName:        Params.MinioBucketName,
			CreateBucket:      true,
		}

		b.kv, err = miniokv.NewMinIOKV(b.loopCtx, option)
		if err != nil {
			return err
		}
		return nil
	}
	err = Retry(10, time.Millisecond*200, connectMinIOFn)
	if err != nil {
		return nil, err
	}

	b.sched, err = NewTaskScheduler(b.loopCtx, b.idAllocator, b.kv, b.metaTable)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// AddStartCallback adds a callback in the startServer phase.
func (b *Builder) AddStartCallback(callbacks ...func()) {
	b.startCallbacks = append(b.startCallbacks, callbacks...)
}

func (b *Builder) startBuilder() error {

	b.sched.Start()

	// Start callbacks
	for _, cb := range b.startCallbacks {
		cb()
	}

	b.idAllocator.Start()

	b.loopWg.Add(1)
	go b.grpcLoop()

	return nil
}

// AddCloseCallback adds a callback in the Close phase.
func (b *Builder) AddCloseCallback(callbacks ...func()) {
	b.closeCallbacks = append(b.closeCallbacks, callbacks...)
}

func (b *Builder) grpcLoop() {
	defer b.loopWg.Done()

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(Params.Port))
	if err != nil {
		log.Fatalf("Builder grpc server fatal error=%v", err)
	}

	b.grpcServer = grpc.NewServer()
	indexpb.RegisterIndexServiceServer(b.grpcServer, b)
	if err = b.grpcServer.Serve(lis); err != nil {
		log.Fatalf("Builder grpc server fatal error=%v", err)
	}
}

func (b *Builder) Start() error {
	return b.startBuilder()
}

func (b *Builder) stopBuilderLoop() {
	b.loopCancel()

	b.idAllocator.Close()

	if b.grpcServer != nil {
		b.grpcServer.GracefulStop()
	}

	b.sched.Close()
	b.loopWg.Wait()
}

// Close closes the server.
func (b *Builder) Close() {
	b.stopBuilderLoop()

	for _, cb := range b.closeCallbacks {
		cb()
	}
	log.Print("builder  closed.")
}
