package indexnode

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	miniokv "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

const (
	reqTimeoutInterval = time.Second * 10
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type IndexNode struct {
	loopCtx    context.Context
	loopCancel func()

	sched *TaskScheduler

	kv kv.Base

	serviceClient typeutil.IndexServiceInterface // method factory

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

func Init() {
	rand.Seed(time.Now().UnixNano())
	Params.Init()
}

func CreateIndexNode(ctx context.Context) (*IndexNode, error) {
	ctx1, cancel := context.WithCancel(ctx)
	b := &IndexNode{
		loopCtx:    ctx1,
		loopCancel: cancel,
	}

	connectMinIOFn := func() error {
		option := &miniokv.Option{
			Address:           Params.MinIOAddress,
			AccessKeyID:       Params.MinIOAccessKeyID,
			SecretAccessKeyID: Params.MinIOSecretAccessKey,
			UseSSL:            Params.MinIOUseSSL,
			BucketName:        Params.MinioBucketName,
			CreateBucket:      true,
		}
		var err error
		b.kv, err = miniokv.NewMinIOKV(b.loopCtx, option)
		if err != nil {
			return err
		}
		return nil
	}

	err := retry.Retry(10, time.Millisecond*200, connectMinIOFn)
	if err != nil {
		return nil, err
	}

	b.sched, err = NewTaskScheduler(b.loopCtx, b.kv)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// AddStartCallback adds a callback in the startServer phase.
func (i *IndexNode) AddStartCallback(callbacks ...func()) {
	i.startCallbacks = append(i.startCallbacks, callbacks...)
}

// AddCloseCallback adds a callback in the Close phase.
func (i *IndexNode) AddCloseCallback(callbacks ...func()) {
	i.closeCallbacks = append(i.closeCallbacks, callbacks...)
}

func (i *IndexNode) Init() error {
	return nil
}

func (i *IndexNode) Start() error {
	i.sched.Start()

	// Start callbacks
	for _, cb := range i.startCallbacks {
		cb()
	}

	return nil
}

func (i *IndexNode) stopIndexNodeLoop() {
	i.loopCancel()

	i.sched.Close()
}

// Close closes the server.
func (i *IndexNode) Stop() error {
	i.stopIndexNodeLoop()

	for _, cb := range i.closeCallbacks {
		cb()
	}
	log.Print("IndexNode  closed.")
	return nil
}

func (i *IndexNode) SetServiceClient(serviceClient typeutil.IndexServiceInterface) {
	i.serviceClient = serviceClient
}

func (i *IndexNode) BuildIndex(request *indexpb.BuildIndexCmd) (*commonpb.Status, error) {
	t := newIndexBuildTask()
	t.cmd = request
	t.kv = i.kv
	t.serviceClient = i.serviceClient
	t.nodeID = Params.NodeID
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()

	fn := func() error {
		select {
		case <-ctx.Done():
			return errors.New("Enqueue BuildQueue timeout")
		default:
			return i.sched.IndexBuildQueue.Enqueue(t)
		}
	}
	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}

	err := fn()
	if err != nil {
		ret.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		ret.Reason = err.Error()
		return ret, nil
	}
	log.Println("index scheduler successfully with indexID = ", request.IndexID)
	err = t.WaitToFinish()
	log.Println("build index finish ...err = ", err)
	if err != nil {
		ret.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		ret.Reason = err.Error()
		return ret, nil
	}
	return ret, nil
}
