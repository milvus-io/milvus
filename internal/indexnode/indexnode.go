package indexnode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"time"

	"go.uber.org/zap"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	miniokv "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/types"
	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

const (
	reqTimeoutInterval = time.Second * 10
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type IndexNode struct {
	stateCode internalpb.StateCode

	loopCtx    context.Context
	loopCancel func()

	sched *TaskScheduler

	kv kv.Base

	serviceClient types.IndexService // method factory

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()

	closer io.Closer
}

func NewIndexNode(ctx context.Context) (*IndexNode, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	b := &IndexNode{
		loopCtx:    ctx1,
		loopCancel: cancel,
	}
	var err error
	b.sched, err = NewTaskScheduler(b.loopCtx, b.kv)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (i *IndexNode) Init() error {
	ctx := context.Background()
	err := funcutil.WaitForComponentHealthy(ctx, i.serviceClient, "IndexService", 100, time.Millisecond*200)

	if err != nil {
		return err
	}
	request := &indexpb.RegisterNodeRequest{
		Base: nil,
		Address: &commonpb.Address{
			Ip:   Params.IP,
			Port: int64(Params.Port),
		},
	}

	resp, err2 := i.serviceClient.RegisterNode(ctx, request)
	if err2 != nil {
		log.Debug("indexnode", zap.String("Index NodeImpl connect to IndexService failed", err.Error()))
		return err2
	}

	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return errors.New(resp.Status.Reason)
	}

	err = Params.LoadConfigFromInitParams(resp.InitParams)
	if err != nil {
		return err
	}

	// TODO
	cfg := &config.Configuration{
		ServiceName: fmt.Sprintf("index_node_%d", Params.NodeID),
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
	}
	tracer, closer, err := cfg.NewTracer()
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	opentracing.SetGlobalTracer(tracer)
	i.closer = closer

	option := &miniokv.Option{
		Address:           Params.MinIOAddress,
		AccessKeyID:       Params.MinIOAccessKeyID,
		SecretAccessKeyID: Params.MinIOSecretAccessKey,
		UseSSL:            Params.MinIOUseSSL,
		BucketName:        Params.MinioBucketName,
		CreateBucket:      true,
	}
	i.kv, err = miniokv.NewMinIOKV(i.loopCtx, option)
	if err != nil {
		return err
	}

	i.UpdateStateCode(internalpb.StateCode_Healthy)

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

// Close closes the server.
func (i *IndexNode) Stop() error {
	if err := i.closer.Close(); err != nil {
		return err
	}
	i.loopCancel()
	if i.sched != nil {
		i.sched.Close()
	}
	for _, cb := range i.closeCallbacks {
		cb()
	}
	log.Debug("NodeImpl  closed.")
	return nil
}

func (i *IndexNode) UpdateStateCode(code internalpb.StateCode) {
	i.stateCode = code
}

func (i *IndexNode) SetIndexServiceClient(serviceClient types.IndexService) {
	i.serviceClient = serviceClient
}

func (i *IndexNode) BuildIndex(ctx context.Context, request *indexpb.BuildIndexRequest) (*commonpb.Status, error) {
	t := &IndexBuildTask{
		BaseTask: BaseTask{
			ctx:  ctx,
			done: make(chan error),
		},
		req:           request,
		kv:            i.kv,
		serviceClient: i.serviceClient,
		nodeID:        Params.NodeID,
	}

	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}

	err := i.sched.IndexBuildQueue.Enqueue(t)
	if err != nil {
		ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Reason = err.Error()
		return ret, nil
	}
	log.Debug("indexnode", zap.Int64("indexnode successfully schedule with indexBuildID", request.IndexBuildID))
	return ret, nil
}

func (i *IndexNode) DropIndex(ctx context.Context, request *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	i.sched.IndexBuildQueue.tryToRemoveUselessIndexBuildTask(request.IndexID)
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

// AddStartCallback adds a callback in the startServer phase.
func (i *IndexNode) AddStartCallback(callbacks ...func()) {
	i.startCallbacks = append(i.startCallbacks, callbacks...)
}

// AddCloseCallback adds a callback in the Close phase.
func (i *IndexNode) AddCloseCallback(callbacks ...func()) {
	i.closeCallbacks = append(i.closeCallbacks, callbacks...)
}

func (i *IndexNode) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {

	stateInfo := &internalpb.ComponentInfo{
		NodeID:    Params.NodeID,
		Role:      "NodeImpl",
		StateCode: i.stateCode,
	}

	ret := &internalpb.ComponentStates{
		State:              stateInfo,
		SubcomponentStates: nil, // todo add subcomponents states
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}
	return ret, nil
}

func (i *IndexNode) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

func (i *IndexNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}
