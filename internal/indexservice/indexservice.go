package indexservice

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/allocator"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	miniokv "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/tso"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"go.etcd.io/etcd/clientv3"
)

const (
	reqTimeoutInterval = time.Second * 10
)

type IndexService struct {
	nodeClients *PriorityQueue
	nodeStates  map[UniqueID]*internalpb2.ComponentStates
	stateCode   internalpb2.StateCode

	ID UniqueID

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup

	sched *TaskScheduler

	idAllocator *allocator.GlobalIDAllocator

	kv kv.Base

	metaTable *metaTable

	nodeLock sync.RWMutex

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

func NewIndexService(ctx context.Context) (*IndexService, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	i := &IndexService{
		loopCtx:     ctx1,
		loopCancel:  cancel,
		nodeClients: &PriorityQueue{},
	}

	return i, nil
}

func (i *IndexService) Init() error {
	etcdAddress := Params.EtcdAddress
	log.Debug("indexservice", zap.String("etcd address", etcdAddress))
	connectEtcdFn := func() error {
		etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
		if err != nil {
			return err
		}
		etcdKV := etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)
		metakv, err := NewMetaTable(etcdKV)
		if err != nil {
			return err
		}
		i.metaTable = metakv
		return nil
	}
	err := retry.Retry(200, time.Millisecond*200, connectEtcdFn)
	if err != nil {
		return err
	}

	//init idAllocator
	kvRootPath := Params.KvRootPath
	i.idAllocator = allocator.NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase([]string{etcdAddress}, kvRootPath, "index_gid"))
	if err := i.idAllocator.Initialize(); err != nil {
		return err
	}

	i.ID, err = i.idAllocator.AllocOne()
	if err != nil {
		return err
	}

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

	i.sched, err = NewTaskScheduler(i.loopCtx, i.idAllocator, i.kv, i.metaTable)
	if err != nil {
		return err
	}
	i.UpdateStateCode(internalpb2.StateCode_Healthy)
	return nil
}

func (i *IndexService) Start() error {
	i.loopWg.Add(1)
	go i.tsLoop()

	i.sched.Start()
	// Start callbacks
	for _, cb := range i.startCallbacks {
		cb()
	}
	log.Debug("IndexService  start")

	return nil
}

func (i *IndexService) Stop() error {
	i.loopCancel()
	i.sched.Close()
	for _, cb := range i.closeCallbacks {
		cb()
	}
	return nil
}

func (i *IndexService) UpdateStateCode(code internalpb2.StateCode) {
	i.stateCode = code
}

func (i *IndexService) GetComponentStates(ctx context.Context) (*internalpb2.ComponentStates, error) {

	stateInfo := &internalpb2.ComponentInfo{
		NodeID:    i.ID,
		Role:      "IndexService",
		StateCode: i.stateCode,
	}

	ret := &internalpb2.ComponentStates{
		State:              stateInfo,
		SubcomponentStates: nil, // todo add subcomponents states
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_ERROR_CODE_SUCCESS,
		},
	}
	return ret, nil
}

func (i *IndexService) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_ERROR_CODE_SUCCESS,
			Reason:    "",
		},
		Value: "",
	}, nil
}

func (i *IndexService) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_ERROR_CODE_SUCCESS,
			Reason:    "",
		},
		Value: "",
	}, nil
}

func (i *IndexService) BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
	fmt.Println("builder building index ..., indexName = ", req.IndexName, "indexID = ", req.IndexID, "dataPath = ", req.DataPaths)
	ret := &indexpb.BuildIndexResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_ERROR_CODE_UNEXPECTED_ERROR,
		},
	}
	t := &IndexAddTask{
		BaseTask: BaseTask{
			ctx:   ctx,
			done:  make(chan error),
			table: i.metaTable,
		},
		req:         req,
		idAllocator: i.idAllocator,
		kv:          i.kv,
	}

	if i.nodeClients == nil || i.nodeClients.Len() <= 0 {
		ret.Status.Reason = "IndexBuilding Service not available"
		return ret, nil
	}
	t.nodeClients = i.nodeClients

	var cancel func()
	t.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()

	fn := func() error {
		select {
		case <-ctx.Done():
			return errors.New("IndexAddQueue enqueue timeout")
		default:
			return i.sched.IndexAddQueue.Enqueue(t)
		}
	}

	err := fn()
	if err != nil {
		ret.Status.ErrorCode = commonpb.ErrorCode_ERROR_CODE_UNEXPECTED_ERROR
		ret.Status.Reason = err.Error()
		return ret, nil
	}

	err = t.WaitToFinish()
	if err != nil {
		ret.Status.ErrorCode = commonpb.ErrorCode_ERROR_CODE_UNEXPECTED_ERROR
		ret.Status.Reason = err.Error()
		return ret, nil
	}
	ret.Status.ErrorCode = commonpb.ErrorCode_ERROR_CODE_SUCCESS
	ret.IndexBuildID = t.indexBuildID
	return ret, nil
}

func (i *IndexService) GetIndexStates(ctx context.Context, req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error) {
	var indexStates []*indexpb.IndexInfo
	for _, indexID := range req.IndexBuildIDs {
		indexState, err := i.metaTable.GetIndexState(indexID)
		if err != nil {
			indexState.Reason = err.Error()
		}
		indexStates = append(indexStates, indexState)
	}
	ret := &indexpb.IndexStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_ERROR_CODE_SUCCESS,
		},
		States: indexStates,
	}
	return ret, nil
}

func (i *IndexService) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	i.sched.IndexAddQueue.tryToRemoveUselessIndexAddTask(req.IndexID)

	err := i.metaTable.MarkIndexAsDeleted(req.IndexID)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_ERROR_CODE_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	defer func() {
		go func() {
			allNodeClients := i.nodeClients.PeekAllClients()
			for _, client := range allNodeClients {
				client.DropIndex(ctx, req)
			}
		}()
		go func() {
			i.metaTable.removeIndexFile(req.IndexID)
			i.metaTable.removeMeta(req.IndexID)
		}()
	}()

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_ERROR_CODE_SUCCESS,
	}, nil
}

func (i *IndexService) GetIndexFilePaths(ctx context.Context, req *indexpb.IndexFilePathsRequest) (*indexpb.IndexFilePathsResponse, error) {
	var indexPaths []*indexpb.IndexFilePathInfo = nil

	for _, indexID := range req.IndexBuildIDs {
		indexPathInfo, err := i.metaTable.GetIndexFilePathInfo(indexID)
		if err != nil {
			return nil, err
		}
		indexPaths = append(indexPaths, indexPathInfo)
	}

	ret := &indexpb.IndexFilePathsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_ERROR_CODE_SUCCESS,
		},
		FilePaths: indexPaths,
	}
	return ret, nil
}

func (i *IndexService) NotifyBuildIndex(ctx context.Context, nty *indexpb.BuildIndexNotification) (*commonpb.Status, error) {
	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_ERROR_CODE_SUCCESS,
	}
	log.Debug("indexservice", zap.String("[IndexService][NotifyBuildIndex]", nty.String()))
	if err := i.metaTable.NotifyBuildIndex(nty); err != nil {
		ret.ErrorCode = commonpb.ErrorCode_ERROR_CODE_BUILD_INDEX_ERROR
		ret.Reason = err.Error()
		log.Debug("indexservice", zap.String("[IndexService][NotifyBuildIndex][metaTable][NotifyBuildIndex]", err.Error()))
	}
	i.nodeClients.IncPriority(nty.NodeID, -1)
	return ret, nil
}

func (i *IndexService) tsLoop() {
	tsoTicker := time.NewTicker(tso.UpdateTimestampStep)
	defer tsoTicker.Stop()
	ctx, cancel := context.WithCancel(i.loopCtx)
	defer cancel()
	defer i.loopWg.Done()
	for {
		select {
		case <-tsoTicker.C:
			if err := i.idAllocator.UpdateID(); err != nil {
				log.Debug("indexservice", zap.String("failed to update id", err.Error()))
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Debug("tsLoop is closed")
			return
		}
	}
}
