package indexservice

import (
	"context"
	"log"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	miniokv "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"
	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

const (
	reqTimeoutInterval = time.Second * 10
)

type ServiceImpl struct {
	nodeClients *PriorityQueue
	nodeStates  map[UniqueID]*internalpb2.ComponentStates
	stateCode   internalpb2.StateCode

	ID UniqueID

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup

	sched *TaskScheduler

	idAllocator *GlobalIDAllocator

	kv kv.Base

	metaTable *metaTable

	nodeLock sync.RWMutex

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

func NewServiceImpl(ctx context.Context) (*ServiceImpl, error) {
	ctx1, cancel := context.WithCancel(ctx)
	i := &ServiceImpl{
		loopCtx:     ctx1,
		loopCancel:  cancel,
		nodeClients: &PriorityQueue{},
	}

	return i, nil
}

func (i *ServiceImpl) Init() error {
	etcdAddress := Params.EtcdAddress
	log.Println("etcd address = ", etcdAddress)
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
	err := retry.Retry(10, time.Millisecond*200, connectEtcdFn)
	if err != nil {
		return err
	}

	//init idAllocator
	kvRootPath := Params.KvRootPath
	i.idAllocator = NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase([]string{etcdAddress}, kvRootPath, "index_gid"))
	if err := i.idAllocator.Initialize(); err != nil {
		return err
	}

	i.ID, err = i.idAllocator.AllocOne()
	if err != nil {
		return err
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

		i.kv, err = miniokv.NewMinIOKV(i.loopCtx, option)
		if err != nil {
			return err
		}
		return nil
	}
	err = retry.Retry(10, time.Millisecond*200, connectMinIOFn)
	if err != nil {
		return err
	}

	i.sched, err = NewTaskScheduler(i.loopCtx, i.idAllocator, i.kv, i.metaTable)
	if err != nil {
		return err
	}
	i.UpdateStateCode(internalpb2.StateCode_HEALTHY)
	return nil
}

func (i *ServiceImpl) Start() error {
	i.sched.Start()
	// Start callbacks
	for _, cb := range i.startCallbacks {
		cb()
	}
	log.Print("ServiceImpl  start")

	return nil
}

func (i *ServiceImpl) Stop() error {
	i.loopCancel()
	i.sched.Close()
	for _, cb := range i.closeCallbacks {
		cb()
	}
	return nil
}

func (i *ServiceImpl) UpdateStateCode(code internalpb2.StateCode) {
	i.stateCode = code
}

func (i *ServiceImpl) GetComponentStates() (*internalpb2.ComponentStates, error) {

	stateInfo := &internalpb2.ComponentInfo{
		NodeID:    i.ID,
		Role:      "ServiceImpl",
		StateCode: i.stateCode,
	}

	ret := &internalpb2.ComponentStates{
		State:              stateInfo,
		SubcomponentStates: nil, // todo add subcomponents states
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}
	return ret, nil
}

func (i *ServiceImpl) GetTimeTickChannel() (string, error) {
	return "", nil
}

func (i *ServiceImpl) GetStatisticsChannel() (string, error) {
	return "", nil
}

func (i *ServiceImpl) BuildIndex(req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
	ret := &indexpb.BuildIndexResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
	}
	t := NewIndexAddTask()
	t.req = req
	t.idAllocator = i.idAllocator
	t.table = i.metaTable
	t.kv = i.kv

	if i.nodeClients == nil || i.nodeClients.Len() <= 0 {
		ret.Status.Reason = "IndexBuilding Service not available"
		return ret, nil
	}
	t.nodeClients = i.nodeClients

	var cancel func()
	ctx := context.Background()
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
		ret.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		ret.Status.Reason = err.Error()
		return ret, nil
	}

	err = t.WaitToFinish()
	if err != nil {
		ret.Status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		ret.Status.Reason = err.Error()
		return ret, nil
	}
	ret.IndexBuildID = t.indexBuildID
	return ret, nil
}

func (i *ServiceImpl) GetIndexStates(req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error) {
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
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		States: indexStates,
	}
	return ret, nil
}

func (i *ServiceImpl) GetIndexFilePaths(req *indexpb.IndexFilePathsRequest) (*indexpb.IndexFilePathsResponse, error) {
	var indexPaths []*indexpb.IndexFilePathInfo

	for _, indexID := range req.IndexBuildIDs {
		indexPathInfo, _ := i.metaTable.GetIndexFilePathInfo(indexID)
		indexPaths = append(indexPaths, indexPathInfo)
	}

	ret := &indexpb.IndexFilePathsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		FilePaths: indexPaths,
	}
	return ret, nil
}

func (i *ServiceImpl) NotifyBuildIndex(nty *indexpb.BuildIndexNotification) (*commonpb.Status, error) {
	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}
	if err := i.metaTable.NotifyBuildIndex(nty); err != nil {
		ret.ErrorCode = commonpb.ErrorCode_BUILD_INDEX_ERROR
		ret.Reason = err.Error()
	}
	i.nodeClients.IncPriority(nty.NodeID, -1)
	return ret, nil
}
