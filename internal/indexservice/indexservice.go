package indexservice

import (
	"context"
	"log"
	"sync"
	"time"

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

	"go.etcd.io/etcd/clientv3"
)

const (
	reqTimeoutInterval = time.Second * 10
)

type IndexService struct {
	nodeClients *PriorityQueue

	//factory method
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

func CreateIndexService(ctx context.Context) (*IndexService, error) {
	ctx1, cancel := context.WithCancel(ctx)
	i := &IndexService{
		loopCtx:     ctx1,
		loopCancel:  cancel,
		nodeClients: &PriorityQueue{},
	}

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
		return nil, err
	}

	//init idAllocator
	kvRootPath := Params.KvRootPath
	i.idAllocator = NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase([]string{etcdAddress}, kvRootPath, "index_gid"))
	if err := i.idAllocator.Initialize(); err != nil {
		return nil, err
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
		return nil, err
	}

	i.sched, err = NewTaskScheduler(i.loopCtx, i.idAllocator, i.kv, i.metaTable)
	if err != nil {
		return nil, err
	}

	return i, nil
}

func (i *IndexService) Init() error {
	return nil
}

func (i *IndexService) Start() error {
	i.sched.Start()
	// Start callbacks
	for _, cb := range i.startCallbacks {
		cb()
	}
	log.Print("IndexService  closed.")

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

func (i *IndexService) GetComponentStates() (*internalpb2.ComponentStates, error) {
	panic("implement me")
}

func (i *IndexService) GetTimeTickChannel() (string, error) {
	return "", nil
}

func (i *IndexService) GetStatisticsChannel() (string, error) {
	panic("implement me")
}

func (i *IndexService) BuildIndex(req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
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
	ret.IndexID = t.indexID
	return ret, nil
}

func (i *IndexService) GetIndexStates(req *indexpb.IndexStatesRequest) (*indexpb.IndexStatesResponse, error) {
	var indexStates []*indexpb.IndexInfo
	for _, indexID := range req.IndexIDs {
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

func (i *IndexService) GetIndexFilePaths(req *indexpb.IndexFilePathsRequest) (*indexpb.IndexFilePathsResponse, error) {
	var indexPaths []*indexpb.IndexFilePathInfo

	for _, indexID := range req.IndexIDs {
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

func (i *IndexService) NotifyBuildIndex(nty *indexpb.BuildIndexNotification) (*commonpb.Status, error) {
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
