package proxynode

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
)

const (
	reqTimeoutInterval = time.Second * 10
)

func (node *NodeImpl) UpdateStateCode(code internalpb2.StateCode) {
	node.stateCode = code
}

func (node *NodeImpl) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	collectionName := request.CollectionName
	globalMetaCache.RemoveCollection(collectionName) // no need to return error, though collection may be not cached
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
		Reason:    "",
	}, nil
}

func (node *NodeImpl) CreateCollection(request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	log.Println("create collection: ", request)
	ctx := context.Background()
	cct := &CreateCollectionTask{
		Condition:               NewTaskCondition(ctx),
		CreateCollectionRequest: request,
		masterClient:            node.masterClient,
		dataServiceClient:       node.dataServiceClient,
	}
	var cancel func()
	cct.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()

	fn := func() error {
		select {
		case <-ctx.Done():
			return errors.New("create collection timeout")
		default:
			return node.sched.DdQueue.Enqueue(cct)
		}
	}
	err := fn()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	err = cct.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	return cct.result, nil
}

func (node *NodeImpl) DropCollection(request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	log.Println("drop collection: ", request)
	ctx := context.Background()
	dct := &DropCollectionTask{
		Condition:             NewTaskCondition(ctx),
		DropCollectionRequest: request,
		masterClient:          node.masterClient,
		dataServiceClient:     node.dataServiceClient,
	}
	var cancel func()
	dct.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()

	fn := func() error {
		select {
		case <-ctx.Done():
			return errors.New("create collection timeout")
		default:
			return node.sched.DdQueue.Enqueue(dct)
		}
	}
	err := fn()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	err = dct.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	return dct.result, nil
}

func (node *NodeImpl) HasCollection(request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	log.Println("has collection: ", request)
	ctx := context.Background()
	hct := &HasCollectionTask{
		Condition:            NewTaskCondition(ctx),
		HasCollectionRequest: request,
		masterClient:         node.masterClient,
	}
	var cancel func()
	hct.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()

	fn := func() error {
		select {
		case <-ctx.Done():
			return errors.New("create collection timeout")
		default:
			return node.sched.DdQueue.Enqueue(hct)
		}
	}
	err := fn()
	if err != nil {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = hct.WaitToFinish()
	if err != nil {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	return hct.result, nil
}

func (node *NodeImpl) LoadCollection(request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (node *NodeImpl) ReleaseCollection(request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (node *NodeImpl) DescribeCollection(request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	log.Println("describe collection: ", request)
	ctx := context.Background()
	dct := &DescribeCollectionTask{
		Condition:                 NewTaskCondition(ctx),
		DescribeCollectionRequest: request,
		masterClient:              node.masterClient,
	}
	var cancel func()
	dct.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()

	fn := func() error {
		select {
		case <-ctx.Done():
			return errors.New("create collection timeout")
		default:
			return node.sched.DdQueue.Enqueue(dct)
		}
	}
	err := fn()
	if err != nil {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = dct.WaitToFinish()
	if err != nil {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	return dct.result, nil
}

func (node *NodeImpl) GetCollectionStatistics(request *milvuspb.CollectionStatsRequest) (*milvuspb.CollectionStatsResponse, error) {
	panic("implement me")
}

func (node *NodeImpl) ShowCollections(request *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error) {
	log.Println("show collections")
	ctx := context.Background()
	sct := &ShowCollectionsTask{
		Condition:             NewTaskCondition(ctx),
		ShowCollectionRequest: request,
		masterClient:          node.masterClient,
	}
	var cancel func()
	sct.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()

	fn := func() error {
		select {
		case <-ctx.Done():
			return errors.New("create collection timeout")
		default:
			return node.sched.DdQueue.Enqueue(sct)
		}
	}
	err := fn()
	if err != nil {
		return &milvuspb.ShowCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = sct.WaitToFinish()
	if err != nil {
		return &milvuspb.ShowCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	return sct.result, nil
}

func (node *NodeImpl) CreatePartition(request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	log.Println("create partition", request)
	ctx := context.Background()
	cpt := &CreatePartitionTask{
		Condition:              NewTaskCondition(ctx),
		CreatePartitionRequest: request,
		masterClient:           node.masterClient,
		result:                 nil,
		ctx:                    nil,
	}
	var cancel func()
	cpt.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()

	err := func() error {
		select {
		case <-ctx.Done():
			return errors.New("create partition timeout")
		default:
			return node.sched.DdQueue.Enqueue(cpt)
		}
	}()

	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}
	err = cpt.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}
	return cpt.result, nil
}

func (node *NodeImpl) DropPartition(request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	log.Println("drop partition: ", request)
	ctx := context.Background()
	dpt := &DropPartitionTask{
		Condition:            NewTaskCondition(ctx),
		DropPartitionRequest: request,
		masterClient:         node.masterClient,
		result:               nil,
		ctx:                  nil,
	}

	var cancel func()
	dpt.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()

	err := func() error {
		select {
		case <-ctx.Done():
			return errors.New("drop partition timeout")
		default:
			return node.sched.DdQueue.Enqueue(dpt)
		}
	}()

	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}
	err = dpt.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}
	return dpt.result, nil
}

func (node *NodeImpl) HasPartition(request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	log.Println("has partition: ", request)
	ctx := context.Background()
	hpt := &HasPartitionTask{
		Condition:           NewTaskCondition(ctx),
		HasPartitionRequest: request,
		masterClient:        node.masterClient,
		result:              nil,
		ctx:                 nil,
	}

	var cancel func()
	hpt.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()

	err := func() error {
		select {
		case <-ctx.Done():
			return errors.New("has partition timeout")
		default:
			return node.sched.DdQueue.Enqueue(hpt)
		}
	}()

	if err != nil {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
			Value: false,
		}, nil
	}
	err = hpt.WaitToFinish()
	if err != nil {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
			Value: false,
		}, nil
	}
	return hpt.result, nil
}

func (node *NodeImpl) LoadPartitions(request *milvuspb.LoadPartitonRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (node *NodeImpl) ReleasePartitions(request *milvuspb.ReleasePartitionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (node *NodeImpl) GetPartitionStatistics(request *milvuspb.PartitionStatsRequest) (*milvuspb.PartitionStatsResponse, error) {
	panic("implement me")
}

func (node *NodeImpl) ShowPartitions(request *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error) {
	log.Println("show partitions: ", request)
	ctx := context.Background()
	spt := &ShowPartitionsTask{
		Condition:            NewTaskCondition(ctx),
		ShowPartitionRequest: request,
		masterClient:         node.masterClient,
		result:               nil,
		ctx:                  nil,
	}

	var cancel func()
	spt.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()

	err := func() error {
		select {
		case <-ctx.Done():
			return errors.New("show partition timeout")
		default:
			return node.sched.DdQueue.Enqueue(spt)
		}
	}()

	if err != nil {
		return &milvuspb.ShowPartitionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = spt.WaitToFinish()
	if err != nil {
		return &milvuspb.ShowPartitionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}
	return spt.result, nil
}

func (node *NodeImpl) CreateIndex(request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	log.Println("create index for: ", request)
	ctx := context.Background()
	cit := &CreateIndexTask{
		Condition:          NewTaskCondition(ctx),
		CreateIndexRequest: request,
		masterClient:       node.masterClient,
	}

	var cancel func()
	cit.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()

	fn := func() error {
		select {
		case <-ctx.Done():
			return errors.New("create index timeout")
		default:
			return node.sched.DdQueue.Enqueue(cit)
		}
	}
	err := fn()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	err = cit.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	return cit.result, nil
}

func (node *NodeImpl) DescribeIndex(request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	log.Println("Describe index for: ", request)
	ctx := context.Background()
	dit := &DescribeIndexTask{
		Condition:            NewTaskCondition(ctx),
		DescribeIndexRequest: request,
		masterClient:         node.masterClient,
	}

	var cancel func()
	dit.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()

	fn := func() error {
		select {
		case <-ctx.Done():
			return errors.New("create index timeout")
		default:
			return node.sched.DdQueue.Enqueue(dit)
		}
	}
	err := fn()
	if err != nil {
		return &milvuspb.DescribeIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = dit.WaitToFinish()
	if err != nil {
		return &milvuspb.DescribeIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	return dit.result, nil
}

func (node *NodeImpl) GetIndexState(request *milvuspb.IndexStateRequest) (*milvuspb.IndexStateResponse, error) {
	// log.Println("Describe index progress for: ", request)
	ctx := context.Background()
	dipt := &GetIndexStateTask{
		Condition:         NewTaskCondition(ctx),
		IndexStateRequest: request,
	}

	var cancel func()
	dipt.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()

	fn := func() error {
		select {
		case <-ctx.Done():
			return errors.New("create index timeout")
		default:
			return node.sched.DdQueue.Enqueue(dipt)
		}
	}
	err := fn()
	if err != nil {
		return &milvuspb.IndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = dipt.WaitToFinish()
	if err != nil {
		return &milvuspb.IndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	return dipt.result, nil
}

func (node *NodeImpl) Insert(request *milvuspb.InsertRequest) (*milvuspb.InsertResponse, error) {
	ctx := context.Background()
	span, ctx := opentracing.StartSpanFromContext(ctx, "insert grpc received")
	defer span.Finish()
	span.SetTag("collection name", request.CollectionName)
	span.SetTag("partition tag", request.PartitionName)
	log.Println("insert into: ", request.CollectionName)
	it := &InsertTask{
		ctx:               ctx,
		Condition:         NewTaskCondition(ctx),
		dataServiceClient: node.dataServiceClient,
		BaseInsertTask: BaseInsertTask{
			BaseMsg: msgstream.BaseMsg{
				HashValues: request.HashKeys,
			},
			InsertRequest: internalpb2.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_kInsert,
					MsgID:   0,
				},
				CollectionName: request.CollectionName,
				PartitionName:  request.PartitionName,
				RowData:        request.RowData,
			},
		},
		rowIDAllocator: node.idAllocator,
	}
	if len(it.PartitionName) <= 0 {
		it.PartitionName = Params.DefaultPartitionTag
	}

	var cancel func()
	it.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)

	defer cancel()

	fn := func() error {
		select {
		case <-ctx.Done():
			return errors.New("insert timeout")
		default:
			return node.sched.DmQueue.Enqueue(it)
		}
	}
	err := fn()

	if err != nil {
		return &milvuspb.InsertResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = it.WaitToFinish()
	if err != nil {
		return &milvuspb.InsertResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	return it.result, nil
}

func (node *NodeImpl) Search(request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	ctx := context.Background()
	span, ctx := opentracing.StartSpanFromContext(ctx, "search grpc received")
	defer span.Finish()
	span.SetTag("collection name", request.CollectionName)
	span.SetTag("partition tag", request.PartitionNames)
	span.SetTag("dsl", request.Dsl)
	log.Println("search: ", request.CollectionName, request.Dsl)
	qt := &SearchTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		SearchRequest: internalpb2.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_kSearch,
				SourceID: Params.ProxyID,
			},
			ResultChannelID: strconv.FormatInt(Params.ProxyID, 10),
		},
		queryMsgStream: node.queryMsgStream,
		resultBuf:      make(chan []*internalpb2.SearchResults),
		query:          request,
	}
	var cancel func()
	qt.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	log.Printf("grpc address of query task: %p", qt)
	defer cancel()

	fn := func() error {
		select {
		case <-ctx.Done():
			return errors.New("create collection timeout")
		default:
			return node.sched.DqQueue.Enqueue(qt)
		}
	}
	err := fn()
	if err != nil {
		return &milvuspb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = qt.WaitToFinish()
	if err != nil {
		return &milvuspb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	return qt.result, nil
}

func (node *NodeImpl) Flush(request *milvuspb.FlushRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (node *NodeImpl) GetDdChannel(request *commonpb.Empty) (*milvuspb.StringResponse, error) {
	panic("implement me")
}
