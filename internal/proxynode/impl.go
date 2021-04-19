package proxynode

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
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
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()

	cct := &CreateCollectionTask{
		Condition:               NewTaskCondition(ctx),
		CreateCollectionRequest: request,
		masterClient:            node.masterClient,
		dataServiceClient:       node.dataServiceClient,
	}

	err := node.sched.DdQueue.Enqueue(cct)
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
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()

	dct := &DropCollectionTask{
		Condition:             NewTaskCondition(ctx),
		DropCollectionRequest: request,
		masterClient:          node.masterClient,
	}

	err := node.sched.DdQueue.Enqueue(dct)
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
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()

	hct := &HasCollectionTask{
		Condition:            NewTaskCondition(ctx),
		HasCollectionRequest: request,
		masterClient:         node.masterClient,
	}

	err := node.sched.DdQueue.Enqueue(hct)
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
	log.Println("load collection: ", request)
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()

	lct := &LoadCollectionTask{
		Condition:             NewTaskCondition(ctx),
		LoadCollectionRequest: request,
		queryserviceClient:    node.queryServiceClient,
	}

	err := node.sched.DdQueue.Enqueue(lct)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	err = lct.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	return lct.result, nil
}

func (node *NodeImpl) ReleaseCollection(request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	log.Println("release collection: ", request)
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()

	rct := &ReleaseCollectionTask{
		Condition:                NewTaskCondition(ctx),
		ReleaseCollectionRequest: request,
		queryserviceClient:       node.queryServiceClient,
	}

	err := node.sched.DdQueue.Enqueue(rct)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	err = rct.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	return rct.result, nil
}

func (node *NodeImpl) DescribeCollection(request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	log.Println("describe collection: ", request)
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()

	dct := &DescribeCollectionTask{
		Condition:                 NewTaskCondition(ctx),
		DescribeCollectionRequest: request,
		masterClient:              node.masterClient,
	}

	err := node.sched.DdQueue.Enqueue(dct)
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
	log.Println("get collection statistics")
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()
	g := &GetCollectionsStatisticsTask{
		Condition:              NewTaskCondition(ctx),
		CollectionStatsRequest: request,
		dataServiceClient:      node.dataServiceClient,
	}

	err := node.sched.DdQueue.Enqueue(g)
	if err != nil {
		return &milvuspb.CollectionStatsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = g.WaitToFinish()
	if err != nil {
		return &milvuspb.CollectionStatsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, nil
	}

	return g.result, nil
}

func (node *NodeImpl) ShowCollections(request *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error) {
	log.Println("show collections")
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()
	sct := &ShowCollectionsTask{
		Condition:             NewTaskCondition(ctx),
		ShowCollectionRequest: request,
		masterClient:          node.masterClient,
	}

	err := node.sched.DdQueue.Enqueue(sct)
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
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()
	cpt := &CreatePartitionTask{
		Condition:              NewTaskCondition(ctx),
		CreatePartitionRequest: request,
		masterClient:           node.masterClient,
		result:                 nil,
	}

	err := node.sched.DdQueue.Enqueue(cpt)
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
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()
	dpt := &DropPartitionTask{
		Condition:            NewTaskCondition(ctx),
		DropPartitionRequest: request,
		masterClient:         node.masterClient,
		result:               nil,
	}

	err := node.sched.DdQueue.Enqueue(dpt)

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
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()
	hpt := &HasPartitionTask{
		Condition:           NewTaskCondition(ctx),
		HasPartitionRequest: request,
		masterClient:        node.masterClient,
		result:              nil,
	}

	err := node.sched.DdQueue.Enqueue(hpt)

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
	log.Println("load partitions: ", request)
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()

	lpt := &LoadPartitionTask{
		Condition:           NewTaskCondition(ctx),
		LoadPartitonRequest: request,
		queryserviceClient:  node.queryServiceClient,
	}

	err := node.sched.DdQueue.Enqueue(lpt)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	err = lpt.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	return lpt.result, nil
}

func (node *NodeImpl) ReleasePartitions(request *milvuspb.ReleasePartitionRequest) (*commonpb.Status, error) {
	log.Println("load partitions: ", request)
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()

	rpt := &ReleasePartitionTask{
		Condition:               NewTaskCondition(ctx),
		ReleasePartitionRequest: request,
		queryserviceClient:      node.queryServiceClient,
	}

	err := node.sched.DdQueue.Enqueue(rpt)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	err = rpt.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	return rpt.result, nil
}

func (node *NodeImpl) GetPartitionStatistics(request *milvuspb.PartitionStatsRequest) (*milvuspb.PartitionStatsResponse, error) {
	panic("implement me")
}

func (node *NodeImpl) ShowPartitions(request *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error) {
	log.Println("show partitions: ", request)
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()
	spt := &ShowPartitionsTask{
		Condition:            NewTaskCondition(ctx),
		ShowPartitionRequest: request,
		masterClient:         node.masterClient,
		result:               nil,
	}

	err := node.sched.DdQueue.Enqueue(spt)

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
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()
	cit := &CreateIndexTask{
		Condition:          NewTaskCondition(ctx),
		CreateIndexRequest: request,
		masterClient:       node.masterClient,
	}

	err := node.sched.DdQueue.Enqueue(cit)
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
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()
	dit := &DescribeIndexTask{
		Condition:            NewTaskCondition(ctx),
		DescribeIndexRequest: request,
		masterClient:         node.masterClient,
	}

	err := node.sched.DdQueue.Enqueue(dit)
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
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()
	dipt := &GetIndexStateTask{
		Condition:             NewTaskCondition(ctx),
		IndexStateRequest:     request,
		indexServiceClient:    node.indexServiceClient,
		masterClientInterface: node.masterClient,
	}

	err := node.sched.DdQueue.Enqueue(dipt)
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
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()

	it := &InsertTask{
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

	err := node.sched.DmQueue.Enqueue(it)

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
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()

	qt := &SearchTask{
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

	err := node.sched.DqQueue.Enqueue(qt)
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
	log.Println("AA Flush collections: ", request.CollectionNames)
	ctx, cancel := context.WithTimeout(context.Background(), reqTimeoutInterval)
	defer cancel()
	ft := &FlushTask{
		Condition:         NewTaskCondition(ctx),
		FlushRequest:      request,
		dataServiceClient: node.dataServiceClient,
	}

	err := node.sched.DdQueue.Enqueue(ft)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	err = ft.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, nil
	}

	return ft.result, nil
}

func (node *NodeImpl) GetDdChannel(request *commonpb.Empty) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

func (node *NodeImpl) GetPersistentSegmentInfo(req *milvuspb.PersistentSegmentInfoRequest) (*milvuspb.PersistentSegmentInfoResponse, error) {
	resp := &milvuspb.PersistentSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
	}
	segments, err := node.getSegmentsOfCollection(req.DbName, req.CollectionName)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	infoResp, err := node.dataServiceClient.GetSegmentInfo(&datapb.SegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kSegmentInfo,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyID,
		},
		SegmentIDs: segments,
	})
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	if infoResp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		resp.Status.Reason = infoResp.Status.Reason
		return resp, nil
	}
	persistentInfos := make([]*milvuspb.PersistentSegmentInfo, len(infoResp.Infos))
	for i, info := range infoResp.Infos {
		persistentInfos[i] = &milvuspb.PersistentSegmentInfo{
			SegmentID:    info.SegmentID,
			CollectionID: info.CollectionID,
			PartitionID:  info.PartitionID,
			OpenTime:     info.OpenTime,
			SealedTime:   info.SealedTime,
			FlushedTime:  info.FlushedTime,
			NumRows:      info.NumRows,
			MemSize:      info.MemSize,
			State:        info.State,
		}
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
	resp.Infos = persistentInfos
	return resp, nil
}

func (node *NodeImpl) GetQuerySegmentInfo(req *milvuspb.QuerySegmentInfoRequest) (*milvuspb.QuerySegmentInfoResponse, error) {
	resp := &milvuspb.QuerySegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		},
	}
	segments, err := node.getSegmentsOfCollection(req.DbName, req.CollectionName)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	infoResp, err := node.queryServiceClient.GetSegmentInfo(&querypb.SegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kSegmentInfo,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyID,
		},
		SegmentIDs: segments,
	})
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	if infoResp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		resp.Status.Reason = infoResp.Status.Reason
		return resp, nil
	}
	queryInfos := make([]*milvuspb.QuerySegmentInfo, len(infoResp.Infos))
	for i, info := range infoResp.Infos {
		queryInfos[i] = &milvuspb.QuerySegmentInfo{
			SegmentID:    info.SegmentID,
			CollectionID: info.CollectionID,
			PartitionID:  info.PartitionID,
			NumRows:      info.NumRows,
			MemSize:      info.MemSize,
			IndexName:    info.IndexName,
			IndexID:      info.IndexID,
		}
	}
	resp.Status.ErrorCode = commonpb.ErrorCode_SUCCESS
	resp.Infos = queryInfos
	return resp, nil
}

func (node *NodeImpl) getSegmentsOfCollection(dbName string, collectionName string) ([]UniqueID, error) {
	describeCollectionResponse, err := node.masterClient.DescribeCollection(&milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kDescribeCollection,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyID,
		},
		DbName:         dbName,
		CollectionName: collectionName,
	})
	if err != nil {
		return nil, err
	}
	if describeCollectionResponse.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return nil, errors.New(describeCollectionResponse.Status.Reason)
	}
	collectionID := describeCollectionResponse.CollectionID
	showPartitionsResp, err := node.masterClient.ShowPartitions(&milvuspb.ShowPartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kShowPartitions,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyID,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		CollectionID:   collectionID,
	})
	if err != nil {
		return nil, err
	}
	if showPartitionsResp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return nil, errors.New(showPartitionsResp.Status.Reason)
	}

	ret := make([]UniqueID, 0)
	for _, partitionID := range showPartitionsResp.PartitionIDs {
		showSegmentResponse, err := node.masterClient.ShowSegments(&milvuspb.ShowSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_kShowSegment,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  Params.ProxyID,
			},
			CollectionID: collectionID,
			PartitionID:  partitionID,
		})
		if err != nil {
			return nil, err
		}
		if showSegmentResponse.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			return nil, errors.New(showSegmentResponse.Status.Reason)
		}
		ret = append(ret, showSegmentResponse.SegmentIDs...)
	}
	return ret, nil
}
