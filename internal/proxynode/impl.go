package proxynode

import (
	"context"
	"errors"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

const (
	reqTimeoutInterval = time.Second * 10
)

func (node *ProxyNode) UpdateStateCode(code internalpb.StateCode) {
	node.stateCode.Store(code)
}

func (node *ProxyNode) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	stats := &internalpb.ComponentStates{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}
	code, ok := node.stateCode.Load().(internalpb.StateCode)
	if !ok {
		errMsg := "unexpected error in type assertion"
		stats.Status = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    errMsg,
		}
		return stats, errors.New(errMsg)
	}
	info := &internalpb.ComponentInfo{
		NodeID:    Params.ProxyID,
		Role:      typeutil.ProxyNodeRole,
		StateCode: code,
	}
	stats.State = info
	return stats, nil
}

func (node *ProxyNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "",
	}, nil
}

func (node *ProxyNode) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	collectionName := request.CollectionName
	globalMetaCache.RemoveCollection(ctx, collectionName) // no need to return error, though collection may be not cached
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (node *ProxyNode) CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	log.Debug("create collection...")

	cct := &CreateCollectionTask{
		ctx:                     ctx,
		Condition:               NewTaskCondition(ctx),
		CreateCollectionRequest: request,
		masterService:           node.masterService,
		dataServiceClient:       node.dataService,
	}

	err := node.sched.DdQueue.Enqueue(cct)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	err = cct.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return cct.result, nil
}

func (node *ProxyNode) DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	log.Debug("drop collection... ")

	dct := &DropCollectionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		DropCollectionRequest: request,
		masterService:         node.masterService,
	}

	err := node.sched.DdQueue.Enqueue(dct)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	err = dct.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return dct.result, nil
}

func (node *ProxyNode) HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	log.Debug("has collection... ")

	hct := &HasCollectionTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		HasCollectionRequest: request,
		masterService:        node.masterService,
	}

	err := node.sched.DdQueue.Enqueue(hct)
	if err != nil {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = hct.WaitToFinish()
	if err != nil {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return hct.result, nil
}

func (node *ProxyNode) LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
	log.Debug("load collection...")
	//ctx, cancel := context.WithTimeout(ctx, reqTimeoutInterval)
	//defer cancel()

	lct := &LoadCollectionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadCollectionRequest: request,
		queryService:          node.queryService,
	}

	err := node.sched.DdQueue.Enqueue(lct)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	err = lct.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return lct.result, nil
}

func (node *ProxyNode) ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	log.Debug("release collection...")

	rct := &ReleaseCollectionTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		ReleaseCollectionRequest: request,
		queryService:             node.queryService,
	}

	err := node.sched.DdQueue.Enqueue(rct)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	err = rct.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return rct.result, nil
}

func (node *ProxyNode) DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	log.Debug("describe collection...")

	dct := &DescribeCollectionTask{
		ctx:                       ctx,
		Condition:                 NewTaskCondition(ctx),
		DescribeCollectionRequest: request,
		masterService:             node.masterService,
	}

	err := node.sched.DdQueue.Enqueue(dct)
	if err != nil {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = dct.WaitToFinish()
	if err != nil {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return dct.result, nil
}

func (node *ProxyNode) GetCollectionStatistics(ctx context.Context, request *milvuspb.GetCollectionStatisticsRequest) (*milvuspb.GetCollectionStatisticsResponse, error) {
	log.Debug("get collection statistics...")
	g := &GetCollectionsStatisticsTask{
		ctx:                            ctx,
		Condition:                      NewTaskCondition(ctx),
		GetCollectionStatisticsRequest: request,
		dataService:                    node.dataService,
	}

	err := node.sched.DdQueue.Enqueue(g)
	if err != nil {
		return &milvuspb.GetCollectionStatisticsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = g.WaitToFinish()
	if err != nil {
		return &milvuspb.GetCollectionStatisticsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return g.result, nil
}

func (node *ProxyNode) ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	log.Debug("show collections...")
	sct := &ShowCollectionsTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		ShowCollectionsRequest: request,
		masterService:          node.masterService,
	}

	err := node.sched.DdQueue.Enqueue(sct)
	if err != nil {
		return &milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = sct.WaitToFinish()
	if err != nil {
		return &milvuspb.ShowCollectionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return sct.result, nil
}

func (node *ProxyNode) CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	log.Debug("create partition...")
	cpt := &CreatePartitionTask{
		ctx:                    ctx,
		Condition:              NewTaskCondition(ctx),
		CreatePartitionRequest: request,
		masterService:          node.masterService,
		result:                 nil,
	}

	err := node.sched.DdQueue.Enqueue(cpt)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}
	err = cpt.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}
	return cpt.result, nil
}

func (node *ProxyNode) DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	log.Debug("drop partition...")
	dpt := &DropPartitionTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DropPartitionRequest: request,
		masterService:        node.masterService,
		result:               nil,
	}

	err := node.sched.DdQueue.Enqueue(dpt)

	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}
	err = dpt.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}
	return dpt.result, nil
}

func (node *ProxyNode) HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	log.Debug("has partition...")
	hpt := &HasPartitionTask{
		ctx:                 ctx,
		Condition:           NewTaskCondition(ctx),
		HasPartitionRequest: request,
		masterService:       node.masterService,
		result:              nil,
	}

	err := node.sched.DdQueue.Enqueue(hpt)

	if err != nil {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Value: false,
		}, nil
	}
	err = hpt.WaitToFinish()
	if err != nil {
		return &milvuspb.BoolResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Value: false,
		}, nil
	}
	return hpt.result, nil
}

func (node *ProxyNode) LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
	log.Debug("load partitions...")

	lpt := &LoadPartitionTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		LoadPartitionsRequest: request,
		queryService:          node.queryService,
	}

	err := node.sched.DdQueue.Enqueue(lpt)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	err = lpt.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return lpt.result, nil
}

func (node *ProxyNode) ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	log.Debug("load partitions...")

	rpt := &ReleasePartitionTask{
		ctx:                      ctx,
		Condition:                NewTaskCondition(ctx),
		ReleasePartitionsRequest: request,
		queryService:             node.queryService,
	}

	err := node.sched.DdQueue.Enqueue(rpt)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	err = rpt.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return rpt.result, nil
}

func (node *ProxyNode) GetPartitionStatistics(ctx context.Context, request *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error) {
	panic("implement me")
}

func (node *ProxyNode) ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	log.Debug("show partitions...")
	spt := &ShowPartitionsTask{
		ctx:                   ctx,
		Condition:             NewTaskCondition(ctx),
		ShowPartitionsRequest: request,
		masterService:         node.masterService,
		result:                nil,
	}

	err := node.sched.DdQueue.Enqueue(spt)

	if err != nil {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = spt.WaitToFinish()
	if err != nil {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}
	return spt.result, nil
}

func (node *ProxyNode) CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error) {
	log.Debug("create index for...")
	cit := &CreateIndexTask{
		ctx:                ctx,
		Condition:          NewTaskCondition(ctx),
		CreateIndexRequest: request,
		masterService:      node.masterService,
	}

	err := node.sched.DdQueue.Enqueue(cit)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	err = cit.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return cit.result, nil
}

func (node *ProxyNode) DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error) {
	log.Debug("Describe index for...")
	dit := &DescribeIndexTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		DescribeIndexRequest: request,
		masterService:        node.masterService,
	}

	err := node.sched.DdQueue.Enqueue(dit)
	if err != nil {
		return &milvuspb.DescribeIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = dit.WaitToFinish()
	if err != nil {
		return &milvuspb.DescribeIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: dit.result.Status.GetErrorCode(),
				Reason:    err.Error(),
			},
		}, nil
	}

	return dit.result, nil
}

func (node *ProxyNode) DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error) {
	log.Debug("Drop index for...")
	dit := &DropIndexTask{
		ctx:              ctx,
		Condition:        NewTaskCondition(ctx),
		DropIndexRequest: request,
		masterService:    node.masterService,
	}
	err := node.sched.DdQueue.Enqueue(dit)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}
	err = dit.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}
	return dit.result, nil
}

func (node *ProxyNode) GetIndexState(ctx context.Context, request *milvuspb.GetIndexStateRequest) (*milvuspb.GetIndexStateResponse, error) {
	dipt := &GetIndexStateTask{
		ctx:                  ctx,
		Condition:            NewTaskCondition(ctx),
		GetIndexStateRequest: request,
		indexService:         node.indexService,
		masterService:        node.masterService,
	}

	err := node.sched.DdQueue.Enqueue(dipt)
	if err != nil {
		return &milvuspb.GetIndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = dipt.WaitToFinish()
	if err != nil {
		return &milvuspb.GetIndexStateResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return dipt.result, nil
}

func (node *ProxyNode) Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.InsertResponse, error) {
	it := &InsertTask{
		ctx:         ctx,
		Condition:   NewTaskCondition(ctx),
		dataService: node.dataService,
		BaseInsertTask: BaseInsertTask{
			BaseMsg: msgstream.BaseMsg{
				HashValues: request.HashKeys,
			},
			InsertRequest: internalpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
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
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = it.WaitToFinish()
	if err != nil {
		return &milvuspb.InsertResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return it.result, nil
}

func (node *ProxyNode) Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
	qt := &SearchTask{
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Search,
				SourceID: Params.ProxyID,
			},
			ResultChannelID: strconv.FormatInt(Params.ProxyID, 10),
		},
		queryMsgStream: node.queryMsgStream,
		resultBuf:      make(chan []*internalpb.SearchResults),
		query:          request,
	}

	err := node.sched.DqQueue.Enqueue(qt)
	if err != nil {
		return &milvuspb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	err = qt.WaitToFinish()
	if err != nil {
		return &milvuspb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return qt.result, nil
}

func (node *ProxyNode) Flush(ctx context.Context, request *milvuspb.FlushRequest) (*commonpb.Status, error) {
	log.Debug("proxynode", zap.Strings("Flush collections: ", request.CollectionNames))
	ft := &FlushTask{
		ctx:          ctx,
		Condition:    NewTaskCondition(ctx),
		FlushRequest: request,
		dataService:  node.dataService,
	}

	err := node.sched.DdQueue.Enqueue(ft)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	err = ft.WaitToFinish()
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return ft.result, nil
}

func (node *ProxyNode) GetDdChannel(ctx context.Context, request *internalpb.GetDdChannelRequest) (*milvuspb.StringResponse, error) {
	panic("implement me")
}

func (node *ProxyNode) GetPersistentSegmentInfo(ctx context.Context, req *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
	resp := &milvuspb.GetPersistentSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	segments, err := node.getSegmentsOfCollection(ctx, req.DbName, req.CollectionName)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	infoResp, err := node.dataService.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentInfo,
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
	if infoResp.Status.ErrorCode != commonpb.ErrorCode_Success {
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
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.Infos = persistentInfos
	return resp, nil
}

func (node *ProxyNode) GetQuerySegmentInfo(ctx context.Context, req *milvuspb.GetQuerySegmentInfoRequest) (*milvuspb.GetQuerySegmentInfoResponse, error) {
	resp := &milvuspb.GetQuerySegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	segments, err := node.getSegmentsOfCollection(ctx, req.DbName, req.CollectionName)
	if err != nil {
		resp.Status.Reason = err.Error()
		return resp, nil
	}
	infoResp, err := node.queryService.GetSegmentInfo(ctx, &querypb.GetSegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentInfo,
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
	if infoResp.Status.ErrorCode != commonpb.ErrorCode_Success {
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
	resp.Status.ErrorCode = commonpb.ErrorCode_Success
	resp.Infos = queryInfos
	return resp, nil
}

func (node *ProxyNode) getSegmentsOfCollection(ctx context.Context, dbName string, collectionName string) ([]UniqueID, error) {
	describeCollectionResponse, err := node.masterService.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeCollection,
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
	if describeCollectionResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, errors.New(describeCollectionResponse.Status.Reason)
	}
	collectionID := describeCollectionResponse.CollectionID
	showPartitionsResp, err := node.masterService.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_ShowPartitions,
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
	if showPartitionsResp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, errors.New(showPartitionsResp.Status.Reason)
	}

	ret := make([]UniqueID, 0)
	for _, partitionID := range showPartitionsResp.PartitionIDs {
		showSegmentResponse, err := node.masterService.ShowSegments(ctx, &milvuspb.ShowSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowSegments,
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
		if showSegmentResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
			return nil, errors.New(showSegmentResponse.Status.Reason)
		}
		ret = append(ret, showSegmentResponse.SegmentIDs...)
	}
	return ret, nil
}

func (node *ProxyNode) RegisterLink(ctx context.Context, req *milvuspb.RegisterLinkRequest) (*milvuspb.RegisterLinkResponse, error) {
	code := node.stateCode.Load().(internalpb.StateCode)
	if code != internalpb.StateCode_Healthy {
		return &milvuspb.RegisterLinkResponse{
			Address: nil,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "proxy node not healthy",
			},
		}, nil
	}
	return &milvuspb.RegisterLinkResponse{
		Address: nil,
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
	}, nil
}
