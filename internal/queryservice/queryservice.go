package queryservice

import (
	"context"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/distributed/masterservice"
	grpcquerynodeclient "github.com/zilliztech/milvus-distributed/internal/distributed/querynode/client"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/querynode"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type Interface = typeutil.QueryServiceInterface

type QueryService struct {
	loopCtx    context.Context
	loopCancel context.CancelFunc

	QueryServiceID uint64
	replica        metaReplica

	// type(masterServiceClient) should be interface
	// masterServiceClient masterService.Service
	masterServiceClient *masterservice.GrpcClient
	queryNodeClient     map[int]querynode.Node
	numQueryNode        int
	numQueryChannel     int

	stateCode atomic.Value
	isInit    atomic.Value
}

type InitParams struct {
	Distributed bool
}

//serverBase interface
func (qs *QueryService) Init() error {
	if Params.Distributed {
		var err error
		//TODO:: alter 2*second
		qs.masterServiceClient, err = masterservice.NewGrpcClient(Params.MasterServiceAddress, 2*time.Second)
		if err != nil {
			return err

		}
	} else {
		//TODO:: create masterService.Core{}
		log.Fatal(errors.New("should not use grpc client"))
	}

	qs.isInit.Store(true)
	return nil
}

func (qs *QueryService) InitParams(params *InitParams) {
	Params.Distributed = params.Distributed
}

func (qs *QueryService) Start() error {
	isInit := qs.isInit.Load().(bool)
	if !isInit {
		return errors.New("call start before init")
	}
	qs.stateCode.Store(internalpb2.StateCode_HEALTHY)
	return nil
}

func (qs *QueryService) Stop() error {
	qs.loopCancel()
	qs.stateCode.Store(internalpb2.StateCode_ABNORMAL)
	return nil
}

//func (qs *QueryService) SetDataService(p querynode.DataServiceInterface) error {
//	for k, v := range qs.queryNodeClient {
//		v.Set
//	}
//	return c.SetDataService(p)
//}
//
//func (qs *QueryService) SetIndexService(p querynode.IndexServiceInterface) error {
//	c, ok := s.core.(*cms.Core)
//	if !ok {
//		return errors.Errorf("set index service failed")
//	}
//	return c.SetIndexService(p)
//}

func (qs *QueryService) GetComponentStates() (*internalpb2.ComponentStates, error) {
	serviceComponentInfo := &internalpb2.ComponentInfo{
		NodeID:    Params.QueryServiceID,
		StateCode: qs.stateCode.Load().(internalpb2.StateCode),
	}
	subComponentInfos := make([]*internalpb2.ComponentInfo, 0)
	for nodeID, nodeClient := range qs.queryNodeClient {
		componentStates, err := nodeClient.GetComponentStates()
		if err != nil {
			subComponentInfos = append(subComponentInfos, &internalpb2.ComponentInfo{
				NodeID:    int64(nodeID),
				StateCode: internalpb2.StateCode_ABNORMAL,
			})
			continue
		}
		subComponentInfos = append(subComponentInfos, componentStates.State)
	}
	return &internalpb2.ComponentStates{
		State:              serviceComponentInfo,
		SubcomponentStates: subComponentInfos,
	}, nil
}

func (qs *QueryService) GetTimeTickChannel() (string, error) {
	return Params.TimeTickChannelName, nil
}

func (qs *QueryService) GetStatisticsChannel() (string, error) {
	return Params.StatsChannelName, nil
}

func (qs *QueryService) RegisterNode(req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error) {
	allocatedID := qs.numQueryNode
	qs.numQueryNode++

	registerNodeAddress := req.Address.Ip + ":" + strconv.FormatInt(req.Address.Port, 10)
	var client querynode.Node
	if Params.Distributed {
		client = grpcquerynodeclient.NewClient(registerNodeAddress)
	} else {
		log.Fatal(errors.New("should be queryNodeImpl.QueryNode"))
	}
	qs.queryNodeClient[allocatedID] = client

	return &querypb.RegisterNodeResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		InitParams: &internalpb2.InitParams{
			NodeID: int64(allocatedID),
		},
	}, nil
}

func (qs *QueryService) ShowCollections(req *querypb.ShowCollectionRequest) (*querypb.ShowCollectionResponse, error) {
	dbID := req.DbID
	collectionIDs, err := qs.replica.getCollectionIDs(dbID)
	if err != nil {
		return nil, err
	}
	return &querypb.ShowCollectionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		CollectionIDs: collectionIDs,
	}, nil
}

func (qs *QueryService) LoadCollection(req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (qs *QueryService) ReleaseCollection(req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (qs *QueryService) ShowPartitions(req *querypb.ShowPartitionRequest) (*querypb.ShowPartitionResponse, error) {
	dbID := req.DbID
	collectionID := req.CollectionID
	partitionIDs, err := qs.replica.getPartitionIDs(dbID, collectionID)
	if err != nil {
		return nil, err
	}
	return &querypb.ShowPartitionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		PartitionIDs: partitionIDs,
	}, nil

}

func (qs *QueryService) LoadPartitions(req *querypb.LoadPartitionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (qs *QueryService) ReleasePartitions(req *querypb.ReleasePartitionRequest) (*commonpb.Status, error) {
	panic("implement me")
}

func (qs *QueryService) CreateQueryChannel() (*querypb.CreateQueryChannelResponse, error) {
	channelID := qs.numQueryChannel
	qs.numQueryChannel++
	allocatedQueryChannel := "query-" + strconv.FormatInt(int64(channelID), 10)
	allocatedQueryResultChannel := "queryResult-" + strconv.FormatInt(int64(channelID), 10)

	return &querypb.CreateQueryChannelResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		RequestChannel: allocatedQueryChannel,
		ResultChannel:  allocatedQueryResultChannel,
	}, nil
}

func (qs *QueryService) GetPartitionStates(req *querypb.PartitionStatesRequest) (*querypb.PartitionStatesResponse, error) {
	panic("implement me")
}

func NewQueryService(ctx context.Context) (Interface, error) {
	Params.Init()
	nodeClients := make(map[int]querynode.Node)
	ctx1, cancel := context.WithCancel(ctx)
	service := &QueryService{
		loopCtx:         ctx1,
		loopCancel:      cancel,
		numQueryNode:    0,
		queryNodeClient: nodeClients,
		numQueryChannel: 0,
	}
	service.stateCode.Store(internalpb2.StateCode_INITIALIZING)
	service.isInit.Store(false)
	return service, nil
}
