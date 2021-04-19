package queryservice

import (
	"context"
	"log"
	"sort"
	"strconv"
	"sync/atomic"

	nodeclient "github.com/zilliztech/milvus-distributed/internal/distributed/querynode/client"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/querynode"
)

type MasterServiceInterface interface {
	ShowPartitions(in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error)
	ShowSegments(in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error)
}

type DataServiceInterface interface {
	GetSegmentStates(req *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error)
}

type QueryNodeInterface interface {
	GetComponentStates() (*internalpb2.ComponentStates, error)

	AddQueryChannel(in *querypb.AddQueryChannelsRequest) (*commonpb.Status, error)
	RemoveQueryChannel(in *querypb.RemoveQueryChannelsRequest) (*commonpb.Status, error)
	WatchDmChannels(in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error)
	LoadSegments(in *querypb.LoadSegmentRequest) (*commonpb.Status, error)
	ReleaseSegments(in *querypb.ReleaseSegmentRequest) (*commonpb.Status, error)
}

type QueryService struct {
	loopCtx    context.Context
	loopCancel context.CancelFunc

	queryServiceID uint64
	replica        metaReplica

	dataServiceClient   DataServiceInterface
	masterServiceClient MasterServiceInterface
	queryNodes          []*queryNode
	//TODO:: nodeID use UniqueID
	numRegisterNode uint64
	numQueryChannel uint64

	stateCode  atomic.Value
	isInit     atomic.Value
	enableGrpc bool
}

func (qs *QueryService) Init() error {
	Params.Init()
	qs.isInit.Store(true)
	return nil
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

func (qs *QueryService) GetComponentStates() (*internalpb2.ComponentStates, error) {
	serviceComponentInfo := &internalpb2.ComponentInfo{
		NodeID:    Params.QueryServiceID,
		StateCode: qs.stateCode.Load().(internalpb2.StateCode),
	}
	subComponentInfos := make([]*internalpb2.ComponentInfo, 0)
	for nodeID, node := range qs.queryNodes {
		componentStates, err := node.GetComponentStates()
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
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
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

// TODO:: do addWatchDmChannel to query node after registerNode
func (qs *QueryService) RegisterNode(req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error) {
	allocatedID := qs.numRegisterNode
	qs.numRegisterNode++

	if allocatedID > Params.QueryNodeNum {
		log.Fatal("allocated queryNodeID should lower than Params.QueryNodeNum")
	}

	registerNodeAddress := req.Address.Ip + ":" + strconv.FormatInt(req.Address.Port, 10)
	var node *queryNode
	if qs.enableGrpc {
		client := nodeclient.NewClient(registerNodeAddress)
		node = &queryNode{
			client: client,
			nodeID: allocatedID,
		}
	} else {
		client := querynode.NewQueryNode(qs.loopCtx, allocatedID)
		node = &queryNode{
			client: client,
			nodeID: allocatedID,
		}
	}
	qs.queryNodes[allocatedID] = node

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
	dbID := req.DbID
	collectionID := req.CollectionID
	qs.replica.loadCollection(dbID, collectionID)

	fn := func(err error) *commonpb.Status {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}
	}

	// get partitionIDs
	showPartitionRequest := &milvuspb.ShowPartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_kShowPartitions,
		},
		CollectionID: collectionID,
	}

	showPartitionResponse, err := qs.masterServiceClient.ShowPartitions(showPartitionRequest)
	if err != nil {
		return fn(err), err
	}
	if showPartitionResponse.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		log.Fatal("show partition fail, v%", showPartitionResponse.Status.Reason)
	}
	partitionIDs := showPartitionResponse.PartitionIDs

	loadPartitionsRequest := &querypb.LoadPartitionRequest{
		Base:         req.Base,
		DbID:         dbID,
		CollectionID: collectionID,
		PartitionIDs: partitionIDs,
	}

	status, err := qs.LoadPartitions(loadPartitionsRequest)

	return status, err
}

func (qs *QueryService) ReleaseCollection(req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	dbID := req.DbID
	collectionID := req.CollectionID
	partitionsIDs, err := qs.replica.getPartitionIDs(dbID, collectionID)
	if err != nil {
		log.Fatal("get partition ids error")
	}
	releasePartitionRequest := &querypb.ReleasePartitionRequest{
		Base:         req.Base,
		DbID:         dbID,
		CollectionID: collectionID,
		PartitionIDs: partitionsIDs,
	}

	status, err := qs.ReleasePartitions(releasePartitionRequest)

	return status, err
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
	dbID := req.DbID
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs
	qs.replica.loadPartitions(dbID, collectionID, partitionIDs)
	fn := func(err error) *commonpb.Status {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}
	}

	// get segments and load segment to query node
	for _, partitionID := range partitionIDs {
		showSegmentRequest := &milvuspb.ShowSegmentRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_kShowSegment,
			},
			CollectionID: collectionID,
			PartitionID:  partitionID,
		}
		showSegmentResponse, err := qs.masterServiceClient.ShowSegments(showSegmentRequest)
		if err != nil {
			return fn(err), err
		}
		if showSegmentResponse.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			log.Fatal("showSegment fail, v%", showSegmentResponse.Status.Reason)
		}
		segmentIDs := showSegmentResponse.SegmentIDs
		segmentStates := make(map[UniqueID]*datapb.SegmentStatesResponse)
		channel2id := make(map[string]int)
		id2channels := make(map[int][]string)
		id2segs := make(map[int][]UniqueID)
		offset := 0

		for _, segmentID := range segmentIDs {
			state, err := qs.dataServiceClient.GetSegmentStates(&datapb.SegmentStatesRequest{
				SegmentID: segmentID,
			})
			if err != nil {
				log.Fatal("get segment states fail")
			}
			segmentStates[segmentID] = state
			var flatChannelName string
			channelNames := make([]string, 0)
			for i, str := range state.StartPositions {
				flatChannelName += str.ChannelName
				channelNames = append(channelNames, str.ChannelName)
				if i < len(state.StartPositions) {
					flatChannelName += "/"
				}
			}
			if _, ok := channel2id[flatChannelName]; !ok {
				channel2id[flatChannelName] = offset
				id2channels[offset] = channelNames
				id2segs[offset] = make([]UniqueID, 0)
				id2segs[offset] = append(id2segs[offset], segmentID)
				offset++
			} else {
				//TODO::check channel name
				id := channel2id[flatChannelName]
				id2segs[id] = append(id2segs[id], segmentID)
			}
		}
		for key, value := range id2segs {
			sort.Slice(value, func(i, j int) bool { return segmentStates[value[i]].CreateTime < segmentStates[value[j]].CreateTime })
			selectedSegs := make([]UniqueID, 0)
			for i, v := range value {
				if segmentStates[v].State == datapb.SegmentState_SegmentFlushed {
					selectedSegs = append(selectedSegs, v)
				} else {
					if i > 0 && segmentStates[v-1].State != datapb.SegmentState_SegmentFlushed {
						break
					}
					selectedSegs = append(selectedSegs, v)
				}
			}
			id2segs[key] = selectedSegs
		}

		qs.replica.updatePartitionState(dbID, collectionID, partitionID, querypb.PartitionState_PartialInMemory)

		// TODO:: filter channel for query node
		for channels, i := range channel2id {
			for key, node := range qs.queryNodes {
				if channels == node.insertChannels {
					statesID := id2segs[i][len(id2segs[i])-1]
					loadSegmentRequest := &querypb.LoadSegmentRequest{
						CollectionID:     collectionID,
						PartitionID:      partitionID,
						SegmentIDs:       id2segs[i],
						LastSegmentState: segmentStates[statesID],
					}
					status, err := qs.queryNodes[key].LoadSegments(loadSegmentRequest)
					if err != nil {
						return status, err
					}
				}
			}
		}
		qs.replica.updatePartitionState(dbID, collectionID, partitionID, querypb.PartitionState_InMemory)
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil
}

func (qs *QueryService) ReleasePartitions(req *querypb.ReleasePartitionRequest) (*commonpb.Status, error) {
	dbID := req.DbID
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs
	segmentIDs := make([]UniqueID, 0)
	for _, partitionID := range partitionIDs {
		res, err := qs.replica.getSegmentIDs(dbID, collectionID, partitionID)
		if err != nil {
			log.Fatal("get segment ids error")
		}
		segmentIDs = append(segmentIDs, res...)
	}
	releaseSegmentRequest := &querypb.ReleaseSegmentRequest{
		Base:         req.Base,
		DbID:         dbID,
		CollectionID: collectionID,
		PartitionIDs: partitionIDs,
		SegmentIDs:   segmentIDs,
	}

	for _, node := range qs.queryNodes {
		status, err := node.client.ReleaseSegments(releaseSegmentRequest)
		if err != nil {
			return status, err
		}
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil
}

func (qs *QueryService) CreateQueryChannel() (*querypb.CreateQueryChannelResponse, error) {
	channelID := qs.numQueryChannel
	qs.numQueryChannel++
	allocatedQueryChannel := "query-" + strconv.FormatInt(int64(channelID), 10)
	allocatedQueryResultChannel := "queryResult-" + strconv.FormatInt(int64(channelID), 10)

	//TODO:: query node watch query channels
	return &querypb.CreateQueryChannelResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		RequestChannel: allocatedQueryChannel,
		ResultChannel:  allocatedQueryResultChannel,
	}, nil
}

func (qs *QueryService) GetPartitionStates(req *querypb.PartitionStatesRequest) (*querypb.PartitionStatesResponse, error) {
	states, err := qs.replica.getPartitionStates(req.DbID, req.CollectionID, req.PartitionIDs)
	if err != nil {
		return &querypb.PartitionStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
			PartitionDescriptions: states,
		}, err
	}
	return &querypb.PartitionStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		PartitionDescriptions: states,
	}, nil
}

func NewQueryService(ctx context.Context) (*QueryService, error) {
	nodes := make([]*queryNode, 0)
	ctx1, cancel := context.WithCancel(ctx)
	replica := newMetaReplica()
	service := &QueryService{
		loopCtx:         ctx1,
		loopCancel:      cancel,
		queryNodes:      nodes,
		replica:         replica,
		numRegisterNode: 0,
		numQueryChannel: 0,
		enableGrpc:      false,
	}
	service.stateCode.Store(internalpb2.StateCode_INITIALIZING)
	service.isInit.Store(false)
	return service, nil
}

func (qs *QueryService) SetMasterService(masterService MasterServiceInterface) {
	qs.masterServiceClient = masterService
}

func (qs *QueryService) SetDataService(dataService DataServiceInterface) {
	qs.dataServiceClient = dataService
}

func (qs *QueryService) SetEnableGrpc(en bool) {
	qs.enableGrpc = en
}
