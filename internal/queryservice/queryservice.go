package queryservice

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	nodeclient "github.com/zilliztech/milvus-distributed/internal/distributed/querynode/client"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"
)

type MasterServiceInterface interface {
	ShowPartitions(in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error)
	ShowSegments(in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error)
}

type DataServiceInterface interface {
	GetSegmentStates(req *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error)
	GetInsertChannels(req *datapb.InsertChannelRequest) (*internalpb2.StringList, error)
}

type QueryNodeInterface interface {
	GetComponentStates() (*internalpb2.ComponentStates, error)

	AddQueryChannel(in *querypb.AddQueryChannelsRequest) (*commonpb.Status, error)
	RemoveQueryChannel(in *querypb.RemoveQueryChannelsRequest) (*commonpb.Status, error)
	WatchDmChannels(in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error)
	LoadSegments(in *querypb.LoadSegmentRequest) (*commonpb.Status, error)
	ReleaseSegments(in *querypb.ReleaseSegmentRequest) (*commonpb.Status, error)
	GetSegmentInfo(req *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error)
}

type queryChannelInfo struct {
	requestChannel  string
	responseChannel string
}

type QueryService struct {
	loopCtx    context.Context
	loopCancel context.CancelFunc

	queryServiceID uint64
	replica        metaReplica

	dataServiceClient   DataServiceInterface
	masterServiceClient MasterServiceInterface
	queryNodes          map[int64]*queryNodeInfo
	queryChannels       []*queryChannelInfo
	qcMutex             *sync.Mutex

	stateCode  atomic.Value
	isInit     atomic.Value
	enableGrpc bool

	msFactory msgstream.Factory
}

func (qs *QueryService) Init() error {
	return nil
}

func (qs *QueryService) Start() error {
	qs.UpdateStateCode(internalpb2.StateCode_HEALTHY)
	return nil
}

func (qs *QueryService) Stop() error {
	qs.loopCancel()
	qs.UpdateStateCode(internalpb2.StateCode_ABNORMAL)
	return nil
}

func (qs *QueryService) UpdateStateCode(code internalpb2.StateCode) {
	qs.stateCode.Store(code)
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
				NodeID:    nodeID,
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

func (qs *QueryService) RegisterNode(req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error) {
	fmt.Println("register query node =", req.Address)
	// TODO:: add mutex
	nodeID := req.Base.SourceID
	if _, ok := qs.queryNodes[nodeID]; ok {
		err := errors.New("nodeID already exists")
		return &querypb.RegisterNodeResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, err
	}

	registerNodeAddress := req.Address.Ip + ":" + strconv.FormatInt(req.Address.Port, 10)
	client := nodeclient.NewClient(registerNodeAddress)
	if err := client.Init(); err != nil {
		return &querypb.RegisterNodeResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			},
			InitParams: new(internalpb2.InitParams),
		}, err
	}
	if err := client.Start(); err != nil {
		return nil, err
	}
	qs.queryNodes[nodeID] = newQueryNodeInfo(client)

	//TODO::return init params to queryNode
	startParams := []*commonpb.KeyValuePair{
		{Key: "StatsChannelName", Value: Params.StatsChannelName},
		{Key: "TimeTickChannelName", Value: Params.TimeTickChannelName},
	}
	qs.qcMutex.Lock()
	for _, queryChannel := range qs.queryChannels {
		startParams = append(startParams, &commonpb.KeyValuePair{
			Key:   "QueryChannelName",
			Value: queryChannel.requestChannel,
		})
		startParams = append(startParams, &commonpb.KeyValuePair{
			Key:   "QueryResultChannelName",
			Value: queryChannel.responseChannel,
		})
	}
	qs.qcMutex.Unlock()

	return &querypb.RegisterNodeResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		InitParams: &internalpb2.InitParams{
			NodeID:      nodeID,
			StartParams: startParams,
		},
	}, nil
}

func (qs *QueryService) ShowCollections(req *querypb.ShowCollectionRequest) (*querypb.ShowCollectionResponse, error) {
	dbID := req.DbID
	fmt.Println("show collection start, dbID = ", dbID)
	collections, err := qs.replica.getCollections(dbID)
	collectionIDs := make([]UniqueID, 0)
	for _, collection := range collections {
		collectionIDs = append(collectionIDs, collection.id)
	}
	if err != nil {
		return &querypb.ShowCollectionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, err
	}
	fmt.Println("show collection end")
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
	schema := req.Schema
	fn := func(err error) *commonpb.Status {
		if err != nil {
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			}
		}
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		}
	}

	fmt.Println("load collection start, collectionID = ", collectionID)
	_, err := qs.replica.getCollectionByID(dbID, collectionID)
	if err == nil {
		fmt.Println("load collection end, collection already exist, collectionID = ", collectionID)
		return fn(nil), nil
	}

	err = qs.replica.addCollection(dbID, collectionID, schema)
	if err != nil {
		return fn(err), err
	}

	err = qs.watchDmChannels(dbID, collectionID)
	if err != nil {
		return fn(err), err
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
		return showPartitionResponse.Status, err
	}
	partitionIDs := showPartitionResponse.PartitionIDs

	loadPartitionsRequest := &querypb.LoadPartitionRequest{
		Base:         req.Base,
		DbID:         dbID,
		CollectionID: collectionID,
		PartitionIDs: partitionIDs,
		Schema:       schema,
	}

	status, err := qs.LoadPartitions(loadPartitionsRequest)

	fmt.Println("load collection end, collectionID = ", collectionID)
	return status, err
}

func (qs *QueryService) ReleaseCollection(req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	dbID := req.DbID
	collectionID := req.CollectionID
	fmt.Println("release collection start, collectionID = ", collectionID)
	partitions, err := qs.replica.getPartitions(dbID, collectionID)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, err
	}

	partitionIDs := make([]UniqueID, 0)
	for _, partition := range partitions {
		partitionIDs = append(partitionIDs, partition.id)
	}

	releasePartitionRequest := &querypb.ReleasePartitionRequest{
		Base:         req.Base,
		DbID:         dbID,
		CollectionID: collectionID,
		PartitionIDs: partitionIDs,
	}

	status, err := qs.ReleasePartitions(releasePartitionRequest)

	err = qs.replica.releaseCollection(dbID, collectionID)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, err
	}

	fmt.Println("release collection end")
	//TODO:: queryNode cancel subscribe dmChannels
	return status, err
}

func (qs *QueryService) ShowPartitions(req *querypb.ShowPartitionRequest) (*querypb.ShowPartitionResponse, error) {
	dbID := req.DbID
	collectionID := req.CollectionID
	partitions, err := qs.replica.getPartitions(dbID, collectionID)
	partitionIDs := make([]UniqueID, 0)
	for _, partition := range partitions {
		partitionIDs = append(partitionIDs, partition.id)
	}
	if err != nil {
		return &querypb.ShowPartitionResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			},
		}, err
	}
	return &querypb.ShowPartitionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		PartitionIDs: partitionIDs,
	}, nil
}

func (qs *QueryService) LoadPartitions(req *querypb.LoadPartitionRequest) (*commonpb.Status, error) {
	//TODO::suggest different partitions have different dm channel
	dbID := req.DbID
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs
	schema := req.Schema

	fn := func(err error) *commonpb.Status {
		if err != nil {
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			}
		}
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		}
	}
	fmt.Println("load partitions start, partitionIDs = ", partitionIDs)

	if len(partitionIDs) == 0 {
		err := errors.New("partitionIDs are empty")
		return fn(err), err
	}

	_, err := qs.replica.getCollectionByID(dbID, collectionID)
	if err != nil {
		err = qs.replica.addCollection(dbID, collectionID, schema)
		if err != nil {
			return fn(err), err
		}
		err = qs.watchDmChannels(dbID, collectionID)
		if err != nil {
			return fn(err), err
		}
	}

	for _, partitionID := range partitionIDs {
		_, err = qs.replica.getPartitionByID(dbID, collectionID, partitionID)
		if err == nil {
			continue
		}
		err = qs.replica.addPartition(dbID, collectionID, partitionID)
		if err != nil {
			return fn(err), err
		}

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
		segmentIDs := showSegmentResponse.SegmentIDs
		if len(segmentIDs) == 0 {
			loadSegmentRequest := &querypb.LoadSegmentRequest{
				CollectionID: collectionID,
				PartitionID:  partitionID,
				Schema:       schema,
			}
			for _, node := range qs.queryNodes {
				_, err := node.LoadSegments(loadSegmentRequest)
				if err != nil {
					return fn(err), nil
				}
			}
		}
		qs.replica.updatePartitionState(dbID, collectionID, partitionID, querypb.PartitionState_PartialInMemory)

		segmentStates := make(map[UniqueID]*datapb.SegmentStateInfo)
		channel2segs := make(map[string][]UniqueID)
		resp, err := qs.dataServiceClient.GetSegmentStates(&datapb.SegmentStatesRequest{
			SegmentIDs: segmentIDs,
		})
		if err != nil {
			return fn(err), err
		}
		for _, state := range resp.States {
			fmt.Println("segment ", state.SegmentID, " 's state is ", state.StartPosition)
			segmentID := state.SegmentID
			segmentStates[segmentID] = state
			channelName := state.StartPosition.ChannelName
			if _, ok := channel2segs[channelName]; !ok {
				segments := make([]UniqueID, 0)
				segments = append(segments, segmentID)
				channel2segs[channelName] = segments
			} else {
				channel2segs[channelName] = append(channel2segs[channelName], segmentID)
			}
		}

		for channel, segmentIDs := range channel2segs {
			sort.Slice(segmentIDs, func(i, j int) bool {
				return segmentStates[segmentIDs[i]].StartPosition.Timestamp < segmentStates[segmentIDs[j]].StartPosition.Timestamp
			})

			states := make([]*datapb.SegmentStateInfo, 0)
			for _, id := range segmentIDs {
				states = append(states, segmentStates[id])
			}
			loadSegmentRequest := &querypb.LoadSegmentRequest{
				CollectionID:  collectionID,
				PartitionID:   partitionID,
				SegmentIDs:    segmentIDs,
				SegmentStates: states,
				Schema:        schema,
			}
			nodeID, err := qs.replica.getAssignedNodeIDByChannelName(dbID, collectionID, channel)
			if err != nil {
				return fn(err), err
			}
			queryNode := qs.queryNodes[nodeID]
			//TODO:: seek when loadSegment may cause more msgs consumed
			//TODO:: all query node should load partition's msg
			status, err := queryNode.LoadSegments(loadSegmentRequest)
			if err != nil {
				return status, err
			}
		}

		qs.replica.updatePartitionState(dbID, collectionID, partitionID, querypb.PartitionState_InMemory)
	}

	fmt.Println("load partitions end, partitionIDs = ", partitionIDs)
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil
}

func (qs *QueryService) ReleasePartitions(req *querypb.ReleasePartitionRequest) (*commonpb.Status, error) {
	dbID := req.DbID
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs
	segmentIDs := make([]UniqueID, 0)
	fmt.Println("start release partitions start, partitionIDs = ", partitionIDs)
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
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			}, err
		}

		segmentIDs = append(segmentIDs, showSegmentResponse.SegmentIDs...)
		err = qs.replica.releasePartition(dbID, collectionID, partitionID)
		if err != nil {
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			}, err
		}
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

	fmt.Println("start release partitions end")
	//TODO:: queryNode cancel subscribe dmChannels
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil
}

func (qs *QueryService) CreateQueryChannel() (*querypb.CreateQueryChannelResponse, error) {
	channelID := len(qs.queryChannels)
	allocatedQueryChannel := "query-" + strconv.FormatInt(int64(channelID), 10)
	allocatedQueryResultChannel := "queryResult-" + strconv.FormatInt(int64(channelID), 10)

	qs.qcMutex.Lock()
	qs.queryChannels = append(qs.queryChannels, &queryChannelInfo{
		requestChannel:  allocatedQueryChannel,
		responseChannel: allocatedQueryResultChannel,
	})

	addQueryChannelsRequest := &querypb.AddQueryChannelsRequest{
		RequestChannelID: allocatedQueryChannel,
		ResultChannelID:  allocatedQueryResultChannel,
	}
	fmt.Println("query service create query channel, queryChannelName = ", allocatedQueryChannel)
	for nodeID, node := range qs.queryNodes {
		fmt.Println("node ", nodeID, " watch query channel")
		fn := func() error {
			_, err := node.AddQueryChannel(addQueryChannelsRequest)
			return err
		}
		err := retry.Retry(10, time.Millisecond*200, fn)
		if err != nil {
			qs.qcMutex.Unlock()
			return &querypb.CreateQueryChannelResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
					Reason:    err.Error(),
				},
			}, err
		}
	}
	qs.qcMutex.Unlock()

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

func (qs *QueryService) GetSegmentInfo(req *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error) {
	segmentInfos := make([]*querypb.SegmentInfo, 0)
	for _, node := range qs.queryNodes {
		segmentInfo, err := node.client.GetSegmentInfo(req)
		if err != nil {
			return &querypb.SegmentInfoResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
					Reason:    err.Error(),
				},
			}, err
		}
		segmentInfos = append(segmentInfos, segmentInfo.Infos...)
	}
	return &querypb.SegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Infos: segmentInfos,
	}, nil
}

func NewQueryService(ctx context.Context, factory msgstream.Factory) (*QueryService, error) {
	nodes := make(map[int64]*queryNodeInfo)
	queryChannels := make([]*queryChannelInfo, 0)
	ctx1, cancel := context.WithCancel(ctx)
	replica := newMetaReplica()
	service := &QueryService{
		loopCtx:       ctx1,
		loopCancel:    cancel,
		replica:       replica,
		queryNodes:    nodes,
		queryChannels: queryChannels,
		qcMutex:       &sync.Mutex{},
		msFactory:     factory,
	}
	service.UpdateStateCode(internalpb2.StateCode_ABNORMAL)
	return service, nil
}

func (qs *QueryService) SetMasterService(masterService MasterServiceInterface) {
	qs.masterServiceClient = masterService
}

func (qs *QueryService) SetDataService(dataService DataServiceInterface) {
	qs.dataServiceClient = dataService
}

func (qs *QueryService) watchDmChannels(dbID UniqueID, collectionID UniqueID) error {
	collection, err := qs.replica.getCollectionByID(0, collectionID)
	if err != nil {
		return err
	}
	channelRequest := datapb.InsertChannelRequest{
		DbID:         dbID,
		CollectionID: collectionID,
	}
	resp, err := qs.dataServiceClient.GetInsertChannels(&channelRequest)
	if err != nil {
		return err
	}
	if len(resp.Values) == 0 {
		err = errors.New("haven't assign dm channel to collection")
		return err
	}

	dmChannels := resp.Values
	watchedChannels2NodeID := make(map[string]int64)
	unWatchedChannels := make([]string, 0)
	for _, channel := range dmChannels {
		findChannel := false
		for nodeID, node := range qs.queryNodes {
			watchedChannels := node.dmChannelNames
			for _, watchedChannel := range watchedChannels {
				if channel == watchedChannel {
					findChannel = true
					watchedChannels2NodeID[channel] = nodeID
					break
				}
			}
		}
		if !findChannel {
			unWatchedChannels = append(unWatchedChannels, channel)
		}
	}
	channels2NodeID := qs.shuffleChannelsToQueryNode(unWatchedChannels)
	err = qs.replica.addDmChannels(dbID, collection.id, channels2NodeID)
	if err != nil {
		return err
	}
	err = qs.replica.addDmChannels(dbID, collection.id, watchedChannels2NodeID)
	if err != nil {
		return err
	}
	node2channels := make(map[int64][]string)
	for channel, nodeID := range channels2NodeID {
		if _, ok := node2channels[nodeID]; ok {
			node2channels[nodeID] = append(node2channels[nodeID], channel)
		} else {
			channels := make([]string, 0)
			channels = append(channels, channel)
			node2channels[nodeID] = channels
		}
	}

	for nodeID, channels := range node2channels {
		node := qs.queryNodes[nodeID]
		request := &querypb.WatchDmChannelsRequest{
			ChannelIDs: channels,
		}
		_, err := node.WatchDmChannels(request)
		if err != nil {
			return err
		}
		fmt.Println("query node ", nodeID, "watch channels = ", channels)
		node.AddDmChannels(channels)
	}

	return nil
}

func (qs *QueryService) shuffleChannelsToQueryNode(dmChannels []string) map[string]int64 {
	maxNumDMChannel := 0
	res := make(map[string]int64)
	if len(dmChannels) == 0 {
		return res
	}
	node2lens := make(map[int64]int)
	for id, node := range qs.queryNodes {
		node2lens[id] = len(node.dmChannelNames)
	}
	offset := 0
	for {
		lastOffset := offset
		for id, len := range node2lens {
			if len >= maxNumDMChannel {
				maxNumDMChannel = len
			} else {
				res[dmChannels[offset]] = id
				node2lens[id]++
				offset++
			}
		}
		if lastOffset == offset {
			for id := range node2lens {
				res[dmChannels[offset]] = id
				node2lens[id]++
				offset++
				break
			}
		}
		if offset == len(dmChannels) {
			break
		}
	}
	return res
}
