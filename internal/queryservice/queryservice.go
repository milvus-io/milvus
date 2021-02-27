package queryservice

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"
	"go.uber.org/zap"

	nodeclient "github.com/zilliztech/milvus-distributed/internal/distributed/querynode/client"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/util/retry"
)

type MasterServiceInterface interface {
	ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error)
	ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error)
}

type DataServiceInterface interface {
	GetSegmentStates(ctx context.Context, req *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error)
	GetInsertChannels(ctx context.Context, req *datapb.InsertChannelRequest) (*internalpb2.StringList, error)
}

type QueryNodeInterface interface {
	GetComponentStates(ctx context.Context) (*internalpb2.ComponentStates, error)

	AddQueryChannel(ctx context.Context, in *querypb.AddQueryChannelsRequest) (*commonpb.Status, error)
	RemoveQueryChannel(ctx context.Context, in *querypb.RemoveQueryChannelsRequest) (*commonpb.Status, error)
	WatchDmChannels(ctx context.Context, in *querypb.WatchDmChannelsRequest) (*commonpb.Status, error)
	LoadSegments(ctx context.Context, in *querypb.LoadSegmentRequest) (*commonpb.Status, error)
	ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error)
	ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionRequest) (*commonpb.Status, error)
	ReleaseSegments(ctx context.Context, in *querypb.ReleaseSegmentRequest) (*commonpb.Status, error)
	GetSegmentInfo(ctx context.Context, req *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error)
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

	closer io.Closer
}

func (qs *QueryService) Init() error {
	return nil
}

func (qs *QueryService) Start() error {
	qs.UpdateStateCode(internalpb2.StateCode_HEALTHY)
	return nil
}

func (qs *QueryService) Stop() error {
	if err := qs.closer.Close(); err != nil {
		return err
	}
	qs.loopCancel()
	qs.UpdateStateCode(internalpb2.StateCode_ABNORMAL)
	return nil
}

func (qs *QueryService) UpdateStateCode(code internalpb2.StateCode) {
	qs.stateCode.Store(code)
}

func (qs *QueryService) GetComponentStates(ctx context.Context) (*internalpb2.ComponentStates, error) {
	serviceComponentInfo := &internalpb2.ComponentInfo{
		NodeID:    Params.QueryServiceID,
		StateCode: qs.stateCode.Load().(internalpb2.StateCode),
	}
	subComponentInfos := make([]*internalpb2.ComponentInfo, 0)
	for nodeID, node := range qs.queryNodes {
		componentStates, err := node.GetComponentStates(ctx)
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

func (qs *QueryService) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: Params.TimeTickChannelName,
	}, nil
}

func (qs *QueryService) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
		Value: Params.StatsChannelName,
	}, nil
}

func (qs *QueryService) RegisterNode(ctx context.Context, req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error) {
	log.Debug("register query node", zap.String("address", req.Address.String()))
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

func (qs *QueryService) ShowCollections(ctx context.Context, req *querypb.ShowCollectionRequest) (*querypb.ShowCollectionResponse, error) {
	dbID := req.DbID
	log.Debug("show collection start, dbID = ", zap.String("dbID", strconv.FormatInt(dbID, 10)))
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
	log.Debug("show collection end")
	return &querypb.ShowCollectionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		CollectionIDs: collectionIDs,
	}, nil
}

func (qs *QueryService) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
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

	log.Debug("load collection start", zap.String("collectionID", fmt.Sprintln(collectionID)))
	_, err := qs.replica.getCollectionByID(dbID, collectionID)
	if err == nil {
		log.Error("load collection end, collection already exist", zap.String("collectionID", fmt.Sprintln(collectionID)))
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

	showPartitionResponse, err := qs.masterServiceClient.ShowPartitions(ctx, showPartitionRequest)
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

	status, err := qs.LoadPartitions(ctx, loadPartitionsRequest)

	log.Debug("load collection end", zap.String("collectionID", fmt.Sprintln(collectionID)))
	return status, err
}

func (qs *QueryService) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	dbID := req.DbID
	collectionID := req.CollectionID
	log.Debug("release collection start", zap.String("collectionID", fmt.Sprintln(collectionID)))
	_, err := qs.replica.getCollectionByID(dbID, collectionID)
	if err != nil {
		log.Error("release collection end, query service don't have the log of", zap.String("collectionID", fmt.Sprintln(collectionID)))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		}, nil
	}

	for nodeID, node := range qs.queryNodes {
		status, err := node.ReleaseCollection(ctx, req)
		if err != nil {
			log.Error("release collection end, node occur error", zap.String("nodeID", fmt.Sprintln(nodeID)))
			return status, err
		}
	}

	err = qs.replica.releaseCollection(dbID, collectionID)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}, err
	}

	log.Debug("release collection end")
	//TODO:: queryNode cancel subscribe dmChannels
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil
}

func (qs *QueryService) ShowPartitions(ctx context.Context, req *querypb.ShowPartitionRequest) (*querypb.ShowPartitionResponse, error) {
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

func (qs *QueryService) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionRequest) (*commonpb.Status, error) {
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
	log.Debug("load partitions start", zap.String("partitionIDs", fmt.Sprintln(partitionIDs)))

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
		showSegmentResponse, err := qs.masterServiceClient.ShowSegments(ctx, showSegmentRequest)
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
				_, err := node.LoadSegments(ctx, loadSegmentRequest)
				if err != nil {
					return fn(err), nil
				}
			}
		}
		qs.replica.updatePartitionState(dbID, collectionID, partitionID, querypb.PartitionState_PartialInMemory)

		segmentStates := make(map[UniqueID]*datapb.SegmentStateInfo)
		channel2segs := make(map[string][]UniqueID)
		resp, err := qs.dataServiceClient.GetSegmentStates(ctx, &datapb.SegmentStatesRequest{
			SegmentIDs: segmentIDs,
		})
		if err != nil {
			return fn(err), err
		}
		for _, state := range resp.States {
			log.Error("segment ", zap.String("state.SegmentID", fmt.Sprintln(state.SegmentID)), zap.String("state", fmt.Sprintln(state.StartPosition)))
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
			status, err := queryNode.LoadSegments(ctx, loadSegmentRequest)
			if err != nil {
				return status, err
			}
		}

		qs.replica.updatePartitionState(dbID, collectionID, partitionID, querypb.PartitionState_InMemory)
	}

	log.Debug("load partitions end", zap.String("partitionIDs", fmt.Sprintln(partitionIDs)))
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil
}

func (qs *QueryService) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionRequest) (*commonpb.Status, error) {
	dbID := req.DbID
	collectionID := req.CollectionID
	partitionIDs := req.PartitionIDs
	log.Debug("start release partitions start", zap.String("partitionIDs", fmt.Sprintln(partitionIDs)))
	toReleasedPartitionID := make([]UniqueID, 0)
	for _, partitionID := range partitionIDs {
		_, err := qs.replica.getPartitionByID(dbID, collectionID, partitionID)
		if err == nil {
			toReleasedPartitionID = append(toReleasedPartitionID, partitionID)
		}
	}

	req.PartitionIDs = toReleasedPartitionID

	for _, node := range qs.queryNodes {
		status, err := node.client.ReleasePartitions(ctx, req)
		if err != nil {
			return status, err
		}
	}

	for _, partitionID := range toReleasedPartitionID {
		err := qs.replica.releasePartition(dbID, collectionID, partitionID)
		if err != nil {
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			}, err
		}
	}

	log.Debug("start release partitions end")
	//TODO:: queryNode cancel subscribe dmChannels
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil
}

func (qs *QueryService) CreateQueryChannel(ctx context.Context) (*querypb.CreateQueryChannelResponse, error) {
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
	log.Debug("query service create query channel", zap.String("queryChannelName", allocatedQueryChannel))
	for nodeID, node := range qs.queryNodes {
		log.Debug("node watch query channel", zap.String("nodeID", fmt.Sprintln(nodeID)))
		fn := func() error {
			_, err := node.AddQueryChannel(ctx, addQueryChannelsRequest)
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

func (qs *QueryService) GetPartitionStates(ctx context.Context, req *querypb.PartitionStatesRequest) (*querypb.PartitionStatesResponse, error) {
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

func (qs *QueryService) GetSegmentInfo(ctx context.Context, req *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error) {
	segmentInfos := make([]*querypb.SegmentInfo, 0)
	for _, node := range qs.queryNodes {
		segmentInfo, err := node.client.GetSegmentInfo(ctx, req)
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

	cfg := &config.Configuration{
		ServiceName: "query_service",
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
	service.closer = closer

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
	ctx := context.TODO()
	collection, err := qs.replica.getCollectionByID(0, collectionID)
	if err != nil {
		return err
	}
	channelRequest := datapb.InsertChannelRequest{
		DbID:         dbID,
		CollectionID: collectionID,
	}
	resp, err := qs.dataServiceClient.GetInsertChannels(ctx, &channelRequest)
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
		_, err := node.WatchDmChannels(ctx, request)
		if err != nil {
			return err
		}
		log.Debug("query node ", zap.String("nodeID", strconv.FormatInt(nodeID, 10)), zap.String("watch channels", fmt.Sprintln(channels)))
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
