package querynode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"

*/
import "C"

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/rmqms"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	queryPb "github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type Node interface {
	typeutil.Component

	AddQueryChannel(ctx context.Context, in *queryPb.AddQueryChannelsRequest) (*commonpb.Status, error)
	RemoveQueryChannel(ctx context.Context, in *queryPb.RemoveQueryChannelsRequest) (*commonpb.Status, error)
	WatchDmChannels(ctx context.Context, in *queryPb.WatchDmChannelsRequest) (*commonpb.Status, error)
	LoadSegments(ctx context.Context, in *queryPb.LoadSegmentRequest) (*commonpb.Status, error)
	ReleaseCollection(ctx context.Context, in *queryPb.ReleaseCollectionRequest) (*commonpb.Status, error)
	ReleasePartitions(ctx context.Context, in *queryPb.ReleasePartitionRequest) (*commonpb.Status, error)
	ReleaseSegments(ctx context.Context, in *queryPb.ReleaseSegmentRequest) (*commonpb.Status, error)
	GetSegmentInfo(ctx context.Context, in *queryPb.SegmentInfoRequest) (*queryPb.SegmentInfoResponse, error)
}

type QueryService = typeutil.QueryServiceInterface

type QueryNode struct {
	typeutil.Service

	queryNodeLoopCtx    context.Context
	queryNodeLoopCancel context.CancelFunc

	QueryNodeID UniqueID
	stateCode   atomic.Value

	replica collectionReplica

	// internal services
	dataSyncService *dataSyncService
	metaService     *metaService
	searchService   *searchService
	loadService     *loadService
	statsService    *statsService

	// clients
	masterClient MasterServiceInterface
	queryClient  QueryServiceInterface
	indexClient  IndexServiceInterface
	dataClient   DataServiceInterface

	msFactory msgstream.Factory
}

func NewQueryNode(ctx context.Context, queryNodeID UniqueID, factory msgstream.Factory) *QueryNode {
	ctx1, cancel := context.WithCancel(ctx)
	node := &QueryNode{
		queryNodeLoopCtx:    ctx1,
		queryNodeLoopCancel: cancel,
		QueryNodeID:         queryNodeID,

		dataSyncService: nil,
		metaService:     nil,
		searchService:   nil,
		statsService:    nil,

		msFactory: factory,
	}

	node.replica = newCollectionReplicaImpl()
	node.UpdateStateCode(internalpb2.StateCode_ABNORMAL)
	return node
}

func NewQueryNodeWithoutID(ctx context.Context, factory msgstream.Factory) *QueryNode {
	ctx1, cancel := context.WithCancel(ctx)
	node := &QueryNode{
		queryNodeLoopCtx:    ctx1,
		queryNodeLoopCancel: cancel,

		dataSyncService: nil,
		metaService:     nil,
		searchService:   nil,
		statsService:    nil,

		msFactory: factory,
	}

	node.replica = newCollectionReplicaImpl()
	node.UpdateStateCode(internalpb2.StateCode_ABNORMAL)

	return node
}

func (node *QueryNode) Init() error {
	ctx := context.Background()
	registerReq := &queryPb.RegisterNodeRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_kNone,
			SourceID: Params.QueryNodeID,
		},
		Address: &commonpb.Address{
			Ip:   Params.QueryNodeIP,
			Port: Params.QueryNodePort,
		},
	}

	resp, err := node.queryClient.RegisterNode(ctx, registerReq)
	if err != nil {
		panic(err)
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		panic(resp.Status.Reason)
	}

	for _, kv := range resp.InitParams.StartParams {
		switch kv.Key {
		case "StatsChannelName":
			Params.StatsChannelName = kv.Value
		case "TimeTickChannelName":
			Params.QueryTimeTickChannelName = kv.Value
		case "QueryChannelName":
			Params.SearchChannelNames = append(Params.SearchChannelNames, kv.Value)
		case "QueryResultChannelName":
			Params.SearchResultChannelNames = append(Params.SearchResultChannelNames, kv.Value)
		default:
			return errors.Errorf("Invalid key: %v", kv.Key)
		}
	}

	fmt.Println("QueryNodeID is", Params.QueryNodeID)

	if node.masterClient == nil {
		log.Println("WARN: null master service detected")
	}

	if node.indexClient == nil {
		log.Println("WARN: null index service detected")
	}

	if node.dataClient == nil {
		log.Println("WARN: null data service detected")
	}

	return nil
}

func (node *QueryNode) Start() error {
	var err error
	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024}
	err = node.msFactory.SetParams(m)
	if err != nil {
		return err
	}

	// init services and manager
	node.dataSyncService = newDataSyncService(node.queryNodeLoopCtx, node.replica, node.msFactory)
	node.searchService = newSearchService(node.queryNodeLoopCtx, node.replica, node.msFactory)
	//node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	node.loadService = newLoadService(node.queryNodeLoopCtx, node.masterClient, node.dataClient, node.indexClient, node.replica, node.dataSyncService.dmStream)
	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica, node.loadService.segLoader.indexLoader.fieldStatsChan, node.msFactory)

	// start services
	go node.dataSyncService.start()
	go node.searchService.start()
	//go node.metaService.start()
	go node.loadService.start()
	go node.statsService.start()
	node.UpdateStateCode(internalpb2.StateCode_HEALTHY)
	return nil
}

func (node *QueryNode) Stop() error {
	node.UpdateStateCode(internalpb2.StateCode_ABNORMAL)
	node.queryNodeLoopCancel()

	// free collectionReplica
	node.replica.freeAll()

	// close services
	if node.dataSyncService != nil {
		node.dataSyncService.close()
	}
	if node.searchService != nil {
		node.searchService.close()
	}
	if node.loadService != nil {
		node.loadService.close()
	}
	if node.statsService != nil {
		node.statsService.close()
	}
	return nil
}

func (node *QueryNode) UpdateStateCode(code internalpb2.StateCode) {
	node.stateCode.Store(code)
}

func (node *QueryNode) SetMasterService(master MasterServiceInterface) error {
	if master == nil {
		return errors.New("null master service interface")
	}
	node.masterClient = master
	return nil
}

func (node *QueryNode) SetQueryService(query QueryServiceInterface) error {
	if query == nil {
		return errors.New("null query service interface")
	}
	node.queryClient = query
	return nil
}

func (node *QueryNode) SetIndexService(index IndexServiceInterface) error {
	if index == nil {
		return errors.New("null index service interface")
	}
	node.indexClient = index
	return nil
}

func (node *QueryNode) SetDataService(data DataServiceInterface) error {
	if data == nil {
		return errors.New("null data service interface")
	}
	node.dataClient = data
	return nil
}

func (node *QueryNode) GetComponentStates() (*internalpb2.ComponentStates, error) {
	stats := &internalpb2.ComponentStates{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}
	code, ok := node.stateCode.Load().(internalpb2.StateCode)
	if !ok {
		errMsg := "unexpected error in type assertion"
		stats.Status = &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    errMsg,
		}
		return stats, errors.New(errMsg)
	}
	info := &internalpb2.ComponentInfo{
		NodeID:    Params.QueryNodeID,
		Role:      typeutil.QueryNodeRole,
		StateCode: code,
	}
	stats.State = info
	return stats, nil
}

func (node *QueryNode) GetTimeTickChannel() (string, error) {
	return Params.QueryTimeTickChannelName, nil
}

func (node *QueryNode) GetStatisticsChannel() (string, error) {
	return Params.StatsChannelName, nil
}

func (node *QueryNode) AddQueryChannel(in *queryPb.AddQueryChannelsRequest) (*commonpb.Status, error) {
	if node.searchService == nil || node.searchService.searchMsgStream == nil {
		errMsg := "null search service or null search message stream"
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    errMsg,
		}

		return status, errors.New(errMsg)
	}

	// add request channel
	consumeChannels := []string{in.RequestChannelID}
	consumeSubName := Params.MsgChannelSubName
	node.searchService.searchMsgStream.AsConsumer(consumeChannels, consumeSubName)

	// add result channel
	producerChannels := []string{in.ResultChannelID}
	node.searchService.searchResultMsgStream.AsProducer(producerChannels)

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}
	return status, nil
}

func (node *QueryNode) RemoveQueryChannel(in *queryPb.RemoveQueryChannelsRequest) (*commonpb.Status, error) {
	if node.searchService == nil || node.searchService.searchMsgStream == nil {
		errMsg := "null search service or null search result message stream"
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    errMsg,
		}

		return status, errors.New(errMsg)
	}

	searchStream, ok := node.searchService.searchMsgStream.(*pulsarms.PulsarMsgStream)
	if !ok {
		errMsg := "type assertion failed for search message stream"
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    errMsg,
		}

		return status, errors.New(errMsg)
	}

	resultStream, ok := node.searchService.searchResultMsgStream.(*pulsarms.PulsarMsgStream)
	if !ok {
		errMsg := "type assertion failed for search result message stream"
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    errMsg,
		}

		return status, errors.New(errMsg)
	}

	// remove request channel
	consumeChannels := []string{in.RequestChannelID}
	consumeSubName := Params.MsgChannelSubName
	// TODO: searchStream.RemovePulsarConsumers(producerChannels)
	searchStream.AsConsumer(consumeChannels, consumeSubName)

	// remove result channel
	producerChannels := []string{in.ResultChannelID}
	// TODO: resultStream.RemovePulsarProducer(producerChannels)
	resultStream.AsProducer(producerChannels)

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}
	return status, nil
}

func (node *QueryNode) WatchDmChannels(in *queryPb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	if node.dataSyncService == nil || node.dataSyncService.dmStream == nil {
		errMsg := "null data sync service or null data manipulation stream"
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    errMsg,
		}

		return status, errors.New(errMsg)
	}

	switch t := node.dataSyncService.dmStream.(type) {
	case *pulsarms.PulsarTtMsgStream:
	case *rmqms.RmqTtMsgStream:
	default:
		_ = t
		errMsg := "type assertion failed for dm message stream"
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    errMsg,
		}

		return status, errors.New(errMsg)
	}

	// add request channel
	consumeChannels := in.ChannelIDs
	consumeSubName := Params.MsgChannelSubName
	node.dataSyncService.dmStream.AsConsumer(consumeChannels, consumeSubName)

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}
	return status, nil
}

func (node *QueryNode) LoadSegments(in *queryPb.LoadSegmentRequest) (*commonpb.Status, error) {
	// TODO: support db
	collectionID := in.CollectionID
	partitionID := in.PartitionID
	segmentIDs := in.SegmentIDs
	fieldIDs := in.FieldIDs
	schema := in.Schema

	fmt.Println("query node load segment ,info = ", in)

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}
	hasCollection := node.replica.hasCollection(collectionID)
	hasPartition := node.replica.hasPartition(partitionID)
	if !hasCollection {
		err := node.replica.addCollection(collectionID, schema)
		if err != nil {
			status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
			status.Reason = err.Error()
			return status, err
		}
	}
	if !hasPartition {
		err := node.replica.addPartition(collectionID, partitionID)
		if err != nil {
			status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
			status.Reason = err.Error()
			return status, err
		}
	}
	err := node.replica.enablePartition(partitionID)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		status.Reason = err.Error()
		return status, err
	}

	if len(segmentIDs) == 0 {
		return status, nil
	}

	if len(in.SegmentIDs) != len(in.SegmentStates) {
		err := errors.New("len(segmentIDs) should equal to len(segmentStates)")
		status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		status.Reason = err.Error()
		return status, err
	}

	// segments are ordered before LoadSegments calling
	var position *internalpb2.MsgPosition = nil
	for i, state := range in.SegmentStates {
		thisPosition := state.StartPosition
		if state.State <= commonpb.SegmentState_SegmentGrowing {
			if position == nil {
				position = &internalpb2.MsgPosition{
					ChannelName: thisPosition.ChannelName,
				}
			}
			segmentIDs = segmentIDs[:i]
			break
		}
		position = state.StartPosition
	}

	err = node.dataSyncService.seekSegment(position)
	if err != nil {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}
		return status, err
	}

	err = node.loadService.loadSegment(collectionID, partitionID, segmentIDs, fieldIDs)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
		status.Reason = err.Error()
		return status, err
	}
	return status, nil
}

func (node *QueryNode) ReleaseCollection(in *queryPb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	err := node.replica.removeCollection(in.CollectionID)
	if err != nil {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}
		return status, err
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil
}

func (node *QueryNode) ReleasePartitions(in *queryPb.ReleasePartitionRequest) (*commonpb.Status, error) {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}
	for _, id := range in.PartitionIDs {
		err := node.loadService.segLoader.replica.removePartition(id)
		if err != nil {
			// not return, try to release all partitions
			status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
			status.Reason = err.Error()
		}
	}
	return status, nil
}

func (node *QueryNode) ReleaseSegments(in *queryPb.ReleaseSegmentRequest) (*commonpb.Status, error) {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}
	for _, id := range in.SegmentIDs {
		err2 := node.loadService.segLoader.replica.removeSegment(id)
		if err2 != nil {
			// not return, try to release all segments
			status.ErrorCode = commonpb.ErrorCode_UNEXPECTED_ERROR
			status.Reason = err2.Error()
		}
	}
	return status, nil
}

func (node *QueryNode) GetSegmentInfo(in *queryPb.SegmentInfoRequest) (*queryPb.SegmentInfoResponse, error) {
	infos := make([]*queryPb.SegmentInfo, 0)
	for _, id := range in.SegmentIDs {
		segment, err := node.replica.getSegmentByID(id)
		if err != nil {
			continue
		}
		info := &queryPb.SegmentInfo{
			SegmentID:    segment.ID(),
			CollectionID: segment.collectionID,
			PartitionID:  segment.partitionID,
			MemSize:      segment.getMemSize(),
			NumRows:      segment.getRowCount(),
			IndexName:    segment.getIndexName(),
			IndexID:      segment.getIndexID(),
		}
		infos = append(infos, info)
	}
	return &queryPb.SegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Infos: infos,
	}, nil
}
