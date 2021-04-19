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
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	queryPb "github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/types"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type QueryNode struct {
	queryNodeLoopCtx    context.Context
	queryNodeLoopCancel context.CancelFunc

	QueryNodeID UniqueID
	stateCode   atomic.Value

	replica ReplicaInterface

	// internal services
	metaService      *metaService
	searchService    *searchService
	loadService      *loadService
	statsService     *statsService
	dsServicesMu     sync.Mutex // guards dataSyncServices
	dataSyncServices map[UniqueID]*dataSyncService

	// clients
	masterService types.MasterService
	queryService  types.QueryService
	indexService  types.IndexService
	dataService   types.DataService

	msFactory msgstream.Factory
}

func NewQueryNode(ctx context.Context, queryNodeID UniqueID, factory msgstream.Factory) *QueryNode {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	node := &QueryNode{
		queryNodeLoopCtx:    ctx1,
		queryNodeLoopCancel: cancel,
		QueryNodeID:         queryNodeID,

		dataSyncServices: make(map[UniqueID]*dataSyncService),
		metaService:      nil,
		searchService:    nil,
		statsService:     nil,

		msFactory: factory,
	}

	node.replica = newCollectionReplica()
	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	return node
}

func NewQueryNodeWithoutID(ctx context.Context, factory msgstream.Factory) *QueryNode {
	ctx1, cancel := context.WithCancel(ctx)
	node := &QueryNode{
		queryNodeLoopCtx:    ctx1,
		queryNodeLoopCancel: cancel,

		dataSyncServices: make(map[UniqueID]*dataSyncService),
		metaService:      nil,
		searchService:    nil,
		statsService:     nil,

		msFactory: factory,
	}

	node.replica = newCollectionReplica()
	node.UpdateStateCode(internalpb.StateCode_Abnormal)

	return node
}

func (node *QueryNode) Init() error {
	ctx := context.Background()
	registerReq := &queryPb.RegisterNodeRequest{
		Base: &commonpb.MsgBase{
			SourceID: Params.QueryNodeID,
		},
		Address: &commonpb.Address{
			Ip:   Params.QueryNodeIP,
			Port: Params.QueryNodePort,
		},
	}

	resp, err := node.queryService.RegisterNode(ctx, registerReq)
	if err != nil {
		panic(err)
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
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
			return fmt.Errorf("Invalid key: %v", kv.Key)
		}
	}

	log.Debug("", zap.Int64("QueryNodeID", Params.QueryNodeID))

	if node.masterService == nil {
		log.Error("null master service detected")
	}

	if node.indexService == nil {
		log.Error("null index service detected")
	}

	if node.dataService == nil {
		log.Error("null data service detected")
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
	node.searchService = newSearchService(node.queryNodeLoopCtx, node.replica, node.msFactory)
	//node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)

	node.loadService = newLoadService(node.queryNodeLoopCtx, node.masterService, node.dataService, node.indexService, node.replica)
	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica, node.loadService.segLoader.indexLoader.fieldStatsChan, node.msFactory)

	// start services
	go node.searchService.start()
	//go node.metaService.start()
	go node.loadService.start()
	go node.statsService.start()
	node.UpdateStateCode(internalpb.StateCode_Healthy)
	return nil
}

func (node *QueryNode) Stop() error {
	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	node.queryNodeLoopCancel()

	// free collectionReplica
	node.replica.freeAll()

	// close services
	for _, dsService := range node.dataSyncServices {
		if dsService != nil {
			dsService.close()
		}
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

func (node *QueryNode) UpdateStateCode(code internalpb.StateCode) {
	node.stateCode.Store(code)
}

func (node *QueryNode) SetMasterService(master types.MasterService) error {
	if master == nil {
		return errors.New("null master service interface")
	}
	node.masterService = master
	return nil
}

func (node *QueryNode) SetQueryService(query types.QueryService) error {
	if query == nil {
		return errors.New("null query service interface")
	}
	node.queryService = query
	return nil
}

func (node *QueryNode) SetIndexService(index types.IndexService) error {
	if index == nil {
		return errors.New("null index service interface")
	}
	node.indexService = index
	return nil
}

func (node *QueryNode) SetDataService(data types.DataService) error {
	if data == nil {
		return errors.New("null data service interface")
	}
	node.dataService = data
	return nil
}

func (node *QueryNode) getDataSyncService(collectionID UniqueID) (*dataSyncService, error) {
	node.dsServicesMu.Lock()
	defer node.dsServicesMu.Unlock()
	ds, ok := node.dataSyncServices[collectionID]
	if !ok {
		return nil, errors.New("cannot found dataSyncService, collectionID =" + fmt.Sprintln(collectionID))
	}
	return ds, nil
}

func (node *QueryNode) addDataSyncService(collectionID UniqueID, ds *dataSyncService) error {
	node.dsServicesMu.Lock()
	defer node.dsServicesMu.Unlock()
	if _, ok := node.dataSyncServices[collectionID]; ok {
		return errors.New("dataSyncService has been existed, collectionID =" + fmt.Sprintln(collectionID))
	}
	node.dataSyncServices[collectionID] = ds
	return nil
}

func (node *QueryNode) removeDataSyncService(collectionID UniqueID) {
	node.dsServicesMu.Lock()
	defer node.dsServicesMu.Unlock()
	delete(node.dataSyncServices, collectionID)
}

func (node *QueryNode) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
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
		NodeID:    Params.QueryNodeID,
		Role:      typeutil.QueryNodeRole,
		StateCode: code,
	}
	stats.State = info
	return stats, nil
}

func (node *QueryNode) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.QueryTimeTickChannelName,
	}, nil
}

func (node *QueryNode) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: Params.StatsChannelName,
	}, nil
}

func (node *QueryNode) AddQueryChannel(ctx context.Context, in *queryPb.AddQueryChannelRequest) (*commonpb.Status, error) {
	if node.searchService == nil || node.searchService.searchMsgStream == nil {
		errMsg := "null search service or null search message stream"
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    errMsg,
		}

		return status, errors.New(errMsg)
	}

	// add request channel
	consumeChannels := []string{in.RequestChannelID}
	consumeSubName := Params.MsgChannelSubName
	node.searchService.searchMsgStream.AsConsumer(consumeChannels, consumeSubName)
	log.Debug("querynode AsConsumer: " + strings.Join(consumeChannels, ", ") + " : " + consumeSubName)

	// add result channel
	producerChannels := []string{in.ResultChannelID}
	node.searchService.searchResultMsgStream.AsProducer(producerChannels)
	log.Debug("querynode AsProducer: " + strings.Join(producerChannels, ", "))

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return status, nil
}

func (node *QueryNode) RemoveQueryChannel(ctx context.Context, in *queryPb.RemoveQueryChannelRequest) (*commonpb.Status, error) {
	// if node.searchService == nil || node.searchService.searchMsgStream == nil {
	// 	errMsg := "null search service or null search result message stream"
	// 	status := &commonpb.Status{
	// 		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	// 		Reason:    errMsg,
	// 	}

	// 	return status, errors.New(errMsg)
	// }

	// searchStream, ok := node.searchService.searchMsgStream.(*pulsarms.PulsarMsgStream)
	// if !ok {
	// 	errMsg := "type assertion failed for search message stream"
	// 	status := &commonpb.Status{
	// 		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	// 		Reason:    errMsg,
	// 	}

	// 	return status, errors.New(errMsg)
	// }

	// resultStream, ok := node.searchService.searchResultMsgStream.(*pulsarms.PulsarMsgStream)
	// if !ok {
	// 	errMsg := "type assertion failed for search result message stream"
	// 	status := &commonpb.Status{
	// 		ErrorCode: commonpb.ErrorCode_UnexpectedError,
	// 		Reason:    errMsg,
	// 	}

	// 	return status, errors.New(errMsg)
	// }

	// // remove request channel
	// consumeChannels := []string{in.RequestChannelID}
	// consumeSubName := Params.MsgChannelSubName
	// // TODO: searchStream.RemovePulsarConsumers(producerChannels)
	// searchStream.AsConsumer(consumeChannels, consumeSubName)

	// // remove result channel
	// producerChannels := []string{in.ResultChannelID}
	// // TODO: resultStream.RemovePulsarProducer(producerChannels)
	// resultStream.AsProducer(producerChannels)

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return status, nil
}

func (node *QueryNode) WatchDmChannels(ctx context.Context, in *queryPb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	log.Debug("starting WatchDmChannels ...", zap.String("ChannelIDs", fmt.Sprintln(in.ChannelIDs)))
	collectionID := in.CollectionID
	ds, err := node.getDataSyncService(collectionID)
	if err != nil || ds.dmStream == nil {
		errMsg := "null data sync service or null data manipulation stream, collectionID = " + fmt.Sprintln(collectionID)
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    errMsg,
		}
		log.Error(errMsg)
		return status, errors.New(errMsg)
	}

	switch t := ds.dmStream.(type) {
	case *msgstream.MqTtMsgStream:
	default:
		_ = t
		errMsg := "type assertion failed for dm message stream"
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    errMsg,
		}
		log.Error(errMsg)
		return status, errors.New(errMsg)
	}

	getUniqueSubName := func() string {
		prefixName := Params.MsgChannelSubName
		return prefixName + "-" + strconv.FormatInt(collectionID, 10)
	}

	// add request channel
	consumeChannels := in.ChannelIDs
	toSeekInfo := make([]*internalpb.MsgPosition, 0)
	toDirSubChannels := make([]string, 0)

	consumeSubName := getUniqueSubName()

	for _, info := range in.Infos {
		if len(info.Pos.MsgID) == 0 {
			toDirSubChannels = append(toDirSubChannels, info.ChannelID)
			continue
		}
		info.Pos.MsgGroup = consumeSubName
		toSeekInfo = append(toSeekInfo, info.Pos)

		log.Debug("prevent inserting segments", zap.String("segmentIDs", fmt.Sprintln(info.ExcludedSegments)))
		err := node.replica.addExcludedSegments(collectionID, info.ExcludedSegments)
		if err != nil {
			status := &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			}
			log.Error(err.Error())
			return status, err
		}
	}

	ds.dmStream.AsConsumer(toDirSubChannels, consumeSubName)
	for _, pos := range toSeekInfo {
		err := ds.dmStream.Seek(pos)
		if err != nil {
			errMsg := "msgStream seek error :" + err.Error()
			status := &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    errMsg,
			}
			log.Error(errMsg)
			return status, errors.New(errMsg)
		}
	}
	log.Debug("querynode AsConsumer: " + strings.Join(consumeChannels, ", ") + " : " + consumeSubName)

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	log.Debug("WatchDmChannels done", zap.String("ChannelIDs", fmt.Sprintln(in.ChannelIDs)))
	return status, nil
}

func (node *QueryNode) LoadSegments(ctx context.Context, in *queryPb.LoadSegmentsRequest) (*commonpb.Status, error) {
	// TODO: support db
	collectionID := in.CollectionID
	partitionID := in.PartitionID
	segmentIDs := in.SegmentIDs
	fieldIDs := in.FieldIDs
	schema := in.Schema

	log.Debug("query node load segment", zap.String("loadSegmentRequest", fmt.Sprintln(in)))

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	hasCollection := node.replica.hasCollection(collectionID)
	hasPartition := node.replica.hasPartition(partitionID)
	if !hasCollection {
		// loading init
		err := node.replica.addCollection(collectionID, schema)
		if err != nil {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
			return status, err
		}
		node.replica.initExcludedSegments(collectionID)
		newDS := newDataSyncService(node.queryNodeLoopCtx, node.replica, node.msFactory, collectionID)
		// ignore duplicated dataSyncService error
		node.addDataSyncService(collectionID, newDS)
		ds, err := node.getDataSyncService(collectionID)
		if err != nil {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
			return status, err
		}
		go ds.start()
		node.searchService.startSearchCollection(collectionID)
	}
	if !hasPartition {
		err := node.replica.addPartition(collectionID, partitionID)
		if err != nil {
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
			return status, err
		}
	}
	err := node.replica.enablePartition(partitionID)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, err
	}

	if len(segmentIDs) == 0 {
		return status, nil
	}

	err = node.loadService.loadSegmentPassively(collectionID, partitionID, segmentIDs, fieldIDs)
	if err != nil {
		status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		status.Reason = err.Error()
		return status, err
	}

	log.Debug("LoadSegments done", zap.String("segmentIDs", fmt.Sprintln(in.SegmentIDs)))
	return status, nil
}

func (node *QueryNode) ReleaseCollection(ctx context.Context, in *queryPb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	ds, err := node.getDataSyncService(in.CollectionID)
	if err == nil && ds != nil {
		ds.close()
		node.removeDataSyncService(in.CollectionID)
		node.replica.removeTSafe(in.CollectionID)
		node.replica.removeExcludedSegments(in.CollectionID)
	}

	if node.searchService.hasSearchCollection(in.CollectionID) {
		node.searchService.stopSearchCollection(in.CollectionID)
	}

	err = node.replica.removeCollection(in.CollectionID)
	if err != nil {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		return status, err
	}

	log.Debug("ReleaseCollection done", zap.Int64("collectionID", in.CollectionID))
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (node *QueryNode) ReleasePartitions(ctx context.Context, in *queryPb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	for _, id := range in.PartitionIDs {
		err := node.loadService.segLoader.replica.removePartition(id)
		if err != nil {
			// not return, try to release all partitions
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err.Error()
		}
	}
	return status, nil
}

func (node *QueryNode) ReleaseSegments(ctx context.Context, in *queryPb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	for _, id := range in.SegmentIDs {
		err2 := node.loadService.segLoader.replica.removeSegment(id)
		if err2 != nil {
			// not return, try to release all segments
			status.ErrorCode = commonpb.ErrorCode_UnexpectedError
			status.Reason = err2.Error()
		}
	}
	return status, nil
}

func (node *QueryNode) GetSegmentInfo(ctx context.Context, in *queryPb.GetSegmentInfoRequest) (*queryPb.GetSegmentInfoResponse, error) {
	infos := make([]*queryPb.SegmentInfo, 0)
	for _, id := range in.SegmentIDs {
		segment, err := node.replica.getSegmentByID(id)
		if err != nil {
			continue
		}
		var indexName string
		var indexID int64
		// TODO:: segment has multi vec column
		if len(segment.indexInfos) > 0 {
			for fieldID := range segment.indexInfos {
				indexName = segment.getIndexName(fieldID)
				indexID = segment.getIndexID(fieldID)
				break
			}
		}
		info := &queryPb.SegmentInfo{
			SegmentID:    segment.ID(),
			CollectionID: segment.collectionID,
			PartitionID:  segment.partitionID,
			MemSize:      segment.getMemSize(),
			NumRows:      segment.getRowCount(),
			IndexName:    indexName,
			IndexID:      indexID,
		}
		infos = append(infos, info)
	}
	return &queryPb.GetSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Infos: infos,
	}, nil
}
