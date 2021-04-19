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
	"io"
	"sync/atomic"

	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	queryserviceimpl "github.com/zilliztech/milvus-distributed/internal/queryservice"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"

	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/util"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	queryPb "github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type Node interface {
	typeutil.Service

	GetComponentStates() (*internalpb2.ComponentStates, error)
	GetTimeTickChannel() (string, error)
	GetStatisticsChannel() (string, error)

	AddQueryChannel(in *queryPb.AddQueryChannelsRequest) (*commonpb.Status, error)
	RemoveQueryChannel(in *queryPb.RemoveQueryChannelsRequest) (*commonpb.Status, error)
	WatchDmChannels(in *queryPb.WatchDmChannelsRequest) (*commonpb.Status, error)
	LoadSegments(in *queryPb.LoadSegmentRequest) (*commonpb.Status, error)
	ReleaseSegments(in *queryPb.ReleaseSegmentRequest) (*commonpb.Status, error)
	GetPartitionState(in *queryPb.PartitionStatesRequest) (*queryPb.PartitionStatesResponse, error)
}

type QueryNode struct {
	queryNodeLoopCtx    context.Context
	queryNodeLoopCancel context.CancelFunc

	QueryNodeID uint64
	stateCode   atomic.Value

	replica collectionReplica

	// internal services
	dataSyncService  *dataSyncService
	metaService      *metaService
	searchService    *searchService
	loadIndexService *loadIndexService
	statsService     *statsService

	segManager *segmentManager

	//opentracing
	tracer opentracing.Tracer
	closer io.Closer
}

func NewQueryNode(ctx context.Context, queryNodeID uint64) Node {
	var node Node = newQueryNode(ctx, queryNodeID)
	return node
}

func newQueryNode(ctx context.Context, queryNodeID uint64) *QueryNode {
	ctx1, cancel := context.WithCancel(ctx)
	node := &QueryNode{
		queryNodeLoopCtx:    ctx1,
		queryNodeLoopCancel: cancel,
		QueryNodeID:         queryNodeID,

		dataSyncService: nil,
		metaService:     nil,
		searchService:   nil,
		statsService:    nil,
		segManager:      nil,
	}

	var err error
	cfg := &config.Configuration{
		ServiceName: "query_node",
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
	}
	node.tracer, node.closer, err = cfg.NewTracer()
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	opentracing.SetGlobalTracer(node.tracer)

	segmentsMap := make(map[int64]*Segment)
	collections := make([]*Collection, 0)

	tSafe := newTSafe()

	node.replica = &collectionReplicaImpl{
		collections: collections,
		segments:    segmentsMap,

		tSafe: tSafe,
	}
	node.stateCode.Store(internalpb2.StateCode_INITIALIZING)
	return node
}

// TODO: delete this and call node.Init()
func Init() {
	Params.Init()
}

func (node *QueryNode) Init() error {
	registerReq := queryPb.RegisterNodeRequest{
		Address: &commonpb.Address{
			Ip:   Params.QueryNodeIP,
			Port: Params.QueryNodePort,
		},
	}
	var client queryserviceimpl.Interface // TODO: init interface
	response, err := client.RegisterNode(registerReq)
	if err != nil {
		panic(err)
	}
	if response.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		panic(response.Status.Reason)
	}
	// TODO: use response.initParams

	Params.Init()
	return nil
}

func (node *QueryNode) Start() error {
	// todo add connectMaster logic
	// init services and manager
	node.dataSyncService = newDataSyncService(node.queryNodeLoopCtx, node.replica)
	node.searchService = newSearchService(node.queryNodeLoopCtx, node.replica)
	node.metaService = newMetaService(node.queryNodeLoopCtx, node.replica)
	node.loadIndexService = newLoadIndexService(node.queryNodeLoopCtx, node.replica)
	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica, node.loadIndexService.fieldStatsChan)
	node.segManager = newSegmentManager(node.queryNodeLoopCtx, node.replica, node.loadIndexService.loadIndexReqChan)

	// start services
	go node.dataSyncService.start()
	go node.searchService.start()
	go node.metaService.start()
	go node.loadIndexService.start()
	go node.statsService.start()

	node.stateCode.Store(internalpb2.StateCode_HEALTHY)
	<-node.queryNodeLoopCtx.Done()
	return nil
}

func (node *QueryNode) Stop() error {
	node.stateCode.Store(internalpb2.StateCode_ABNORMAL)
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
	if node.loadIndexService != nil {
		node.loadIndexService.close()
	}
	if node.statsService != nil {
		node.statsService.close()
	}
	if node.closer != nil {
		node.closer.Close()
	}
	return nil
}

func (node *QueryNode) GetComponentStates() (*internalpb2.ComponentStates, error) {
	code, ok := node.stateCode.Load().(internalpb2.StateCode)
	if !ok {
		return nil, errors.New("unexpected error in type assertion")
	}
	info := &internalpb2.ComponentInfo{
		NodeID:    Params.QueryNodeID,
		Role:      "query-node",
		StateCode: code,
	}
	stats := &internalpb2.ComponentStates{
		State: info,
	}
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

	// add request channel
	pulsarBufSize := Params.SearchPulsarBufSize
	consumeChannels := []string{in.RequestChannelID}
	consumeSubName := Params.MsgChannelSubName
	unmarshalDispatcher := util.NewUnmarshalDispatcher()
	searchStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, pulsarBufSize)

	// add result channel
	producerChannels := []string{in.ResultChannelID}
	resultStream.CreatePulsarProducers(producerChannels)

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
	pulsarBufSize := Params.SearchPulsarBufSize
	consumeChannels := []string{in.RequestChannelID}
	consumeSubName := Params.MsgChannelSubName
	unmarshalDispatcher := util.NewUnmarshalDispatcher()
	// TODO: searchStream.RemovePulsarConsumers(producerChannels)
	searchStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, pulsarBufSize)

	// remove result channel
	producerChannels := []string{in.ResultChannelID}
	// TODO: resultStream.RemovePulsarProducer(producerChannels)
	resultStream.CreatePulsarProducers(producerChannels)

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

	fgDMMsgStream, ok := node.dataSyncService.dmStream.(*pulsarms.PulsarMsgStream)
	if !ok {
		errMsg := "type assertion failed for dm message stream"
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    errMsg,
		}

		return status, errors.New(errMsg)
	}

	// add request channel
	pulsarBufSize := Params.SearchPulsarBufSize
	consumeChannels := in.ChannelIDs
	consumeSubName := Params.MsgChannelSubName
	unmarshalDispatcher := util.NewUnmarshalDispatcher()
	fgDMMsgStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, pulsarBufSize)

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
	err := node.segManager.loadSegment(collectionID, partitionID, segmentIDs, fieldIDs)
	if err != nil {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    err.Error(),
		}
		return status, err
	}
	return nil, nil
}

func (node *QueryNode) ReleaseSegments(in *queryPb.ReleaseSegmentRequest) (*commonpb.Status, error) {
	// release all fields in the segments
	for _, id := range in.SegmentIDs {
		err := node.segManager.releaseSegment(id)
		if err != nil {
			status := &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    err.Error(),
			}
			return status, err
		}
	}
	return nil, nil
}

func (node *QueryNode) GetPartitionState(in *queryPb.PartitionStatesRequest) (*queryPb.PartitionStatesResponse, error) {
	// TODO: implement
	return nil, nil
}
