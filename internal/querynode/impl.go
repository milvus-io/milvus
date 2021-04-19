package querynode

import (
	"context"
	"errors"
	"strings"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	queryPb "github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

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
	dct := &watchDmChannelsTask{
		baseTask: baseTask{
			ctx:  ctx,
			done: make(chan error),
		},
		req:  in,
		node: node,
	}

	err := node.scheduler.queue.Enqueue(dct)
	if err != nil {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		log.Error(err.Error())
		return status, err
	}
	log.Debug("watchDmChannelsTask Enqueue done", zap.Any("collectionID", in.CollectionID))

	func() {
		err = dct.WaitToFinish()
		if err != nil {
			log.Error(err.Error())
			return
		}
		log.Debug("watchDmChannelsTask WaitToFinish done", zap.Any("collectionID", in.CollectionID))
	}()

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return status, nil
}

func (node *QueryNode) LoadSegments(ctx context.Context, in *queryPb.LoadSegmentsRequest) (*commonpb.Status, error) {
	dct := &loadSegmentsTask{
		baseTask: baseTask{
			ctx:  ctx,
			done: make(chan error),
		},
		req:  in,
		node: node,
	}

	err := node.scheduler.queue.Enqueue(dct)
	if err != nil {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		log.Error(err.Error())
		return status, err
	}
	log.Debug("loadSegmentsTask Enqueue done", zap.Any("collectionID", in.CollectionID))

	func() {
		err = dct.WaitToFinish()
		if err != nil {
			log.Error(err.Error())
			return
		}
		log.Debug("loadSegmentsTask WaitToFinish done", zap.Any("collectionID", in.CollectionID))
	}()

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return status, nil
}

func (node *QueryNode) ReleaseCollection(ctx context.Context, in *queryPb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	dct := &releaseCollectionTask{
		baseTask: baseTask{
			ctx:  ctx,
			done: make(chan error),
		},
		req:  in,
		node: node,
	}

	err := node.scheduler.queue.Enqueue(dct)
	if err != nil {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		log.Error(err.Error())
		return status, err
	}
	log.Debug("releaseCollectionTask Enqueue done", zap.Any("collectionID", in.CollectionID))

	func() {
		err = dct.WaitToFinish()
		if err != nil {
			log.Error(err.Error())
			return
		}
		log.Debug("releaseCollectionTask WaitToFinish done", zap.Any("collectionID", in.CollectionID))
	}()

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return status, nil
}

func (node *QueryNode) ReleasePartitions(ctx context.Context, in *queryPb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	dct := &releasePartitionsTask{
		baseTask: baseTask{
			ctx:  ctx,
			done: make(chan error),
		},
		req:  in,
		node: node,
	}

	err := node.scheduler.queue.Enqueue(dct)
	if err != nil {
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}
		log.Error(err.Error())
		return status, err
	}
	log.Debug("releasePartitionsTask Enqueue done", zap.Any("collectionID", in.CollectionID))

	func() {
		err = dct.WaitToFinish()
		if err != nil {
			log.Error(err.Error())
			return
		}
		log.Debug("releasePartitionsTask WaitToFinish done", zap.Any("collectionID", in.CollectionID))
	}()

	status := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	return status, nil
}

// deprecated
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
