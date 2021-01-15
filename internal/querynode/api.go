package querynodeimp

import (
	"context"
	"errors"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	queryPb "github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

func (node *QueryNode) AddQueryChannel(ctx context.Context, in *queryPb.AddQueryChannelsRequest) (*commonpb.Status, error) {
	select {
	case <-ctx.Done():
		errMsg := "context exceeded"
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    errMsg,
		}

		return status, errors.New(errMsg)
	default:
		searchStream, ok := node.searchService.searchMsgStream.(*msgstream.PulsarMsgStream)
		if !ok {
			errMsg := "type assertion failed for search message stream"
			status := &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    errMsg,
			}

			return status, errors.New(errMsg)
		}

		resultStream, ok := node.searchService.searchResultMsgStream.(*msgstream.PulsarMsgStream)
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
		unmarshalDispatcher := msgstream.NewUnmarshalDispatcher()
		searchStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, pulsarBufSize)

		// add result channel
		producerChannels := []string{in.ResultChannelID}
		resultStream.CreatePulsarProducers(producerChannels)

		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		}
		return status, nil
	}
}

func (node *QueryNode) RemoveQueryChannel(ctx context.Context, in *queryPb.RemoveQueryChannelsRequest) (*commonpb.Status, error) {
	select {
	case <-ctx.Done():
		errMsg := "context exceeded"
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    errMsg,
		}

		return status, errors.New(errMsg)
	default:
		searchStream, ok := node.searchService.searchMsgStream.(*msgstream.PulsarMsgStream)
		if !ok {
			errMsg := "type assertion failed for search message stream"
			status := &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    errMsg,
			}

			return status, errors.New(errMsg)
		}

		resultStream, ok := node.searchService.searchResultMsgStream.(*msgstream.PulsarMsgStream)
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
		unmarshalDispatcher := msgstream.NewUnmarshalDispatcher()
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
}

func (node *QueryNode) WatchDmChannels(ctx context.Context, in *queryPb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	select {
	case <-ctx.Done():
		errMsg := "context exceeded"
		status := &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			Reason:    errMsg,
		}

		return status, errors.New(errMsg)
	default:
		// TODO: add dmMsgStream reference to dataSyncService
		//fgDMMsgStream, ok := node.dataSyncService.dmMsgStream.(*msgstream.PulsarMsgStream)
		//if !ok {
		//	errMsg := "type assertion failed for dm message stream"
		//	status := &commonpb.Status{
		//		ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
		//		Reason:    errMsg,
		//	}
		//
		//	return status, errors.New(errMsg)
		//}
		//
		//// add request channel
		//pulsarBufSize := Params.SearchPulsarBufSize
		//consumeChannels := in.ChannelIDs
		//consumeSubName := Params.MsgChannelSubName
		//unmarshalDispatcher := msgstream.NewUnmarshalDispatcher()
		//fgDMMsgStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, pulsarBufSize)
		//
		//status := &commonpb.Status{
		//	ErrorCode: commonpb.ErrorCode_SUCCESS,
		//}
		//return status, nil
		return nil, nil
	}
}

func (node *QueryNode) LoadSegments(ctx context.Context, in *queryPb.LoadSegmentRequest) (*commonpb.Status, error) {
	// TODO: implement
	return nil, nil
}

func (node *QueryNode) ReleaseSegments(ctx context.Context, in *queryPb.ReleaseSegmentRequest) (*commonpb.Status, error) {
	// TODO: implement
	return nil, nil
}

func (node *QueryNode) GetPartitionState(ctx context.Context, in *queryPb.PartitionStatesRequest) (*queryPb.PartitionStatesResponse, error) {
	// TODO: implement
	return nil, nil
}
