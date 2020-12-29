package client

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type LoadIndexClient struct {
	inputStream *msgstream.MsgStream
}

func NewLoadIndexClient(ctx context.Context, pulsarAddress string, loadIndexChannels []string) *LoadIndexClient {
	loadIndexStream := msgstream.NewPulsarMsgStream(ctx, 0)
	loadIndexStream.SetPulsarClient(pulsarAddress)
	loadIndexStream.CreatePulsarProducers(loadIndexChannels)
	var input msgstream.MsgStream = loadIndexStream
	return &LoadIndexClient{
		inputStream: &input,
	}
}

func (lic *LoadIndexClient) LoadIndex(indexPaths []string, segmentID int64, fieldID int64, fieldName string, indexParams map[string]string) error {
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{0},
	}

	var indexParamsKV []*commonpb.KeyValuePair
	for key, value := range indexParams {
		indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	loadIndexRequest := internalPb.LoadIndex{
		MsgType:     internalPb.MsgType_kLoadIndex,
		SegmentID:   segmentID,
		FieldName:   fieldName,
		FieldID:     fieldID,
		IndexPaths:  indexPaths,
		IndexParams: indexParamsKV,
	}

	loadIndexMsg := &msgstream.LoadIndexMsg{
		BaseMsg:   baseMsg,
		LoadIndex: loadIndexRequest,
	}
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, loadIndexMsg)

	err := (*lic.inputStream).Produce(&msgPack)
	return err
}

func (lic *LoadIndexClient) Close() {
	(*lic.inputStream).Close()
}
