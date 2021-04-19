package client

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
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

func (lic *LoadIndexClient) LoadIndex(indexPaths []string, segmentID int64, fieldID int64) error {
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{0},
	}
	loadIndexRequest := internalPb.LoadIndex{
		MsgType:    internalPb.MsgType_kLoadIndex,
		SegmentID:  segmentID,
		FieldID:    fieldID,
		IndexPaths: indexPaths,
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
