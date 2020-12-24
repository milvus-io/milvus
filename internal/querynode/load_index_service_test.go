package querynode

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

func TestLoadIndexService_PulsarAddress(t *testing.T) {
	node := newQueryNode()
	collectionID := rand.Int63n(1000000)
	segmentID := rand.Int63n(1000000)
	fieldID := rand.Int63n(1000000)
	initTestMeta(t, node, "collection0", collectionID, segmentID)

	// loadIndexService and statsService
	node.loadIndexService = newLoadIndexService(node.queryNodeLoopCtx, node.replica)
	go node.loadIndexService.start()
	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica, node.loadIndexService.fieldStatsChan)
	go node.statsService.start()

	// gen load index message pack
	const msgLength = 10
	indexParams := make([]*commonpb.KeyValuePair, 0)
	// init IVF_FLAT index params
	const (
		KeyDim        = "dim"
		KeyTopK       = "k"
		KeyNList      = "nlist"
		KeyNProbe     = "nprobe"
		KeyMetricType = "metric_type"
		KeySliceSize  = "SLICE_SIZE"
		KeyDeviceID   = "gpu_id"
	)
	const (
		ValueDim        = "128"
		ValueTopK       = "10"
		ValueNList      = "100"
		ValueNProbe     = "4"
		ValueMetricType = "L2"
		ValueSliceSize  = "4"
		ValueDeviceID   = "0"
	)

	indexParams = append(indexParams, &commonpb.KeyValuePair{
		Key:   KeyDim,
		Value: ValueDim,
	})
	indexParams = append(indexParams, &commonpb.KeyValuePair{
		Key:   KeyTopK,
		Value: ValueTopK,
	})
	indexParams = append(indexParams, &commonpb.KeyValuePair{
		Key:   KeyNList,
		Value: ValueNList,
	})
	indexParams = append(indexParams, &commonpb.KeyValuePair{
		Key:   KeyNProbe,
		Value: ValueNProbe,
	})
	indexParams = append(indexParams, &commonpb.KeyValuePair{
		Key:   KeyMetricType,
		Value: ValueMetricType,
	})
	indexParams = append(indexParams, &commonpb.KeyValuePair{
		Key:   KeySliceSize,
		Value: ValueSliceSize,
	})
	indexParams = append(indexParams, &commonpb.KeyValuePair{
		Key:   KeyDeviceID,
		Value: ValueDeviceID,
	})

	loadIndex := internalPb.LoadIndex{
		MsgType:     internalPb.MsgType_kLoadIndex,
		SegmentID:   segmentID,
		FieldID:     fieldID,
		IndexPaths:  []string{"tmp/index"}, // TODO:
		IndexParams: indexParams,
	}

	loadIndexMsg := msgstream.LoadIndexMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{uint32(0)},
		},
		LoadIndex: loadIndex,
	}

	messages := make([]msgstream.TsMsg, 0)
	for i := 0; i < msgLength; i++ {
		var msg msgstream.TsMsg = &loadIndexMsg
		messages = append(messages, msg)
	}

	msgPack := msgstream.MsgPack{
		BeginTs: 0,
		EndTs:   math.MaxUint64,
		Msgs:    messages,
	}

	// init message stream producer
	loadIndexChannelNames := Params.LoadIndexChannelNames
	pulsarURL := Params.PulsarAddress

	loadIndexStream := msgstream.NewPulsarMsgStream(node.queryNodeLoopCtx, Params.LoadIndexReceiveBufSize)
	loadIndexStream.SetPulsarClient(pulsarURL)
	loadIndexStream.CreatePulsarProducers(loadIndexChannelNames)

	var loadIndexMsgStream msgstream.MsgStream = loadIndexStream
	loadIndexMsgStream.Start()

	err := loadIndexMsgStream.Produce(&msgPack)
	assert.NoError(t, err)

	// init message stream consumer and do checks
	statsMs := msgstream.NewPulsarMsgStream(node.queryNodeLoopCtx, Params.StatsReceiveBufSize)
	statsMs.SetPulsarClient(pulsarURL)
	statsMs.CreatePulsarConsumers([]string{Params.StatsChannelName}, Params.MsgChannelSubName, msgstream.NewUnmarshalDispatcher(), Params.StatsReceiveBufSize)
	statsMs.Start()

	receiveMsg := msgstream.MsgStream(statsMs).Consume()
	assert.NotNil(t, receiveMsg)
	assert.NotEqual(t, len(receiveMsg.Msgs), 0)
	statsMsg, ok := receiveMsg.Msgs[0].(*msgstream.QueryNodeStatsMsg)
	assert.Equal(t, ok, true)
	assert.Equal(t, len(statsMsg.FieldStats), 1)
	fieldStats0 := statsMsg.FieldStats[0]
	assert.Equal(t, fieldStats0.FieldID, fieldID)
	assert.Equal(t, fieldStats0.CollectionID, collectionID)
	assert.Equal(t, len(fieldStats0.IndexStats), 1)
	indexStats0 := fieldStats0.IndexStats[0]

	params := indexStats0.IndexParams
	// sort index params by key
	sort.Slice(indexParams, func(i, j int) bool { return indexParams[i].Key < indexParams[j].Key })
	indexEqual := node.loadIndexService.indexParamsEqual(params, indexParams)
	assert.Equal(t, indexEqual, true)

	<-node.queryNodeLoopCtx.Done()
	node.Close()
}
