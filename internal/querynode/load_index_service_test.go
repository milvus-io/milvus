package querynode

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/indexbuilder"
	minioKV "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/querynode/client"
)

func TestLoadIndexService(t *testing.T) {
	node := newQueryNode()
	collectionID := rand.Int63n(1000000)
	segmentID := rand.Int63n(1000000)
	initTestMeta(t, node, "collection0", collectionID, segmentID)

	// loadIndexService and statsService
	node.loadIndexService = newLoadIndexService(node.queryNodeLoopCtx, node.replica)
	go node.loadIndexService.start()
	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica, node.loadIndexService.fieldStatsChan)
	go node.statsService.start()

	// gen load index message pack
	const msgLength = 10000
	indexParams := make(map[string]string)
	indexParams["index_type"] = "IVF_PQ"
	indexParams["index_mode"] = "cpu"
	indexParams["dim"] = "16"
	indexParams["k"] = "10"
	indexParams["nlist"] = "100"
	indexParams["nprobe"] = "4"
	indexParams["m"] = "4"
	indexParams["nbits"] = "8"
	indexParams["metric_type"] = "L2"
	indexParams["SLICE_SIZE"] = "4"

	var indexParamsKV []*commonpb.KeyValuePair
	for key, value := range indexParams {
		indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	// generator index
	typeParams := make(map[string]string)
	typeParams["dim"] = "16"
	index, err := indexbuilder.NewCIndex(typeParams, indexParams)
	assert.Nil(t, err)
	const DIM = 16
	var vec = [DIM]float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	var indexRowData []float32
	for i := 0; i < msgLength; i++ {
		for i, ele := range vec {
			indexRowData = append(indexRowData, ele+float32(i*4))
		}
	}
	err = index.BuildFloatVecIndexWithoutIds(indexRowData)
	assert.Equal(t, err, nil)
	binarySet, err := index.Serialize()
	assert.Equal(t, err, nil)

	//save index to minio
	minioClient, err := minio.New(Params.MinioEndPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(Params.MinioAccessKeyID, Params.MinioSecretAccessKey, ""),
		Secure: Params.MinioUseSSLStr,
	})
	assert.Equal(t, err, nil)
	minioKV, err := minioKV.NewMinIOKV(node.queryNodeLoopCtx, minioClient, Params.MinioBucketName)
	assert.Equal(t, err, nil)
	indexPaths := make([]string, 0)
	for _, index := range binarySet {
		indexPaths = append(indexPaths, index.Key)
		minioKV.Save(index.Key, string(index.Value))
	}

	// create loadIndexClient
	fieldID := UniqueID(100)
	loadIndexChannelNames := Params.LoadIndexChannelNames
	pulsarURL := Params.PulsarAddress
	client := client.NewLoadIndexClient(node.queryNodeLoopCtx, pulsarURL, loadIndexChannelNames)
	client.LoadIndex(indexPaths, segmentID, fieldID, "vec", indexParams)

	// init message stream consumer and do checks
	statsMs := msgstream.NewPulsarMsgStream(node.queryNodeLoopCtx, Params.StatsReceiveBufSize)
	statsMs.SetPulsarClient(pulsarURL)
	statsMs.CreatePulsarConsumers([]string{Params.StatsChannelName}, Params.MsgChannelSubName, msgstream.NewUnmarshalDispatcher(), Params.StatsReceiveBufSize)
	statsMs.Start()

	findFiledStats := false
	for {
		receiveMsg := msgstream.MsgStream(statsMs).Consume()
		assert.NotNil(t, receiveMsg)
		assert.NotEqual(t, len(receiveMsg.Msgs), 0)

		for _, msg := range receiveMsg.Msgs {
			statsMsg, ok := msg.(*msgstream.QueryNodeStatsMsg)
			if statsMsg.FieldStats == nil || len(statsMsg.FieldStats) == 0 {
				continue
			}
			findFiledStats = true
			assert.Equal(t, ok, true)
			assert.Equal(t, len(statsMsg.FieldStats), 1)
			fieldStats0 := statsMsg.FieldStats[0]
			assert.Equal(t, fieldStats0.FieldID, fieldID)
			assert.Equal(t, fieldStats0.CollectionID, collectionID)
			assert.Equal(t, len(fieldStats0.IndexStats), 1)
			indexStats0 := fieldStats0.IndexStats[0]
			params := indexStats0.IndexParams
			// sort index params by key
			sort.Slice(indexParamsKV, func(i, j int) bool { return indexParamsKV[i].Key < indexParamsKV[j].Key })
			indexEqual := node.loadIndexService.indexParamsEqual(params, indexParamsKV)
			assert.Equal(t, indexEqual, true)
		}

		if findFiledStats {
			break
		}
	}

	defer assert.Equal(t, findFiledStats, true)
	<-node.queryNodeLoopCtx.Done()
	node.Close()
}
