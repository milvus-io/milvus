package reader

import (
	"context"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/msgclient"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// NOTE: start pulsar before test
func TestUtilFunctions_GetKey2Segments(t *testing.T) {
	conf.LoadConfig("config.yaml")
	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	mc := msgclient.ReaderMessageClient{}
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	mc.InitClient(ctx, pulsarAddr)

	mc.ReceiveMessage()
	node := CreateQueryNode(ctx, 0, 0, &mc)

	node.messageClient.PrepareKey2SegmentMsg()
	var _, _, _ = node.GetKey2Segments()

	node.Close()
}

func TestUtilFunctions_GetCollectionByCollectionName(t *testing.T) {
	ctx := context.Background()
	// 1. Construct node, and collections
	node := NewQueryNode(ctx, 0, 0)
	var _ = node.NewCollection(0, "collection0", "")

	// 2. Get collection by collectionName
	var c0, err = node.GetCollectionByCollectionName("collection0")
	assert.NoError(t, err)
	assert.Equal(t, c0.CollectionName, "collection0")
	c0 = node.GetCollectionByID(0)
	assert.NotNil(t, c0)
	assert.Equal(t, c0.CollectionID, uint64(0))

	node.Close()
}

func TestUtilFunctions_GetSegmentBySegmentID(t *testing.T) {
	ctx := context.Background()
	// 1. Construct node, collection, partition and segment
	node := NewQueryNode(ctx, 0, 0)
	var collection = node.NewCollection(0, "collection0", "")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	// 2. Get segment by segment id
	var s0, err = node.GetSegmentBySegmentID(0)
	assert.NoError(t, err)
	assert.Equal(t, s0.SegmentId, int64(0))

	node.Close()
}
