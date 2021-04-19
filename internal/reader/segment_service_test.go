package reader

import (
	"context"
	"github.com/czs007/suvlim/internal/conf"
	"github.com/czs007/suvlim/internal/msgclient"
	"strconv"
	"testing"
	"time"
)

func TestSegmentManagement_SegmentsManagement(t *testing.T) {
	// Construct node, collection, partition and segment
	ctx := context.Background()
	node := NewQueryNode(ctx, 0, 0)
	var collection = node.NewCollection(0, "collection0", "")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	node.SegmentsManagement()

	node.Close()
}

func TestSegmentManagement_SegmentService(t *testing.T) {
	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
	ctx, _ := context.WithDeadline(context.Background(), d)

	// Construct node, collection, partition and segment
	node := NewQueryNode(ctx, 0, 0)
	var collection = node.NewCollection(0, "collection0", "")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	node.SegmentManagementService()

	node.Close()
}

// NOTE: start pulsar before test
func TestSegmentManagement_SegmentStatistic(t *testing.T) {
	conf.LoadConfig("config.yaml")
	ctx, _ := context.WithCancel(context.Background())

	mc := msgclient.ReaderMessageClient{}
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	mc.InitClient(ctx, pulsarAddr)

	mc.ReceiveMessage()
	node := CreateQueryNode(ctx, 0, 0, &mc)

	// Construct node, collection, partition and segment
	var collection = node.NewCollection(0, "collection0", "")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	node.SegmentStatistic(1000)

	node.Close()
}

// NOTE: start pulsar before test
func TestSegmentManagement_SegmentStatisticService(t *testing.T) {
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

	// Construct node, collection, partition and segment
	var collection = node.NewCollection(0, "collection0", "")
	var partition = collection.NewPartition("partition0")
	var segment = partition.NewSegment(0)
	node.SegmentsMap[0] = segment

	node.SegmentStatisticService()

	node.Close()
}
