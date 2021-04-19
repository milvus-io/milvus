package querynode

import (
	"testing"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

// NOTE: start pulsar before test
func TestStatsService_start(t *testing.T) {
	node := newQueryNode()
	initTestMeta(t, node, "collection0", 0, 0)
	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica)
	node.statsService.start()
	node.Close()
}

//NOTE: start pulsar before test
func TestSegmentManagement_SegmentStatisticService(t *testing.T) {
	node := newQueryNode()
	initTestMeta(t, node, "collection0", 0, 0)

	const receiveBufSize = 1024
	// start pulsar
	producerChannels := []string{Params.statsChannelName()}

	pulsarURL, _ := Params.pulsarAddress()

	statsStream := msgstream.NewPulsarMsgStream(node.queryNodeLoopCtx, receiveBufSize)
	statsStream.SetPulsarClient(pulsarURL)
	statsStream.CreatePulsarProducers(producerChannels)

	var statsMsgStream msgstream.MsgStream = statsStream

	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica)
	node.statsService.statsStream = statsMsgStream
	node.statsService.statsStream.Start()

	// send stats
	node.statsService.sendSegmentStatistic()
	node.Close()
}
