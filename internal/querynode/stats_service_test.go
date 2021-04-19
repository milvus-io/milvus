package querynode

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
)

// NOTE: start pulsar before test
func TestStatsService_start(t *testing.T) {
	node := newQueryNodeMock()
	initTestMeta(t, node, 0, 0)
	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica, nil)
	node.statsService.start()
	node.Stop()
}

//NOTE: start pulsar before test
func TestSegmentManagement_sendSegmentStatistic(t *testing.T) {
	node := newQueryNodeMock()
	initTestMeta(t, node, 0, 0)

	const receiveBufSize = 1024
	// start pulsar
	producerChannels := []string{Params.StatsChannelName}

	pulsarURL := Params.PulsarAddress
	factory := pulsarms.NewFactory(pulsarURL, receiveBufSize, 1024)
	statsStream, err := factory.NewMsgStream(node.queryNodeLoopCtx)
	assert.Nil(t, err)
	statsStream.AsProducer(producerChannels)

	var statsMsgStream msgstream.MsgStream = statsStream

	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica, nil)
	node.statsService.statsStream = statsMsgStream
	node.statsService.statsStream.Start()

	// send stats
	node.statsService.publicStatistic(nil)
	node.Stop()
}
