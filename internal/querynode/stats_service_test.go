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

	msFactory := pulsarms.NewFactory()
	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024}
	msFactory.SetParams(m)
	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica, nil, msFactory)
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

	msFactory := pulsarms.NewFactory()
	m := map[string]interface{}{
		"receiveBufSize": receiveBufSize,
		"pulsarAddress":  Params.PulsarAddress,
		"pulsarBufSize":  1024}
	err := msFactory.SetParams(m)
	assert.Nil(t, err)

	statsStream, err := msFactory.NewMsgStream(node.queryNodeLoopCtx)
	assert.Nil(t, err)
	statsStream.AsProducer(producerChannels)

	var statsMsgStream msgstream.MsgStream = statsStream

	node.statsService = newStatsService(node.queryNodeLoopCtx, node.replica, nil, msFactory)
	node.statsService.statsStream = statsMsgStream
	node.statsService.statsStream.Start()

	// send stats
	node.statsService.publicStatistic(nil)
	node.Stop()
}
