package reader

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable_Init(t *testing.T) {
	Params.Init()
}

func TestParamTable_PulsarAddress(t *testing.T) {
	Params.Init()
	address, err := Params.PulsarAddress()
	assert.NoError(t, err)
	assert.Equal(t, address, "pulsar://localhost:6650")
}

func TestParamTable_QueryNodeID(t *testing.T) {
	Params.Init()
	id := Params.QueryNodeID()
	assert.Equal(t, id, 0)
}

func TestParamTable_TopicStart(t *testing.T) {
	Params.Init()
	topicStart := Params.TopicStart()
	assert.Equal(t, topicStart, 0)
}

func TestParamTable_TopicEnd(t *testing.T) {
	Params.Init()
	topicEnd := Params.TopicEnd()
	assert.Equal(t, topicEnd, 128)
}

func TestParamTable_statsServiceTimeInterval(t *testing.T) {
	Params.Init()
	interval := Params.statsServiceTimeInterval()
	assert.Equal(t, interval, 1000)
}

func TestParamTable_statsMsgStreamReceiveBufSize(t *testing.T) {
	Params.Init()
	bufSize := Params.statsMsgStreamReceiveBufSize()
	assert.Equal(t, bufSize, int64(64))
}

func TestParamTable_dmMsgStreamReceiveBufSize(t *testing.T) {
	Params.Init()
	bufSize := Params.dmMsgStreamReceiveBufSize()
	assert.Equal(t, bufSize, int64(1024))
}

func TestParamTable_searchMsgStreamReceiveBufSize(t *testing.T) {
	Params.Init()
	bufSize := Params.searchMsgStreamReceiveBufSize()
	assert.Equal(t, bufSize, int64(512))
}

func TestParamTable_searchResultMsgStreamReceiveBufSize(t *testing.T) {
	Params.Init()
	bufSize := Params.searchResultMsgStreamReceiveBufSize()
	assert.Equal(t, bufSize, int64(64))
}

func TestParamTable_searchPulsarBufSize(t *testing.T) {
	Params.Init()
	bufSize := Params.searchPulsarBufSize()
	assert.Equal(t, bufSize, int64(512))
}

func TestParamTable_dmPulsarBufSize(t *testing.T) {
	Params.Init()
	bufSize := Params.dmPulsarBufSize()
	assert.Equal(t, bufSize, int64(1024))
}
