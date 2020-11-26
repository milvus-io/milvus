package querynode

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable_Init(t *testing.T) {
	Params.Init()
}

func TestParamTable_PulsarAddress(t *testing.T) {
	Params.Init()
	address, err := Params.pulsarAddress()
	assert.NoError(t, err)
	split := strings.Split(address, ":")
	assert.Equal(t, split[0], "pulsar")
	assert.Equal(t, split[len(split)-1], "6650")
}

func TestParamTable_QueryNodeID(t *testing.T) {
	Params.Init()
	id := Params.queryNodeID()
	assert.Equal(t, id, 0)
}

func TestParamTable_insertChannelRange(t *testing.T) {
	Params.Init()
	channelRange := Params.insertChannelRange()
	assert.Equal(t, len(channelRange), 2)
	assert.Equal(t, channelRange[0], 0)
	assert.Equal(t, channelRange[1], 1)
}

func TestParamTable_statsServiceTimeInterval(t *testing.T) {
	Params.Init()
	interval := Params.statsPublishInterval()
	assert.Equal(t, interval, 1000)
}

func TestParamTable_statsMsgStreamReceiveBufSize(t *testing.T) {
	Params.Init()
	bufSize := Params.statsReceiveBufSize()
	assert.Equal(t, bufSize, int64(64))
}

func TestParamTable_insertMsgStreamReceiveBufSize(t *testing.T) {
	Params.Init()
	bufSize := Params.insertReceiveBufSize()
	assert.Equal(t, bufSize, int64(1024))
}

func TestParamTable_searchMsgStreamReceiveBufSize(t *testing.T) {
	Params.Init()
	bufSize := Params.searchReceiveBufSize()
	assert.Equal(t, bufSize, int64(512))
}

func TestParamTable_searchResultMsgStreamReceiveBufSize(t *testing.T) {
	Params.Init()
	bufSize := Params.searchResultReceiveBufSize()
	assert.Equal(t, bufSize, int64(64))
}

func TestParamTable_searchPulsarBufSize(t *testing.T) {
	Params.Init()
	bufSize := Params.searchPulsarBufSize()
	assert.Equal(t, bufSize, int64(512))
}

func TestParamTable_insertPulsarBufSize(t *testing.T) {
	Params.Init()
	bufSize := Params.insertPulsarBufSize()
	assert.Equal(t, bufSize, int64(1024))
}

func TestParamTable_flowGraphMaxQueueLength(t *testing.T) {
	Params.Init()
	length := Params.flowGraphMaxQueueLength()
	assert.Equal(t, length, int32(1024))
}

func TestParamTable_flowGraphMaxParallelism(t *testing.T) {
	Params.Init()
	maxParallelism := Params.flowGraphMaxParallelism()
	assert.Equal(t, maxParallelism, int32(1024))
}

func TestParamTable_insertChannelNames(t *testing.T) {
	Params.Init()
	names := Params.insertChannelNames()
	assert.Equal(t, len(names), 1)
	assert.Equal(t, names[0], "insert-0")
}

func TestParamTable_searchChannelNames(t *testing.T) {
	Params.Init()
	names := Params.searchChannelNames()
	assert.Equal(t, len(names), 1)
	assert.Equal(t, names[0], "search-0")
}

func TestParamTable_searchResultChannelNames(t *testing.T) {
	Params.Init()
	names := Params.searchResultChannelNames()
	assert.Equal(t, len(names), 1)
	assert.Equal(t, names[0], "searchResult-0")
}

func TestParamTable_msgChannelSubName(t *testing.T) {
	Params.Init()
	name := Params.msgChannelSubName()
	assert.Equal(t, name, "queryNode")
}

func TestParamTable_statsChannelName(t *testing.T) {
	Params.Init()
	name := Params.statsChannelName()
	assert.Equal(t, name, "query-node-stats")
}
