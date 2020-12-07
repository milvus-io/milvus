package querynode

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable_PulsarAddress(t *testing.T) {
	address, err := Params.pulsarAddress()
	assert.NoError(t, err)
	split := strings.Split(address, ":")
	assert.Equal(t, "pulsar", split[0])
	assert.Equal(t, "6650", split[len(split)-1])
}

func TestParamTable_QueryNodeID(t *testing.T) {
	id := Params.QueryNodeID()
	assert.Contains(t, Params.queryNodeIDList(), id)
}

func TestParamTable_insertChannelRange(t *testing.T) {
	channelRange := Params.insertChannelRange()
	assert.Equal(t, 2, len(channelRange))
}

func TestParamTable_statsServiceTimeInterval(t *testing.T) {
	interval := Params.statsPublishInterval()
	assert.Equal(t, 1000, interval)
}

func TestParamTable_statsMsgStreamReceiveBufSize(t *testing.T) {
	bufSize := Params.statsReceiveBufSize()
	assert.Equal(t, int64(64), bufSize)
}

func TestParamTable_insertMsgStreamReceiveBufSize(t *testing.T) {
	bufSize := Params.insertReceiveBufSize()
	assert.Equal(t, int64(1024), bufSize)
}

func TestParamTable_searchMsgStreamReceiveBufSize(t *testing.T) {
	bufSize := Params.searchReceiveBufSize()
	assert.Equal(t, int64(512), bufSize)
}

func TestParamTable_searchResultMsgStreamReceiveBufSize(t *testing.T) {
	bufSize := Params.searchResultReceiveBufSize()
	assert.Equal(t, int64(64), bufSize)
}

func TestParamTable_searchPulsarBufSize(t *testing.T) {
	bufSize := Params.searchPulsarBufSize()
	assert.Equal(t, int64(512), bufSize)
}

func TestParamTable_insertPulsarBufSize(t *testing.T) {
	bufSize := Params.insertPulsarBufSize()
	assert.Equal(t, int64(1024), bufSize)
}

func TestParamTable_flowGraphMaxQueueLength(t *testing.T) {
	length := Params.flowGraphMaxQueueLength()
	assert.Equal(t, int32(1024), length)
}

func TestParamTable_flowGraphMaxParallelism(t *testing.T) {
	maxParallelism := Params.flowGraphMaxParallelism()
	assert.Equal(t, int32(1024), maxParallelism)
}

func TestParamTable_insertChannelNames(t *testing.T) {
	names := Params.insertChannelNames()
	channelRange := Params.insertChannelRange()
	num := channelRange[1] - channelRange[0]
	num = num / Params.queryNodeNum()
	assert.Equal(t, num, len(names))
	start := num * Params.sliceIndex()
	assert.Equal(t, fmt.Sprintf("insert-%d", channelRange[start]), names[0])
}

func TestParamTable_searchChannelNames(t *testing.T) {
	names := Params.searchChannelNames()
	assert.Equal(t, len(names), 1)
	assert.Equal(t, "search-0", names[0])
}

func TestParamTable_searchResultChannelNames(t *testing.T) {
	names := Params.searchResultChannelNames()
	assert.NotNil(t, names)
}

func TestParamTable_msgChannelSubName(t *testing.T) {
	name := Params.msgChannelSubName()
	expectName := fmt.Sprintf("queryNode-%d", Params.QueryNodeID())
	assert.Equal(t, expectName, name)
}

func TestParamTable_statsChannelName(t *testing.T) {
	name := Params.statsChannelName()
	assert.Equal(t, "query-node-stats", name)
}

func TestParamTable_metaRootPath(t *testing.T) {
	path := Params.metaRootPath()
	assert.Equal(t, "by-dev/meta", path)
}
