package writenode

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable_WriteNode(t *testing.T) {

	Params.Init()

	t.Run("Test PulsarAddress", func(t *testing.T) {
		address, err := Params.pulsarAddress()
		assert.NoError(t, err)
		split := strings.Split(address, ":")
		assert.Equal(t, split[0], "pulsar")
		assert.Equal(t, split[len(split)-1], "6650")
	})

	t.Run("Test WriteNodeID", func(t *testing.T) {
		id := Params.WriteNodeID()
		assert.Equal(t, id, UniqueID(3))
	})

	t.Run("Test insertChannelRange", func(t *testing.T) {
		channelRange := Params.insertChannelRange()
		assert.Equal(t, len(channelRange), 2)
		assert.Equal(t, channelRange[0], 0)
		assert.Equal(t, channelRange[1], 2)
	})

	t.Run("Test statsServiceTimeInterval", func(t *testing.T) {
		interval := Params.statsPublishInterval()
		assert.Equal(t, interval, 1000)
	})

	t.Run("Test statsMsgStreamReceiveBufSize", func(t *testing.T) {
		bufSize := Params.statsReceiveBufSize()
		assert.Equal(t, bufSize, int64(64))
	})

	t.Run("Test insertMsgStreamReceiveBufSize", func(t *testing.T) {
		bufSize := Params.insertReceiveBufSize()
		assert.Equal(t, bufSize, int64(1024))
	})

	t.Run("Test searchMsgStreamReceiveBufSize", func(t *testing.T) {
		bufSize := Params.searchReceiveBufSize()
		assert.Equal(t, bufSize, int64(512))
	})

	t.Run("Test searchResultMsgStreamReceiveBufSize", func(t *testing.T) {
		bufSize := Params.searchResultReceiveBufSize()
		assert.Equal(t, bufSize, int64(64))
	})

	t.Run("Test searchPulsarBufSize", func(t *testing.T) {
		bufSize := Params.searchPulsarBufSize()
		assert.Equal(t, bufSize, int64(512))
	})

	t.Run("Test insertPulsarBufSize", func(t *testing.T) {
		bufSize := Params.insertPulsarBufSize()
		assert.Equal(t, bufSize, int64(1024))
	})

	t.Run("Test flowGraphMaxQueueLength", func(t *testing.T) {
		length := Params.flowGraphMaxQueueLength()
		assert.Equal(t, length, int32(1024))
	})

	t.Run("Test flowGraphMaxParallelism", func(t *testing.T) {
		maxParallelism := Params.flowGraphMaxParallelism()
		assert.Equal(t, maxParallelism, int32(1024))
	})

	t.Run("Test insertChannelNames", func(t *testing.T) {
		names := Params.insertChannelNames()
		assert.Equal(t, len(names), 2)
		assert.Equal(t, names[0], "insert0")
		assert.Equal(t, names[1], "insert1")
	})

	t.Run("Test searchChannelNames", func(t *testing.T) {
		names := Params.searchChannelNames()
		assert.Equal(t, len(names), 1)
		assert.Equal(t, names[0], "search0")
	})

	t.Run("Test searchResultChannelName", func(t *testing.T) {
		names := Params.searchResultChannelNames()
		assert.Equal(t, len(names), 1)
		assert.Equal(t, names[0], "searchResult-0")
	})

	t.Run("Test msgChannelSubName", func(t *testing.T) {
		name := Params.msgChannelSubName()
		assert.Equal(t, name, "writeNode-3")
	})

	t.Run("Test timeTickChannelName", func(t *testing.T) {
		name := Params.writeNodeTimeTickChannelName()
		assert.Equal(t, name, "writeNodeTimeTick")
	})

	t.Run("Test metaRootPath", func(t *testing.T) {
		path := Params.metaRootPath()
		assert.Equal(t, path, "by-dev/meta")
	})
}
