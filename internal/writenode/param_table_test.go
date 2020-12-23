package writenode

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable_WriteNode(t *testing.T) {

	Params.Init()

	t.Run("Test PulsarAddress", func(t *testing.T) {
		address := Params.PulsarAddress
		split := strings.Split(address, ":")
		assert.Equal(t, split[0], "pulsar")
		assert.Equal(t, split[len(split)-1], "6650")
	})

	t.Run("Test WriteNodeID", func(t *testing.T) {
		id := Params.WriteNodeID
		assert.Equal(t, id, UniqueID(3))
	})

	t.Run("Test insertChannelRange", func(t *testing.T) {
		channelRange := Params.InsertChannelRange
		assert.Equal(t, len(channelRange), 2)
		assert.Equal(t, channelRange[0], 0)
		assert.Equal(t, channelRange[1], 2)
	})

	t.Run("Test insertMsgStreamReceiveBufSize", func(t *testing.T) {
		bufSize := Params.InsertReceiveBufSize
		assert.Equal(t, bufSize, int64(1024))
	})

	t.Run("Test insertPulsarBufSize", func(t *testing.T) {
		bufSize := Params.InsertPulsarBufSize
		assert.Equal(t, bufSize, int64(1024))
	})

	t.Run("Test flowGraphMaxQueueLength", func(t *testing.T) {
		length := Params.FlowGraphMaxQueueLength
		assert.Equal(t, length, int32(1024))
	})

	t.Run("Test flowGraphMaxParallelism", func(t *testing.T) {
		maxParallelism := Params.FlowGraphMaxParallelism
		assert.Equal(t, maxParallelism, int32(1024))
	})

	t.Run("Test insertChannelNames", func(t *testing.T) {
		names := Params.InsertChannelNames
		assert.Equal(t, len(names), 2)
		assert.Equal(t, names[0], "insert-0")
		assert.Equal(t, names[1], "insert-1")
	})

	t.Run("Test msgChannelSubName", func(t *testing.T) {
		name := Params.MsgChannelSubName
		assert.Equal(t, name, "writeNode-3")
	})

	t.Run("Test timeTickChannelName", func(t *testing.T) {
		name := Params.WriteNodeTimeTickChannelName
		assert.Equal(t, name, "writeNodeTimeTick")
	})

	t.Run("Test minioAccessKeyID", func(t *testing.T) {
		id := Params.MinioAccessKeyID
		assert.Equal(t, id, "minioadmin")
	})

	t.Run("Test minioSecretAccessKey", func(t *testing.T) {
		id := Params.MinioSecretAccessKey
		assert.Equal(t, id, "minioadmin")
	})

	t.Run("Test MinioUseSSL", func(t *testing.T) {
		id := Params.MinioUseSSL
		assert.Equal(t, id, false)
	})

	t.Run("Test MinioBucketName", func(t *testing.T) {
		name := Params.MinioBucketName
		assert.Equal(t, name, "A-bucket")
	})

	t.Run("Test FlushInsertBufSize", func(t *testing.T) {
		name := Params.FlushInsertBufSize
		assert.Equal(t, name, 20)
	})

	t.Run("Test FlushDdBufSize", func(t *testing.T) {
		name := Params.FlushDdBufSize
		assert.Equal(t, name, 20)
	})

	t.Run("Test InsertLogRootPath", func(t *testing.T) {
		name := Params.InsertLogRootPath
		assert.Equal(t, name, "by-dev/insert_log")
	})

	t.Run("Test DdLogRootPath", func(t *testing.T) {
		name := Params.DdLogRootPath
		assert.Equal(t, name, "by-dev/data_definition_log")
	})
}
