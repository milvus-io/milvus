package datanode

import (
	"log"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable_DataNode(t *testing.T) {

	Params.Init()
	log.Println("Params in ParamTable test: ", Params)

	t.Run("Test DataNodeID", func(t *testing.T) {
		id := Params.DataNodeID
		assert.Equal(t, id, UniqueID(3))
	})

	t.Run("Test flowGraphMaxQueueLength", func(t *testing.T) {
		length := Params.FlowGraphMaxQueueLength
		assert.Equal(t, length, int32(1024))
	})

	t.Run("Test flowGraphMaxParallelism", func(t *testing.T) {
		maxParallelism := Params.FlowGraphMaxParallelism
		assert.Equal(t, maxParallelism, int32(1024))
	})

	t.Run("Test FlushInsertBufSize", func(t *testing.T) {
		size := Params.FlushInsertBufferSize
		assert.Equal(t, int32(500), size)
	})

	t.Run("Test FlushDdBufSize", func(t *testing.T) {
		size := Params.FlushDdBufferSize
		assert.Equal(t, int32(20), size)
	})

	t.Run("Test InsertBinlogRootPath", func(t *testing.T) {
		path := Params.InsertBinlogRootPath
		assert.Equal(t, "by-dev/insert_log", path)
	})

	t.Run("Test DdBinlogRootPath", func(t *testing.T) {
		path := Params.DdBinlogRootPath
		assert.Equal(t, "by-dev/data_definition_log", path)
	})

	t.Run("Test MasterAddress", func(t *testing.T) {
		address := Params.MasterAddress
		split := strings.Split(address, ":")
		assert.Equal(t, "localhost", split[0])
		assert.Equal(t, "53100", split[1])
	})

	t.Run("Test PulsarAddress", func(t *testing.T) {
		address := Params.PulsarAddress
		split := strings.Split(address, ":")
		assert.Equal(t, split[0], "pulsar")
		assert.Equal(t, split[len(split)-1], "6650")
	})

	t.Run("Test insertChannelNames", func(t *testing.T) {
		names := Params.InsertChannelNames
		assert.Equal(t, len(names), 2)
		assert.Equal(t, names[0], "insert-0")
		assert.Equal(t, names[1], "insert-1")
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

	t.Run("Test ddChannelNames", func(t *testing.T) {
		names := Params.DDChannelNames
		assert.Equal(t, len(names), 1)
		assert.Equal(t, names[0], "data-definition-0")
	})

	t.Run("Test DdMsgStreamReceiveBufSize", func(t *testing.T) {
		bufSize := Params.DDReceiveBufSize
		assert.Equal(t, int64(64), bufSize)
	})

	t.Run("Test DdPulsarBufSize", func(t *testing.T) {
		bufSize := Params.DDPulsarBufSize
		assert.Equal(t, int64(64), bufSize)
	})

	t.Run("Test SegmentStatisticsChannelName", func(t *testing.T) {
		name := Params.SegmentStatisticsChannelName
		assert.Equal(t, "dataNodeSegStatistics", name)
	})

	t.Run("Test SegmentStatisticsBufSize", func(t *testing.T) {
		size := Params.SegmentStatisticsBufSize
		assert.Equal(t, int64(64), size)
	})

	t.Run("Test SegmentStatisticsPublishInterval", func(t *testing.T) {
		interval := Params.SegmentStatisticsPublishInterval
		assert.Equal(t, 1000, interval)
	})

	t.Run("Test timeTickChannelName", func(t *testing.T) {
		name := Params.TimeTickChannelName
		assert.Equal(t, "dataNodeTimeTick-3", name)
	})

	t.Run("Test msgChannelSubName", func(t *testing.T) {
		name := Params.MsgChannelSubName
		assert.Equal(t, "dataNode-3", name)
	})

	t.Run("Test EtcdAddress", func(t *testing.T) {
		addr := Params.EtcdAddress
		split := strings.Split(addr, ":")
		assert.Equal(t, "localhost", split[0])
		assert.Equal(t, "2379", split[1])
	})

	t.Run("Test MetaRootPath", func(t *testing.T) {
		path := Params.MetaRootPath
		assert.Equal(t, "by-dev/meta", path)
	})

	t.Run("Test SegFlushMetaSubPath", func(t *testing.T) {
		path := Params.SegFlushMetaSubPath
		assert.Equal(t, "writer/segment", path)
	})

	t.Run("Test DDLFlushMetaSubPath", func(t *testing.T) {
		path := Params.DDLFlushMetaSubPath
		assert.Equal(t, "writer/ddl", path)
	})

	t.Run("Test minioAccessKeyID", func(t *testing.T) {
		id := Params.MinioAccessKeyID
		assert.Equal(t, "minioadmin", id)
	})

	t.Run("Test minioSecretAccessKey", func(t *testing.T) {
		key := Params.MinioSecretAccessKey
		assert.Equal(t, "minioadmin", key)
	})

	t.Run("Test MinioUseSSL", func(t *testing.T) {
		useSSL := Params.MinioUseSSL
		assert.Equal(t, false, useSSL)
	})

	t.Run("Test MinioBucketName", func(t *testing.T) {
		name := Params.MinioBucketName
		assert.Equal(t, "a-bucket", name)
	})

}
