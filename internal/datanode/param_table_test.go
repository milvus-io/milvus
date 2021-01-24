package datanode

import (
	"log"
	"testing"
)

func TestParamTable_DataNode(t *testing.T) {

	Params.Init()

	t.Run("Test NodeID", func(t *testing.T) {
		id := Params.NodeID
		log.Println("NodeID:", id)
	})

	t.Run("Test flowGraphMaxQueueLength", func(t *testing.T) {
		length := Params.FlowGraphMaxQueueLength
		log.Println("flowGraphMaxQueueLength:", length)
	})

	t.Run("Test flowGraphMaxParallelism", func(t *testing.T) {
		maxParallelism := Params.FlowGraphMaxParallelism
		log.Println("flowGraphMaxParallelism:", maxParallelism)
	})

	t.Run("Test FlushInsertBufSize", func(t *testing.T) {
		size := Params.FlushInsertBufferSize
		log.Println("FlushInsertBufferSize:", size)
	})

	t.Run("Test FlushDdBufSize", func(t *testing.T) {
		size := Params.FlushDdBufferSize
		log.Println("FlushDdBufferSize:", size)
	})

	t.Run("Test InsertBinlogRootPath", func(t *testing.T) {
		path := Params.InsertBinlogRootPath
		log.Println("InsertBinlogRootPath:", path)
	})

	t.Run("Test DdBinlogRootPath", func(t *testing.T) {
		path := Params.DdBinlogRootPath
		log.Println("DdBinlogRootPath:", path)
	})

	t.Run("Test MasterAddress", func(t *testing.T) {
		address := Params.MasterAddress
		log.Println("MasterAddress:", address)
	})

	t.Run("Test PulsarAddress", func(t *testing.T) {
		address := Params.PulsarAddress
		log.Println("PulsarAddress:", address)
	})

	t.Run("Test insertChannelNames", func(t *testing.T) {
		names := Params.InsertChannelNames
		log.Println("InsertChannelNames:", names)
	})

	t.Run("Test insertChannelRange", func(t *testing.T) {
		channelRange := Params.InsertChannelRange
		log.Println("InsertChannelRange:", channelRange)
	})

	t.Run("Test insertMsgStreamReceiveBufSize", func(t *testing.T) {
		bufSize := Params.InsertReceiveBufSize
		log.Println("InsertReceiveBufSize:", bufSize)
	})

	t.Run("Test insertPulsarBufSize", func(t *testing.T) {
		bufSize := Params.InsertPulsarBufSize
		log.Println("InsertPulsarBufSize:", bufSize)
	})

	t.Run("Test ddChannelNames", func(t *testing.T) {
		names := Params.DDChannelNames
		log.Println("DDChannelNames:", names)
	})

	t.Run("Test SegmentStatisticsChannelName", func(t *testing.T) {
		name := Params.SegmentStatisticsChannelName
		log.Println("SegmentStatisticsChannelName:", name)
	})

	t.Run("Test timeTickChannelName", func(t *testing.T) {
		name := Params.TimeTickChannelName
		log.Println("TimeTickChannelName:", name)
	})

	t.Run("Test msgChannelSubName", func(t *testing.T) {
		name := Params.MsgChannelSubName
		log.Println("MsgChannelSubName:", name)
	})

	t.Run("Test EtcdAddress", func(t *testing.T) {
		addr := Params.EtcdAddress
		log.Println("EtcdAddress:", addr)
	})

	t.Run("Test MetaRootPath", func(t *testing.T) {
		path := Params.MetaRootPath
		log.Println("MetaRootPath:", path)
	})

	t.Run("Test SegFlushMetaSubPath", func(t *testing.T) {
		path := Params.SegFlushMetaSubPath
		log.Println("SegFlushMetaSubPath:", path)
	})

	t.Run("Test DDLFlushMetaSubPath", func(t *testing.T) {
		path := Params.DDLFlushMetaSubPath
		log.Println("DDLFlushMetaSubPath:", path)
	})

	t.Run("Test minioAccessKeyID", func(t *testing.T) {
		id := Params.MinioAccessKeyID
		log.Println("MinioAccessKeyID:", id)
	})

	t.Run("Test minioSecretAccessKey", func(t *testing.T) {
		key := Params.MinioSecretAccessKey
		log.Println("MinioSecretAccessKey:", key)
	})

	t.Run("Test MinioUseSSL", func(t *testing.T) {
		useSSL := Params.MinioUseSSL
		log.Println("MinioUseSSL:", useSSL)
	})

	t.Run("Test MinioBucketName", func(t *testing.T) {
		name := Params.MinioBucketName
		log.Println("MinioBucketName:", name)
	})

}
