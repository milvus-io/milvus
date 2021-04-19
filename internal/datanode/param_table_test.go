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

	t.Run("Test DdlBinlogRootPath", func(t *testing.T) {
		path := Params.DdlBinlogRootPath
		log.Println("DdBinlogRootPath:", path)
	})

	t.Run("Test PulsarAddress", func(t *testing.T) {
		address := Params.PulsarAddress
		log.Println("PulsarAddress:", address)
	})

	t.Run("Test insertChannelNames", func(t *testing.T) {
		names := Params.InsertChannelNames
		log.Println("InsertChannelNames:", names)
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

	t.Run("Test sliceIndex", func(t *testing.T) {
		idx := Params.sliceIndex()
		log.Println("sliceIndex:", idx)
	})
}
