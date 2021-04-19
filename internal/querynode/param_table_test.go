package querynodeimp

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable_PulsarAddress(t *testing.T) {
	address := Params.PulsarAddress
	split := strings.Split(address, ":")
	assert.Equal(t, "pulsar", split[0])
	assert.Equal(t, "6650", split[len(split)-1])
}

func TestParamTable_minio(t *testing.T) {
	t.Run("Test endPoint", func(t *testing.T) {
		endPoint := Params.MinioEndPoint
		equal := endPoint == "localhost:9000" || endPoint == "minio:9000"
		assert.Equal(t, equal, true)
	})

	t.Run("Test accessKeyID", func(t *testing.T) {
		accessKeyID := Params.MinioAccessKeyID
		assert.Equal(t, accessKeyID, "minioadmin")
	})

	t.Run("Test secretAccessKey", func(t *testing.T) {
		secretAccessKey := Params.MinioSecretAccessKey
		assert.Equal(t, secretAccessKey, "minioadmin")
	})

	t.Run("Test useSSL", func(t *testing.T) {
		useSSL := Params.MinioUseSSLStr
		assert.Equal(t, useSSL, false)
	})
}

func TestParamTable_LoadIndex(t *testing.T) {
	t.Run("Test channel names", func(t *testing.T) {
		names := Params.LoadIndexChannelNames
		assert.Equal(t, len(names), 1)
		assert.Contains(t, names[0], "cmd")
	})

	t.Run("Test recvBufSize", func(t *testing.T) {
		size := Params.LoadIndexReceiveBufSize
		assert.Equal(t, size, int64(512))
	})

	t.Run("Test pulsarBufSize", func(t *testing.T) {
		size := Params.LoadIndexPulsarBufSize
		assert.Equal(t, size, int64(512))
	})
}

func TestParamTable_QueryNodeID(t *testing.T) {
	id := Params.QueryNodeID
	assert.Contains(t, Params.QueryNodeIDList(), id)
}

func TestParamTable_insertChannelRange(t *testing.T) {
	channelRange := Params.InsertChannelRange
	assert.Equal(t, 2, len(channelRange))
}

func TestParamTable_statsServiceTimeInterval(t *testing.T) {
	interval := Params.StatsPublishInterval
	assert.Equal(t, 1000, interval)
}

func TestParamTable_statsMsgStreamReceiveBufSize(t *testing.T) {
	bufSize := Params.StatsReceiveBufSize
	assert.Equal(t, int64(64), bufSize)
}

func TestParamTable_insertMsgStreamReceiveBufSize(t *testing.T) {
	bufSize := Params.InsertReceiveBufSize
	assert.Equal(t, int64(1024), bufSize)
}

func TestParamTable_ddMsgStreamReceiveBufSize(t *testing.T) {
	bufSize := Params.DDReceiveBufSize
	assert.Equal(t, bufSize, int64(64))
}

func TestParamTable_searchMsgStreamReceiveBufSize(t *testing.T) {
	bufSize := Params.SearchReceiveBufSize
	assert.Equal(t, int64(512), bufSize)
}

func TestParamTable_searchResultMsgStreamReceiveBufSize(t *testing.T) {
	bufSize := Params.SearchResultReceiveBufSize
	assert.Equal(t, int64(64), bufSize)
}

func TestParamTable_searchPulsarBufSize(t *testing.T) {
	bufSize := Params.SearchPulsarBufSize
	assert.Equal(t, int64(512), bufSize)
}

func TestParamTable_insertPulsarBufSize(t *testing.T) {
	bufSize := Params.InsertPulsarBufSize
	assert.Equal(t, int64(1024), bufSize)
}

func TestParamTable_ddPulsarBufSize(t *testing.T) {
	bufSize := Params.DDPulsarBufSize
	assert.Equal(t, bufSize, int64(64))
}

func TestParamTable_flowGraphMaxQueueLength(t *testing.T) {
	length := Params.FlowGraphMaxQueueLength
	assert.Equal(t, int32(1024), length)
}

func TestParamTable_flowGraphMaxParallelism(t *testing.T) {
	maxParallelism := Params.FlowGraphMaxParallelism
	assert.Equal(t, int32(1024), maxParallelism)
}

func TestParamTable_insertChannelNames(t *testing.T) {
	names := Params.InsertChannelNames
	channelRange := Params.InsertChannelRange
	num := channelRange[1] - channelRange[0]
	num = num / Params.QueryNodeNum
	assert.Equal(t, num, len(names))
	start := num * Params.SliceIndex
	contains := strings.Contains(names[0], fmt.Sprintf("insert-%d", channelRange[start]))
	assert.Equal(t, contains, true)
}

func TestParamTable_searchChannelNames(t *testing.T) {
	names := Params.SearchChannelNames
	assert.Equal(t, len(names), 1)
	contains := strings.Contains(names[0], "search-0")
	assert.Equal(t, contains, true)
}

func TestParamTable_searchResultChannelNames(t *testing.T) {
	names := Params.SearchResultChannelNames
	assert.NotNil(t, names)
}

func TestParamTable_msgChannelSubName(t *testing.T) {
	name := Params.MsgChannelSubName
	expectName := fmt.Sprintf("queryNode-%d", Params.QueryNodeID)
	assert.Equal(t, expectName, name)
}

func TestParamTable_statsChannelName(t *testing.T) {
	name := Params.StatsChannelName
	contains := strings.Contains(name, "query-node-stats")
	assert.Equal(t, contains, true)
}

func TestParamTable_metaRootPath(t *testing.T) {
	path := Params.MetaRootPath
	fmt.Println(path)
}

func TestParamTable_ddChannelName(t *testing.T) {
	names := Params.DDChannelNames
	contains := strings.Contains(names[0], "data-definition-0")
	assert.Equal(t, contains, true)
}

func TestParamTable_defaultPartitionTag(t *testing.T) {
	tag := Params.DefaultPartitionTag
	assert.Equal(t, tag, "_default")
}
