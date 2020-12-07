package master

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable_Init(t *testing.T) {
	Params.Init()
}

func TestParamTable_Address(t *testing.T) {
	address := Params.Address
	assert.Equal(t, address, "localhost")
}

func TestParamTable_Port(t *testing.T) {
	port := Params.Port
	assert.Equal(t, port, 53100)
}

func TestParamTable_MetaRootPath(t *testing.T) {
	path := Params.MetaRootPath
	assert.Equal(t, path, "by-dev/meta")
}

func TestParamTable_KVRootPath(t *testing.T) {
	path := Params.KvRootPath
	assert.Equal(t, path, "by-dev/kv")
}

func TestParamTable_TopicNum(t *testing.T) {
	num := Params.TopicNum
	assert.Equal(t, num, 1)
}

func TestParamTable_SegmentSize(t *testing.T) {
	size := Params.SegmentSize
	assert.Equal(t, size, float64(512))
}

func TestParamTable_SegmentSizeFactor(t *testing.T) {
	factor := Params.SegmentSizeFactor
	assert.Equal(t, factor, 0.75)
}

func TestParamTable_DefaultRecordSize(t *testing.T) {
	size := Params.DefaultRecordSize
	assert.Equal(t, size, int64(1024))
}

func TestParamTable_MinSegIDAssignCnt(t *testing.T) {
	cnt := Params.MinSegIDAssignCnt
	assert.Equal(t, cnt, int64(1024))
}

func TestParamTable_MaxSegIDAssignCnt(t *testing.T) {
	cnt := Params.MaxSegIDAssignCnt
	assert.Equal(t, cnt, int64(16384))
}

func TestParamTable_SegIDAssignExpiration(t *testing.T) {
	expiration := Params.SegIDAssignExpiration
	assert.Equal(t, expiration, int64(2000))
}

func TestParamTable_QueryNodeNum(t *testing.T) {
	num := Params.QueryNodeNum
	assert.Equal(t, num, 1)
}

func TestParamTable_QueryNodeStatsChannelName(t *testing.T) {
	name := Params.QueryNodeStatsChannelName
	assert.Equal(t, name, "query-node-stats")
}

func TestParamTable_ProxyIDList(t *testing.T) {
	ids := Params.ProxyIDList
	assert.Equal(t, len(ids), 1)
	assert.Equal(t, ids[0], int64(0))
}

func TestParamTable_ProxyTimeTickChannelNames(t *testing.T) {
	names := Params.ProxyTimeTickChannelNames
	assert.Equal(t, len(names), 1)
	assert.Equal(t, names[0], "proxyTimeTick-0")
}

func TestParamTable_MsgChannelSubName(t *testing.T) {
	name := Params.MsgChannelSubName
	assert.Equal(t, name, "master")
}

func TestParamTable_SoftTimeTickBarrierInterval(t *testing.T) {
	interval := Params.SoftTimeTickBarrierInterval
	assert.Equal(t, interval, Timestamp(0x7d00000))
}

func TestParamTable_WriteNodeIDList(t *testing.T) {
	ids := Params.WriteNodeIDList
	assert.Equal(t, len(ids), 1)
	assert.Equal(t, ids[0], int64(3))
}

func TestParamTable_WriteNodeTimeTickChannelNames(t *testing.T) {
	names := Params.WriteNodeTimeTickChannelNames
	assert.Equal(t, len(names), 1)
	assert.Equal(t, names[0], "writeNodeTimeTick-3")
}

func TestParamTable_InsertChannelNames(t *testing.T) {
	names := Params.InsertChannelNames
	assert.Equal(t, len(names), 1)
	assert.Equal(t, names[0], "insert-0")
}

func TestParamTable_K2SChannelNames(t *testing.T) {
	names := Params.K2SChannelNames
	assert.Equal(t, len(names), 1)
	assert.Equal(t, names[0], "k2s-3")
}
