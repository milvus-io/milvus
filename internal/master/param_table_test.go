package master

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable_Init(t *testing.T) {
	Params.Init()
}

func TestParamTable_Address(t *testing.T) {
	Params.Init()
	address := Params.Address
	assert.Equal(t, address, "localhost")
}

func TestParamTable_Port(t *testing.T) {
	Params.Init()
	port := Params.Port
	assert.Equal(t, port, 53100)
}

func TestParamTable_EtcdRootPath(t *testing.T) {
	Params.Init()
	addr := Params.EtcdRootPath
	assert.Equal(t, addr, "by-dev")
}

func TestParamTable_TopicNum(t *testing.T) {
	Params.Init()
	num := Params.TopicNum
	assert.Equal(t, num, 1)
}

func TestParamTable_SegmentSize(t *testing.T) {
	Params.Init()
	size := Params.SegmentSize
	assert.Equal(t, size, float64(512))
}

func TestParamTable_SegmentSizeFactor(t *testing.T) {
	Params.Init()
	factor := Params.SegmentSizeFactor
	assert.Equal(t, factor, 0.75)
}

func TestParamTable_DefaultRecordSize(t *testing.T) {
	Params.Init()
	size := Params.DefaultRecordSize
	assert.Equal(t, size, int64(1024))
}

func TestParamTable_MinSegIDAssignCnt(t *testing.T) {
	Params.Init()
	cnt := Params.MinSegIDAssignCnt
	assert.Equal(t, cnt, int64(1024))
}

func TestParamTable_MaxSegIDAssignCnt(t *testing.T) {
	Params.Init()
	cnt := Params.MaxSegIDAssignCnt
	assert.Equal(t, cnt, int64(16384))
}

func TestParamTable_SegIDAssignExpiration(t *testing.T) {
	Params.Init()
	expiration := Params.SegIDAssignExpiration
	assert.Equal(t, expiration, int64(2000))
}

func TestParamTable_QueryNodeNum(t *testing.T) {
	Params.Init()
	num := Params.QueryNodeNum
	assert.Equal(t, num, 1)
}

func TestParamTable_QueryNodeStatsChannelName(t *testing.T) {
	Params.Init()
	name := Params.QueryNodeStatsChannelName
	assert.Equal(t, name, "query-node-stats")
}

func TestParamTable_ProxyIDList(t *testing.T) {
	Params.Init()
	ids := Params.ProxyIDList
	assert.Equal(t, len(ids), 1)
	assert.Equal(t, ids[0], int64(0))
}

func TestParamTable_ProxyTimeTickChannelNames(t *testing.T) {
	Params.Init()
	names := Params.ProxyTimeTickChannelNames
	assert.Equal(t, len(names), 1)
	assert.Equal(t, names[0], "proxyTimeTick-0")
}

func TestParamTable_MsgChannelSubName(t *testing.T) {
	Params.Init()
	name := Params.MsgChannelSubName
	assert.Equal(t, name, "master")
}

func TestParamTable_SoftTimeTickBarrierInterval(t *testing.T) {
	Params.Init()
	interval := Params.SoftTimeTickBarrierInterval
	assert.Equal(t, interval, Timestamp(0x7d00000))
}

func TestParamTable_WriteNodeIDList(t *testing.T) {
	Params.Init()
	ids := Params.WriteNodeIDList
	assert.Equal(t, len(ids), 1)
	assert.Equal(t, ids[0], int64(3))
}

func TestParamTable_WriteNodeTimeTickChannelNames(t *testing.T) {
	Params.Init()
	names := Params.WriteNodeTimeTickChannelNames
	assert.Equal(t, len(names), 1)
	assert.Equal(t, names[0], "writeNodeTimeTick-3")
}

func TestParamTable_InsertChannelNames(t *testing.T) {
	Params.Init()
	names := Params.InsertChannelNames
	assert.Equal(t, len(names), 1)
	assert.Equal(t, names[0], "insert-0")
}

func TestParamTable_K2SChannelNames(t *testing.T) {
	Params.Init()
	names := Params.K2SChannelNames
	assert.Equal(t, len(names), 1)
	assert.Equal(t, names[0], "k2s-3")
}
