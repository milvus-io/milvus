package masterservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable(t *testing.T) {
	Params.Init()

	assert.NotEqual(t, Params.Address, "")
	t.Logf("master address = %s", Params.Address)

	assert.NotEqual(t, Params.Port, 0)
	t.Logf("master port = %d", Params.Port)

	assert.NotEqual(t, Params.NodeID, 0)
	t.Logf("master node ID = %d", Params.NodeID)

	assert.NotEqual(t, Params.PulsarAddress, "")
	t.Logf("pulsar address = %s", Params.PulsarAddress)

	assert.NotEqual(t, Params.EtcdAddress, "")
	t.Logf("etcd address = %s", Params.EtcdAddress)

	assert.NotEqual(t, Params.MetaRootPath, "")
	t.Logf("meta root path = %s", Params.MetaRootPath)

	assert.NotEqual(t, Params.KvRootPath, "")
	t.Logf("kv root path = %s", Params.KvRootPath)

	assert.NotEqual(t, Params.MsgChannelSubName, "")
	t.Logf("msg channel sub name = %s", Params.MsgChannelSubName)

	assert.NotEqual(t, Params.TimeTickChannel, "")
	t.Logf("master time tick channel = %s", Params.TimeTickChannel)

	assert.NotEqual(t, Params.DdChannel, "")
	t.Logf("master dd channel = %s", Params.DdChannel)

	assert.NotEqual(t, Params.StatisticsChannel, "")
	t.Logf("master statistics channel = %s", Params.StatisticsChannel)

	assert.NotEqual(t, Params.MaxPartitionNum, 0)
	t.Logf("master initMaxPartitionNum = %d", Params.MaxPartitionNum)

	assert.NotEqual(t, Params.DefaultPartitionName, "")
	t.Logf("default partition name = %s", Params.DefaultPartitionName)

	assert.NotEqual(t, Params.DefaultIndexName, "")
	t.Logf("default index name = %s", Params.DefaultIndexName)
}
