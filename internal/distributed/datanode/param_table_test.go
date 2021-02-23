package grpcdatanode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable(t *testing.T) {
	Params.Init()

	assert.NotEqual(t, Params.Port, 0)
	t.Logf("DataNode Port:%d", Params.Port)

	assert.NotEqual(t, Params.DataServiceAddress, "")
	t.Logf("DataServiceAddress:%s", Params.DataServiceAddress)

	assert.NotEqual(t, Params.MasterAddress, "")
	t.Logf("MasterAddress:%s", Params.MasterAddress)

}
