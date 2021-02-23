package grpcquerynode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable(t *testing.T) {
	Params.Init()

	assert.NotEqual(t, Params.IndexServiceAddress, "")
	t.Logf("IndexServiceAddress:%s", Params.IndexServiceAddress)

	assert.NotEqual(t, Params.DataServiceAddress, "")
	t.Logf("DataServiceAddress:%s", Params.DataServiceAddress)

	assert.NotEqual(t, Params.MasterAddress, "")
	t.Logf("MasterAddress:%s", Params.MasterAddress)

	assert.NotEqual(t, Params.QueryServiceAddress, "")
	t.Logf("QueryServiceAddress:%s", Params.QueryServiceAddress)
}
