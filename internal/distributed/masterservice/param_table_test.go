package grpcmasterservice

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

	assert.NotEqual(t, Params.IndexServiceAddress, "")
	t.Logf("IndexServiceAddress:%s", Params.IndexServiceAddress)

	assert.NotEqual(t, Params.DataServiceAddress, "")
	t.Logf("DataServiceAddress:%s", Params.DataServiceAddress)

	assert.NotEqual(t, Params.QueryServiceAddress, "")
	t.Logf("QueryServiceAddress:%s", Params.QueryServiceAddress)

	assert.NotEqual(t, Params.ProxyServiceAddress, "")
	t.Logf("ProxyServiceAddress:%s", Params.ProxyServiceAddress)
}
