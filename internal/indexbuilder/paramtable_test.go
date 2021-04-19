package indexbuilder

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
	assert.Equal(t, port, 310310)
}

func TestParamTable_MetaRootPath(t *testing.T) {
	path := Params.MetaRootPath
	assert.Equal(t, path, "by-dev/meta")
}
