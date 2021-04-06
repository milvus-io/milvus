package proxyservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGlobalNodeInfoTable_Register(t *testing.T) {
	table := newGlobalNodeInfoTable()

	idInfoMaps := map[UniqueID]*nodeInfo{
		0: {"localhost", 1080},
		1: {"localhost", 1081},
	}

	var err error

	err = table.Register(0, idInfoMaps[0])
	assert.Equal(t, nil, err)

	err = table.Register(1, idInfoMaps[1])
	assert.Equal(t, nil, err)

	/************** duplicated register ***************/

	err = table.Register(0, idInfoMaps[0])
	assert.Equal(t, nil, err)

	err = table.Register(1, idInfoMaps[1])
	assert.Equal(t, nil, err)
}

func TestGlobalNodeInfoTable_Pick(t *testing.T) {
	table := newGlobalNodeInfoTable()

	var err error

	_, err = table.Pick()
	assert.NotEqual(t, nil, err)

	idInfoMaps := map[UniqueID]*nodeInfo{
		0: {"localhost", 1080},
		1: {"localhost", 1081},
	}

	err = table.Register(0, idInfoMaps[0])
	assert.Equal(t, nil, err)

	err = table.Register(1, idInfoMaps[1])
	assert.Equal(t, nil, err)

	num := 10
	for i := 0; i < num; i++ {
		_, err = table.Pick()
		assert.Equal(t, nil, err)
	}
}

func TestGlobalNodeInfoTable_ObtainAllClients(t *testing.T) {
	table := newGlobalNodeInfoTable()

	var err error

	clients, err := table.ObtainAllClients()
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(clients))
}

func TestGlobalNodeInfoTable_ReleaseAllClients(t *testing.T) {
	table := newGlobalNodeInfoTable()

	err := table.ReleaseAllClients()
	assert.Equal(t, nil, err)
}
