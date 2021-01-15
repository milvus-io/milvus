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
	assert.Equal(t, address, "localhost:31000")
}

func TestParamTable_Port(t *testing.T) {
	port := Params.Port
	assert.Equal(t, port, 31000)
}

func TestParamTable_MetaRootPath(t *testing.T) {
	path := Params.MetaRootPath
	assert.Equal(t, path, "by-dev/meta")
}

func TestParamTable_MinIOAddress(t *testing.T) {
	address := Params.MinIOAccessKeyID
	assert.Equal(t, address, "localhost")
}

func TestParamTable_MinIOPort(t *testing.T) {
	port := Params.MinIOPort
	assert.Equal(t, port, 9000)
}

func TestParamTable_MinIOAccessKeyID(t *testing.T) {
	accessKeyID := Params.MinIOAccessKeyID
	assert.Equal(t, accessKeyID, "minioadmin")
}

func TestParamTable_MinIOSecretAccessKey(t *testing.T) {
	secretAccessKey := Params.MinIOSecretAccessKey
	assert.Equal(t, secretAccessKey, "minioadmin")
}

func TestParamTable_MinIOUseSSL(t *testing.T) {
	useSSL := Params.MinIOUseSSL
	assert.Equal(t, useSSL, false)
}
