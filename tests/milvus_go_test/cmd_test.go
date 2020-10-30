package main

import (
	"testing"

	"github.com/milvus-io/milvus-sdk-go/milvus"
	"github.com/stretchr/testify/assert"
)

func TestServerVersion(t *testing.T) {
	// client := GetClient()
	client, _ := Collection(true, milvus.VECTORFLOAT)
	res, status, _ = client.ServerVersion()
	t.Log(res)
	assert.True(t, status.Ok())
}
