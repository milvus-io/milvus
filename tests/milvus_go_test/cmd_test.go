package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCMDServerVersion(t *testing.T) {
	client := GetClient()
	// client, _ := Collection(true, milvus.VECTORFLOAT)
	res, status, _ := client.ServerVersion()
	t.Log(res)
	assert.True(t, status.Ok())
}

func TestCMDServerStatus(t *testing.T) {
	client := GetClient()
	// client, _ := Collection(true, milvus.VECTORFLOAT)
	res, status, _ := client.ServerStatus()
	t.Log(res)
	assert.True(t, status.Ok())
}

func TestCMDGetConfig(t *testing.T) {
	client := GetClient()
	// client, _ := Collection(true, milvus.VECTORFLOAT)
	res, status, _ := client.GetConfig("cache.cache_size")
	t.Log(res)
	assert.True(t, status.Ok())
}
