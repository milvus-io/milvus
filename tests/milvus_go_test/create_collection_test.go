package main

import (
	"testing"

	"milvus_go_test/utils"

	"github.com/milvus-io/milvus-sdk-go/milvus"
	"github.com/stretchr/testify/assert"
)

// TODO issue: failed sometimes
func TestCreateCollection(t *testing.T) {
	client, name := Collection(false, milvus.VECTORFLOAT)
	value, status, _ := client.HasCollection(name)
	t.Log(value)
	t.Log(status)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, value, true)
}

func TestCreateCollectionWithInvalidName(t *testing.T) {
	for _, name := range utils.GenInvalidStrs() {
		client, mapping := GenCollectionParams(name)
		status, _ := client.CreateCollection(mapping)
		assert.Equal(t, status.Ok(), false)
	}
}
