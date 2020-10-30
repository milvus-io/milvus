package main

import (
	"testing"

	"github.com/milvus-io/milvus-sdk-go/milvus"
	"github.com/stretchr/testify/assert"
)

// TODO issue: failed sometimes
func TestInsert(t *testing.T) {
	client, name := Collection(false, milvus.VECTORFLOAT)
	value, status, _ := client.HasCollection(name)
	t.Log(value)
	t.Log(status)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, value, true)
}
