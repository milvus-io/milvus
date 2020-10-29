package main

import (
	"milvus_go_test/utils"
	"testing"

	"github.com/milvus-io/milvus-sdk-go/milvus"
	"github.com/stretchr/testify/assert"
)

func TestCount(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		nil,
		""}
	ids, _, _ := client.Insert(insertParam)
	assert.Equal(t, len(ids), utils.DefaultNb)
	client.Flush([]string{name})
	count, status, _ := client.CountEntities(name)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, int(count), utils.DefaultNb)
}

func TestCountCustomIds(t *testing.T) {
	client, name := Collection(false, milvus.VECTORFLOAT)
	var customIds []int64 = utils.DefaultIntValues
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		customIds,
		""}
	ids, _, _ := client.Insert(insertParam)
	assert.Equal(t, len(ids), utils.DefaultNb)
	client.Flush([]string{name})
	count, status, _ := client.CountEntities(name)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, int(count), len(customIds))
}

func TestCountEmpty(t *testing.T) {
	client, name := Collection(false, milvus.VECTORFLOAT)
	count, status, _ := client.CountEntities(name)
	t.Log(status)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, int(count), 0)
}

func TestCountInvalidName(t *testing.T) {
	client, _ := Collection(false, milvus.VECTORFLOAT)
	for _, _name := range utils.GenInvalidStrs() {
		count, status, _ := client.CountEntities(_name)
		t.Log(status)
		assert.Equal(t, status.Ok(), false)
		assert.Equal(t, int(count), 0)
	}
}
