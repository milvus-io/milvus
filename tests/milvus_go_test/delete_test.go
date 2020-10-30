package main

import (
	"milvus_go_test/utils"
	"testing"

	"github.com/milvus-io/milvus-sdk-go/milvus"
	"github.com/stretchr/testify/assert"
)

func TestDelete(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		nil,
		""}
	ids, status, _ := client.Insert(insertParam)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, len(ids), utils.DefaultNb)
	client.Flush([]string{name})
	getIds := ids[0:1]
	status, _ = client.DeleteEntityByID(name, getIds)
	assert.Equal(t, status.Ok(), true)
	client.Flush([]string{name})
	count, status, _ := client.CountEntities(name)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, int(count), utils.DefaultNb-1)
}

func TestDeleteMultiIds(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		nil,
		""}
	ids, status, _ := client.Insert(insertParam)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, len(ids), utils.DefaultNb)
	client.Flush([]string{name})
	deleteLen := 100
	deleteIds := ids[0:deleteLen]
	status, _ = client.DeleteEntityByID(name, deleteIds)
	assert.Equal(t, status.Ok(), true)
	client.Flush([]string{name})
	count, status, _ := client.CountEntities(name)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, int(count), utils.DefaultNb-deleteLen)
}

func TestDeleteEmptyCollection(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	deleteIds := []int64{0}
	status, _ := client.DeleteEntityByID(name, deleteIds)
	assert.Equal(t, status.Ok(), true)
}

func TestDeleteCollectionNotExisted(t *testing.T) {
	client, _ := Collection(true, milvus.VECTORFLOAT)
	_name := utils.RandString(8)
	deleteIds := []int64{0}
	status, _ := client.DeleteEntityByID(_name, deleteIds)
	t.Log(status)
	assert.Equal(t, status.Ok(), false)
}
