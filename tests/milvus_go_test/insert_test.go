package main

import (
	"milvus_go_test/utils"
	"testing"

	"github.com/milvus-io/milvus-sdk-go/milvus"
	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		nil,
		""}
	ids, status, _ := client.Insert(insertParam)
	// t.Log(ids)
	t.Log(status)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, len(ids), utils.DefaultNb)
}

func TestInsertBinary(t *testing.T) {
	client, name := Collection(true, milvus.VECTORBINARY)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORBINARY),
		nil,
		""}
	t.Log(insertParam)
	ids, status, _ := client.Insert(insertParam)
	// t.Log(ids)
	t.Log(status)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, len(ids), utils.DefaultNb)
}

func TestInsertWithCustomIds(t *testing.T) {
	client, name := Collection(false, milvus.VECTORFLOAT)
	var customIds []int64 = utils.DefaultIntValues
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		customIds,
		""}
	ids, status, _ := client.Insert(insertParam)
	// t.Log(ids)
	t.Log(status)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, len(ids), utils.DefaultNb)
	assert.Equal(t, ids, customIds)
}

func TestInsertWithCustomIdsNotMatch(t *testing.T) {
	client, name := Collection(false, milvus.VECTORFLOAT)
	var customIds []int64 = utils.GenDefaultIntValues(utils.DefaultNb - 1)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		customIds,
		""}
	ids, status, _ := client.Insert(insertParam)
	// t.Log(ids)
	t.Log(status)
	assert.Equal(t, status.Ok(), false)
	assert.Equal(t, len(ids), 0)
}

func TestInsertCollectionNotExisted(t *testing.T) {
	client, _ := Collection(true, milvus.VECTORFLOAT)
	_name := utils.RandString(8)
	insertParam := milvus.InsertParam{
		_name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		nil,
		""}
	ids, status, _ := client.Insert(insertParam)
	t.Log(ids)
	t.Log(status)
	assert.Equal(t, status.Ok(), false)
}

func TestInsertWithPartition(t *testing.T)  {
	client, name := Collection(true, milvus.VECTORFLOAT)
	tag := utils.RandString(8)
	pp := milvus.PartitionParam{CollectionName: name, PartitionTag: tag}
	client.CreatePartition(pp)
	insertParam := milvus.InsertParam{
		CollectionName: name,
		Fields:         GenDefaultFieldValues(milvus.VECTORFLOAT),
		PartitionTag: tag}
	ids, status, _ := client.Insert(insertParam)
	t.Log(status)
	assert.True(t, status.Ok())
	assert.Equal(t, len(ids), nb)
	client.Flush([]string{name})
	count, _, _ := client.CountEntities(name)
	assert.Equal(t, nb, int(count))
}

func TestInsertIdCollectionWithoutIds(t *testing.T)  {
	client, name := Collection(false, milvus.VECTORFLOAT)
	tag := utils.RandString(8)
	pp := milvus.PartitionParam{CollectionName: name, PartitionTag: tag}
	client.CreatePartition(pp)
	insertParam := milvus.InsertParam{
		CollectionName: name,
		Fields:         GenDefaultFieldValues(milvus.VECTORFLOAT),
		IDArray: nil,
		PartitionTag: tag}
	ids, status, _ := client.Insert(insertParam)
	t.Log(len(ids))
	t.Log(status)
	assert.False(t, status.Ok())
}
