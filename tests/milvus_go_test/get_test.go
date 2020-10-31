package main

import (
	"milvus_go_test/utils"
	"testing"

	"github.com/milvus-io/milvus-sdk-go/milvus"
	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
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
	getIds := []int64{ids[0]}
	entities, status, _ := client.GetEntityByID(name, nil, getIds)
	t.Log(status)
	t.Log(entities[0].Entity)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, entities[0].EntityId, ids[0])
	assert.Equal(t, entities[0].Entity[utils.DefaultFieldFloatVectorName], utils.DefaultFloatVectors[0])
	assert.Equal(t, entities[0].Entity[utils.DefaultFieldFloatName], utils.DefaultFloatValues[0])
}

func TestGetBinary(t *testing.T) {
	client, name := Collection(true, milvus.VECTORBINARY)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORBINARY),
		nil,
		""}
	ids, status, _ := client.Insert(insertParam)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, len(ids), utils.DefaultNb)
	client.Flush([]string{name})
	getIds := []int64{ids[0]}
	entities, status, _ := client.GetEntityByID(name, nil, getIds)
	t.Log(status)
	t.Log(entities[0].Entity)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, entities[0].EntityId, ids[0])
	assert.Equal(t, entities[0].Entity[utils.DefaultFieldBinaryVectorName], utils.DefaultBinaryVectors[0])
	assert.Equal(t, entities[0].Entity[utils.DefaultFieldFloatName], utils.DefaultFloatValues[0])
}

func TestGetMultiIds(t *testing.T) {
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
	var genLen int = 100
	getIds := ids[0:genLen]
	entities, status, _ := client.GetEntityByID(name, nil, getIds)
	t.Log(status)
	// t.Log(entities)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, len(entities), genLen)
}

func TestGetMultiIdsPartNil(t *testing.T) {
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
	getIds = append(getIds, 1)
	getIds = append(getIds, ids[2])
	t.Log(getIds)
	entities, status, _ := client.GetEntityByID(name, nil, getIds)
	t.Log(status)
	// t.Log(entities)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, len(entities), 3)
	assert.Equal(t, entities[1].EntityId, int64(1))
	assert.Equal(t, entities[1].Entity, map[string]interface{}(nil))
	assert.Equal(t, entities[2].EntityId, ids[2])
	t.Log(entities)
	t.Log(entities[2].Entity)
	t.Log(ids[2])
	t.Log(entities[2].EntityId)
	assert.Equal(t, entities[2].Entity[utils.DefaultFieldFloatName], utils.DefaultFloatValues[2])
}

func TestGetEmptyCollection(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	getIds := []int64{0}
	entities, status, _ := client.GetEntityByID(name, nil, getIds)
	t.Log(status)
	t.Log(entities)
	t.Log(entities[0].Entity)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, len(entities), 1)
	assert.Equal(t, entities[0].EntityId, int64(0))
	assert.Equal(t, entities[0].Entity, map[string]interface{}(nil))
}

func TestGetCollectionNotExisted(t *testing.T) {
	client, _ := Collection(true, milvus.VECTORFLOAT)
	_name := utils.RandString(8)
	getIds := []int64{0}
	entities, status, _ := client.GetEntityByID(_name, nil, getIds)
	t.Log(status)
	t.Log(entities)
	assert.Equal(t, status.Ok(), false)
}
