package main

import (
	"encoding/json"
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
		client, mapping := GenCollectionParams(name, autoId, segmentRowLimit, dimension)
		status, _ := client.CreateCollection(mapping)
		assert.Equal(t, status.Ok(), false)
	}
}

func TestCreateCollectionInvalidDimension(t *testing.T) {
	client := GetClient()
	var dimension int = 0
	dimParams := map[string]interface{}{
		"dim": dimension,
	}
	extraParams, _ := json.Marshal(dimParams)
	fields := []milvus.Field{
		{
			fieldFloatName,
			milvus.FLOAT,
			"",
			"",
		},
		{
			fieldFloatVectorName,
			milvus.VECTORFLOAT,
			"",
			string(extraParams),
		},
	}
	name := utils.RandString(8)
	params := map[string]interface{}{
		"auto_id":           autoId,
		"segment_row_limit": segmentRowLimit,
	}
	paramsStr, _ := json.Marshal(params)
	mapping := milvus.Mapping{CollectionName: name, Fields: fields, ExtraParams: string(paramsStr)}
	status, _ := client.CreateCollection(mapping)
	assert.Equal(t, status.Ok(), false)
}

func TestShowCollections(t *testing.T)  {
	client := GetClient()
	originCollections := make([]string, 10)
	for i := 0; i< 10; i++ {
		name := utils.RandString(8)
		_, mapping := GenCollectionParams(name, autoId, segmentRowLimit, dimension)
		status, _ := client.CreateCollection(mapping)
		assert.Equal(t, status.Ok(), true)
		originCollections[i] = name
	}
	listCollections, listStatus, _ := client.ListCollections()
	assert.True(t, listStatus.Ok(), true)
	assert.Equal(t, len(originCollections), len(listCollections))
	for i :=0; i<len(originCollections); i++ {
		assert.Contains(t, listCollections, originCollections[i])
	}
}

func TestDropCollections(t *testing.T)  {
	client, name := Collection(false, milvus.VECTORFLOAT)
	client.HasCollection(name)
	status, _ := client.DropCollection(name)
	assert.True(t, status.Ok())
	isHas, _, _ := client.HasCollection(name)
	assert.False(t, isHas)
	listCollections, _, _ := client.ListCollections()
	assert.Nil(t, listCollections)
	//assert.NotContains(t, listCollections, name)
}

func TestDropCollectionNotExisted(t *testing.T)  {
	client := GetClient()
	name := utils.RandString(8)
	status, _ := client.DropCollection( name);
	assert.False(t, status.Ok())
}

func TestDropCollectionWithoutConnect(t *testing.T)  {
	
}
