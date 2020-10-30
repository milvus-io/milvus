package main

import (
	"milvus_go_test/utils"
	"testing"

	"github.com/milvus-io/milvus-sdk-go/milvus"
	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		nil,
		""}
	_, status, _ := client.Insert(insertParam)
	assert.Equal(t, status.Ok(), true)
	client.Flush([]string{name})
	for _, _index := range utils.L2Indexes {
		t.Log(_index.IndexType)
		var index = utils.Struct2Map(_index)
		indexParam := milvus.IndexParam{name, utils.DefaultFieldFloatVectorName, index}
		status, _ = client.CreateIndex(&indexParam)
		t.Log(status)
		assert.Equal(t, status.Ok(), true)
	}
}

// TODO: assert index type
func TestIndexBinary(t *testing.T) {
	client, name := Collection(true, milvus.VECTORBINARY)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORBINARY),
		nil,
		""}
	_, status, _ := client.Insert(insertParam)
	assert.Equal(t, status.Ok(), true)
	client.Flush([]string{name})
	var index = utils.DefaultBinaryIndex
	indexParam := milvus.IndexParam{name, utils.DefaultFieldBinaryVectorName, index}
	status, _ = client.CreateIndex(&indexParam)
	t.Log(status)
	assert.Equal(t, status.Ok(), true)
}

func TestIndexCollectionNotExisted(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		nil,
		""}
	_, status, _ := client.Insert(insertParam)
	assert.Equal(t, status.Ok(), true)
	client.Flush([]string{name})
	_name := utils.RandString(8)
	index := utils.DefaultL2Index
	indexParam := milvus.IndexParam{_name, utils.DefaultFieldFloatVectorName, index}
	status, _ = client.CreateIndex(&indexParam)
	t.Log(status)
	assert.Equal(t, status.Ok(), false)
}

func TestIndexFieldNameNotExisted(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		nil,
		""}
	_, status, _ := client.Insert(insertParam)
	assert.Equal(t, status.Ok(), true)
	client.Flush([]string{name})
	_name := utils.RandString(8)
	index := utils.DefaultL2Index
	indexParam := milvus.IndexParam{name, _name, index}
	status, _ = client.CreateIndex(&indexParam)
	t.Log(status)
	assert.Equal(t, status.Ok(), false)
}

func TestIndexInvalidIndexName(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		nil,
		""}
	_, status, _ := client.Insert(insertParam)
	assert.Equal(t, status.Ok(), true)
	client.Flush([]string{name})
	index := utils.DefaultL2Index
	t.Log(index)
	index["index_type"] = "string"
	t.Log(index)
	indexParam := milvus.IndexParam{name, utils.DefaultFieldFloatVectorName, index}
	status, _ = client.CreateIndex(&indexParam)
	t.Log(status)
	assert.Equal(t, status.Ok(), false)
}

// drop index
func TestIndexDrop(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		nil,
		""}
	_, status, _ := client.Insert(insertParam)
	assert.Equal(t, status.Ok(), true)
	client.Flush([]string{name})
	for _, _index := range utils.L2Indexes {
		t.Log(_index.IndexType)
		var index = utils.Struct2Map(_index)
		indexParam := milvus.IndexParam{name, utils.DefaultFieldFloatVectorName, index}
		status, _ = client.CreateIndex(&indexParam)
		t.Log(status)
		assert.Equal(t, status.Ok(), true)
		status, _ = client.DropIndex(name, utils.DefaultFieldFloatVectorName)
		assert.Equal(t, status.Ok(), true)
	}
}

func TestIndexDropCollectionNotExisted(t *testing.T) {
	client, _ := Collection(true, milvus.VECTORFLOAT)
	_name := utils.RandString(8)
	status, _ := client.DropIndex(_name, utils.DefaultFieldFloatVectorName)
	assert.Equal(t, status.Ok(), false)
}

// func TestIndexDropFieldNameNotExisted(t *testing.T) {
// 	client, name := Collection(true, milvus.VECTORFLOAT)
// 	_name := utils.RandString(8)
// 	status, _ := client.DropIndex(name, _name)
// 	assert.Equal(t, status.Ok(), false)
// }
