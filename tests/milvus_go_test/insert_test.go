package main

import (
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
	assert.Equal(t, len(ids), defaultNb)
}

func TestInsertWithCustomIds(t *testing.T) {
	client, name := Collection(false, milvus.VECTORFLOAT)
	var customIds []int64 = defaultIntValues
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		customIds,
		""}
	ids, status, _ := client.Insert(insertParam)
	// t.Log(ids)
	t.Log(status)
	assert.Equal(t, status.Ok(), true)
	assert.Equal(t, len(ids), defaultNb)
}
