package main

import (
	"testing"

	"github.com/milvus-io/milvus-sdk-go/milvus"
)

func TestInsert(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		nil,
		""}
	ids, status, _ := client.Insert(insertParam)
	t.Log(ids)
	t.Log(status)
	// assert.Equal(t, status.Ok(), true)
	// assert.Equal(t, len(ids), defaultNb)
}
