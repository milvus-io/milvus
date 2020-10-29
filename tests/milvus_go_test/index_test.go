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
