package main

import (
	"milvus_go_test/utils"
	"testing"

	"github.com/milvus-io/milvus-sdk-go/milvus"
	"github.com/stretchr/testify/assert"
)

func TestSearch(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		nil,
		""}
	_, status, _ := client.Insert(insertParam)
	assert.Equal(t, status.Ok(), true)
	client.Flush([]string{name})
	// t.Log(utils.DefaultDsl)
	searchParam := milvus.SearchParam{name, utils.DefaultDsl, nil}
	res, status, _ := client.Search(searchParam)
	t.Log(res)
	t.Log(status)
	assert.Equal(t, status.Ok(), true)
	t.Log(len(res.QueryResultList))
	t.Log(len(res.QueryResultList[1].Ids))
	t.Log(int(utils.DefaultNq))
	assert.Equal(t, int(len(res.QueryResultList)), int(utils.DefaultNq))
	for i := 0; i < utils.DefaultNq; i++ {
		assert.Equal(t, len(res.QueryResultList[i].Ids), utils.DefaultTopk)
	}
}
