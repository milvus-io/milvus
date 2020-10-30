package main
//
//import (
//	"milvus_go_test/utils"
//	"testing"
//
//	"github.com/milvus-io/milvus-sdk-go/milvus"
//	"github.com/stretchr/testify/assert"
//)
//
//func TestGet(t *testing.T) {
//	client, name := Collection(true, milvus.VECTORFLOAT)
//	insertParam := milvus.InsertParam{
//		name,
//		GenDefaultFieldValues(milvus.VECTORFLOAT),
//		nil,
//		""}
//	ids, status, _ := client.Insert(insertParam)
//	assert.Equal(t, status.Ok(), true)
//	assert.Equal(t, len(ids), utils.DefaultNb)
//	getIds := []int64{ids[0]}
//	entities, status, _ := client.GetEntityByID(name, getIds)
//	t.Log(status)
//	t.Log(entities)
//	assert.Equal(t, status.Ok(), true)
//}
//
//func TestGetEmptyCollection(t *testing.T) {
//	client, name := Collection(true, milvus.VECTORFLOAT)
//	getIds := []int64{0}
//	entities, status, _ := client.GetEntityByID(name, getIds)
//	t.Log(status)
//	t.Log(entities)
//	assert.Equal(t, status.Ok(), false)
//}
//
//func TestGetCollectionNotExisted(t *testing.T) {
//	client, _ := Collection(true, milvus.VECTORFLOAT)
//	_name := utils.RandString(8)
//	getIds := []int64{0}
//	entities, status, _ := client.GetEntityByID(_name, getIds)
//	t.Log(status)
//	t.Log(entities)
//	assert.Equal(t, status.Ok(), false)
//}
