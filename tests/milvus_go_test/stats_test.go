package main

import (
	"encoding/json"
	"fmt"
	"milvus_go_test/utils"
	"testing"

	"github.com/milvus-io/milvus-sdk-go/milvus"
	"github.com/stretchr/testify/assert"
)

var nb = utils.DefaultNb

func TestStats(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	insertParam := milvus.InsertParam{
		name,
		GenDefaultFieldValues(milvus.VECTORFLOAT),
		nil,
		""}
	ids, _, _ := client.Insert(insertParam)
	assert.Equal(t, len(ids), utils.DefaultNb)
	client.Flush([]string{name})
	stats, status, _ := client.GetCollectionStats(name)
	assert.True(t, status.Ok())
	var dat map[string]interface{}
	json.Unmarshal([]byte(stats), &dat)
	assert.Equal(t, nb, int(dat["row_count"].(float64)))
}

func TestStatsCollectionNotExisted(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	_, status, error := client.GetCollectionStats(name + "_")
	assert.False(t, status.Ok())
	t.Log(error)
}

func TestStatsAfterDeleteEntities(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	insertParam := milvus.InsertParam{
		CollectionName: name,
		Fields:         GenDefaultFieldValues(milvus.VECTORFLOAT)}
	ids, _, _ := client.Insert(insertParam)
	assert.Equal(t, len(ids), utils.DefaultNb)
	client.Flush([]string{name})
	client.DeleteEntityByID(name, ids[:nb/2])
	client.Flush([]string{name})
	stats, status, _ := client.GetCollectionStats(name)
	fmt.Println(stats)
	assert.True(t, status.Ok())
	var dat map[string]interface{}
	json.Unmarshal([]byte(stats), &dat)
	assert.Equal(t, nb/2, int(dat["row_count"].(float64)))
}

func TestStatsAfterIndex(t *testing.T) {
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
	stats, status, _ := client.GetCollectionStats(name)
	fmt.Println(stats)
	assert.True(t, status.Ok())
	assert.True(t, AssertIndexFromStats(stats, "BIN_IVF_FLAT", utils.DefaultFieldBinaryVectorName))
}

func TestStatsEmptyCollection(t *testing.T) {
	client, name := Collection(true, milvus.VECTORFLOAT)
	stats, status, _ := client.GetCollectionStats(name)
	assert.True(t, status.Ok())
	fmt.Println(stats)
	var statsMap map[string]interface{}
	json.Unmarshal([]byte(stats), &statsMap)
	assert.Equal(t, 0, int(statsMap["data_size"].(float64)))
	assert.Nil(t, statsMap["partitions"].([]interface{})[0].(map[string]interface{})["segments"])
}

func AssertIndexFromStats(stats string, indexType string, fieldName string) bool {
	var statsMap map[string]interface{}
	json.Unmarshal([]byte(stats), &statsMap)
	segments := statsMap["partitions"].([]interface{})[0].(map[string]interface{})["segments"]
	for _, segment := range segments.([]interface{}) {
		for k, v := range segment.(map[string]interface{}) {
			if k == "files" {
				for _, file := range v.([]interface{}){
					fmt.Println(file)
					file := file.(map[string]interface{})
					if file["field"] == fieldName && file["name"] != "_raw" {
						return indexType == file["index_type"]
					}
				}
			}
		}
	}
	return false
}
