package main

import (
	"fmt"
	"github.com/milvus-io/milvus-sdk-go/milvus"
	"github.com/stretchr/testify/assert"
	"milvus_go_test/utils"
	"os"
	"testing"
)

func CreatePartition(tag string) (milvus.MilvusClient, milvus.PartitionParam, string) {
	client, name := Collection(autoId, milvus.VECTORFLOAT)
	pp := milvus.PartitionParam{CollectionName: name, PartitionTag: tag}
	status, _ := client.CreatePartition(pp)
	if !status.Ok() {
		fmt.Println(status)
		os.Exit(-2)
	}
	return client, pp, name
}

func TestCreatePartition(t *testing.T)  {
	client, name := Collection(autoId, milvus.VECTORFLOAT)
	tag := utils.RandString(8)
	pp := milvus.PartitionParam{CollectionName: name, PartitionTag: tag}
	status, _ := client.CreatePartition(pp)
	assert.True(t, status.Ok())
	partitions, _, _ := client.ListPartitions(name)
	assert.Contains(t, partitions, pp)
}

func TestCreatePartitionNameExisted(t *testing.T)  {
	client, name := Collection(autoId, milvus.VECTORFLOAT)
	tag := utils.RandString(8)
	pp := milvus.PartitionParam{CollectionName: name, PartitionTag: tag}
	client.CreatePartition(pp)
	status, error := client.CreatePartition(pp)
	t.Log(error)
	assert.False(t, status.Ok())
}

func TestHasPartitionTagNameNotExisted(t *testing.T)  {
	client, name := Collection(autoId, milvus.VECTORFLOAT)
	tag := utils.RandString(8)
	pp := milvus.PartitionParam{CollectionName: name, PartitionTag: tag}
	isHas, status, _ := client.HasPartition(pp)
	t.Log(status)
	assert.True(t, status.Ok())
	assert.False(t, isHas)
}

func TestHasPartition(t *testing.T)  {
	tag := utils.RandString(8)
	client, pp, _ := CreatePartition(tag)
	isHas, status, _ := client.HasPartition(pp)
	t.Log(status)
	assert.True(t, status.Ok())
	assert.True(t, isHas)
}


func TestDropPartition(t *testing.T)  {
	tag := utils.RandString(8)
	client, pp, name := CreatePartition(tag)
	status, _ := client.DropPartition(pp)
	assert.True(t, status.Ok())
	partitions, _, _ := client.ListPartitions(name)
	t.Log(partitions)
	assert.NotContains(t, partitions, pp)
}


func TestDropPartitionAfterInsert(t *testing.T)  {
	tag := utils.RandString(8)
	client, pp, name := CreatePartition(tag)
	// insert
	insertParam := milvus.InsertParam{
		CollectionName: name,
		Fields:         GenDefaultFieldValues(milvus.VECTORFLOAT),
		IDArray: nil,
	    PartitionTag: tag}
	ids, _, _ := client.Insert(insertParam)
	assert.Equal(t, len(ids), utils.DefaultNb)
	client.Flush([]string{name})
	client.DropPartition(pp)
	count, _, _ := client.CountEntities(name)
	assert.Equal(t, 0, int(count))
	//t.Log(client.GetCollectionStats(name))
}

func TestDropPartitionDefault(t *testing.T)  {
	client, name := Collection(autoId, milvus.VECTORFLOAT)
	tag := "_default"
	pp := milvus.PartitionParam{CollectionName: name, PartitionTag: tag}
	status, _ := client.DropPartition(pp)
	t.Log(status)
	assert.False(t, status.Ok())
}

func TestDropPartitionRepeat(t *testing.T)  {
	tag := utils.RandString(8)
	client, pp, _ := CreatePartition(tag)
	client.DropPartition(pp)
	status, _ := client.DropPartition(pp)
	t.Log(status)
	assert.False(t, status.Ok())
}

func TestShowPartitionsMulti(t *testing.T)  {
	tag := utils.RandString(8)
	tagTwo := utils.RandString(8)
	client, pp, name := CreatePartition(tag)
	ppTwo := milvus.PartitionParam{CollectionName: name, PartitionTag: tagTwo}
	status, _ := client.CreatePartition(ppTwo)
	t.Log(status)
	partitions, _, _ := client.ListPartitions(name)
	assert.Contains(t, partitions, pp)
	assert.Contains(t, partitions, ppTwo)
}


