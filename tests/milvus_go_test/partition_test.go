package main

import (
	"github.com/milvus-io/milvus-sdk-go/milvus"
	"github.com/stretchr/testify/assert"
	"milvus_go_test/utils"
	"testing"
)

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
	
}

func TestHasPartitionTagNameExisted(t *testing.T)  {
	
}

func TestDropPartition(t *testing.T)  {
	client, name := Collection(autoId, milvus.VECTORFLOAT)
	tag := utils.RandString(8)
	pp := milvus.PartitionParam{CollectionName: name, PartitionTag: tag}
	client.CreatePartition(pp)
	status, _ := client.DropPartition(pp)
	assert.True(t, status.Ok())
	partitions, _, _ := client.ListPartitions(name)
	t.Log(partitions)
	assert.NotContains(t, partitions, pp)
}

func TestDropPartitionAfterInsert(t *testing.T)  {
	client, name := Collection(autoId, milvus.VECTORFLOAT)
	tag := utils.RandString(8)
	pp := milvus.PartitionParam{CollectionName: name, PartitionTag: tag}
	client.CreatePartition(pp)
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
	t.Log(client.GetCollectionInfo(name))
}
