package reader

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/conf"
)

func TestQueryNode_CreateQueryNode(t *testing.T) {
	conf.LoadConfig("config.yaml")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := CreateQueryNode(ctx, 0, 0, nil)
	assert.NotNil(t, node)
}

func TestQueryNode_NewQueryNode(t *testing.T) {
	conf.LoadConfig("config.yaml")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := NewQueryNode(ctx, 0, 0)
	assert.NotNil(t, node)
}

func TestQueryNode_Close(t *testing.T) {
	conf.LoadConfig("config.yaml")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := CreateQueryNode(ctx, 0, 0, nil)
	assert.NotNil(t, node)

	node.Close()
}

func TestQueryNode_QueryNodeDataInit(t *testing.T) {
	conf.LoadConfig("config.yaml")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := CreateQueryNode(ctx, 0, 0, nil)
	assert.NotNil(t, node)

	node.QueryNodeDataInit()

	assert.NotNil(t, node.deletePreprocessData)
	assert.NotNil(t, node.insertData)
	assert.NotNil(t, node.deleteData)
}

func TestQueryNode_NewCollection(t *testing.T) {
	conf.LoadConfig("config.yaml")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := CreateQueryNode(ctx, 0, 0, nil)
	assert.NotNil(t, node)

	var collection = node.NewCollection(0, "collection0", "")

	assert.Equal(t, collection.CollectionName, "collection0")
	assert.Equal(t, len(node.Collections), 1)
}

func TestQueryNode_DeleteCollection(t *testing.T) {
	conf.LoadConfig("config.yaml")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := CreateQueryNode(ctx, 0, 0, nil)
	assert.NotNil(t, node)

	var collection = node.NewCollection(0, "collection0", "")

	assert.Equal(t, collection.CollectionName, "collection0")
	assert.Equal(t, len(node.Collections), 1)

	node.DeleteCollection(collection)

	assert.Equal(t, len(node.Collections), 0)
}
