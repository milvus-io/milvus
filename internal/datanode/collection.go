package datanode

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type Collection struct {
	schema *schemapb.CollectionSchema
	id     UniqueID
}

func (c *Collection) Name() string {
	return c.schema.Name
}

func (c *Collection) ID() UniqueID {
	return c.id
}

func (c *Collection) Schema() *schemapb.CollectionSchema {
	return c.schema
}

func newCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) *Collection {
	var newCollection = &Collection{
		schema: schema,
		id:     collectionID,
	}
	return newCollection
}
