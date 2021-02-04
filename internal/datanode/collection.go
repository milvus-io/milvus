package datanode

import (
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type Collection struct {
	schema *schemapb.CollectionSchema
	id     UniqueID
}

func (c *Collection) GetName() string {
	if c.schema == nil {
		return ""
	}
	return c.schema.Name
}

func (c *Collection) GetID() UniqueID {
	return c.id
}

func (c *Collection) GetSchema() *schemapb.CollectionSchema {
	return c.schema
}

func newCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) (*Collection, error) {
	if schema == nil {
		return nil, errors.Errorf("Invalid schema")
	}

	var newCollection = &Collection{
		schema: schema,
		id:     collectionID,
	}
	return newCollection, nil
}
