package writenode

import (
	"log"

	"github.com/golang/protobuf/proto"
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

func newCollection(collectionID UniqueID, schemaStr string) *Collection {

	var schema schemapb.CollectionSchema
	err := proto.UnmarshalText(schemaStr, &schema)
	if err != nil {
		log.Println(err)
		return nil
	}

	var newCollection = &Collection{
		schema: &schema,
		id:     collectionID,
	}
	return newCollection
}
