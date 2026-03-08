package metacache

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestVersionlessSchemaManager(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test",
	}

	manager := newVersionlessSchemaManager(schema)
	assert.Equal(t, schema, manager.GetSchema(0))
}
