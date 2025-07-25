package metacache

import "github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

// SchemaManager is a manager for collection schema.
type SchemaManager interface {
	// GetSchema returns the schema at the given timetick
	GetSchema(timetick uint64) *schemapb.CollectionSchema
}

// newVersionlessSchemaManager creates a new versionless schema manager.
func newVersionlessSchemaManager(schema *schemapb.CollectionSchema) SchemaManager {
	return &versionlessSchemaManager{schema: schema}
}

type versionlessSchemaManager struct {
	schema *schemapb.CollectionSchema
}

func (m *versionlessSchemaManager) GetSchema(timetick uint64) *schemapb.CollectionSchema {
	return m.schema
}
