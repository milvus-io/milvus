package importutilv2

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// BuildDeleteKeySchema projects a collection schema down to its primary key field only.
// A reader built from the result reads just the primary key column and ignores every other
// column in the file.
//
// AutoID is cleared because isSchemaEqual skips auto primary key fields when validating a
// file against a schema; leaving it set would make the primary key column silently unread.
func BuildDeleteKeySchema(schema *schemapb.CollectionSchema) (*schemapb.CollectionSchema, error) {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	projected := typeutil.Clone(pkField)
	projected.AutoID = false
	projected.Nullable = false
	projected.DefaultValue = nil
	return &schemapb.CollectionSchema{
		Name:        schema.GetName(),
		Description: schema.GetDescription(),
		Fields:      []*schemapb.FieldSchema{projected},
	}, nil
}
