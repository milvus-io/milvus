package qviews

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ResolveRequiredFields normalizes historical LoadInfo snapshots which predate
// required_fields. Callers supply the exact View schema, not the latest schema.
func ResolveRequiredFields(schema *schemapb.CollectionSchema, fields []*viewpb.LoadFieldConfig) []*viewpb.LoadFieldConfig {
	if len(fields) != 0 {
		return fields
	}
	out := make([]*viewpb.LoadFieldConfig, 0)
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if field.GetFieldID() >= common.StartOfUserFieldID {
			out = append(out, &viewpb.LoadFieldConfig{FieldId: field.GetFieldID()})
		}
	}
	return out
}
