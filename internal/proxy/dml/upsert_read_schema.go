package dml

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// upsertReadSchema selects the published schema for the read-before-write stage.
// The mutation still uses the WAL write schema, which can be ahead of publication.
func (t *UpsertTask) upsertReadSchema(ctx context.Context, outputFields []string) (*metacache.SchemaInfo, error) {
	info, err := t.GetMetaCache().GetCollectionInfo(ctx, t.req.GetDbName(), t.collectionName, t.collectionID)
	if err != nil {
		return nil, err
	}
	if info.CollID != t.collectionID {
		return nil, merr.WrapErrCollectionNotFound(t.collectionID)
	}
	readSchema := info.Schema
	if readSchema.GetVersion() == t.schema.GetVersion() {
		return readSchema, nil
	}
	if t.req.GetPartialUpdate() {
		// A wildcard read supplies every inherited value. Require the same input
		// columns, including StructArray children and dynamic-field identity;
		// otherwise absent data could silently become NULL/default during merge.
		// Function outputs are regenerated from the merged inputs.
		if !proto.Equal(upsertInputSchema(readSchema.CollectionSchema), upsertInputSchema(t.schema.CollectionSchema)) {
			return nil, merr.WrapErrServiceUnavailableMsg("partial upsert awaits publication of changed input fields")
		}
	} else {
		// Full AutoID/RLS upserts only retrieve their explicit lookup dependencies.
		for _, name := range outputFields {
			writeField, err := t.schema.SchemaHelper.GetFieldFromName(name)
			if err != nil {
				return nil, err
			}
			readField, err := readSchema.SchemaHelper.GetFieldFromName(name)
			if err != nil || !proto.Equal(readField, writeField) {
				return nil, merr.WrapErrServiceUnavailableMsg("upsert awaits publication of lookup field %s", name)
			}
		}
	}
	return readSchema, nil
}

func upsertInputSchema(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	fields := make([]*schemapb.FieldSchema, 0, len(schema.GetFields()))
	for _, field := range schema.GetFields() {
		if !field.GetIsFunctionOutput() {
			fields = append(fields, field)
		}
	}
	return &schemapb.CollectionSchema{
		Fields: fields, StructArrayFields: schema.GetStructArrayFields(),
		EnableDynamicField: schema.GetEnableDynamicField(),
	}
}
