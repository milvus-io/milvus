package datacoord

import (
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// importSchemaProjection keeps only schema facts that can change Import V3's
// physical output, function execution, or the fields consumed by current
// indexes. Presentation-only names/descriptions and schema version are omitted.
type importSchemaProjection struct {
	Fields          []*schemapb.FieldSchema
	StructFields    []*schemapb.StructArrayFieldSchema
	Functions       []*schemapb.FunctionSchema
	Properties      []*commonpb.KeyValuePair
	IndexedFieldIDs []int64
	EnableDynamic   bool
	EnableNamespace bool
	FileResourceIDs []int64
}

// The projection is compared through a deterministic, ordinary Go value rather
// than adding another wire protocol.  Clone helpers below strip display-only
// fields before proto.Equal is used on the nested schema messages.
func compareImportSchemaProjection(frozen, current *schemapb.CollectionSchema, indexes []*model.Index) (bool, string) {
	if frozen == nil || current == nil {
		return false, "schema is missing"
	}
	frozenProjection := buildImportSchemaProjection(frozen, indexes)
	currentProjection := buildImportSchemaProjection(current, indexes)
	if frozenProjection.equal(currentProjection) {
		return true, ""
	}
	return false, fmt.Sprintf("frozen schema version %d is not import-compatible with current version %d", frozen.GetVersion(), current.GetVersion())
}

func buildImportSchemaProjection(schema *schemapb.CollectionSchema, indexes []*model.Index) *importSchemaProjection {
	projection := &importSchemaProjection{
		EnableDynamic:   schema.GetEnableDynamicField(),
		EnableNamespace: schema.GetEnableNamespace(),
		FileResourceIDs: append([]int64(nil), schema.GetFileResourceIds()...),
	}
	for _, field := range schema.GetFields() {
		cloned := proto.Clone(field).(*schemapb.FieldSchema)
		cloned.Name = ""
		cloned.Description = ""
		cloned.IndexParams = nil
		projection.Fields = append(projection.Fields, cloned)
	}
	for _, field := range schema.GetStructArrayFields() {
		cloned := proto.Clone(field).(*schemapb.StructArrayFieldSchema)
		cloned.Name = ""
		cloned.Description = ""
		for _, nested := range cloned.GetFields() {
			nested.Name = ""
			nested.Description = ""
			nested.IndexParams = nil
		}
		projection.StructFields = append(projection.StructFields, cloned)
	}
	for _, function := range schema.GetFunctions() {
		cloned := proto.Clone(function).(*schemapb.FunctionSchema)
		cloned.Name = ""
		cloned.Description = ""
		cloned.InputFieldNames = nil
		cloned.OutputFieldNames = nil
		projection.Functions = append(projection.Functions, cloned)
	}
	for _, property := range schema.GetProperties() {
		switch property.GetKey() {
		case common.CollectionTTLConfigKey,
			common.CollectionTTLFieldKey,
			common.TimezoneKey,
			common.CollectionAllowInsertNonBM25FunctionOutputs,
			common.NamespaceModeKey,
			common.NamespaceShardingEnabledKey:
			projection.Properties = append(projection.Properties, proto.Clone(property).(*commonpb.KeyValuePair))
		}
	}
	for _, index := range indexes {
		if index != nil && !index.IsDeleted {
			projection.IndexedFieldIDs = append(projection.IndexedFieldIDs, index.FieldID)
		}
	}
	sort.Slice(projection.Fields, func(i, j int) bool { return projection.Fields[i].GetFieldID() < projection.Fields[j].GetFieldID() })
	sort.Slice(projection.StructFields, func(i, j int) bool {
		return projection.StructFields[i].GetFieldID() < projection.StructFields[j].GetFieldID()
	})
	sort.Slice(projection.Functions, func(i, j int) bool { return projection.Functions[i].GetId() < projection.Functions[j].GetId() })
	sort.Slice(projection.Properties, func(i, j int) bool { return projection.Properties[i].GetKey() < projection.Properties[j].GetKey() })
	sort.Slice(projection.IndexedFieldIDs, func(i, j int) bool { return projection.IndexedFieldIDs[i] < projection.IndexedFieldIDs[j] })
	sort.Slice(projection.FileResourceIDs, func(i, j int) bool { return projection.FileResourceIDs[i] < projection.FileResourceIDs[j] })
	return projection
}

func (p *importSchemaProjection) equal(other *importSchemaProjection) bool {
	if p == nil || other == nil || p.EnableDynamic != other.EnableDynamic || p.EnableNamespace != other.EnableNamespace {
		return false
	}
	if len(p.Fields) != len(other.Fields) || len(p.StructFields) != len(other.StructFields) || len(p.Functions) != len(other.Functions) ||
		len(p.Properties) != len(other.Properties) || len(p.IndexedFieldIDs) != len(other.IndexedFieldIDs) || len(p.FileResourceIDs) != len(other.FileResourceIDs) {
		return false
	}
	for i := range p.Fields {
		if !proto.Equal(p.Fields[i], other.Fields[i]) {
			return false
		}
	}
	for i := range p.StructFields {
		if !proto.Equal(p.StructFields[i], other.StructFields[i]) {
			return false
		}
	}
	for i := range p.Functions {
		if !proto.Equal(p.Functions[i], other.Functions[i]) {
			return false
		}
	}
	for i := range p.Properties {
		if !proto.Equal(p.Properties[i], other.Properties[i]) {
			return false
		}
	}
	for i := range p.IndexedFieldIDs {
		if p.IndexedFieldIDs[i] != other.IndexedFieldIDs[i] {
			return false
		}
	}
	for i := range p.FileResourceIDs {
		if p.FileResourceIDs[i] != other.FileResourceIDs[i] {
			return false
		}
	}
	return true
}

func validateImportV3Schema(meta *meta, collectionID int64, frozen *schemapb.CollectionSchema) (int32, error) {
	collection := meta.GetCollection(collectionID)
	if collection == nil || collection.Schema == nil {
		return 0, merr.WrapErrImportSysFailedMsg("import v3 collection schema is unavailable")
	}
	indexes := meta.indexMeta.GetIndexesForCollection(collectionID, "")
	if equal, difference := compareImportSchemaProjection(frozen, collection.Schema, indexes); !equal {
		return 0, merr.WrapErrImportSysFailedMsg("import v3 schema projection mismatch: %s", difference)
	}
	return collection.Schema.GetVersion(), nil
}
