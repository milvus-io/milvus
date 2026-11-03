package schemautil

import (
	"encoding/json"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SchemaInfo is a helper function wraps *schemapb.CollectionSchema
// with extra fields mapping and methods
type SchemaInfo struct {
	*schemapb.CollectionSchema
	FieldMap              *typeutil.ConcurrentMap[string, int64] // field name to id mapping
	HasPartitionKeyField  bool
	PkField               *schemapb.FieldSchema
	MultiAnalyzerFieldMap *typeutil.ConcurrentMap[int64, int64] // multi analzyer field id to dependent field id mapping
	SchemaHelper          *typeutil.SchemaHelper
}

func NewSchemaInfo(schema *schemapb.CollectionSchema) (*SchemaInfo, error) {
	fieldMap := typeutil.NewConcurrentMap[string, int64]()
	hasPartitionkey := false
	var pkField *schemapb.FieldSchema
	for _, field := range schema.GetFields() {
		fieldMap.Insert(field.GetName(), field.GetFieldID())
		if field.GetIsPartitionKey() {
			hasPartitionkey = true
		}
		if field.GetIsPrimaryKey() {
			pkField = field
		}
	}
	for _, structField := range schema.GetStructArrayFields() {
		fieldMap.Insert(structField.GetName(), structField.GetFieldID())
		for _, field := range structField.GetFields() {
			fieldMap.Insert(field.GetName(), field.GetFieldID())
		}
	}
	// skip load fields logic for now
	// partial load shall be processed as hint after tiered storage feature
	// A malformed Schema (nil, duplicate field name/id, multiple primary keys)
	// only reaches here on a RootCoord bug, so this is defensive; propagate it as
	// a retriable internal error rather than leaving SchemaHelper nil for a later
	// dereference to panic on.
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to create Schema helper")
	}
	return &SchemaInfo{
		CollectionSchema:      schema,
		FieldMap:              fieldMap,
		HasPartitionKeyField:  hasPartitionkey,
		PkField:               pkField,
		MultiAnalyzerFieldMap: typeutil.NewConcurrentMap[int64, int64](),
		SchemaHelper:          schemaHelper,
	}, nil
}

func (s *SchemaInfo) MapFieldID(name string) (int64, bool) {
	return s.FieldMap.Get(name)
}

func (s *SchemaInfo) IsPartitionKeyCollection() bool {
	return s.HasPartitionKeyField
}

func (s *SchemaInfo) GetPkField() (*schemapb.FieldSchema, error) {
	if s.PkField == nil {
		return nil, merr.WrapErrFieldNotFound("pk field")
	}
	return s.PkField, nil
}

func (s *SchemaInfo) GetMultiAnalyzerNameFieldID(id int64) (int64, error) {
	if id, ok := s.MultiAnalyzerFieldMap.Get(id); ok {
		return id, nil
	}

	field, err := s.SchemaHelper.GetFieldFromID(id)
	if err != nil {
		return 0, err
	}

	helper := typeutil.CreateFieldSchemaHelper(field)

	params, ok := helper.GetMultiAnalyzerParams()
	if !ok {
		s.MultiAnalyzerFieldMap.Insert(id, 0)
		return 0, nil
	}

	var raw map[string]json.RawMessage
	err = json.Unmarshal([]byte(params), &raw)
	if err != nil {
		return 0, err
	}

	jsonFieldID, ok := raw["by_field"]
	if !ok {
		return 0, merr.WrapErrServiceInternal("multi_analyzer_params missing required 'by_field' key")
	}
	var analyzerFieldName string
	err = json.Unmarshal(jsonFieldID, &analyzerFieldName)
	if err != nil {
		return 0, err
	}
	analyzerField, err := s.SchemaHelper.GetFieldFromName(analyzerFieldName)
	if err != nil {
		return 0, err
	}

	s.MultiAnalyzerFieldMap.Insert(id, analyzerField.GetFieldID())
	return analyzerField.GetFieldID(), nil
}

// GetLoadFieldIDs returns field id for load field list.
// If input `loadFields` is empty, use collection Schema definition.
// Otherwise, perform load field list constraint check then return field id.
func (s *SchemaInfo) GetLoadFieldIDs(loadFields []string, skipDynamicField bool) ([]int64, error) {
	if len(loadFields) == 0 {
		// skip check logic since create collection already did the rule check already
		return common.GetCollectionLoadFields(s.CollectionSchema, skipDynamicField), nil
	}

	fieldIDs := typeutil.NewSet[int64]()
	// fieldIDs := make([]int64, 0, len(loadFields))
	fields := make([]*schemapb.FieldSchema, 0, len(loadFields))
	for _, name := range loadFields {
		// todo(SpadeA): check struct field
		if structArrayField := s.SchemaHelper.GetStructArrayFieldFromName(name); structArrayField != nil {
			for _, field := range structArrayField.GetFields() {
				fields = append(fields, field)
				fieldIDs.Insert(field.GetFieldID())
			}
			continue
		}

		fieldSchema, err := s.SchemaHelper.GetFieldFromName(name)
		if err != nil {
			return nil, err
		}

		fields = append(fields, fieldSchema)
		fieldIDs.Insert(fieldSchema.GetFieldID())
	}

	// only append dynamic field when skipFlag == false
	if !skipDynamicField {
		// find dynamic field
		dynamicField := lo.FindOrElse(s.Fields, nil, func(field *schemapb.FieldSchema) bool {
			return field.IsDynamic
		})

		// if dynamic field not nil
		if dynamicField != nil {
			fieldIDs.Insert(dynamicField.GetFieldID())
			fields = append(fields, dynamicField)
		}
	}

	// validate load fields list
	if err := s.validateLoadFields(loadFields, fields); err != nil {
		return nil, err
	}

	return fieldIDs.Collect(), nil
}

func (s *SchemaInfo) validateLoadFields(names []string, fields []*schemapb.FieldSchema) error {
	// ignore error if not found
	partitionKeyField, _ := s.SchemaHelper.GetPartitionKeyField()
	clusteringKeyField, _ := s.SchemaHelper.GetClusteringKeyField()

	var hasPrimaryKey, hasPartitionKey, hasClusteringKey, hasVector bool
	for _, field := range fields {
		if field.GetFieldID() == s.PkField.GetFieldID() {
			hasPrimaryKey = true
		}
		if typeutil.IsVectorType(field.GetDataType()) {
			hasVector = true
		}
		if field.IsPartitionKey {
			hasPartitionKey = true
		}
		if field.IsClusteringKey {
			hasClusteringKey = true
		}
	}

	if !hasPrimaryKey {
		return merr.WrapErrParameterInvalidMsg("load field list %v does not contain primary key field %s", names, s.PkField.GetName())
	}
	if !hasVector {
		return merr.WrapErrParameterInvalidMsg("load field list %v does not contain vector field", names)
	}
	if partitionKeyField != nil && !hasPartitionKey {
		return merr.WrapErrParameterInvalidMsg("load field list %v does not contain partition key field %s", names, partitionKeyField.GetName())
	}
	if clusteringKeyField != nil && !hasClusteringKey {
		return merr.WrapErrParameterInvalidMsg("load field list %v does not contain clustering key field %s", names, clusteringKeyField.GetName())
	}
	return nil
}

func (s *SchemaInfo) CanRetrieveRawFieldData(field *schemapb.FieldSchema) bool {
	return s.SchemaHelper.CanRetrieveRawFieldData(field)
}

// ShallowSchema copies the protobuf envelope. Nested nodes remain immutable.
func ShallowSchema(s *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	copy := &schemapb.CollectionSchema{
		Name: s.Name, Description: s.Description, AutoID: s.AutoID,
		Fields: s.Fields, StructArrayFields: s.StructArrayFields, Functions: s.Functions,
		EnableDynamicField: s.EnableDynamicField, EnableNamespace: s.EnableNamespace,
		Properties: s.Properties, DbName: s.DbName, Version: s.Version,
		FileResourceIds: s.FileResourceIds, ExternalSource: s.ExternalSource, ExternalSpec: s.ExternalSpec,
		DoPhysicalBackfill: s.DoPhysicalBackfill,
	}
	copy.ProtoReflect().SetUnknown(s.ProtoReflect().GetUnknown())
	return copy
}
