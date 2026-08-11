package typeutil

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type FieldSchemaHelper struct {
	schema      *schemapb.FieldSchema
	typeParams  *kvPairsHelper[string, string]
	indexParams *kvPairsHelper[string, string]
}

func (h *FieldSchemaHelper) GetDim() (int64, error) {
	if !IsVectorType(h.schema.GetDataType()) {
		return 0, merr.WrapErrParameterInvalidMsg("%s is not of vector type", h.schema.GetDataType())
	}
	if IsSparseFloatVectorType(h.schema.GetDataType()) {
		return 0, merr.WrapErrParameterInvalidMsg("typeutil.GetDim should not invoke on sparse vector type")
	}

	getDim := func(kvPairs *kvPairsHelper[string, string]) (int64, error) {
		dimStr, err := kvPairs.Get(common.DimKey)
		if err != nil {
			return 0, merr.WrapErrParameterInvalidMsg("dim not found")
		}
		dim, err := strconv.Atoi(dimStr)
		if err != nil {
			return 0, merr.WrapErrParameterInvalidMsg("invalid dimension: %s", dimStr)
		}
		return int64(dim), nil
	}

	if dim, err := getDim(h.typeParams); err == nil {
		return dim, nil
	}

	return getDim(h.indexParams)
}

func (h *FieldSchemaHelper) EnableMatch() bool {
	if !IsStringType(h.schema.GetDataType()) {
		return false
	}
	s, err := h.typeParams.Get("enable_match")
	if err != nil {
		return false
	}
	enable, err := strconv.ParseBool(s)
	return err == nil && enable
}

func (h *FieldSchemaHelper) EnableJSONKeyStatsIndex() bool {
	return IsJSONType(h.schema.GetDataType())
}

func (h *FieldSchemaHelper) EnableAnalyzer() bool {
	if !IsStringType(h.schema.GetDataType()) {
		return false
	}
	s, err := h.typeParams.Get("enable_analyzer")
	if err != nil {
		return false
	}
	enable, err := strconv.ParseBool(s)
	return err == nil && enable
}

func (h *FieldSchemaHelper) GetMultiAnalyzerParams() (string, bool) {
	if !h.EnableAnalyzer() {
		return "", false
	}
	value, err := h.typeParams.Get("multi_analyzer_params")
	return value, err == nil
}

func (h *FieldSchemaHelper) HasAnalyzerParams() bool {
	_, err := h.typeParams.Get("analyzer_params")
	return err == nil
}

func CreateFieldSchemaHelper(schema *schemapb.FieldSchema) *FieldSchemaHelper {
	return &FieldSchemaHelper{
		schema:      schema,
		typeParams:  NewKvPairs(schema.GetTypeParams()),
		indexParams: NewKvPairs(schema.GetIndexParams()),
	}
}

// validateTypeSchemaNode validates the recursive encoding of a TypeSchema
// node. Parameter validation is handled by callers' Array schema validators.
func validateTypeSchemaNode(fieldName string, typeSchema *schemapb.TypeSchema) error {
	if typeSchema == nil {
		return merr.WrapErrParameterInvalidMsg(
			"type_schema kind should be specified for field %s", fieldName)
	}

	switch kind := typeSchema.GetKind().(type) {
	case *schemapb.TypeSchema_ArrayElement:
		if kind.ArrayElement == nil {
			return merr.WrapErrParameterInvalidMsg(
				"type_schema array_element should be specified for field %s", fieldName)
		}
		return validateTypeSchemaNode(fieldName, kind.ArrayElement)
	case *schemapb.TypeSchema_LeafType:
		if _, ok := schemapb.DataType_name[int32(kind.LeafType)]; !ok || kind.LeafType == schemapb.DataType_None {
			return merr.WrapErrParameterInvalidMsg(
				"type_schema leaf_type %s is not valid for field %s",
				kind.LeafType.String(), fieldName)
		}
		if kind.LeafType == schemapb.DataType_Array {
			return merr.WrapErrParameterInvalidMsg(
				"type_schema leaf_type Array must use array_element for field %s", fieldName)
		}
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg(
			"type_schema kind should be specified for field %s", fieldName)
	}
}

// IsNestedArrayTypeSchema reports whether typeSchema describes an Array whose
// direct element is another Array.
func IsNestedArrayTypeSchema(typeSchema *schemapb.TypeSchema) bool {
	if typeSchema == nil {
		return false
	}
	elementSchema := typeSchema.GetArrayElement()
	if elementSchema == nil {
		return false
	}
	_, ok := elementSchema.GetKind().(*schemapb.TypeSchema_ArrayElement)
	return ok
}

// ValidateFieldTypeSchema validates the wire representation of nested Arrays.
// Non-nested fields use only data_type/element_type. Nested Arrays use
// data_type=Array, element_type=Array, and a recursive type_schema.
func ValidateFieldTypeSchema(field *schemapb.FieldSchema) error {
	typeSchema := field.GetTypeSchema()
	if typeSchema == nil {
		if field.GetDataType() == schemapb.DataType_Array &&
			field.GetElementType() == schemapb.DataType_Array {
			return merr.WrapErrParameterInvalidMsg(
				"element type Array is not supported without type_schema; nested array field %s must specify type_schema",
				field.GetName())
		}
		return nil
	}

	if err := validateTypeSchemaNode(field.GetName(), typeSchema); err != nil {
		return err
	}
	if !IsNestedArrayTypeSchema(typeSchema) {
		return merr.WrapErrParameterInvalidMsg(
			"type_schema is only supported for nested array field %s",
			field.GetName())
	}
	if field.GetDataType() != schemapb.DataType_Array ||
		field.GetElementType() != schemapb.DataType_Array {
		return merr.WrapErrParameterInvalidMsg(
			"nested array field %s must specify data_type Array and element_type Array",
			field.GetName())
	}

	return nil
}
