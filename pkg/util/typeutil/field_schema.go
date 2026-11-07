package typeutil

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
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

// CheckDupKvPairs rejects duplicate keys in a key-value pair list.
func CheckDupKvPairs(params []*commonpb.KeyValuePair, paramType string) error {
	seen := make(map[string]struct{}, len(params))
	for _, param := range params {
		key := param.GetKey()
		if _, ok := seen[key]; ok {
			return merr.WrapErrParameterInvalidMsg(
				"duplicated %s param key %q", paramType, key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// ValidateArrayElementType validates a scalar type supported by ARRAY fields.
func ValidateArrayElementType(dataType schemapb.DataType) error {
	switch dataType {
	case schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_VarChar:
		return nil
	case schemapb.DataType_String:
		return merr.WrapErrParameterInvalidMsg(
			"string data type not supported yet, please use VarChar type instead")
	case schemapb.DataType_None:
		return merr.WrapErrParameterInvalidMsg(
			"element data type None is not valid")
	default:
		return merr.WrapErrParameterInvalidMsg(
			"element type %s is not supported", dataType.String())
	}
}

// validateTypeSchemaNode validates the recursive encoding of a TypeSchema
// node, including the parameter key set owned by this node.
func validateTypeSchemaNode(fieldName string, typeSchema *schemapb.TypeSchema) error {
	if typeSchema == nil {
		return merr.WrapErrParameterInvalidMsg(
			"type_schema kind should be specified for field %s", fieldName)
	}
	if err := CheckDupKvPairs(typeSchema.GetTypeParams(), "type_schema"); err != nil {
		return err
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
		return ValidateArrayElementType(kind.LeafType)
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

// GetArrayLeaf walks the type chain of one Array column: from typeSchema down,
// every node is an array_element until a leaf_type node. It returns the leaf
// type, whether the leaf (the innermost element) is nullable, and the number
// of Array levels above the leaf: 1 for Array<T>, 2 for Array<Array<T>>.
//
// It is defined on a single column only. Any other node kind, such as a
// future struct node, is rejected: a type that describes several columns has
// to be projected into its columns first.
func GetArrayLeaf(typeSchema *schemapb.TypeSchema) (schemapb.DataType, bool, int, error) {
	if typeSchema == nil {
		return schemapb.DataType_None, false, 0, merr.WrapErrParameterInvalidMsg("array type_schema is nil")
	}
	depth := 0
	node := typeSchema
	for {
		switch kind := node.GetKind().(type) {
		case *schemapb.TypeSchema_ArrayElement:
			if kind.ArrayElement == nil {
				return schemapb.DataType_None, false, 0, merr.WrapErrParameterInvalidMsg(
					"array type_schema has a nil array_element at level %d", depth+1)
			}
			depth++
			node = kind.ArrayElement
		case *schemapb.TypeSchema_LeafType:
			if depth == 0 {
				return schemapb.DataType_None, false, 0, merr.WrapErrParameterInvalidMsg(
					"type_schema with leaf type %s is not an Array", kind.LeafType)
			}
			return kind.LeafType, node.GetNullable(), depth, nil
		default:
			return schemapb.DataType_None, false, 0, merr.WrapErrParameterInvalidMsg(
				"array type_schema has unsupported kind %T at level %d", node.GetKind(), depth)
		}
	}
}

// GetFieldArrayLeaf returns GetArrayLeaf for an Array or ArrayOfVector column.
// A nested Array carries its chain in FieldSchema.type_schema. A single-level
// Array carries none on the wire; its chain is Array -> leaf(element_type,
// element_nullable), the same one segcore FieldMeta synthesizes, and it is
// evaluated here without being materialized because storage calls this per
// row. This fallback goes away once every Array carries a type_schema.
func GetFieldArrayLeaf(field *schemapb.FieldSchema) (schemapb.DataType, bool, int, error) {
	if typeSchema := field.GetTypeSchema(); typeSchema != nil {
		return GetArrayLeaf(typeSchema)
	}
	switch field.GetDataType() {
	case schemapb.DataType_Array, schemapb.DataType_ArrayOfVector:
		if field.GetElementType() == schemapb.DataType_Array {
			return schemapb.DataType_None, false, 0, merr.WrapErrParameterInvalidMsg(
				"nested array field %s must specify type_schema", field.GetName())
		}
		return field.GetElementType(), field.GetElementNullable(), 1, nil
	default:
		return schemapb.DataType_None, false, 0, merr.WrapErrParameterInvalidMsg(
			"field %s of type %s is not an Array", field.GetName(), field.GetDataType())
	}
}

// IsNativeListArrayField reports whether an Array / ArrayOfVector field is
// persisted in the native Arrow list format instead of the legacy format.
//
// Legacy formats are kept only for the field kinds that exist in released
// 2.6/3.0 clusters: a single-level element-non-nullable scalar Array (one
// proto-encoded ScalarField per row in an Arrow Binary column) and a
// single-level element-non-nullable ArrayOfVector (list<FixedSizeBinary>).
//
// Every other Array kind uses the native format, regardless of row-level
// nullability:
//   - element-nullable scalar Array: list<T>
//   - element-nullable ArrayOfVector: list<Binary>
//   - nested scalar Array (type_schema with Array<Array<T>>): list<list<T>>
func IsNativeListArrayField(field *schemapb.FieldSchema) bool {
	switch field.GetDataType() {
	case schemapb.DataType_Array:
		return field.GetElementNullable() || IsNestedArrayTypeSchema(field.GetTypeSchema())
	case schemapb.DataType_ArrayOfVector:
		return field.GetElementNullable()
	default:
		return false
	}
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
