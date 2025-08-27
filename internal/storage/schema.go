package storage

import (
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func ConvertToArrowSchema(schema *schemapb.CollectionSchema) (*arrow.Schema, error) {
	fieldCount := typeutil.GetTotalFieldsNum(schema)
	arrowFields := make([]arrow.Field, 0, fieldCount)
	appendArrowField := func(field *schemapb.FieldSchema) error {
		if serdeMap[field.DataType].arrowType == nil {
			return merr.WrapErrParameterInvalidMsg("unknown field data type [%s] for field [%s]", field.DataType, field.GetName())
		}
		var dim int
		switch field.DataType {
		case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector,
			schemapb.DataType_Int8Vector, schemapb.DataType_FloatVector, schemapb.DataType_ArrayOfVector:
			var err error
			dim, err = GetDimFromParams(field.TypeParams)
			if err != nil {
				return merr.WrapErrParameterInvalidMsg("dim not found in field [%s] params", field.GetName())
			}
		default:
			dim = 0
		}

		elementType := schemapb.DataType_None
		if field.DataType == schemapb.DataType_ArrayOfVector {
			elementType = field.GetElementType()
		}

		arrowType := serdeMap[field.DataType].arrowType(dim, elementType)
		arrowField := ConvertToArrowField(field, arrowType)

		// Add extra metadata for ArrayOfVector
		if field.DataType == schemapb.DataType_ArrayOfVector {
			arrowField.Metadata = arrow.NewMetadata(
				[]string{packed.ArrowFieldIdMetadataKey, "elementType", "dim"},
				[]string{strconv.Itoa(int(field.GetFieldID())), strconv.Itoa(int(elementType)), strconv.Itoa(dim)},
			)
		}

		arrowFields = append(arrowFields, arrowField)
		return nil
	}
	for _, field := range schema.GetFields() {
		if err := appendArrowField(field); err != nil {
			return nil, err
		}
	}

	for _, structField := range schema.GetStructArrayFields() {
		for _, field := range structField.GetFields() {
			if err := appendArrowField(field); err != nil {
				return nil, err
			}
		}
	}

	return arrow.NewSchema(arrowFields, nil), nil
}

func ConvertToArrowField(field *schemapb.FieldSchema, dataType arrow.DataType) arrow.Field {
	return arrow.Field{
		Name:     field.GetName(),
		Type:     dataType,
		Metadata: arrow.NewMetadata([]string{packed.ArrowFieldIdMetadataKey}, []string{strconv.Itoa(int(field.GetFieldID()))}),
		Nullable: field.GetNullable(),
	}
}
