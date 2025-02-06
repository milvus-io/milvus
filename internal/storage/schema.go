package storage

import (
	"strconv"

	"github.com/apache/arrow/go/v12/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func ConvertToArrowSchema(fields []*schemapb.FieldSchema) (*arrow.Schema, error) {
	arrowFields := make([]arrow.Field, 0, len(fields))
	for _, field := range fields {
		if serdeMap[field.DataType].arrowType == nil {
			return nil, merr.WrapErrParameterInvalidMsg("unknown type %v", field.DataType.String())
		}
		switch field.DataType {
		case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector,
			schemapb.DataType_Int8Vector, schemapb.DataType_FloatVector:
			dim, err := GetDimFromParams(field.TypeParams)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("dim not found in params")
			}
			arrowFields = append(arrowFields, arrow.Field{
				Name:     field.Name,
				Type:     serdeMap[field.DataType].arrowType(dim),
				Nullable: field.GetNullable(),
				Metadata: arrow.NewMetadata([]string{FieldIDMetaKey}, []string{strconv.Itoa(int(field.FieldID))}),
			})
		default:
			arrowFields = append(arrowFields, arrow.Field{
				Name:     field.Name,
				Type:     serdeMap[field.DataType].arrowType(0),
				Nullable: field.Nullable,
				Metadata: arrow.NewMetadata([]string{FieldIDMetaKey}, []string{strconv.Itoa(int(field.FieldID))}),
			})
		}
	}

	return arrow.NewSchema(arrowFields, nil), nil
}

func convertToArrowType(dataType schemapb.DataType) (arrow.DataType, error) {
	switch dataType {
	case schemapb.DataType_Bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case schemapb.DataType_Int8:
		return arrow.PrimitiveTypes.Int8, nil
	case schemapb.DataType_Int16:
		return arrow.PrimitiveTypes.Int16, nil
	case schemapb.DataType_Int32:
		return arrow.PrimitiveTypes.Int32, nil
	case schemapb.DataType_Int64:
		return arrow.PrimitiveTypes.Int64, nil
	case schemapb.DataType_Float:
		return arrow.PrimitiveTypes.Float32, nil
	case schemapb.DataType_Double:
		return arrow.PrimitiveTypes.Float64, nil
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return arrow.BinaryTypes.String, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unknown type %v", dataType.String())
	}
}
