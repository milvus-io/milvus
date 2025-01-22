package typeutil

import (
	"github.com/apache/arrow/go/v12/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func ConvertToArrowSchema(fields []*schemapb.FieldSchema) (*arrow.Schema, error) {
	arrowFields := make([]arrow.Field, 0, len(fields))
	for _, field := range fields {
		switch field.DataType {
		case schemapb.DataType_Bool:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.FixedWidthTypes.Boolean,
			})
		case schemapb.DataType_Int8:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.PrimitiveTypes.Int8,
			})
		case schemapb.DataType_Int16:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.PrimitiveTypes.Int16,
			})
		case schemapb.DataType_Int32:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.PrimitiveTypes.Int32,
			})
		case schemapb.DataType_Int64:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.PrimitiveTypes.Int64,
			})
		case schemapb.DataType_Float:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.PrimitiveTypes.Float32,
			})
		case schemapb.DataType_Double:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.PrimitiveTypes.Float64,
			})
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.BinaryTypes.String,
			})
		case schemapb.DataType_Array:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.BinaryTypes.Binary,
			})
		case schemapb.DataType_JSON:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.BinaryTypes.Binary,
			})
		case schemapb.DataType_BinaryVector:
			dim, err := storage.GetDimFromParams(field.TypeParams)
			if err != nil {
				return nil, err
			}
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: &arrow.FixedSizeBinaryType{ByteWidth: dim / 8},
			})
		case schemapb.DataType_FloatVector:
			dim, err := storage.GetDimFromParams(field.TypeParams)
			if err != nil {
				return nil, err
			}
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: &arrow.FixedSizeBinaryType{ByteWidth: dim * 4},
			})
		case schemapb.DataType_Float16Vector:
			dim, err := storage.GetDimFromParams(field.TypeParams)
			if err != nil {
				return nil, err
			}
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: &arrow.FixedSizeBinaryType{ByteWidth: dim * 2},
			})
		case schemapb.DataType_BFloat16Vector:
			dim, err := storage.GetDimFromParams(field.TypeParams)
			if err != nil {
				return nil, err
			}
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: &arrow.FixedSizeBinaryType{ByteWidth: dim * 2},
			})
		case schemapb.DataType_Int8Vector:
			dim, err := storage.GetDimFromParams(field.TypeParams)
			if err != nil {
				return nil, err
			}
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: &arrow.FixedSizeBinaryType{ByteWidth: dim},
			})
		default:
			return nil, merr.WrapErrParameterInvalidMsg("unknown type %v", field.DataType.String())
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
