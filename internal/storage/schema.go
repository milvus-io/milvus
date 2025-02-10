package storage

import (
	"github.com/apache/arrow/go/v12/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func ConvertToArrowSchema(fields []*schemapb.FieldSchema) (*arrow.Schema, error) {
	arrowFields := make([]arrow.Field, 0, len(fields))
	for _, field := range fields {
		if serdeMap[field.DataType].arrowType == nil {
			return nil, merr.WrapErrParameterInvalidMsg("unknown field data type [%s] for field [%s]", field.DataType, field.GetName())
		}
		switch field.DataType {
		case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector,
			schemapb.DataType_Int8Vector, schemapb.DataType_FloatVector:
			dim, err := GetDimFromParams(field.TypeParams)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("dim not found in field [%s] params", field.GetName())
			}
			arrowFields = append(arrowFields, arrow.Field{
				Name:     field.GetName(),
				Type:     serdeMap[field.DataType].arrowType(dim),
				Nullable: field.GetNullable(),
			})
		default:
			arrowFields = append(arrowFields, arrow.Field{
				Name:     field.GetName(),
				Type:     serdeMap[field.DataType].arrowType(0),
				Nullable: field.GetNullable(),
			})
		}
	}

	return arrow.NewSchema(arrowFields, nil), nil
}
