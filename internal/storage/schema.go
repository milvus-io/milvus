package storage

import (
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func ConvertToArrowSchema(fields []*schemapb.FieldSchema) (*arrow.Schema, error) {
	arrowFields := make([]arrow.Field, 0, len(fields))
	for _, field := range fields {
		if serdeMap[field.DataType].arrowType == nil {
			return nil, merr.WrapErrParameterInvalidMsg("unknown field data type [%s] for field [%s]", field.DataType, field.GetName())
		}
		var dim int
		switch field.DataType {
		case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector,
			schemapb.DataType_Int8Vector, schemapb.DataType_FloatVector:
			var err error
			dim, err = GetDimFromParams(field.TypeParams)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("dim not found in field [%s] params", field.GetName())
			}
		default:
			dim = 0
		}
		arrowFields = append(arrowFields, ConvertToArrowField(field, serdeMap[field.DataType].arrowType(dim)))
	}

	return arrow.NewSchema(arrowFields, nil), nil
}

func ConvertToArrowField(field *schemapb.FieldSchema, dataType arrow.DataType) arrow.Field {
	return arrow.Field{
		Name:     field.GetName(),
		Type:     dataType,
		Metadata: arrow.NewMetadata([]string{"FieldID"}, []string{strconv.Itoa(int(field.GetFieldID()))}),
		Nullable: field.GetNullable(),
	}
}
