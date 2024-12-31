package typeutil

import (
	"fmt"
	"math"
	"path"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func GetStorageURI(protocol, pathPrefix string, segmentID int64) (string, error) {
	switch protocol {
	case "s3":
		var scheme string
		if paramtable.Get().MinioCfg.UseSSL.GetAsBool() {
			scheme = "https"
		} else {
			scheme = "http"
		}
		if pathPrefix != "" {
			cleanPath := path.Clean(pathPrefix)
			return fmt.Sprintf("s3://%s:%s@%s/%s/%d?scheme=%s&endpoint_override=%s&allow_bucket_creation=true", paramtable.Get().MinioCfg.AccessKeyID.GetValue(), paramtable.Get().MinioCfg.SecretAccessKey.GetValue(), paramtable.Get().MinioCfg.BucketName.GetValue(), cleanPath, segmentID, scheme, paramtable.Get().MinioCfg.Address.GetValue()), nil
		} else {
			return fmt.Sprintf("s3://%s:%s@%s/%d?scheme=%s&endpoint_override=%s&allow_bucket_creation=true", paramtable.Get().MinioCfg.AccessKeyID.GetValue(), paramtable.Get().MinioCfg.SecretAccessKey.GetValue(), paramtable.Get().MinioCfg.BucketName.GetValue(), segmentID, scheme, paramtable.Get().MinioCfg.Address.GetValue()), nil
		}
	case "file":
		if pathPrefix != "" {
			cleanPath := path.Clean(pathPrefix)
			return fmt.Sprintf("file://%s/%d", cleanPath, segmentID), nil
		} else {
			return fmt.Sprintf("file://%d", segmentID), nil
		}
	default:
		return "", merr.WrapErrParameterInvalidMsg("unsupported schema %s", protocol)
	}
}

func BuildRecord(b *array.RecordBuilder, data *storage.InsertData, fields []*schemapb.FieldSchema) error {
	if data == nil {
		log.Info("no buffer data to flush")
		return nil
	}
	for i, field := range fields {
		fBuilder := b.Field(i)
		switch field.DataType {
		case schemapb.DataType_Bool:
			fBuilder.(*array.BooleanBuilder).AppendValues(data.Data[field.FieldID].(*storage.BoolFieldData).Data, nil)
		case schemapb.DataType_Int8:
			fBuilder.(*array.Int8Builder).AppendValues(data.Data[field.FieldID].(*storage.Int8FieldData).Data, nil)
		case schemapb.DataType_Int16:
			fBuilder.(*array.Int16Builder).AppendValues(data.Data[field.FieldID].(*storage.Int16FieldData).Data, nil)
		case schemapb.DataType_Int32:
			fBuilder.(*array.Int32Builder).AppendValues(data.Data[field.FieldID].(*storage.Int32FieldData).Data, nil)
		case schemapb.DataType_Int64:
			fBuilder.(*array.Int64Builder).AppendValues(data.Data[field.FieldID].(*storage.Int64FieldData).Data, nil)
		case schemapb.DataType_Float:
			fBuilder.(*array.Float32Builder).AppendValues(data.Data[field.FieldID].(*storage.FloatFieldData).Data, nil)
		case schemapb.DataType_Double:
			fBuilder.(*array.Float64Builder).AppendValues(data.Data[field.FieldID].(*storage.DoubleFieldData).Data, nil)
		case schemapb.DataType_VarChar, schemapb.DataType_String:
			fBuilder.(*array.StringBuilder).AppendValues(data.Data[field.FieldID].(*storage.StringFieldData).Data, nil)
		case schemapb.DataType_Array:
			for _, data := range data.Data[field.FieldID].(*storage.ArrayFieldData).Data {
				marsheled, err := proto.Marshal(data)
				if err != nil {
					return err
				}
				fBuilder.(*array.BinaryBuilder).Append(marsheled)
			}
		case schemapb.DataType_JSON:
			fBuilder.(*array.BinaryBuilder).AppendValues(data.Data[field.FieldID].(*storage.JSONFieldData).Data, nil)
		case schemapb.DataType_BinaryVector:
			vecData := data.Data[field.FieldID].(*storage.BinaryVectorFieldData)
			for i := 0; i < len(vecData.Data); i += vecData.Dim / 8 {
				fBuilder.(*array.FixedSizeBinaryBuilder).Append(vecData.Data[i : i+vecData.Dim/8])
			}
		case schemapb.DataType_FloatVector:
			vecData := data.Data[field.FieldID].(*storage.FloatVectorFieldData)
			builder := fBuilder.(*array.FixedSizeBinaryBuilder)
			dim := vecData.Dim
			data := vecData.Data
			byteLength := dim * 4
			length := len(data) / dim

			builder.Reserve(length)
			bytesData := make([]byte, byteLength)
			for i := 0; i < length; i++ {
				vec := data[i*dim : (i+1)*dim]
				for j := range vec {
					bytes := math.Float32bits(vec[j])
					common.Endian.PutUint32(bytesData[j*4:], bytes)
				}
				builder.Append(bytesData)
			}
		case schemapb.DataType_Float16Vector:
			vecData := data.Data[field.FieldID].(*storage.Float16VectorFieldData)
			builder := fBuilder.(*array.FixedSizeBinaryBuilder)
			dim := vecData.Dim
			data := vecData.Data
			byteLength := dim * 2
			length := len(data) / byteLength

			builder.Reserve(length)
			for i := 0; i < length; i++ {
				builder.Append(data[i*byteLength : (i+1)*byteLength])
			}
		case schemapb.DataType_BFloat16Vector:
			vecData := data.Data[field.FieldID].(*storage.BFloat16VectorFieldData)
			builder := fBuilder.(*array.FixedSizeBinaryBuilder)
			dim := vecData.Dim
			data := vecData.Data
			byteLength := dim * 2
			length := len(data) / byteLength

			builder.Reserve(length)
			for i := 0; i < length; i++ {
				builder.Append(data[i*byteLength : (i+1)*byteLength])
			}
		case schemapb.DataType_Int8Vector:
			vecData := data.Data[field.FieldID].(*storage.Int8VectorFieldData)
			builder := fBuilder.(*array.FixedSizeBinaryBuilder)
			dim := vecData.Dim
			data := vecData.Data
			byteLength := dim
			length := len(data) / byteLength

			builder.Reserve(length)
			for i := 0; i < length; i++ {
				builder.Append(arrow.Int8Traits.CastToBytes(data[i*dim : (i+1)*dim]))
			}
		default:
			return merr.WrapErrParameterInvalidMsg("unknown type %v", field.DataType.String())
		}
	}

	return nil
}
