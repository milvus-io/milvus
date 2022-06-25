package querynode

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
	"strconv"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

func toArrowField(field *schemapb.FieldSchema) (*arrow.Field, error) {
	f := arrow.Field{
		Name:     field.GetName(),
		Type:     nil,
		Nullable: true,
		Metadata: arrow.Metadata{},
	}

	kv := make(map[string]string)

	kv["FIELD_ID_KEY"] = strconv.FormatInt(field.GetFieldID(), 10)
	kv["FIELD_TYPE_KEY"] = field.GetDataType().String()

	for _, param := range field.GetTypeParams() {
		kv[param.GetKey()] = param.GetValue()
	}

	for _, param := range field.GetIndexParams() {
		kv[param.GetKey()] = param.GetValue()
	}

	switch field.GetDataType() {
	case schemapb.DataType_Bool:
		f.Type = &arrow.BooleanType{}
	case schemapb.DataType_Int8:
		f.Type = &arrow.Int8Type{}
	case schemapb.DataType_Int16:
		f.Type = &arrow.Int16Type{}
	case schemapb.DataType_Int32:
		f.Type = &arrow.Int32Type{}
	case schemapb.DataType_Int64:
		f.Type = &arrow.Int64Type{}
	case schemapb.DataType_Float:
		f.Type = &arrow.Float32Type{}
	case schemapb.DataType_Double:
		f.Type = &arrow.Float64Type{}
	case schemapb.DataType_String:
	case schemapb.DataType_VarChar:
		f.Type = &arrow.StringType{}
	case schemapb.DataType_BinaryVector:
		dim, err := strconv.Atoi(kv["dim"])
		if err != nil {
			return nil, err
		}
		f.Type = &arrow.FixedSizeBinaryType{ByteWidth: dim / 8}
	case schemapb.DataType_FloatVector:
		dim, err := strconv.Atoi(kv["dim"])
		if err != nil {
			return nil, err
		}
		f.Type = &arrow.FixedSizeBinaryType{ByteWidth: dim * 4}
	}

	f.Metadata = arrow.MetadataFrom(kv)

	return &f, nil
}

func toArrowSchema(fields []*schemapb.FieldSchema) (*arrow.Schema, error) {
	result := make([]arrow.Field, len(fields))
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].GetFieldID() < fields[j].GetFieldID()
	})
	for i, field := range fields {
		f, err := toArrowField(field)
		if err != nil {
			return nil, err
		}
		result[i] = *f
	}
	return arrow.NewSchema(result, nil), nil
}

func toArrowArray(fieldData *schemapb.FieldData, pool memory.Allocator) (arrow.Array, error) {
	switch fieldData.Type {
	case schemapb.DataType_Bool:
		builder := array.NewBooleanBuilder(pool)
		defer builder.Release()
		builder.AppendValues(fieldData.GetScalars().GetBoolData().GetData(), nil)
		return builder.NewArray(), nil
	case schemapb.DataType_Int8:
		builder := array.NewInt8Builder(pool)
		defer builder.Release()
		for _, b := range fieldData.GetScalars().GetIntData().GetData() {
			builder.Append(int8(b))
		}
		return builder.NewArray(), nil
	case schemapb.DataType_Int16:
		builder := array.NewInt16Builder(pool)
		defer builder.Release()
		for _, b := range fieldData.GetScalars().GetIntData().GetData() {
			builder.Append(int16(b))
		}
		return builder.NewArray(), nil
	case schemapb.DataType_Int32:
		builder := array.NewInt32Builder(pool)
		defer builder.Release()
		builder.AppendValues(fieldData.GetScalars().GetIntData().GetData(), nil)
		return builder.NewArray(), nil
	case schemapb.DataType_Int64:
		builder := array.NewInt64Builder(pool)
		defer builder.Release()
		builder.AppendValues(fieldData.GetScalars().GetLongData().GetData(), nil)
		return builder.NewArray(), nil
	case schemapb.DataType_Float:
		builder := array.NewFloat32Builder(pool)
		defer builder.Release()
		builder.AppendValues(fieldData.GetScalars().GetFloatData().GetData(), nil)
		return builder.NewArray(), nil
	case schemapb.DataType_Double:
		builder := array.NewFloat64Builder(pool)
		defer builder.Release()
		builder.AppendValues(fieldData.GetScalars().GetDoubleData().GetData(), nil)
		return builder.NewArray(), nil
	case schemapb.DataType_String:
	case schemapb.DataType_VarChar:
		builder := array.NewStringBuilder(pool)
		defer builder.Release()
		builder.AppendValues(fieldData.GetScalars().GetStringData().GetData(), nil)
		return builder.NewArray(), nil
	case schemapb.DataType_BinaryVector:
		byteWidth := int(fieldData.GetVectors().Dim / 8)
		builder := array.NewFixedSizeBinaryBuilder(pool, &arrow.FixedSizeBinaryType{ByteWidth: byteWidth})
		defer builder.Release()
		buf := make([]byte, byteWidth)
		for j, b := range fieldData.GetVectors().GetBinaryVector() {
			if j%byteWidth == 0 {
			}
			buf[j%byteWidth] = b
			if j%byteWidth == byteWidth-1 {
				builder.Append(buf)
				buf = make([]byte, byteWidth)
			}
		}
		return builder.NewArray(), nil
	case schemapb.DataType_FloatVector:
		dim := int(fieldData.GetVectors().Dim)
		byteWidth := dim * 4
		builder := array.NewFixedSizeBinaryBuilder(pool, &arrow.FixedSizeBinaryType{ByteWidth: byteWidth})
		defer builder.Release()
		floatBuf := make([]float32, dim)
		for j, f := range fieldData.GetVectors().GetFloatVector().GetData() {
			floatBuf[j%dim] = f
			if j%dim == dim-1 {
				buf := new(bytes.Buffer)
				err := binary.Write(buf, binary.LittleEndian, floatBuf)
				if err != nil {
					return nil, err
				}
				bs := make([]byte, byteWidth)
				_, err = buf.Read(bs)
				if err != nil {
					return nil, err
				}
				builder.Append(bs)
				floatBuf = make([]float32, dim)
			}
		}
		return builder.NewArray(), nil
	}

	return nil, errors.New("Cannot find type " + fieldData.Type.String())
}
