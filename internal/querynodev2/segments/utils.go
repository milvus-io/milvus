package segments

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func GetPkField(schema *schemapb.CollectionSchema) *schemapb.FieldSchema {
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() {
			return field
		}
	}
	return nil
}

// TODO: remove this function to proper file
// GetPrimaryKeys would get primary keys by insert messages
func GetPrimaryKeys(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) ([]storage.PrimaryKey, error) {
	if msg.IsRowBased() {
		return getPKsFromRowBasedInsertMsg(msg, schema)
	}
	return getPKsFromColumnBasedInsertMsg(msg, schema)
}

func getPKsFromRowBasedInsertMsg(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) ([]storage.PrimaryKey, error) {
	offset := 0
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			break
		}
		switch field.DataType {
		case schemapb.DataType_Bool:
			offset++
		case schemapb.DataType_Int8:
			offset++
		case schemapb.DataType_Int16:
			offset += 2
		case schemapb.DataType_Int32:
			offset += 4
		case schemapb.DataType_Int64:
			offset += 8
		case schemapb.DataType_Float:
			offset += 4
		case schemapb.DataType_Double:
			offset += 8
		case schemapb.DataType_FloatVector:
			for _, t := range field.TypeParams {
				if t.Key == common.DimKey {
					dim, err := strconv.Atoi(t.Value)
					if err != nil {
						return nil, fmt.Errorf("strconv wrong on get dim, err = %s", err)
					}
					offset += dim * 4
					break
				}
			}
		case schemapb.DataType_BinaryVector:
			for _, t := range field.TypeParams {
				if t.Key == common.DimKey {
					dim, err := strconv.Atoi(t.Value)
					if err != nil {
						return nil, fmt.Errorf("strconv wrong on get dim, err = %s", err)
					}
					offset += dim / 8
					break
				}
			}
		}
	}

	log.Info(strconv.FormatInt(int64(offset), 10))
	blobReaders := make([]io.Reader, len(msg.RowData))
	for i, blob := range msg.RowData {
		blobReaders[i] = bytes.NewReader(blob.GetValue()[offset : offset+8])
	}
	pks := make([]storage.PrimaryKey, len(blobReaders))

	for i, reader := range blobReaders {
		var int64PkValue int64
		err := binary.Read(reader, common.Endian, &int64PkValue)
		if err != nil {
			log.Warn("binary read blob value failed", zap.Error(err))
			return nil, err
		}
		pks[i] = storage.NewInt64PrimaryKey(int64PkValue)
	}

	return pks, nil
}

func getPKsFromColumnBasedInsertMsg(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) ([]storage.PrimaryKey, error) {
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}

	primaryFieldData, err := typeutil.GetPrimaryFieldData(msg.GetFieldsData(), primaryFieldSchema)
	if err != nil {
		return nil, err
	}

	pks, err := storage.ParseFieldData2PrimaryKeys(primaryFieldData)
	if err != nil {
		return nil, err
	}

	return pks, nil
}

func fillBinVecFieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, offsets [][2]int64, endian binary.ByteOrder) error {
	dim := fieldData.GetVectors().GetDim()
	rowBytes := dim / 8
	content, err := vcm.Read(ctx, dataPath)
	if err != nil {
		return err
	}
	x := fieldData.GetVectors().GetData().(*schemapb.VectorField_BinaryVector)
	resultLen := dim / 8
	for _, pair := range offsets {
		i := pair[0]
		offset := pair[1]
		copy(x.BinaryVector[i*resultLen:(i+1)*resultLen], content[offset*rowBytes:])
	}
	return nil
}

func fillFloatVecFieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, offsets [][2]int64, endian binary.ByteOrder) error {
	dim := fieldData.GetVectors().GetDim()
	rowBytes := dim * 4
	content, err := vcm.Read(ctx, dataPath)
	if err != nil {
		return err
	}
	x := fieldData.GetVectors().GetData().(*schemapb.VectorField_FloatVector)
	floatResult := make([]float32, dim)
	resultLen := dim
	for _, pair := range offsets {
		i := pair[0]
		offset := pair[1]
		buf := bytes.NewReader(content[offset*rowBytes : (offset+1)*rowBytes])
		if err = binary.Read(buf, endian, &floatResult); err != nil {
			return err
		}
		copy(x.FloatVector.Data[i*resultLen:(i+1)*resultLen], floatResult)
	}
	return nil
}

func fillBoolFieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, offsets [][2]int64, endian binary.ByteOrder) error {
	// read whole file.
	// TODO: optimize here.
	content, err := vcm.Read(ctx, dataPath)
	if err != nil {
		return err
	}
	var arr schemapb.BoolArray
	err = proto.Unmarshal(content, &arr)
	if err != nil {
		return err
	}
	for _, pair := range offsets {
		i := pair[0]
		offset := pair[1]
		fieldData.GetScalars().GetBoolData().GetData()[i] = arr.Data[offset]
	}
	return nil
}

func fillStringFieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, offsets [][2]int64, endian binary.ByteOrder) error {
	// read whole file.
	// TODO: optimize here.
	content, err := vcm.Read(ctx, dataPath)
	if err != nil {
		return err
	}
	var arr schemapb.StringArray
	err = proto.Unmarshal(content, &arr)
	if err != nil {
		return err
	}
	for _, pair := range offsets {
		i := pair[0]
		offset := pair[1]
		fieldData.GetScalars().GetStringData().GetData()[i] = arr.Data[offset]
	}
	return nil
}

func fillInt8FieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, offsets [][2]int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(1)
	content, err := vcm.Read(ctx, dataPath)
	if err != nil {
		return err
	}
	var i8 int8
	for _, pair := range offsets {
		i := pair[0]
		offset := pair[1]
		if err := funcutil.ReadBinary(endian, content[offset*rowBytes:(offset+1)*rowBytes], &i8); err != nil {
			return err
		}
		fieldData.GetScalars().GetIntData().GetData()[i] = int32(i8)
	}
	return nil
}

func fillInt16FieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, offsets [][2]int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(2)
	content, err := vcm.Read(ctx, dataPath)
	if err != nil {
		return err
	}
	var i16 int16
	for _, pair := range offsets {
		i := pair[0]
		offset := pair[1]
		if err := funcutil.ReadBinary(endian, content[offset*rowBytes:(offset+1)*rowBytes], &i16); err != nil {
			return err
		}
		fieldData.GetScalars().GetIntData().GetData()[i] = int32(i16)
	}
	return nil
}

func fillInt32FieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, offsets [][2]int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(4)
	content, err := vcm.Read(ctx, dataPath)
	if err != nil {
		return err
	}
	var i32 int32
	for _, pair := range offsets {
		i := pair[0]
		offset := pair[1]
		if err := funcutil.ReadBinary(endian, content[offset*rowBytes:(offset+1)*rowBytes], &i32); err != nil {
			return err
		}
		fieldData.GetScalars().GetIntData().GetData()[i] = i32
	}
	return nil
}

func fillInt64FieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, offsets [][2]int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(8)
	content, err := vcm.Read(ctx, dataPath)
	if err != nil {
		return err
	}
	var i64 int64
	for _, pair := range offsets {
		i := pair[0]
		offset := pair[1]
		if err := funcutil.ReadBinary(endian, content[offset*rowBytes:(offset+1)*rowBytes], &i64); err != nil {
			return err
		}
		fieldData.GetScalars().GetLongData().GetData()[i] = i64
	}
	return nil
}

func fillFloatFieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, offsets [][2]int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(4)
	content, err := vcm.Read(ctx, dataPath)
	if err != nil {
		return err
	}
	var f32 float32
	for _, pair := range offsets {
		i := pair[0]
		offset := pair[1]
		if err := funcutil.ReadBinary(endian, content[offset*rowBytes:(offset+1)*rowBytes], &f32); err != nil {
			return err
		}
		fieldData.GetScalars().GetFloatData().GetData()[i] = f32
	}
	return nil
}

func fillDoubleFieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, offsets [][2]int64, endian binary.ByteOrder) error {
	// read by offset.
	rowBytes := int64(8)
	content, err := vcm.Read(ctx, dataPath)
	if err != nil {
		return err
	}
	var f64 float64
	for _, pair := range offsets {
		i := pair[0]
		offset := pair[1]
		if err := funcutil.ReadBinary(endian, content[offset*rowBytes:(offset+1)*rowBytes], &f64); err != nil {
			return err
		}
		fieldData.GetScalars().GetDoubleData().GetData()[i] = f64
	}
	return nil
}

func fillFieldData(ctx context.Context, vcm storage.ChunkManager, dataPath string, fieldData *schemapb.FieldData, offsets [][2]int64, endian binary.ByteOrder) error {
	switch fieldData.Type {
	case schemapb.DataType_BinaryVector:
		return fillBinVecFieldData(ctx, vcm, dataPath, fieldData, offsets, endian)
	case schemapb.DataType_FloatVector:
		return fillFloatVecFieldData(ctx, vcm, dataPath, fieldData, offsets, endian)
	case schemapb.DataType_Bool:
		return fillBoolFieldData(ctx, vcm, dataPath, fieldData, offsets, endian)
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return fillStringFieldData(ctx, vcm, dataPath, fieldData, offsets, endian)
	case schemapb.DataType_Int8:
		return fillInt8FieldData(ctx, vcm, dataPath, fieldData, offsets, endian)
	case schemapb.DataType_Int16:
		return fillInt16FieldData(ctx, vcm, dataPath, fieldData, offsets, endian)
	case schemapb.DataType_Int32:
		return fillInt32FieldData(ctx, vcm, dataPath, fieldData, offsets, endian)
	case schemapb.DataType_Int64:
		return fillInt64FieldData(ctx, vcm, dataPath, fieldData, offsets, endian)
	case schemapb.DataType_Float:
		return fillFloatFieldData(ctx, vcm, dataPath, fieldData, offsets, endian)
	case schemapb.DataType_Double:
		return fillDoubleFieldData(ctx, vcm, dataPath, fieldData, offsets, endian)
	default:
		return fmt.Errorf("invalid data type: %s", fieldData.Type.String())
	}
}
