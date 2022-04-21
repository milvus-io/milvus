package importutil

import (
	"context"
	"errors"
	"io"
	"strconv"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
)

type ColumnDesc struct {
	name         string            // name of the target column
	dt           schemapb.DataType // data type of the target column
	elementCount int               // how many elements need to be read
	dimension    int               // only for vector
}

type NumpyParser struct {
	ctx              context.Context            // for canceling parse process
	collectionSchema *schemapb.CollectionSchema // collection schema
	columnDesc       *ColumnDesc                // description for target column

	columnData    storage.FieldData                   // in-memory column data
	callFlushFunc func(field storage.FieldData) error // call back function to output column data
}

// NewNumpyParser helper function to create a NumpyParser
func NewNumpyParser(ctx context.Context, collectionSchema *schemapb.CollectionSchema,
	flushFunc func(field storage.FieldData) error) *NumpyParser {
	if collectionSchema == nil || flushFunc == nil {
		return nil
	}

	parser := &NumpyParser{
		ctx:              ctx,
		collectionSchema: collectionSchema,
		columnDesc:       &ColumnDesc{},
		callFlushFunc:    flushFunc,
	}

	return parser
}

func (p *NumpyParser) logError(msg string) error {
	log.Error(msg)
	return errors.New(msg)
}

// data type converted from numpy header description, for vector field, the type is int8(binary vector) or float32(float vector)
func convertNumpyType(str string) (schemapb.DataType, error) {
	switch str {
	case "b1", "<b1", "|b1", "bool":
		return schemapb.DataType_Bool, nil
	case "u1", "<u1", "|u1", "uint8": // binary vector data type is uint8
		return schemapb.DataType_BinaryVector, nil
	case "i1", "<i1", "|i1", ">i1", "int8":
		return schemapb.DataType_Int8, nil
	case "i2", "<i2", "|i2", ">i2", "int16":
		return schemapb.DataType_Int16, nil
	case "i4", "<i4", "|i4", ">i4", "int32":
		return schemapb.DataType_Int32, nil
	case "i8", "<i8", "|i8", ">i8", "int64":
		return schemapb.DataType_Int64, nil
	case "f4", "<f4", "|f4", ">f4", "float32":
		return schemapb.DataType_Float, nil
	case "f8", "<f8", "|f8", ">f8", "float64":
		return schemapb.DataType_Double, nil
	default:
		return schemapb.DataType_None, errors.New("unsupported data type " + str)
	}
}

func (p *NumpyParser) validate(adapter *NumpyAdapter, fieldName string) error {
	if adapter == nil {
		return errors.New("numpy adapter is nil")
	}

	// check existence of the target field
	var schema *schemapb.FieldSchema
	for i := 0; i < len(p.collectionSchema.Fields); i++ {
		schema = p.collectionSchema.Fields[i]
		if schema.GetName() == fieldName {
			p.columnDesc.name = fieldName
			break
		}
	}

	if p.columnDesc.name == "" {
		return errors.New("the field " + fieldName + " doesn't exist")
	}

	p.columnDesc.dt = schema.DataType
	elementType, err := convertNumpyType(adapter.GetType())
	if err != nil {
		return err
	}

	shape := adapter.GetShape()

	// 1. field data type should be consist to numpy data type
	// 2. vector field dimension should be consist to numpy shape
	if schemapb.DataType_FloatVector == schema.DataType {
		if elementType != schemapb.DataType_Float && elementType != schemapb.DataType_Double {
			return errors.New("illegal data type " + adapter.GetType() + " for field " + schema.GetName())
		}

		// vector field, the shape should be 2
		if len(shape) != 2 {
			return errors.New("illegal numpy shape " + strconv.Itoa(len(shape)) + " for field " + schema.GetName())
		}

		// shape[0] is row count, shape[1] is element count per row
		p.columnDesc.elementCount = shape[0] * shape[1]

		p.columnDesc.dimension, err = getFieldDimension(schema)
		if err != nil {
			return err
		}

		if shape[1] != p.columnDesc.dimension {
			return errors.New("illegal row width " + strconv.Itoa(shape[1]) + " for field " + schema.GetName() + " dimension " + strconv.Itoa(p.columnDesc.dimension))
		}
	} else if schemapb.DataType_BinaryVector == schema.DataType {
		if elementType != schemapb.DataType_BinaryVector {
			return errors.New("illegal data type " + adapter.GetType() + " for field " + schema.GetName())
		}

		// vector field, the shape should be 2
		if len(shape) != 2 {
			return errors.New("illegal numpy shape " + strconv.Itoa(len(shape)) + " for field " + schema.GetName())
		}

		// shape[0] is row count, shape[1] is element count per row
		p.columnDesc.elementCount = shape[0] * shape[1]

		p.columnDesc.dimension, err = getFieldDimension(schema)
		if err != nil {
			return err
		}

		if shape[1] != p.columnDesc.dimension/8 {
			return errors.New("illegal row width " + strconv.Itoa(shape[1]) + " for field " + schema.GetName() + " dimension " + strconv.Itoa(p.columnDesc.dimension))
		}
	} else {
		if elementType != schema.DataType {
			return errors.New("illegal data type " + adapter.GetType() + " for field " + schema.GetName())
		}

		// scalar field, the shape should be 1
		if len(shape) != 1 {
			return errors.New("illegal numpy shape " + strconv.Itoa(len(shape)) + " for field " + schema.GetName())
		}

		p.columnDesc.elementCount = shape[0]
	}

	return nil
}

// this method read numpy data section into a storage.FieldData
// please note it will require a large memory block(the memory size is almost equal to numpy file size)
func (p *NumpyParser) consume(adapter *NumpyAdapter) error {
	switch p.columnDesc.dt {
	case schemapb.DataType_Bool:
		data, err := adapter.ReadBool(p.columnDesc.elementCount)
		if err != nil {
			return err
		}

		p.columnData = &storage.BoolFieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
		}

	case schemapb.DataType_Int8:
		data, err := adapter.ReadInt8(p.columnDesc.elementCount)
		if err != nil {
			return err
		}

		p.columnData = &storage.Int8FieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
		}
	case schemapb.DataType_Int16:
		data, err := adapter.ReadInt16(p.columnDesc.elementCount)
		if err != nil {
			return err
		}

		p.columnData = &storage.Int16FieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
		}
	case schemapb.DataType_Int32:
		data, err := adapter.ReadInt32(p.columnDesc.elementCount)
		if err != nil {
			return err
		}

		p.columnData = &storage.Int32FieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
		}
	case schemapb.DataType_Int64:
		data, err := adapter.ReadInt64(p.columnDesc.elementCount)
		if err != nil {
			return err
		}

		p.columnData = &storage.Int64FieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
		}
	case schemapb.DataType_Float:
		data, err := adapter.ReadFloat32(p.columnDesc.elementCount)
		if err != nil {
			return err
		}

		p.columnData = &storage.FloatFieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
		}
	case schemapb.DataType_Double:
		data, err := adapter.ReadFloat64(p.columnDesc.elementCount)
		if err != nil {
			return err
		}

		p.columnData = &storage.DoubleFieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
		}
	case schemapb.DataType_BinaryVector:
		data, err := adapter.ReadUint8(p.columnDesc.elementCount)
		if err != nil {
			return err
		}

		p.columnData = &storage.BinaryVectorFieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
			Dim:     p.columnDesc.dimension,
		}
	case schemapb.DataType_FloatVector:
		// for float vector, we support float32 and float64 numpy file because python float value is 64 bit
		// for float64 numpy file, the performance is worse than float32 numpy file
		// we don't check overflow here
		elementType, err := convertNumpyType(adapter.GetType())
		if err != nil {
			return err
		}

		var data []float32
		if elementType == schemapb.DataType_Float {
			data, err = adapter.ReadFloat32(p.columnDesc.elementCount)
			if err != nil {
				return err
			}
		} else if elementType == schemapb.DataType_Double {
			data = make([]float32, 0, p.columnDesc.elementCount)
			data64, err := adapter.ReadFloat64(p.columnDesc.elementCount)
			if err != nil {
				return err
			}

			for _, f64 := range data64 {
				data = append(data, float32(f64))
			}
		}

		p.columnData = &storage.FloatVectorFieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
			Dim:     p.columnDesc.dimension,
		}
	default:
		return errors.New("unsupported data type: " + strconv.Itoa(int(p.columnDesc.dt)))
	}

	return nil
}

func (p *NumpyParser) Parse(reader io.Reader, fieldName string, onlyValidate bool) error {
	adapter, err := NewNumpyAdapter(reader)
	if err != nil {
		return p.logError("Numpy parse: " + err.Error())
	}

	// the validation method only check the file header information
	err = p.validate(adapter, fieldName)
	if err != nil {
		return p.logError("Numpy parse: " + err.Error())
	}

	if onlyValidate {
		return nil
	}

	// read all data from the numpy file
	err = p.consume(adapter)
	if err != nil {
		return p.logError("Numpy parse: " + err.Error())
	}

	return p.callFlushFunc(p.columnData)
}
