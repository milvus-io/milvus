// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importutil

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/storage"
	"go.uber.org/zap"
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

// NewNumpyParser is helper function to create a NumpyParser
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

func (p *NumpyParser) validate(adapter *NumpyAdapter, fieldName string) error {
	if adapter == nil {
		log.Error("Numpy parser: numpy adapter is nil")
		return errors.New("Numpy parser: numpy adapter is nil")
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
		log.Error("Numpy parser: Numpy parser: the field is not found in collection schema", zap.String("fieldName", fieldName))
		return fmt.Errorf("Numpy parser: the field name '%s' is not found in collection schema", fieldName)
	}

	p.columnDesc.dt = schema.DataType
	elementType := adapter.GetType()
	shape := adapter.GetShape()

	var err error
	// 1. field data type should be consist to numpy data type
	// 2. vector field dimension should be consist to numpy shape
	if schemapb.DataType_FloatVector == schema.DataType {
		// float32/float64 numpy file can be used for float vector file, 2 reasons:
		// 1. for float vector, we support float32 and float64 numpy file because python float value is 64 bit
		// 2. for float64 numpy file, the performance is worse than float32 numpy file
		if elementType != schemapb.DataType_Float && elementType != schemapb.DataType_Double {
			log.Error("Numpy parser: illegal data type of numpy file for float vector field", zap.Any("dataType", elementType),
				zap.String("fieldName", fieldName))
			return fmt.Errorf("Numpy parser: illegal data type %s of numpy file for float vector field '%s'", getTypeName(elementType), schema.GetName())
		}

		// vector field, the shape should be 2
		if len(shape) != 2 {
			log.Error("Numpy parser: illegal shape of numpy file for float vector field, shape should be 2", zap.Int("shape", len(shape)),
				zap.String("fieldName", fieldName))
			return fmt.Errorf("Numpy parser: illegal shape %d of numpy file for float vector field '%s', shape should be 2", shape, schema.GetName())
		}

		// shape[0] is row count, shape[1] is element count per row
		p.columnDesc.elementCount = shape[0] * shape[1]

		p.columnDesc.dimension, err = getFieldDimension(schema)
		if err != nil {
			return err
		}

		if shape[1] != p.columnDesc.dimension {
			log.Error("Numpy parser: illegal dimension of numpy file for float vector field", zap.String("fieldName", fieldName),
				zap.Int("numpyDimension", shape[1]), zap.Int("fieldDimension", p.columnDesc.dimension))
			return fmt.Errorf("Numpy parser: illegal dimension %d of numpy file for float vector field '%s', dimension should be %d",
				shape[1], schema.GetName(), p.columnDesc.dimension)
		}
	} else if schemapb.DataType_BinaryVector == schema.DataType {
		if elementType != schemapb.DataType_BinaryVector {
			log.Error("Numpy parser: illegal data type of numpy file for binary vector field", zap.Any("dataType", elementType),
				zap.String("fieldName", fieldName))
			return fmt.Errorf("Numpy parser: illegal data type %s of numpy file for binary vector field '%s'", getTypeName(elementType), schema.GetName())
		}

		// vector field, the shape should be 2
		if len(shape) != 2 {
			log.Error("Numpy parser: illegal shape of numpy file for binary vector field, shape should be 2", zap.Int("shape", len(shape)),
				zap.String("fieldName", fieldName))
			return fmt.Errorf("Numpy parser: illegal shape %d of numpy file for binary vector field '%s', shape should be 2", shape, schema.GetName())
		}

		// shape[0] is row count, shape[1] is element count per row
		p.columnDesc.elementCount = shape[0] * shape[1]

		p.columnDesc.dimension, err = getFieldDimension(schema)
		if err != nil {
			return err
		}

		if shape[1] != p.columnDesc.dimension/8 {
			log.Error("Numpy parser: illegal dimension of numpy file for float vector field", zap.String("fieldName", fieldName),
				zap.Int("numpyDimension", shape[1]*8), zap.Int("fieldDimension", p.columnDesc.dimension))
			return fmt.Errorf("Numpy parser: illegal dimension %d of numpy file for binary vector field '%s', dimension should be %d",
				shape[1]*8, schema.GetName(), p.columnDesc.dimension)
		}
	} else {
		if elementType != schema.DataType {
			log.Error("Numpy parser: illegal data type of numpy file for scalar field", zap.Any("numpyDataType", elementType),
				zap.String("fieldName", fieldName), zap.Any("fieldDataType", schema.DataType))
			return fmt.Errorf("Numpy parser: illegal data type %s of numpy file for scalar field '%s' with type %s",
				getTypeName(elementType), schema.GetName(), getTypeName(schema.DataType))
		}

		// scalar field, the shape should be 1
		if len(shape) != 1 {
			log.Error("Numpy parser: illegal shape of numpy file for scalar field, shape should be 1", zap.Int("shape", len(shape)),
				zap.String("fieldName", fieldName))
			return fmt.Errorf("Numpy parser: illegal shape %d of numpy file for scalar field '%s', shape should be 1", shape, schema.GetName())
		}

		p.columnDesc.elementCount = shape[0]
	}

	return nil
}

// consume method reads numpy data section into a storage.FieldData
// please note it will require a large memory block(the memory size is almost equal to numpy file size)
func (p *NumpyParser) consume(adapter *NumpyAdapter) error {
	switch p.columnDesc.dt {
	case schemapb.DataType_Bool:
		data, err := adapter.ReadBool(p.columnDesc.elementCount)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		p.columnData = &storage.BoolFieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
		}

	case schemapb.DataType_Int8:
		data, err := adapter.ReadInt8(p.columnDesc.elementCount)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		p.columnData = &storage.Int8FieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
		}
	case schemapb.DataType_Int16:
		data, err := adapter.ReadInt16(p.columnDesc.elementCount)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		p.columnData = &storage.Int16FieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
		}
	case schemapb.DataType_Int32:
		data, err := adapter.ReadInt32(p.columnDesc.elementCount)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		p.columnData = &storage.Int32FieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
		}
	case schemapb.DataType_Int64:
		data, err := adapter.ReadInt64(p.columnDesc.elementCount)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		p.columnData = &storage.Int64FieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
		}
	case schemapb.DataType_Float:
		data, err := adapter.ReadFloat32(p.columnDesc.elementCount)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		p.columnData = &storage.FloatFieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
		}
	case schemapb.DataType_Double:
		data, err := adapter.ReadFloat64(p.columnDesc.elementCount)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		p.columnData = &storage.DoubleFieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
		}
	case schemapb.DataType_VarChar:
		data, err := adapter.ReadString(p.columnDesc.elementCount)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		p.columnData = &storage.StringFieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
		}
	case schemapb.DataType_BinaryVector:
		data, err := adapter.ReadUint8(p.columnDesc.elementCount)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		p.columnData = &storage.BinaryVectorFieldData{
			NumRows: []int64{int64(p.columnDesc.elementCount)},
			Data:    data,
			Dim:     p.columnDesc.dimension,
		}
	case schemapb.DataType_FloatVector:
		// float32/float64 numpy file can be used for float vector file, 2 reasons:
		// 1. for float vector, we support float32 and float64 numpy file because python float value is 64 bit
		// 2. for float64 numpy file, the performance is worse than float32 numpy file
		elementType := adapter.GetType()

		var data []float32
		var err error
		if elementType == schemapb.DataType_Float {
			data, err = adapter.ReadFloat32(p.columnDesc.elementCount)
			if err != nil {
				log.Error(err.Error())
				return err
			}
		} else if elementType == schemapb.DataType_Double {
			data = make([]float32, 0, p.columnDesc.elementCount)
			data64, err := adapter.ReadFloat64(p.columnDesc.elementCount)
			if err != nil {
				log.Error(err.Error())
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
		log.Error("Numpy parser: unsupported data type of field", zap.Any("dataType", p.columnDesc.dt), zap.String("fieldName", p.columnDesc.name))
		return fmt.Errorf("Numpy parser: unsupported data type %s of field '%s'", getTypeName(p.columnDesc.dt), p.columnDesc.name)
	}

	return nil
}

func (p *NumpyParser) Parse(reader io.Reader, fieldName string, onlyValidate bool) error {
	adapter, err := NewNumpyAdapter(reader)
	if err != nil {
		return err
	}

	// the validation method only check the file header information
	err = p.validate(adapter, fieldName)
	if err != nil {
		return err
	}

	if onlyValidate {
		return nil
	}

	// read all data from the numpy file
	err = p.consume(adapter)
	if err != nil {
		return err
	}

	return p.callFlushFunc(p.columnData)
}
