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

package numpy

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/sbinet/npyio"
	"github.com/sbinet/npyio/npy"
	"io"
	"unicode/utf8"
)

type ColumnReader struct {
	reader    io.Reader
	npyReader *npy.Reader
	order     binary.ByteOrder

	dim   int64
	field *schemapb.FieldSchema
}

func NewColumnReader(reader io.Reader, field *schemapb.FieldSchema) (*ColumnReader, error) {
	r, err := npyio.NewReader(reader)
	if err != nil {
		return nil, err
	}
	// TODO: dyh, validate header

	//dataType, err := convertNumpyType(r.Header.Descr.Type)
	//if err != nil {
	//	return nil, err
	//}

	// TODO: dyh, check vector?
	//if dataType != field.GetDataType() {
	//	return nil, merr.WrapErrImportFailed(fmt.Sprintf("expect numpy data type '%s', got '%s'",
	//		field.GetDataType().String(), dataType.String()))
	//}

	var dim int64
	if typeutil.IsVectorType(field.GetDataType()) {
		dim, err = typeutil.GetDim(field)
		if err != nil {
			return nil, err
		}
	}

	cr := &ColumnReader{
		reader:    reader,
		npyReader: r,
		dim:       dim,
		field:     field,
	}
	cr.setByteOrder()
	return cr, nil
}

func ReadN[T any](reader io.Reader, order binary.ByteOrder, n int64) ([]T, error) {
	data := make([]T, n)
	err := binary.Read(reader, order, &data) // TODO: dyh, handle EOF
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *ColumnReader) Next(count int64) (storage.FieldData, error) {
	// TODO: validate shape and count
	dt := c.field.GetDataType()
	switch dt {
	case schemapb.DataType_Bool:
		data, err := ReadN[bool](c.reader, c.order, count)
		if err != nil {
			return nil, err
		}
		return &storage.BoolFieldData{Data: data}, nil
	case schemapb.DataType_Int8:
		data, err := ReadN[int8](c.reader, c.order, count)
		if err != nil {
			return nil, err
		}
		return &storage.Int8FieldData{Data: data}, nil
	case schemapb.DataType_Int16:
		data, err := ReadN[int16](c.reader, c.order, count)
		if err != nil {
			return nil, err
		}
		return &storage.Int16FieldData{Data: data}, nil
	case schemapb.DataType_Int32:
		data, err := ReadN[int32](c.reader, c.order, count)
		if err != nil {
			return nil, err
		}
		return &storage.Int32FieldData{Data: data}, nil
	case schemapb.DataType_Int64:
		data, err := ReadN[int64](c.reader, c.order, count)
		if err != nil {
			return nil, err
		}
		return &storage.Int64FieldData{Data: data}, nil
	case schemapb.DataType_Float:
		data, err := ReadN[float32](c.reader, c.order, count)
		if err != nil {
			return nil, err
		}
		return &storage.FloatFieldData{Data: data}, nil
	case schemapb.DataType_Double:
		data, err := ReadN[float64](c.reader, c.order, count)
		if err != nil {
			return nil, err
		}
		return &storage.DoubleFieldData{Data: data}, nil
	case schemapb.DataType_VarChar:
		data, err := c.ReadString(count)
		if err != nil {
			return nil, err
		}
		return &storage.StringFieldData{Data: data}, nil
	case schemapb.DataType_JSON:
		// JSON field read data from string array numpy
		data, err := c.ReadString(count)
		if err != nil {
			return nil, err
		}
		byteArr := make([][]byte, 0)
		for _, str := range data {
			var dummy interface{}
			err = json.Unmarshal([]byte(str), &dummy)
			if err != nil {
				return nil, merr.WrapErrImportFailed(
					fmt.Sprintf("failed to parse value '%v' for JSON field '%s', error: %v",
						str, c.field.GetName(), err))
			}
			byteArr = append(byteArr, []byte(str))
		}
		return &storage.JSONFieldData{Data: byteArr}, nil
	case schemapb.DataType_BinaryVector:
		data, err := ReadN[uint8](c.reader, c.order, count*(c.dim/8))
		if err != nil {
			return nil, err
		}
		return &storage.BinaryVectorFieldData{
			Data: data,
			Dim:  int(c.dim),
		}, nil
	case schemapb.DataType_FloatVector:
		// float32/float64 numpy file can be used for float vector file, 2 reasons:
		// 1. for float vector, we support float32 and float64 numpy file because python float value is 64 bit
		// 2. for float64 numpy file, the performance is worse than float32 numpy file
		elementType, err := convertNumpyType(c.npyReader.Header.Descr.Type)
		if err != nil {
			return nil, err
		}
		var data []float32
		switch elementType {
		case schemapb.DataType_Float:
			data, err = ReadN[float32](c.reader, c.order, count*c.dim)
			if err != nil {
				return nil, err
			}
			err = typeutil.VerifyFloats32(data)
			if err != nil {
				return nil, nil
			}
		case schemapb.DataType_Double:
			data64, err := ReadN[float64](c.reader, c.order, count*c.dim)
			if err != nil {
				return nil, err
			}
			err = typeutil.VerifyFloats64(data64)
			if err != nil {
				return nil, err
			}
			data = make([]float32, 0, count*c.dim)
			for _, f64 := range data64 {
				data = append(data, float32(f64))
			}
		default:
			return nil, merr.WrapErrImportFailed(
				fmt.Sprintf("unexpected element data type '%s' for floatVecField", dt.String()))
		}
		return &storage.FloatVectorFieldData{
			Data: data,
			Dim:  int(c.dim),
		}, nil
	default:
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("unexpected data type: %s", dt.String()))
	}
}

// setByteOrder sets BigEndian/LittleEndian, the logic of this method is copied from npyio lib
func (c *ColumnReader) setByteOrder() {
	var nativeEndian binary.ByteOrder
	v := uint16(1)
	switch byte(v >> 8) {
	case 0:
		nativeEndian = binary.LittleEndian
	case 1:
		nativeEndian = binary.BigEndian
	}

	switch c.npyReader.Header.Descr.Type[0] {
	case '<':
		c.order = binary.LittleEndian
	case '>':
		c.order = binary.BigEndian
	default:
		c.order = nativeEndian
	}
}

func (c *ColumnReader) GetShape() []int {
	return c.npyReader.Header.Descr.Shape
}

func (c *ColumnReader) ReadString(count int64) ([]string, error) {
	// varchar length, this is the max length, some item is shorter than this length, but they also occupy bytes of max length
	maxLen, utf, err := stringLen(c.npyReader.Header.Descr.Type)
	if err != nil || maxLen <= 0 {
		return nil, merr.WrapErrImportFailed(
			fmt.Sprintf("failed to get max length %d of varchar from numpy file header, error: %v", maxLen, err))
	}

	// read data
	data := make([]string, 0, count)
	for len(data) < int(count) {
		if utf {
			// in the numpy file with utf32 encoding, the dType could be like "<U2",
			// "<" is byteorder(LittleEndian), "U" means it is utf32 encoding, "2" means the max length of strings is 2(characters)
			// each character occupy 4 bytes, each string occupies 4*maxLen bytes
			// for example, a numpy file has two strings: "a" and "bb", the maxLen is 2, byte order is LittleEndian
			// the character "a" occupies 2*4=8 bytes(0x97,0x00,0x00,0x00,0x00,0x00,0x00,0x00),
			// the "bb" occupies 8 bytes(0x97,0x00,0x00,0x00,0x98,0x00,0x00,0x00)
			// for non-ascii characters, the unicode could be 1 ~ 4 bytes, each character occupies 4 bytes, too
			raw, err := io.ReadAll(io.LimitReader(c.reader, utf8.UTFMax*int64(maxLen)))
			if err != nil {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read utf32 bytes from numpy file, error: %v", err))
			}
			str, err := decodeUtf32(raw, c.order)
			if err != nil {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to decode utf32 bytes, error: %v", err))
			}
			data = append(data, str)
		} else {
			// in the numpy file with ansi encoding, the dType could be like "S2", maxLen is 2, each string occupies 2 bytes
			// bytes.Index(buf, []byte{0}) tell us which position is the end of the string
			buf, err := io.ReadAll(io.LimitReader(c.reader, int64(maxLen)))
			if err != nil {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read ascii bytes from numpy file, error: %v", err))
			}
			n := bytes.Index(buf, []byte{0})
			if n > 0 {
				buf = buf[:n]
			}
			data = append(data, string(buf))
		}
	}
	return data, nil
}
