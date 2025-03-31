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
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/samber/lo"
	"github.com/sbinet/npyio"
	"github.com/sbinet/npyio/npy"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/parameterutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type FieldReader struct {
	reader    io.Reader
	npyReader *npy.Reader
	order     binary.ByteOrder

	dim   int64
	field *schemapb.FieldSchema

	readPosition int
}

func NewFieldReader(reader io.Reader, field *schemapb.FieldSchema) (*FieldReader, error) {
	r, err := npyio.NewReader(reader)
	if err != nil {
		return nil, err
	}

	var dim int64 = 1
	dataType := field.GetDataType()
	if typeutil.IsVectorType(dataType) && !typeutil.IsSparseFloatVectorType(dataType) {
		dim, err = typeutil.GetDim(field)
		if err != nil {
			return nil, err
		}
	}

	err = validateHeader(r, field, int(dim))
	if err != nil {
		return nil, err
	}

	cr := &FieldReader{
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
	err := binary.Read(reader, order, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *FieldReader) getCount(count int64) int64 {
	shape := c.npyReader.Header.Descr.Shape
	if len(shape) == 0 {
		return 0
	}
	total := 1
	for i := 0; i < len(shape); i++ {
		total *= shape[i]
	}
	if total == 0 {
		return 0
	}
	switch c.field.GetDataType() {
	case schemapb.DataType_BinaryVector:
		count *= c.dim / 8
	case schemapb.DataType_FloatVector:
		count *= c.dim
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		count *= c.dim * 2
	}
	if int(count) > (total - c.readPosition) {
		return int64(total - c.readPosition)
	}
	return count
}

func (c *FieldReader) Next(count int64) (any, error) {
	readCount := c.getCount(count)
	if readCount == 0 {
		return nil, nil
	}
	var (
		data any
		err  error
	)
	dt := c.field.GetDataType()
	switch dt {
	case schemapb.DataType_Bool:
		data, err = ReadN[bool](c.reader, c.order, readCount)
		if err != nil {
			return nil, err
		}
		c.readPosition += int(readCount)
	case schemapb.DataType_Int8:
		data, err = ReadN[int8](c.reader, c.order, readCount)
		if err != nil {
			return nil, err
		}
		c.readPosition += int(readCount)
	case schemapb.DataType_Int16:
		data, err = ReadN[int16](c.reader, c.order, readCount)
		if err != nil {
			return nil, err
		}
		c.readPosition += int(readCount)
	case schemapb.DataType_Int32:
		data, err = ReadN[int32](c.reader, c.order, readCount)
		if err != nil {
			return nil, err
		}
		c.readPosition += int(readCount)
	case schemapb.DataType_Int64:
		data, err = ReadN[int64](c.reader, c.order, readCount)
		if err != nil {
			return nil, err
		}
		c.readPosition += int(readCount)
	case schemapb.DataType_Float:
		data, err = ReadN[float32](c.reader, c.order, readCount)
		if err != nil {
			return nil, err
		}
		c.readPosition += int(readCount)
	case schemapb.DataType_Double:
		data, err = ReadN[float64](c.reader, c.order, readCount)
		if err != nil {
			return nil, err
		}
		c.readPosition += int(readCount)
	case schemapb.DataType_VarChar:
		data, err = c.ReadString(readCount)
		c.readPosition += int(readCount)
		if err != nil {
			return nil, err
		}
	case schemapb.DataType_JSON:
		var strs []string
		strs, err = c.ReadString(readCount)
		if err != nil {
			return nil, err
		}
		byteArr := make([][]byte, 0)
		for _, str := range strs {
			var dummy interface{}
			err = json.Unmarshal([]byte(str), &dummy)
			if err != nil {
				return nil, merr.WrapErrImportFailed(
					fmt.Sprintf("failed to parse value '%v' for JSON field '%s', error: %v", str, c.field.GetName(), err))
			}
			if c.field.GetIsDynamic() {
				var dummy2 map[string]interface{}
				err = json.Unmarshal([]byte(str), &dummy2)
				if err != nil {
					return nil, merr.WrapErrImportFailed(
						fmt.Sprintf("failed to parse value '%v' for dynamic JSON field '%s', error: %v",
							str, c.field.GetName(), err))
				}
			}
			byteArr = append(byteArr, []byte(str))
		}
		data = byteArr
		c.readPosition += int(readCount)
	case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		data, err = ReadN[uint8](c.reader, c.order, readCount)
		if err != nil {
			return nil, err
		}
		c.readPosition += int(readCount)
	case schemapb.DataType_FloatVector:
		var elementType schemapb.DataType
		elementType, err = convertNumpyType(c.npyReader.Header.Descr.Type)
		if err != nil {
			return nil, err
		}
		switch elementType {
		case schemapb.DataType_Float:
			data, err = ReadN[float32](c.reader, c.order, readCount)
			if err != nil {
				return nil, err
			}
			err = typeutil.VerifyFloats32(data.([]float32))
			if err != nil {
				return nil, nil
			}
		case schemapb.DataType_Double:
			var data64 []float64
			data64, err = ReadN[float64](c.reader, c.order, readCount)
			if err != nil {
				return nil, err
			}
			err = typeutil.VerifyFloats64(data64)
			if err != nil {
				return nil, err
			}
			data = lo.Map(data64, func(f float64, _ int) float32 {
				return float32(f)
			})
		}
		c.readPosition += int(readCount)
	default:
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported data type: %s", dt.String()))
	}
	return data, nil
}

func (c *FieldReader) Close() {}

// setByteOrder sets BigEndian/LittleEndian, the logic of this method is copied from npyio lib
func (c *FieldReader) setByteOrder() {
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

func (c *FieldReader) ReadString(count int64) ([]string, error) {
	// varchar length, this is the max length, some item is shorter than this length, but they also occupy bytes of max length
	maxLen, utf, err := stringLen(c.npyReader.Header.Descr.Type)
	if err != nil || maxLen <= 0 {
		return nil, merr.WrapErrImportFailed(
			fmt.Sprintf("failed to get max length %d of varchar from numpy file header, error: %v", maxLen, err))
	}
	maxLength, err := parameterutil.GetMaxLength(c.field)
	if c.field.DataType == schemapb.DataType_VarChar && err != nil {
		return nil, err
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
			if c.field.DataType == schemapb.DataType_VarChar {
				if err = common.CheckVarcharLength(str, maxLength, c.field); err != nil {
					return nil, err
				}
			}
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
			str := string(buf)
			if err = common.CheckValidUTF8(str, c.field); err != nil {
				return nil, err
			}
			data = append(data, str)
		}
	}
	return data, nil
}
