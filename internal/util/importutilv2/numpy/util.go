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
	"encoding/binary"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"unicode/utf8"

	"github.com/sbinet/npyio"
	"github.com/sbinet/npyio/npy"
	"golang.org/x/text/encoding/unicode"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

var (
	reStrPre  = regexp.MustCompile(`^[|]*?(\d.*)[Sa]$`)
	reStrPost = regexp.MustCompile(`^[|]*?[Sa](\d.*)$`)
	reUniPre  = regexp.MustCompile(`^[<|>]*?(\d.*)U$`)
	reUniPost = regexp.MustCompile(`^[<|>]*?U(\d.*)$`)
)

func stringLen(dtype string) (int, bool, error) {
	var utf bool
	switch {
	case reStrPre.MatchString(dtype), reStrPost.MatchString(dtype):
		utf = false
	case reUniPre.MatchString(dtype), reUniPost.MatchString(dtype):
		utf = true
	}

	if m := reStrPre.FindStringSubmatch(dtype); m != nil {
		v, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, false, err
		}
		return v, utf, nil
	}
	if m := reStrPost.FindStringSubmatch(dtype); m != nil {
		v, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, false, err
		}
		return v, utf, nil
	}
	if m := reUniPre.FindStringSubmatch(dtype); m != nil {
		v, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, false, err
		}
		return v, utf, nil
	}
	if m := reUniPost.FindStringSubmatch(dtype); m != nil {
		v, err := strconv.Atoi(m[1])
		if err != nil {
			return 0, false, err
		}
		return v, utf, nil
	}

	return 0, false, merr.WrapErrImportFailed(fmt.Sprintf("dtype '%s' of numpy file is not varchar data type", dtype))
}

func decodeUtf32(src []byte, order binary.ByteOrder) (string, error) {
	if len(src)%4 != 0 {
		return "", merr.WrapErrImportFailed(fmt.Sprintf("invalid utf32 bytes length %d, the byte array length should be multiple of 4", len(src)))
	}

	var str string
	for len(src) > 0 {
		// check the high bytes, if high bytes are 0, the UNICODE is less than U+FFFF, we can use unicode.UTF16 to decode
		isUtf16 := false
		var lowbytesPosition int
		uOrder := unicode.LittleEndian
		if order == binary.LittleEndian {
			if src[2] == 0 && src[3] == 0 {
				isUtf16 = true
			}
			lowbytesPosition = 0
		} else {
			if src[0] == 0 && src[1] == 0 {
				isUtf16 = true
			}
			lowbytesPosition = 2
			uOrder = unicode.BigEndian
		}

		if isUtf16 {
			// use unicode.UTF16 to decode the low bytes to utf8
			// utf32 and utf16 is same if the unicode code is less than 65535
			if src[lowbytesPosition] != 0 || src[lowbytesPosition+1] != 0 {
				decoder := unicode.UTF16(uOrder, unicode.IgnoreBOM).NewDecoder()
				res, err := decoder.Bytes(src[lowbytesPosition : lowbytesPosition+2])
				if err != nil {
					return "", merr.WrapErrImportFailed(fmt.Sprintf("failed to decode utf32 binary bytes, error: %v", err))
				}
				str += string(res)
			}
		} else {
			// convert the 4 bytes to a unicode and encode to utf8
			// Golang strongly opposes utf32 coding, this kind of encoding has been excluded from standard lib
			var x uint32
			if order == binary.LittleEndian {
				x = uint32(src[3])<<24 | uint32(src[2])<<16 | uint32(src[1])<<8 | uint32(src[0])
			} else {
				x = uint32(src[0])<<24 | uint32(src[1])<<16 | uint32(src[2])<<8 | uint32(src[3])
			}
			r := rune(x)
			utf8Code := make([]byte, 4)
			utf8.EncodeRune(utf8Code, r)
			if r == utf8.RuneError {
				return "", merr.WrapErrImportFailed(fmt.Sprintf("failed to convert 4 bytes unicode %d to utf8 rune", x))
			}
			str += string(utf8Code)
		}

		src = src[4:]
	}
	return str, nil
}

// convertNumpyType gets data type converted from numpy header description,
// for vector field, the type is int8(binary vector) or float32(float vector)
func convertNumpyType(typeStr string) (schemapb.DataType, error) {
	switch typeStr {
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
		rt := npyio.TypeFrom(typeStr)
		if rt == reflect.TypeOf((*string)(nil)).Elem() {
			// Note: JSON field and VARCHAR field are using string type numpy
			return schemapb.DataType_VarChar, nil
		}
		return schemapb.DataType_None, merr.WrapErrImportFailed(
			fmt.Sprintf("the numpy file dtype '%s' is not supported", typeStr))
	}
}

func wrapElementTypeError(eleType schemapb.DataType, field *schemapb.FieldSchema) error {
	return merr.WrapErrImportFailed(fmt.Sprintf("expected element type '%s' for field '%s', got type '%T'",
		field.GetDataType().String(), field.GetName(), eleType))
}

func wrapDimError(actualDim int, expectDim int, field *schemapb.FieldSchema) error {
	return merr.WrapErrImportFailed(fmt.Sprintf("expected dim '%d' for %s field '%s', got dim '%d'",
		expectDim, field.GetDataType().String(), field.GetName(), actualDim))
}

func wrapShapeError(actualShape int, expectShape int, field *schemapb.FieldSchema) error {
	return merr.WrapErrImportFailed(fmt.Sprintf("expected shape '%d' for %s field '%s', got shape '%d'",
		expectShape, field.GetDataType().String(), field.GetName(), actualShape))
}

func validateHeader(npyReader *npy.Reader, field *schemapb.FieldSchema, dim int) error {
	elementType, err := convertNumpyType(npyReader.Header.Descr.Type)
	if err != nil {
		return err
	}
	shape := npyReader.Header.Descr.Shape

	switch field.GetDataType() {
	case schemapb.DataType_FloatVector:
		if elementType != schemapb.DataType_Float && elementType != schemapb.DataType_Double {
			return wrapElementTypeError(elementType, field)
		}
		if len(shape) != 2 {
			return wrapShapeError(len(shape), 2, field)
		}
		if shape[1] != dim {
			return wrapDimError(shape[1], dim, field)
		}
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		// TODO: need a better way to check the element type for float16/bfloat16
		if elementType != schemapb.DataType_BinaryVector {
			return wrapElementTypeError(elementType, field)
		}
		if len(shape) != 2 {
			return wrapShapeError(len(shape), 2, field)
		}
		if shape[1] != dim*2 {
			return wrapDimError(shape[1], dim, field)
		}
	case schemapb.DataType_BinaryVector:
		if elementType != schemapb.DataType_BinaryVector {
			return wrapElementTypeError(elementType, field)
		}
		if len(shape) != 2 {
			return wrapShapeError(len(shape), 2, field)
		}
		if shape[1] != dim/8 {
			return wrapDimError(shape[1]*8, dim, field)
		}
	case schemapb.DataType_VarChar, schemapb.DataType_JSON:
		if len(shape) != 1 {
			return wrapShapeError(len(shape), 1, field)
		}
	case schemapb.DataType_None, schemapb.DataType_Array:
		return merr.WrapErrImportFailed(fmt.Sprintf("unsupported data type: %s", field.GetDataType().String()))

	default:
		if elementType != field.GetDataType() {
			return wrapElementTypeError(elementType, field)
		}
		if len(shape) != 1 {
			return wrapShapeError(len(shape), 1, field)
		}
	}
	return nil
}
