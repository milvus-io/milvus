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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"unicode/utf8"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/sbinet/npyio"
	"github.com/sbinet/npyio/npy"
	"go.uber.org/zap"
	"golang.org/x/text/encoding/unicode"
)

var (
	reStrPre  = regexp.MustCompile(`^[|]*?(\d.*)[Sa]$`)
	reStrPost = regexp.MustCompile(`^[|]*?[Sa](\d.*)$`)
	reUniPre  = regexp.MustCompile(`^[<|>]*?(\d.*)U$`)
	reUniPost = regexp.MustCompile(`^[<|>]*?U(\d.*)$`)
)

func CreateNumpyFile(path string, data interface{}) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	err = npyio.Write(f, data)
	if err != nil {
		return err
	}

	return nil
}

func CreateNumpyData(data interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := npyio.Write(buf, data)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// NumpyAdapter is the class to expand other numpy lib ability
// we evaluate two go-numpy lins: github.com/kshedden/gonpy and github.com/sbinet/npyio
// the npyio lib read data one by one, the performance is poor, we expand the read methods
// to read data in one batch, the performance is 100X faster
// the gonpy lib also read data in one batch, but it has no method to read bool data, and the ability
// to handle different data type is not strong as the npylib, so we choose the npyio lib to expand.
type NumpyAdapter struct {
	reader       io.Reader         // data source, typically is os.File
	npyReader    *npy.Reader       // reader of npyio lib
	order        binary.ByteOrder  // LittleEndian or BigEndian
	readPosition int               // how many elements have been read
	dataType     schemapb.DataType // data type parsed from numpy file header
}

func NewNumpyAdapter(reader io.Reader) (*NumpyAdapter, error) {
	r, err := npyio.NewReader(reader)
	if err != nil {
		log.Warn("Numpy adapter: failed to read numpy header", zap.Error(err))
		return nil, err
	}

	dataType, err := convertNumpyType(r.Header.Descr.Type)
	if err != nil {
		log.Warn("Numpy adapter: failed to detect data type", zap.Error(err))
		return nil, err
	}

	adapter := &NumpyAdapter{
		reader:       reader,
		npyReader:    r,
		readPosition: 0,
		dataType:     dataType,
	}
	adapter.setByteOrder()

	log.Info("Numpy adapter: numpy header info",
		zap.Any("shape", r.Header.Descr.Shape),
		zap.String("dType", r.Header.Descr.Type),
		zap.Uint8("majorVer", r.Header.Major),
		zap.Uint8("minorVer", r.Header.Minor),
		zap.String("ByteOrder", adapter.order.String()))

	return adapter, nil
}

// convertNumpyType gets data type converted from numpy header description, for vector field, the type is int8(binary vector) or float32(float vector)
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
		if isStringType(typeStr) {
			// Note: JSON field and VARCHAR field are using string type numpy
			return schemapb.DataType_VarChar, nil
		}
		log.Warn("Numpy adapter: the numpy file data type is not supported", zap.String("dtype", typeStr))
		return schemapb.DataType_None, fmt.Errorf("the numpy file dtype '%s' is not supported", typeStr)
	}
}

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

	log.Warn("Numpy adapter: the numpy file dtype is not varchar data type", zap.String("dtype", dtype))
	return 0, false, fmt.Errorf("dtype '%s' of numpy file is not varchar data type", dtype)
}

func isStringType(typeStr string) bool {
	rt := npyio.TypeFrom(typeStr)
	return rt == reflect.TypeOf((*string)(nil)).Elem()
}

// setByteOrder sets BigEndian/LittleEndian, the logic of this method is copied from npyio lib
func (n *NumpyAdapter) setByteOrder() {
	var nativeEndian binary.ByteOrder
	v := uint16(1)
	switch byte(v >> 8) {
	case 0:
		nativeEndian = binary.LittleEndian
	case 1:
		nativeEndian = binary.BigEndian
	}

	switch n.npyReader.Header.Descr.Type[0] {
	case '<':
		n.order = binary.LittleEndian
	case '>':
		n.order = binary.BigEndian
	default:
		n.order = nativeEndian
	}
}

func (n *NumpyAdapter) Reader() io.Reader {
	return n.reader
}

func (n *NumpyAdapter) NpyReader() *npy.Reader {
	return n.npyReader
}

func (n *NumpyAdapter) GetType() schemapb.DataType {
	return n.dataType
}

func (n *NumpyAdapter) GetShape() []int {
	return n.npyReader.Header.Descr.Shape
}

func (n *NumpyAdapter) checkCount(count int) int {
	shape := n.GetShape()

	// empty file?
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

	// overflow?
	if count > (total - n.readPosition) {
		return total - n.readPosition
	}

	return count
}

func (n *NumpyAdapter) ReadBool(count int) ([]bool, error) {
	if count <= 0 {
		log.Warn("Numpy adapter: cannot read bool data with a zero or nagative count")
		return nil, errors.New("cannot read bool data with a zero or nagative count")
	}

	// incorrect type
	if n.dataType != schemapb.DataType_Bool {
		log.Warn("Numpy adapter: numpy data is not bool type")
		return nil, errors.New("numpy data is not bool type")
	}

	// avoid read overflow
	readSize := n.checkCount(count)
	if readSize <= 0 {
		// end of file, nothing to read
		log.Info("Numpy adapter: read to end of file, type: bool")
		return nil, nil
	}

	// read data
	data := make([]bool, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		log.Warn("Numpy adapter: failed to read bool data", zap.Int("count", count), zap.Error(err))
		return nil, fmt.Errorf(" failed to read bool data with count %d, error: %w", readSize, err)
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func (n *NumpyAdapter) ReadUint8(count int) ([]uint8, error) {
	if count <= 0 {
		log.Warn("Numpy adapter: cannot read uint8 data with a zero or nagative count")
		return nil, errors.New("cannot read uint8 data with a zero or nagative count")
	}

	// incorrect type
	// here we don't use n.dataType to check because currently milvus has no uint8 type
	switch n.npyReader.Header.Descr.Type {
	case "u1", "<u1", "|u1", "uint8":
	default:
		log.Warn("Numpy adapter: numpy data is not uint8 type")
		return nil, errors.New("numpy data is not uint8 type")
	}

	// avoid read overflow
	readSize := n.checkCount(count)
	if readSize <= 0 {
		// end of file, nothing to read
		log.Info("Numpy adapter: read to end of file, type: uint8")
		return nil, nil
	}

	// read data
	data := make([]uint8, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		log.Warn("Numpy adapter: failed to read uint8 data", zap.Int("count", count), zap.Error(err))
		return nil, fmt.Errorf("failed to read uint8 data with count %d, error: %w", readSize, err)
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func (n *NumpyAdapter) ReadInt8(count int) ([]int8, error) {
	if count <= 0 {
		log.Warn("Numpy adapter: cannot read int8 data with a zero or nagative count")
		return nil, errors.New("cannot read int8 data with a zero or nagative count")
	}

	// incorrect type
	if n.dataType != schemapb.DataType_Int8 {
		log.Warn("Numpy adapter: numpy data is not int8 type")
		return nil, errors.New("numpy data is not int8 type")
	}

	// avoid read overflow
	readSize := n.checkCount(count)
	if readSize <= 0 {
		// end of file, nothing to read
		log.Info("Numpy adapter: read to end of file, type: int8")
		return nil, nil
	}

	// read data
	data := make([]int8, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		log.Warn("Numpy adapter: failed to read int8 data", zap.Int("count", count), zap.Error(err))
		return nil, fmt.Errorf("failed to read int8 data with count %d, error: %w", readSize, err)
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func (n *NumpyAdapter) ReadInt16(count int) ([]int16, error) {
	if count <= 0 {
		log.Warn("Numpy adapter: cannot read int16 data with a zero or nagative count")
		return nil, errors.New("cannot read int16 data with a zero or nagative count")
	}

	// incorrect type
	if n.dataType != schemapb.DataType_Int16 {
		log.Warn("Numpy adapter: numpy data is not int16 type")
		return nil, errors.New("numpy data is not int16 type")
	}

	// avoid read overflow
	readSize := n.checkCount(count)
	if readSize <= 0 {
		// end of file, nothing to read
		log.Info("Numpy adapter: read to end of file, type: int16")
		return nil, nil
	}

	// read data
	data := make([]int16, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		log.Warn("Numpy adapter: failed to read int16 data", zap.Int("count", count), zap.Error(err))
		return nil, fmt.Errorf("failed to read int16 data with count %d, error: %w", readSize, err)
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func (n *NumpyAdapter) ReadInt32(count int) ([]int32, error) {
	if count <= 0 {
		log.Warn("Numpy adapter: cannot read int32 data with a zero or nagative count")
		return nil, errors.New("cannot read int32 data with a zero or nagative count")
	}

	// incorrect type
	if n.dataType != schemapb.DataType_Int32 {
		log.Warn("Numpy adapter: numpy data is not int32 type")
		return nil, errors.New("numpy data is not int32 type")
	}

	// avoid read overflow
	readSize := n.checkCount(count)
	if readSize <= 0 {
		// end of file, nothing to read
		log.Info("Numpy adapter: read to end of file, type: int32")
		return nil, nil
	}

	// read data
	data := make([]int32, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		log.Warn("Numpy adapter: failed to read int32 data", zap.Int("count", count), zap.Error(err))
		return nil, fmt.Errorf("failed to read int32 data with count %d, error: %w", readSize, err)
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func (n *NumpyAdapter) ReadInt64(count int) ([]int64, error) {
	if count <= 0 {
		log.Warn("Numpy adapter: cannot read int64 data with a zero or nagative count")
		return nil, errors.New("cannot read int64 data with a zero or nagative count")
	}

	// incorrect type
	if n.dataType != schemapb.DataType_Int64 {
		log.Warn("Numpy adapter: numpy data is not int64 type")
		return nil, errors.New("numpy data is not int64 type")
	}

	// avoid read overflow
	readSize := n.checkCount(count)
	if readSize <= 0 {
		// end of file, nothing to read
		log.Info("Numpy adapter: read to end of file, type: int64")
		return nil, nil
	}

	// read data
	data := make([]int64, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		log.Warn("Numpy adapter: failed to read int64 data", zap.Int("count", count), zap.Error(err))
		return nil, fmt.Errorf("failed to read int64 data with count %d, error: %w", readSize, err)
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func (n *NumpyAdapter) ReadFloat32(count int) ([]float32, error) {
	if count <= 0 {
		log.Warn("Numpy adapter: cannot read float32 data with a zero or nagative count")
		return nil, errors.New("cannot read float32 data with a zero or nagative count")
	}

	// incorrect type
	if n.dataType != schemapb.DataType_Float {
		log.Warn("Numpy adapter: numpy data is not float32 type")
		return nil, errors.New("numpy data is not float32 type")
	}

	// avoid read overflow
	readSize := n.checkCount(count)
	if readSize <= 0 {
		// end of file, nothing to read
		log.Info("Numpy adapter: read to end of file, type: float32")
		return nil, nil
	}

	// read data
	data := make([]float32, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		log.Warn("Numpy adapter: failed to read float32 data", zap.Int("count", count), zap.Error(err))
		return nil, fmt.Errorf("failed to read float32 data with count %d, error: %w", readSize, err)
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func (n *NumpyAdapter) ReadFloat64(count int) ([]float64, error) {
	if count <= 0 {
		log.Warn("Numpy adapter: cannot read float64 data with a zero or nagative count")
		return nil, errors.New("cannot read float64 data with a zero or nagative count")
	}

	// incorrect type
	if n.dataType != schemapb.DataType_Double {
		log.Warn("Numpy adapter: numpy data is not float64 type")
		return nil, errors.New("numpy data is not float64 type")
	}

	// avoid read overflow
	readSize := n.checkCount(count)
	if readSize <= 0 {
		// end of file, nothing to read
		log.Info("Numpy adapter: read to end of file, type: float64")
		return nil, nil
	}

	// read data
	data := make([]float64, readSize)
	err := binary.Read(n.reader, n.order, &data)
	if err != nil {
		log.Warn("Numpy adapter: failed to read float64 data", zap.Int("count", count), zap.Error(err))
		return nil, fmt.Errorf("failed to read float64 data with count %d, error: %w", readSize, err)
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func (n *NumpyAdapter) ReadString(count int) ([]string, error) {
	if count <= 0 {
		log.Warn("Numpy adapter: cannot read varchar data with a zero or nagative count")
		return nil, errors.New("cannot read varchar data with a zero or nagative count")
	}

	// incorrect type
	if n.dataType != schemapb.DataType_VarChar {
		log.Warn("Numpy adapter: numpy data is not varchar type")
		return nil, errors.New("numpy data is not varchar type")
	}

	// varchar length, this is the max length, some item is shorter than this length, but they also occupy bytes of max length
	maxLen, utf, err := stringLen(n.npyReader.Header.Descr.Type)
	if err != nil || maxLen <= 0 {
		log.Warn("Numpy adapter: failed to get max length of varchar from numpy file header", zap.Int("maxLen", maxLen), zap.Error(err))
		return nil, fmt.Errorf("failed to get max length %d of varchar from numpy file header, error: %w", maxLen, err)
	}
	// log.Info("Numpy adapter: get varchar max length from numpy file header", zap.Int("maxLen", maxLen), zap.Bool("utf", utf))

	// avoid read overflow
	readSize := n.checkCount(count)
	if readSize <= 0 {
		// end of file, nothing to read
		log.Info("Numpy adapter: read to end of file, type: varchar")
		return nil, nil
	}

	if n.reader == nil {
		log.Warn("Numpy adapter: reader is nil")
		return nil, errors.New("numpy reader is nil")
	}

	// read data
	data := make([]string, 0)
	for i := 0; i < readSize; i++ {
		if utf {
			// in the numpy file with utf32 encoding, the dType could be like "<U2",
			// "<" is byteorder(LittleEndian), "U" means it is utf32 encoding, "2" means the max length of strings is 2(characters)
			// each character occupy 4 bytes, each string occupys 4*maxLen bytes
			// for example, a numpy file has two strings: "a" and "bb", the maxLen is 2, byte order is LittleEndian
			// the character "a" occupys 2*4=8 bytes(0x97,0x00,0x00,0x00,0x00,0x00,0x00,0x00),
			// the "bb" occupys 8 bytes(0x97,0x00,0x00,0x00,0x98,0x00,0x00,0x00)
			// for non-ascii characters, the unicode could be 1 ~ 4 bytes, each character occupys 4 bytes, too
			raw, err := ioutil.ReadAll(io.LimitReader(n.reader, utf8.UTFMax*int64(maxLen)))
			if err != nil {
				log.Warn("Numpy adapter: failed to read utf32 bytes from numpy file", zap.Int("i", i), zap.Error(err))
				return nil, fmt.Errorf("failed to read utf32 bytes from numpy file, error: %w", err)
			}

			str, err := decodeUtf32(raw, n.order)
			if err != nil {
				log.Warn("Numpy adapter: failed todecode utf32 bytes", zap.Int("i", i), zap.Error(err))
				return nil, fmt.Errorf("failed to decode utf32 bytes, error: %w", err)
			}

			data = append(data, str)
		} else {
			// in the numpy file with ansi encoding, the dType could be like "S2", maxLen is 2, each string occupys 2 bytes
			// bytes.Index(buf, []byte{0}) tell us which position is the end of the string
			buf, err := ioutil.ReadAll(io.LimitReader(n.reader, int64(maxLen)))
			if err != nil {
				log.Warn("Numpy adapter: failed to read ascii bytes from numpy file", zap.Int("i", i), zap.Error(err))
				return nil, fmt.Errorf("failed to read ascii bytes from numpy file, error: %w", err)
			}
			n := bytes.Index(buf, []byte{0})
			if n > 0 {
				buf = buf[:n]
			}

			data = append(data, string(buf))
		}
	}

	// update read position after successfully read
	n.readPosition += readSize

	return data, nil
}

func decodeUtf32(src []byte, order binary.ByteOrder) (string, error) {
	if len(src)%4 != 0 {
		log.Warn("Numpy adapter: invalid utf32 bytes length, the byte array length should be multiple of 4", zap.Int("byteLen", len(src)))
		return "", fmt.Errorf("invalid utf32 bytes length %d, the byte array length should be multiple of 4", len(src))
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
					log.Warn("Numpy adapter: failed to decode utf32 binary bytes", zap.Error(err))
					return "", fmt.Errorf("failed to decode utf32 binary bytes, error: %w", err)
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
				log.Warn("Numpy adapter: failed to convert 4 bytes unicode to utf8 rune", zap.Uint32("code", x))
				return "", fmt.Errorf("failed to convert 4 bytes unicode %d to utf8 rune", x)
			}
			str += string(utf8Code)
		}

		src = src[4:]
	}
	return str, nil
}
