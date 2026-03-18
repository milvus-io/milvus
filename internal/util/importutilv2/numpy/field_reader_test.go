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
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func encodeToGB2312(input string) ([]byte, error) {
	encoder := simplifiedchinese.GB18030.NewEncoder() // GB18030 is compatible with GB2312.
	var buf bytes.Buffer
	writer := transform.NewWriter(&buf, encoder)

	_, err := writer.Write([]byte(input))
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func WriteNonUTF8Npy() io.Reader {
	// Use bytes.Buffer instead of writing to a file
	var buffer bytes.Buffer

	// Step 1: Write Magic Number and version
	buffer.Write([]byte{0x93, 'N', 'U', 'M', 'P', 'Y'})
	buffer.Write([]byte{1, 0}) // Numpy 1.0 version

	// Step 2: Construct the header (using '|S20' for 20-byte fixed-length strings)
	header := "{'descr': '|S20', 'fortran_order': False, 'shape': (6,), }"
	headerBytes := []byte(header)

	// Pad the header to align its length to a multiple of 64 bytes
	padding := 64 - (10+len(headerBytes)+1)%64
	headerBytes = append(headerBytes, make([]byte, padding)...)
	headerBytes = append(headerBytes, '\n')

	// Write the header length and the header itself
	binary.Write(&buffer, binary.LittleEndian, uint16(len(headerBytes)))
	buffer.Write(headerBytes)

	// Step 3: Write non-UTF-8 string data (e.g., Latin1 encoded)
	testStrings := []string{
		"hello, vector database",               // valid utf-8
		string([]byte{0xC3, 0xA9, 0xB5, 0xF1}), // Latin1 encoded "éµñ"
		string([]byte{0xB0, 0xE1, 0xF2, 0xBD}), // Random non-UTF-8 data
		string([]byte{0xD2, 0xA9, 0xC8, 0xFC}), // Another set of non-UTF-8 data
	}

	for _, str := range testStrings {
		data := make([]byte, 20)
		copy(data, str)
		buffer.Write(data)
	}

	// Step 4: Encode and write GB-2312 Chinese strings
	chineseStrings := []string{
		"你好，世界", // "Hello, World"
		"向量数据库", // "Data Storage"
	}

	for _, str := range chineseStrings {
		gb2312Data, err := encodeToGB2312(str)
		if err != nil {
			panic(err)
		}
		data := make([]byte, 20)
		copy(data, gb2312Data)
		buffer.Write(data)
	}

	// Step 5: Convert buffer to a reader to simulate file reading
	reader := bytes.NewReader(buffer.Bytes())
	return reader
}

func TestInvalidUTF8(t *testing.T) {
	fieldSchema := &schemapb.FieldSchema{
		FieldID:    100,
		Name:       "str",
		DataType:   schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "256"}},
	}

	reader := WriteNonUTF8Npy()
	fr, err := NewFieldReader(reader, fieldSchema)
	assert.NoError(t, err)

	_, err = fr.Next(int64(6))
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "contains invalid UTF-8 data"))
}
