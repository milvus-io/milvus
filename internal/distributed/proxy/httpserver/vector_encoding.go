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

package httpserver

import (
	"encoding/base64"
	"encoding/binary"
	"math"
	"net/http"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// floatVectorEncoding controls only FLOAT_VECTOR output, including vectors in
// struct arrays. An omitted/empty option preserves the numeric-array API.
type floatVectorEncoding string

func parseFloatVectorEncoding(header http.Header) (floatVectorEncoding, error) {
	values := header.Values(HTTPHeaderFloatVectorEncoding)
	if len(values) > 1 {
		return "", merr.WrapErrParameterInvalidMsg("%s must contain a single value: array or base64", HTTPHeaderFloatVectorEncoding)
	}
	value := strings.Trim(header.Get(HTTPHeaderFloatVectorEncoding), " \t")
	switch value {
	case "", "array", "base64":
		return floatVectorEncoding(value), nil
	default:
		return "", merr.WrapErrParameterInvalidMsg("%s must be array or base64", HTTPHeaderFloatVectorEncoding)
	}
}

func (e floatVectorEncoding) floatVector(values []float32, vectorEncodingBuffer *[]byte) any {
	if e == "base64" && values != nil {
		return base64FloatVector{values: values, vectorEncodingBuffer: vectorEncodingBuffer}
	}
	return values
}

// base64FloatVector retains the original vector slice. Bytes are materialized
// only when the response encoder visits this field, never for the whole result.
type base64FloatVector struct {
	values               []float32
	vectorEncodingBuffer *[]byte
}

func (v base64FloatVector) MarshalText() ([]byte, error) {
	// The buffer belongs to one projected response and is used by one encoder at
	// a time. Sonic copies this borrowed text into its JSON buffer before visiting
	// the next value. Do not retain the text or encode the same response concurrently.
	// TextMarshaler leaves JSON quoting/escaping to
	// Sonic, without JSONMarshaler's additional JSON compaction pass. It also
	// avoids Sonic's []byte encoder's small capacity increments, which can copy
	// the accumulated batch once per vector (notably on ARM64).
	rawLen := len(v.values) * 4
	encodedLen := base64.StdEncoding.EncodedLen(rawLen)
	size := rawLen + encodedLen
	if cap(*v.vectorEncodingBuffer) < size {
		*v.vectorEncodingBuffer = make([]byte, size)
	}
	storage := (*v.vectorEncodingBuffer)[:size]
	raw, encoded := storage[:rawLen], storage[rawLen:]
	for i, value := range v.values {
		bits := math.Float32bits(value)
		if bits&0x7f800000 == 0x7f800000 {
			return nil, merr.WrapErrServiceInternalMsg("non-finite FLOAT_VECTOR coordinate in result")
		}
		binary.LittleEndian.PutUint32(raw[i*4:], bits)
	}
	base64.StdEncoding.Encode(encoded, raw)
	return encoded, nil
}
