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
	"encoding/json"
	"math"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// outputVectorEncoding controls only FLOAT_VECTOR output, including vectors in
// struct arrays. An omitted/empty option preserves the numeric-array API.
type outputVectorEncoding string

func (e *outputVectorEncoding) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return merr.WrapErrParameterInvalidMsg("outputVectorEncoding must be a string: array or base64")
	}
	switch value {
	case "", "array", "base64":
		*e = outputVectorEncoding(value)
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg("outputVectorEncoding must be array or base64")
	}
}

func (e outputVectorEncoding) floatVector(values []float32, scratch *[]byte) any {
	if e == "base64" && values != nil {
		return base64FloatVector{values: values, scratch: scratch}
	}
	return values
}

// base64FloatVector retains the original vector slice. Bytes are materialized
// only when the response encoder visits this field, never for the whole result.
type base64FloatVector struct {
	values  []float32
	scratch *[]byte
}

func (v base64FloatVector) MarshalText() ([]byte, error) {
	// All vectors in one result share scratch during serial encoding. Sonic
	// copies MarshalText's output into its JSON buffer before visiting the next
	// value, so the next vector may overwrite this storage. Do not share a result
	// between concurrent encoders or retain MarshalText's borrowed bytes.
	// TextMarshaler leaves JSON quoting/escaping to
	// Sonic, without JSONMarshaler's additional JSON compaction pass. It also
	// avoids Sonic's []byte encoder's small capacity increments, which can copy
	// the accumulated batch once per vector (notably on ARM64).
	rawLen := len(v.values) * 4
	encodedLen := base64.StdEncoding.EncodedLen(rawLen)
	size := rawLen + encodedLen
	if cap(*v.scratch) < size {
		*v.scratch = make([]byte, size)
	}
	storage := (*v.scratch)[:size]
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
