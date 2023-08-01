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

package storage

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// PayloadWriterInterface abstracts PayloadWriter
type PayloadWriterInterface interface {
	AddDataToPayload(msgs any, valid ...bool) error
	AddBoolToPayload(msgs []bool, valid []bool) error
	AddByteToPayload(msgs []byte, valid []bool) error
	AddInt8ToPayload(msgs []int8, valid []bool) error
	AddInt16ToPayload(msgs []int16, valid []bool) error
	AddInt32ToPayload(msgs []int32, valid []bool) error
	AddInt64ToPayload(msgs []int64, valid []bool) error
	AddFloatToPayload(msgs []float32, valid []bool) error
	AddDoubleToPayload(msgs []float64, valid []bool) error
	AddOneStringToPayload(msgs string, valid bool) error
	AddOneArrayToPayload(msg *schemapb.ScalarField, valid bool) error
	AddOneJSONToPayload(msg []byte, valid bool) error
	AddBinaryVectorToPayload(binVec []byte, dim int) error
	AddFloatVectorToPayload(binVec []float32, dim int) error
	FinishPayloadWriter() error
	GetPayloadBufferFromWriter() ([]byte, error)
	GetPayloadLengthFromWriter() (int, error)
	ReleasePayloadWriter()
	Close()
}

// PayloadReaderInterface abstracts PayloadReader
type PayloadReaderInterface interface {
	GetDataFromPayload() (any, int, error)
	GetBoolFromPayload() ([]bool, error)
	GetByteFromPayload() ([]byte, error)
	GetInt8FromPayload() ([]int8, error)
	GetInt16FromPayload() ([]int16, error)
	GetInt32FromPayload() ([]int32, error)
	GetInt64FromPayload() ([]int64, error)
	GetFloatFromPayload() ([]float32, error)
	GetDoubleFromPayload() ([]float64, error)
	GetStringFromPayload() ([]string, error)
	GetArrayFromPayload() ([]*schemapb.ScalarField, error)
	GetJSONFromPayload() ([][]byte, error)
	GetBinaryVectorFromPayload() ([]byte, int, error)
	GetFloatVectorFromPayload() ([]float32, int, error)
	GetPayloadLengthFromReader() (int, error)
	ReleasePayloadReader() error
	Close() error
}
