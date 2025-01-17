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
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// PayloadWriterInterface abstracts PayloadWriter
type PayloadWriterInterface interface {
	AddDataToPayload(any, []bool) error
	AddBoolToPayload([]bool, []bool) error
	AddByteToPayload([]byte, []bool) error
	AddInt8ToPayload([]int8, []bool) error
	AddInt16ToPayload([]int16, []bool) error
	AddInt32ToPayload([]int32, []bool) error
	AddInt64ToPayload([]int64, []bool) error
	AddFloatToPayload([]float32, []bool) error
	AddDoubleToPayload([]float64, []bool) error
	AddOneStringToPayload(string, bool) error
	AddOneArrayToPayload(*schemapb.ScalarField, bool) error
	AddOneJSONToPayload([]byte, bool) error
	AddBinaryVectorToPayload([]byte, int) error
	AddFloatVectorToPayload([]float32, int) error
	AddFloat16VectorToPayload([]byte, int) error
	AddBFloat16VectorToPayload([]byte, int) error
	AddSparseFloatVectorToPayload(*SparseFloatVectorFieldData) error
	AddInt8VectorToPayload([]int8, int) error
	FinishPayloadWriter() error
	GetPayloadBufferFromWriter() ([]byte, error)
	GetPayloadLengthFromWriter() (int, error)
	ReleasePayloadWriter()
	Reserve(int)
	Close()
}

// PayloadReaderInterface abstracts PayloadReader
type PayloadReaderInterface interface {
	GetDataFromPayload() (any, []bool, int, error)
	GetBoolFromPayload() ([]bool, []bool, error)
	GetByteFromPayload() ([]byte, []bool, error)
	GetInt8FromPayload() ([]int8, []bool, error)
	GetInt16FromPayload() ([]int16, []bool, error)
	GetInt32FromPayload() ([]int32, []bool, error)
	GetInt64FromPayload() ([]int64, []bool, error)
	GetFloatFromPayload() ([]float32, []bool, error)
	GetDoubleFromPayload() ([]float64, []bool, error)
	GetStringFromPayload() ([]string, []bool, error)
	GetArrayFromPayload() ([]*schemapb.ScalarField, []bool, error)
	GetJSONFromPayload() ([][]byte, []bool, error)
	GetBinaryVectorFromPayload() ([]byte, int, error)
	GetFloat16VectorFromPayload() ([]byte, int, error)
	GetBFloat16VectorFromPayload() ([]byte, int, error)
	GetFloatVectorFromPayload() ([]float32, int, error)
	GetSparseFloatVectorFromPayload() (*SparseFloatVectorFieldData, int, error)
	GetInt8VectorFromPayload() ([]int8, int, error)
	GetPayloadLengthFromReader() (int, error)

	GetByteArrayDataSet() (*DataSet[parquet.ByteArray, *file.ByteArrayColumnChunkReader], error)
	GetArrowRecordReader() (pqarrow.RecordReader, error)

	ReleasePayloadReader() error
	Close() error
}
