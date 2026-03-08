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

package compactor

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func testSegmentWriterBatchSize(b *testing.B, batchSize int) {
	orgLevel := log.GetLevel()
	log.SetLevel(zapcore.InfoLevel)
	defer log.SetLevel(orgLevel)
	paramtable.Init()

	const (
		dim     = 128
		numRows = 1000000
	)

	var (
		rId  = &schemapb.FieldSchema{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64}
		ts   = &schemapb.FieldSchema{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64}
		pk   = &schemapb.FieldSchema{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "100"}}}
		f    = &schemapb.FieldSchema{FieldID: 101, Name: "random", DataType: schemapb.DataType_Double}
		fVec = &schemapb.FieldSchema{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.Itoa(dim)}}}
	)
	schema := &schemapb.CollectionSchema{Name: "test-aaa", Fields: []*schemapb.FieldSchema{rId, ts, pk, f, fVec}}

	// prepare data values
	start := time.Now()
	vec := make([]float32, dim)
	for j := 0; j < dim; j++ {
		vec[j] = rand.Float32()
	}
	values := make([]*storage.Value, numRows)
	for i := 0; i < numRows; i++ {
		value := &storage.Value{}
		value.Value = make(map[int64]interface{}, len(schema.GetFields()))
		m := value.Value.(map[int64]interface{})
		for _, field := range schema.GetFields() {
			switch field.GetDataType() {
			case schemapb.DataType_Int64:
				m[field.GetFieldID()] = int64(i)
			case schemapb.DataType_VarChar:
				k := fmt.Sprintf("test_pk_%d", i)
				m[field.GetFieldID()] = k
				value.PK = &storage.VarCharPrimaryKey{
					Value: k,
				}
			case schemapb.DataType_Double:
				m[field.GetFieldID()] = float64(i)
			case schemapb.DataType_FloatVector:
				m[field.GetFieldID()] = vec
			}
		}
		value.ID = int64(i)
		value.Timestamp = int64(0)
		value.IsDeleted = false
		value.Value = m
		values[i] = value
	}
	log.Info("prepare data done", zap.Int("len", len(values)), zap.Duration("dur", time.Since(start)))

	writer, err := NewSegmentWriter(schema, numRows, batchSize, 1, 2, 3, nil)
	assert.NoError(b, err)

	b.N = 10
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start = time.Now()
		for _, v := range values {
			err = writer.Write(v)
			assert.NoError(b, err)
		}
		log.Info("write done", zap.Int("len", len(values)), zap.Duration("dur", time.Since(start)))
	}
	b.StopTimer()
}

func Benchmark_SegmentWriter_BatchSize_100(b *testing.B) {
	testSegmentWriterBatchSize(b, 100)
}

func Benchmark_SegmentWriter_BatchSize_1000(b *testing.B) {
	testSegmentWriterBatchSize(b, 1000)
}

func Benchmark_SegmentWriter_BatchSize_10000(b *testing.B) {
	testSegmentWriterBatchSize(b, 10000)
}
