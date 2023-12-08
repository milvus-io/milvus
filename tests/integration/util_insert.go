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

package integration

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

func (s *MiniClusterSuite) WaitForFlush(ctx context.Context, segIDs []int64, flushTs uint64, dbName, collectionName string) {
	flushed := func() bool {
		resp, err := s.Cluster.Proxy.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
			SegmentIDs:     segIDs,
			FlushTs:        flushTs,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		if err != nil {
			return false
		}
		return resp.GetFlushed()
	}
	for !flushed() {
		select {
		case <-ctx.Done():
			s.FailNow("failed to wait for flush until ctx done")
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func waitingForFlush(ctx context.Context, cluster *MiniCluster, segIDs []int64) {
	flushed := func() bool {
		resp, err := cluster.Proxy.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
			SegmentIDs: segIDs,
		})
		if err != nil {
			return false
		}
		return resp.GetFlushed()
	}
	for !flushed() {
		select {
		case <-ctx.Done():
			panic("flush timeout")
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func NewInt64FieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: GenerateInt64Array(numRows),
					},
				},
			},
		},
	}
}

func NewStringFieldData(fieldName string, numRows int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: GenerateStringArray(numRows),
					},
				},
			},
		},
	}
}

func NewFloatVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: GenerateFloatVectors(numRows, dim),
					},
				},
			},
		},
	}
}

func NewFloat16VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Float16Vector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_Float16Vector{
					Float16Vector: GenerateFloat16Vectors(numRows, dim),
				},
			},
		},
	}
}

func NewBinaryVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_BinaryVector,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: GenerateBinaryVectors(numRows, dim),
				},
			},
		},
	}
}

func GenerateNumpyFile(filePath string, rowCount int, dType schemapb.DataType, typeParams []*commonpb.KeyValuePair) error {
	const DIM = 128
	if dType == schemapb.DataType_VarChar {
		var data []string
		for i := 0; i < rowCount; i++ {
			data = append(data, "str")
		}
		err := importutil.CreateNumpyFile(filePath, data)
		if err != nil {
			log.Warn("failed to create numpy file", zap.Error(err))
			return err
		}
	}
	if dType == schemapb.DataType_Int64 {
		var data []int64
		for i := 0; i < rowCount; i++ {
			data = append(data, int64(i))
		}
		err := importutil.CreateNumpyFile(filePath, data)
		if err != nil {
			log.Warn("failed to create numpy file", zap.Error(err))
			return err
		}
	}
	if dType == schemapb.DataType_FloatVector {
		dimStr, ok := funcutil.KeyValuePair2Map(typeParams)[common.DimKey]
		if !ok {
			return errors.New("FloatVector field needs dim parameter")
		}
		dim, err := strconv.Atoi(dimStr)
		if err != nil {
			return err
		}
		var data [][DIM]float32
		for i := 0; i < rowCount; i++ {
			vec := [DIM]float32{}
			for j := 0; j < dim; j++ {
				vec[j] = float32(i)
			}
			// v := reflect.Indirect(reflect.ValueOf(vec))
			// log.Info("type", zap.Any("type", v.Kind()))
			data = append(data, vec)
			// v2 := reflect.Indirect(reflect.ValueOf(data))
			// log.Info("type", zap.Any("type", v2.Kind()))
		}
		err = importutil.CreateNumpyFile(filePath, data)
		if err != nil {
			log.Warn("failed to create numpy file", zap.Error(err))
			return err
		}
	}
	return nil
}

func GenerateInt64Array(numRows int) []int64 {
	ret := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		ret[i] = int64(i)
	}
	return ret
}

func GenerateStringArray(numRows int) []string {
	ret := make([]string, numRows)
	for i := 0; i < numRows; i++ {
		ret[i] = fmt.Sprintf("%d", i)
	}
	return ret
}

func GenerateFloatVectors(numRows, dim int) []float32 {
	total := numRows * dim
	ret := make([]float32, 0, total)
	for i := 0; i < total; i++ {
		ret = append(ret, rand.Float32())
	}
	return ret
}

func GenerateBinaryVectors(numRows, dim int) []byte {
	total := (numRows * dim) / 8
	ret := make([]byte, total)
	_, err := rand.Read(ret)
	if err != nil {
		panic(err)
	}
	return ret
}

func GenerateFloat16Vectors(numRows, dim int) []byte {
	total := numRows * dim * 2
	ret := make([]byte, total)
	_, err := rand.Read(ret)
	if err != nil {
		panic(err)
	}
	return ret
}

func GenerateHashKeys(numRows int) []uint32 {
	ret := make([]uint32, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, rand.Uint32())
	}
	return ret
}
