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

package clustering

import (
	"encoding/binary"
	"math"
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

const (
	ClusteringCentroid    = "clustering.centroid"
	ClusteringSize        = "clustering.size"
	ClusteringId          = "clustering.id"
	ClusteringOperationid = "clustering.operationID"

	SearchClusteringFilterRatio = "clustering.filter_ratio"
)

func SearchClusteringOptions(kv []*commonpb.KeyValuePair) (*internalpb.SearchClusteringOptions, error) {
	kvMap := funcutil.KeyValuePair2Map(kv)

	clusteringOptions := &internalpb.SearchClusteringOptions{
		FilterRatio: 0.5, // default
	}

	if clusterBasedFilterRatio, ok := kvMap[SearchClusteringFilterRatio]; ok {
		b, err := strconv.ParseFloat(clusterBasedFilterRatio, 32)
		if err != nil {
			return nil, errors.New("illegal search params clustering.filter_ratio value, should be a float in range (0.0, 1.0]")
		}
		if b <= 0.0 || b > 1.0 {
			return nil, errors.New("invalid clustering.filter_ratio value, should be a float in range (0.0, 1.0]")
		}
		clusteringOptions.FilterRatio = float32(b)
	}

	return clusteringOptions, nil
}

func DeserializeFloatVector(data []byte) []float32 {
	vectorLen := len(data) / 4 // Each float32 occupies 4 bytes
	fv := make([]float32, vectorLen)

	for i := 0; i < vectorLen; i++ {
		bits := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
		fv[i] = math.Float32frombits(bits)
	}

	return fv
}

func CalcVectorDistance(dim int64, dataType schemapb.DataType, left []byte, right *schemapb.VectorField, metric string) ([]float32, error) {
	switch dataType {
	case schemapb.DataType_FloatVector:
		distance, err := distance.CalcFloatDistance(dim, DeserializeFloatVector(left), right.GetFloatVector().GetData(), metric)
		if err != nil {
			return nil, err
		}
		return distance, nil
	// todo support other vector type
	case schemapb.DataType_BinaryVector:
	case schemapb.DataType_Float16Vector:
	case schemapb.DataType_BFloat16Vector:
	default:
		return nil, errors.New("Not supported vector type yet")
	}
	return nil, nil
}

func SerializeFloatVector(fv []float32) []byte {
	data := make([]byte, 0, 4*len(fv)) // float32 occupies 4 bytes
	buf := make([]byte, 4)
	for _, f := range fv {
		binary.LittleEndian.PutUint32(buf, math.Float32bits(f))
		data = append(data, buf...)
	}
	return data
}
