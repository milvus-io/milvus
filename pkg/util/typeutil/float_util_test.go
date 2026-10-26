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

package typeutil

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func Test_VerifyFloat(t *testing.T) {
	value := math.NaN()
	err := VerifyFloat(value)
	assert.Error(t, err)

	value = math.Inf(1)
	err = VerifyFloat(value)
	assert.Error(t, err)

	value = math.Inf(-1)
	err = VerifyFloat(value)
	assert.Error(t, err)
}

func Test_VerifyFloats32(t *testing.T) {
	data := []float32{2.5, 32.2, 53.254}
	err := VerifyFloats32(data)
	assert.NoError(t, err)

	data = []float32{2.5, 32.2, 53.254, float32(math.NaN())}
	err = VerifyFloats32(data)
	assert.Error(t, err)

	data = []float32{2.5, 32.2, 53.254, float32(math.Inf(1))}
	err = VerifyFloats32(data)
	assert.Error(t, err)

	rawValue := uint32(0xffc00000)
	floatValue := math.Float32frombits(rawValue)
	err = VerifyFloats32([]float32{floatValue})
	assert.Error(t, err)

	floatValue = -math.Float32frombits(rawValue)
	err = VerifyFloats32([]float32{floatValue})
	fmt.Println("-nan", floatValue, err)
	assert.Error(t, err)
}

func Test_VerifyFloats64(t *testing.T) {
	data := []float64{2.5, 32.2, 53.254}
	err := VerifyFloats64(data)
	assert.NoError(t, err)

	data = []float64{2.5, 32.2, 53.254, math.NaN()}
	err = VerifyFloats64(data)
	assert.Error(t, err)

	data = []float64{2.5, 32.2, 53.254, math.Inf(-1)}
	err = VerifyFloats64(data)
	assert.Error(t, err)
}

func Test_ConvertFloat32ToFP16BF16BytesAllowsTinyValues(t *testing.T) {
	for _, tt := range []struct {
		name     string
		dataType schemapb.DataType
		values   []float32
	}{
		{
			name:     "float16 tiny values",
			dataType: schemapb.DataType_Float16Vector,
			values:   []float32{0, 1e-9, -1e-9},
		},
		{
			name:     "bfloat16 tiny values",
			dataType: schemapb.DataType_BFloat16Vector,
			values:   []float32{0, 1e-38, -1e-38},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data, err := ConvertFloat32ToFP16BF16Bytes(tt.values, tt.dataType)
			assert.NoError(t, err)
			assert.Len(t, data, len(tt.values)*2)
		})
	}
}

func TestScalarFloatSortableKeys(t *testing.T) {
	values := []float64{
		math.Inf(-1), -3, -math.SmallestNonzeroFloat64,
		math.Copysign(0, -1), 0, math.SmallestNonzeroFloat64, 3, math.Inf(1), math.NaN(),
	}
	var previous uint64
	for i, value := range values {
		key := Float64ToSortableUint64(value)
		if i > 0 {
			assert.LessOrEqual(t, previous, key)
		}
		previous = key
	}
	assert.Equal(t, Float64ToSortableUint64(math.Copysign(0, -1)), Float64ToSortableUint64(0))
	assert.Equal(t, Float32ToSortableUint32(float32(math.Copysign(0, -1))), Float32ToSortableUint32(0))
	for _, bits := range []uint64{0x7ff8000000000000, 0xfff8000000000001, 0x7ff0000000000001, 0x7fffffffffffffff} {
		assert.Equal(t, uint64(math.MaxUint64), Float64ToSortableUint64(math.Float64frombits(bits)))
	}
	for _, bits := range []uint32{0x7fc00000, 0xffc00001, 0x7f800001, 0x7fffffff} {
		assert.Equal(t, uint32(math.MaxUint32), Float32ToSortableUint32(math.Float32frombits(bits)))
	}
	assert.Less(t, Float64ToSortableUint64(math.Inf(1)), Float64ToSortableUint64(math.NaN()))
	assert.Less(t, Float32ToSortableUint32(float32(math.Inf(1))), Float32ToSortableUint32(float32(math.NaN())))
}
